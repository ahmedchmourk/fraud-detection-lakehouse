"""
Streaming engine: Redpanda `raw-transactions` -> MinIO Bronze (Delta Lake).

Responsibilities
  * Schema validation of every event; invalid payloads go to a quarantine table.
  * Rolling 60-second window aggregations (throughput, fraud count, amount) that
    are persisted as a `stream_metrics` Delta table for the monitoring dashboard.
  * Append-only micro-batch writes of validated events to `s3://bronze/transactions`.
  * At-least-once delivery: Kafka offsets are committed only after a successful
    Delta commit. Duplicates are removed downstream in the Silver layer.
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pyarrow as pa

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | streaming-engine | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-writer")
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "2000"))
BATCH_MAX_SECONDS = float(os.getenv("BATCH_MAX_SECONDS", "10"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
METRICS_EMIT_SECONDS = float(os.getenv("METRICS_EMIT_SECONDS", "10"))
COMPACT_EVERY_N_FLUSHES = int(os.getenv("COMPACT_EVERY_N_FLUSHES", "60"))

BRONZE_URI = os.getenv("BRONZE_URI", "s3://bronze")
TRANSACTIONS_URI = f"{BRONZE_URI}/transactions"
METRICS_URI = f"{BRONZE_URI}/stream_metrics"
QUARANTINE_URI = f"{BRONZE_URI}/quarantine"

PCA_FEATURES = [f"V{i}" for i in range(1, 29)]


def storage_options() -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
        # Single writer per table -> safe without an external lock provider.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
REQUIRED_FIELDS: dict[str, tuple[type, ...]] = {
    "transaction_id": (str,),
    "source_row_id": (int,),
    "event_time": (str,),
    "time_offset_s": (int, float),
    "amount": (int, float),
    "is_fraud": (int,),
    "account_id": (str,),
    "merchant_id": (str,),
    "merchant_category": (str,),
    "location": (str,),
    **{f: (int, float) for f in PCA_FEATURES},
}

BRONZE_SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("source_row_id", pa.int64()),
        ("replay_cycle", pa.int32()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("time_offset_s", pa.float64()),
        ("amount", pa.float64()),
        ("is_fraud", pa.int8()),
        ("account_id", pa.string()),
        ("merchant_id", pa.string()),
        ("merchant_category", pa.string()),
        ("location", pa.string()),
        *[(f, pa.float64()) for f in PCA_FEATURES],
        ("produced_at", pa.timestamp("us", tz="UTC")),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("ingest_date", pa.string()),
    ]
)

METRICS_SCHEMA = pa.schema(
    [
        ("window_end", pa.timestamp("us", tz="UTC")),
        ("window_seconds", pa.int32()),
        ("events_in_window", pa.int64()),
        ("events_per_second", pa.float64()),
        ("fraud_in_window", pa.int64()),
        ("amount_in_window", pa.float64()),
        ("invalid_in_window", pa.int64()),
        ("total_events", pa.int64()),
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        ("received_at", pa.timestamp("us", tz="UTC")),
        ("error", pa.string()),
        ("raw_payload", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
    ]
)


class SchemaError(ValueError):
    pass


def _parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def validate_event(payload: dict) -> dict:
    """Validate and normalise one event. Raises SchemaError on contract violations."""
    if not isinstance(payload, dict):
        raise SchemaError("payload is not a JSON object")

    missing = [f for f in REQUIRED_FIELDS if f not in payload or payload[f] is None]
    if missing:
        raise SchemaError(f"missing fields: {', '.join(missing[:5])}")

    for name, types in REQUIRED_FIELDS.items():
        value = payload[name]
        if isinstance(value, bool) or not isinstance(value, types):
            raise SchemaError(f"field '{name}' has invalid type {type(value).__name__}")
        if isinstance(value, float) and not math.isfinite(value):
            raise SchemaError(f"field '{name}' is not finite")

    if payload["amount"] < 0:
        raise SchemaError("amount must be non-negative")
    if payload["is_fraud"] not in (0, 1):
        raise SchemaError("is_fraud must be 0 or 1")

    try:
        event_time = _parse_ts(payload["event_time"])
        produced_at = _parse_ts(payload.get("produced_at") or payload["event_time"])
    except ValueError as exc:
        raise SchemaError(f"invalid timestamp: {exc}") from exc

    record = {name: payload[name] for name in REQUIRED_FIELDS}
    record["amount"] = float(record["amount"])
    record["time_offset_s"] = float(record["time_offset_s"])
    for feat in PCA_FEATURES:
        record[feat] = float(record[feat])
    record["replay_cycle"] = int(payload.get("replay_cycle", 0))
    record["event_time"] = event_time
    record["produced_at"] = produced_at
    return record


# --------------------------------------------------------------------------- #
# Rolling window
# --------------------------------------------------------------------------- #
@dataclass
class RollingWindow:
    """Processing-time sliding window of `seconds` length."""

    seconds: int = WINDOW_SECONDS
    _events: deque = field(default_factory=deque)  # (ts, is_fraud, amount, valid)
    total: int = 0

    def add(self, ts: float, is_fraud: int = 0, amount: float = 0.0, valid: bool = True) -> None:
        self._events.append((ts, is_fraud, amount, valid))
        if valid:
            self.total += 1
        self._evict(ts)

    def _evict(self, now: float) -> None:
        cutoff = now - self.seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def snapshot(self, now: float) -> dict:
        self._evict(now)
        valid = [e for e in self._events if e[3]]
        return {
            "window_end": datetime.fromtimestamp(now, tz=UTC),
            "window_seconds": self.seconds,
            "events_in_window": len(valid),
            "events_per_second": round(len(valid) / self.seconds, 2),
            "fraud_in_window": sum(e[1] for e in valid),
            "amount_in_window": round(sum(e[2] for e in valid), 2),
            "invalid_in_window": len(self._events) - len(valid),
            "total_events": self.total,
        }


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #
class DeltaSink:
    def __init__(self) -> None:
        self.opts = storage_options()

    def append(self, uri: str, rows: list[dict], schema: pa.Schema, partition_by=None) -> None:
        if not rows:
            return
        from deltalake import write_deltalake

        table = pa.Table.from_pylist(rows, schema=schema)
        write_deltalake(
            uri,
            table,
            mode="append",
            partition_by=partition_by,
            storage_options=self.opts,
        )

    def compact(self, uri: str) -> None:
        """Merge small streaming files and drop tombstoned ones (we are the only writer)."""
        from deltalake import DeltaTable

        dt = DeltaTable(uri, storage_options=self.opts)
        stats = dt.optimize.compact()
        dt.vacuum(retention_hours=1, enforce_retention_duration=False, dry_run=False)
        log.info("Compacted %s: +%s / -%s files", uri, stats.get("numFilesAdded"), stats.get("numFilesRemoved"))


def create_consumer():
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
        }
    )


class StreamingEngine:
    def __init__(self, consumer, sink: DeltaSink) -> None:
        self.consumer = consumer
        self.sink = sink
        self.window = RollingWindow()
        self.buffer: list[dict] = []
        self.quarantine: list[dict] = []
        self.metrics: list[dict] = []
        self.last_flush = time.monotonic()
        self.last_metric = time.time()
        self.flushes = 0
        self.running = True

    def handle(self, msg) -> None:
        now = time.time()
        raw = msg.value()
        try:
            record = validate_event(json.loads(raw))
        except (SchemaError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            self.window.add(now, valid=False)
            self.quarantine.append(
                {
                    "received_at": datetime.fromtimestamp(now, tz=UTC),
                    "error": str(exc),
                    "raw_payload": raw.decode("utf-8", errors="replace")[:10000],
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset(),
                }
            )
            return

        ingested = datetime.fromtimestamp(now, tz=UTC)
        record.update(
            ingested_at=ingested,
            ingest_date=ingested.strftime("%Y-%m-%d"),
            kafka_partition=msg.partition(),
            kafka_offset=msg.offset(),
        )
        self.buffer.append(record)
        self.window.add(now, record["is_fraud"], record["amount"])

    def maybe_emit_metrics(self) -> None:
        now = time.time()
        if now - self.last_metric >= METRICS_EMIT_SECONDS:
            snap = self.window.snapshot(now)
            self.metrics.append(snap)
            self.last_metric = now
            log.info(
                "window=%ss events=%d (%.1f/s) fraud=%d amount=%.2f invalid=%d total=%d",
                snap["window_seconds"],
                snap["events_in_window"],
                snap["events_per_second"],
                snap["fraud_in_window"],
                snap["amount_in_window"],
                snap["invalid_in_window"],
                snap["total_events"],
            )

    def should_flush(self) -> bool:
        pending = len(self.buffer) + len(self.quarantine)
        return pending >= BATCH_MAX_RECORDS or (
            (pending or self.metrics) and time.monotonic() - self.last_flush >= BATCH_MAX_SECONDS
        )

    def flush(self) -> None:
        self.sink.append(TRANSACTIONS_URI, self.buffer, BRONZE_SCHEMA, partition_by=["ingest_date"])
        self.sink.append(QUARANTINE_URI, self.quarantine, QUARANTINE_SCHEMA)
        self.sink.append(METRICS_URI, self.metrics, METRICS_SCHEMA)
        if self.buffer or self.quarantine:
            self.consumer.commit(asynchronous=False)
            log.info("Committed bronze batch: %d valid, %d quarantined", len(self.buffer), len(self.quarantine))
        self.buffer.clear()
        self.quarantine.clear()
        self.metrics.clear()
        self.last_flush = time.monotonic()
        self.flushes += 1
        if COMPACT_EVERY_N_FLUSHES and self.flushes % COMPACT_EVERY_N_FLUSHES == 0:
            try:
                self.sink.compact(TRANSACTIONS_URI)
            except Exception as exc:  # noqa: BLE001 - compaction is best-effort
                log.warning("Bronze compaction skipped: %s", exc)

    def run(self) -> None:
        self.consumer.subscribe([TOPIC])
        log.info("Consuming '%s' from %s -> %s", TOPIC, KAFKA_BOOTSTRAP, TRANSACTIONS_URI)
        while self.running:
            msg = self.consumer.poll(0.5)
            if msg is not None:
                if msg.error():
                    log.warning("Kafka error: %s", msg.error())
                else:
                    self.handle(msg)
            self.maybe_emit_metrics()
            if self.should_flush():
                self._flush_with_retry()
        self._flush_with_retry()
        self.consumer.close()

    def _flush_with_retry(self, attempts: int = 5) -> None:
        for attempt in range(1, attempts + 1):
            try:
                self.flush()
                return
            except Exception as exc:  # noqa: BLE001
                log.warning("Flush attempt %d/%d failed: %s", attempt, attempts, exc)
                time.sleep(min(2**attempt, 30))
        raise RuntimeError("Unable to write to Bronze layer; aborting without committing offsets")


def main() -> int:
    engine = StreamingEngine(create_consumer(), DeltaSink())

    def _stop(signum, _frame):
        log.info("Received signal %s, draining and shutting down", signum)
        engine.running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    engine.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
