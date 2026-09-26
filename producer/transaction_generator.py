"""
Kaggle credit-card transaction replayer -> Redpanda.

Loads the real "Credit Card Fraud Detection" dataset (ULB Machine Learning Group,
Kaggle: mlg-ulb/creditcardfraud) and streams every record as a JSON event into
the `raw-transactions` topic at a configurable rate.

Dataset resolution order:
  1. Local cache           ($DATA_DIR/creditcard.csv)
  2. kagglehub download    (uses ~/.kaggle/kaggle.json or KAGGLE_USERNAME/KAGGLE_KEY if present)
  3. Public mirror         ($DATASET_MIRROR_URL, same file hosted by TensorFlow)
"""

from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import signal
import sys
import time
import urllib.request
import uuid
import zlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | producer | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
MSG_RATE = float(os.getenv("MSG_RATE", "75"))  # messages / second
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))  # 0 = whole dataset
LOOP_FOREVER = os.getenv("LOOP_FOREVER", "true").lower() == "true"
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATASET_FILE = DATA_DIR / "creditcard.csv"
CHECKPOINT_FILE = Path(os.getenv("CHECKPOINT_FILE", str(DATA_DIR / ".producer_checkpoint.json")))
RESUME = os.getenv("RESUME_FROM_CHECKPOINT", "true").lower() == "true"
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "mlg-ulb/creditcardfraud")
DATASET_MIRROR_URL = os.getenv(
    "DATASET_MIRROR_URL",
    "https://storage.googleapis.com/download.tensorflow.org/data/creditcard.csv",
)
# The dataset covers two days of September 2013; `Time` is seconds since the first
# transaction. We anchor it to a fixed epoch so event-time analytics are reproducible.
DATASET_EPOCH = datetime.fromisoformat(os.getenv("DATASET_EPOCH", "2013-09-01T00:00:00+00:00"))

PCA_FEATURES = [f"V{i}" for i in range(1, 29)]

# The Kaggle dataset is PCA-anonymised and carries no merchant/account identifiers.
# We derive *synthetic* surrogate keys from a CRC32 hash of the row id only, so they
# are deterministic across replays and statistically independent of the fraud label
# (no target leakage into downstream risk features).
N_ACCOUNTS = int(os.getenv("N_ACCOUNTS", "2500"))
N_MERCHANTS = int(os.getenv("N_MERCHANTS", "400"))
MERCHANT_CATEGORIES = [
    "grocery",
    "electronics",
    "travel",
    "restaurants",
    "fuel",
    "online_retail",
    "entertainment",
    "health",
    "luxury_goods",
    "utilities",
]
LOCATIONS = [
    "New York, US",
    "London, GB",
    "Paris, FR",
    "Berlin, DE",
    "Madrid, ES",
    "Casablanca, MA",
    "Dubai, AE",
    "Singapore, SG",
    "Toronto, CA",
    "Sao Paulo, BR",
    "Tokyo, JP",
    "Sydney, AU",
    "Amsterdam, NL",
    "Brussels, BE",
    "Lagos, NG",
]

TXN_NAMESPACE = uuid.UUID("6f1c0a52-9d1e-4c8b-8a57-0f5a7c2b9e11")

_running = True


def _stop(signum, _frame):  # pragma: no cover - signal handler
    global _running
    log.info("Received signal %s, shutting down gracefully", signum)
    _running = False


# --------------------------------------------------------------------------- #
# Dataset acquisition
# --------------------------------------------------------------------------- #
def _download_via_kagglehub() -> Path | None:
    try:
        import kagglehub

        log.info("Downloading %s via kagglehub ...", KAGGLE_DATASET)
        path = Path(kagglehub.dataset_download(KAGGLE_DATASET))
        candidate = path / "creditcard.csv" if path.is_dir() else path
        if candidate.exists():
            return candidate
        log.warning("kagglehub finished but creditcard.csv was not found in %s", path)
    except Exception as exc:  # noqa: BLE001 - any auth/network failure -> fallback
        log.warning("kagglehub download unavailable (%s); falling back to mirror", exc)
    return None


def _download_via_mirror(dest: Path) -> Path:
    log.info("Downloading dataset from mirror %s ...", DATASET_MIRROR_URL)
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(DATASET_MIRROR_URL, timeout=120) as resp, open(tmp, "wb") as fh:
        shutil.copyfileobj(resp, fh)
    tmp.rename(dest)
    return dest


def ensure_dataset() -> Path:
    """Return the path to creditcard.csv, downloading it if needed."""
    if DATASET_FILE.exists() and DATASET_FILE.stat().st_size > 0:
        log.info("Using cached Kaggle dataset at %s", DATASET_FILE)
        return DATASET_FILE

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = _download_via_kagglehub()
    if downloaded is not None:
        shutil.copy(downloaded, DATASET_FILE)
    else:
        _download_via_mirror(DATASET_FILE)
    log.info("Dataset cached at %s (%.1f MB)", DATASET_FILE, DATASET_FILE.stat().st_size / 1e6)
    return DATASET_FILE


# --------------------------------------------------------------------------- #
# Record -> event
# --------------------------------------------------------------------------- #
def _bucket(key: str, n: int) -> int:
    return zlib.crc32(key.encode()) % n


def build_event(row_id: int, row: dict, replay_cycle: int = 0) -> dict:
    """Convert one CSV row into the canonical `raw-transactions` JSON payload."""
    time_offset = float(row["Time"])
    txn_key = f"{replay_cycle}:{row_id}"
    merchant_idx = _bucket(f"m:{row_id}", N_MERCHANTS)

    event = {
        "transaction_id": str(uuid.uuid5(TXN_NAMESPACE, txn_key)),
        "source_row_id": row_id,
        "replay_cycle": replay_cycle,
        "event_time": (DATASET_EPOCH + timedelta(seconds=time_offset)).isoformat(),
        "time_offset_s": time_offset,
        "amount": round(float(row["Amount"]), 2),
        "is_fraud": int(float(row["Class"])),
        "account_id": f"ACC-{_bucket(f'a:{row_id}', N_ACCOUNTS):05d}",
        "merchant_id": f"MER-{merchant_idx:04d}",
        "merchant_category": MERCHANT_CATEGORIES[merchant_idx % len(MERCHANT_CATEGORIES)],
        "location": LOCATIONS[_bucket(f"l:{row_id}", len(LOCATIONS))],
        "produced_at": datetime.now(UTC).isoformat(),
    }
    for feat in PCA_FEATURES:
        event[feat] = float(row[feat])
    return event


def iter_events(csv_path: Path, replay_cycle: int = 0, start_row: int = 0) -> Iterator[dict]:
    with open(csv_path, newline="") as fh:
        for row_id, row in enumerate(csv.DictReader(fh)):
            if row_id >= start_row:
                yield build_event(row_id, row, replay_cycle)


# --------------------------------------------------------------------------- #
# Checkpointing: resume after restarts instead of replaying from row 0
# --------------------------------------------------------------------------- #
def load_checkpoint() -> tuple[int, int]:
    """Return (replay_cycle, next_row) from the last run, or (0, 0)."""
    if not RESUME or not CHECKPOINT_FILE.exists():
        return 0, 0
    try:
        state = json.loads(CHECKPOINT_FILE.read_text())
        return int(state["replay_cycle"]), int(state["next_row"])
    except (ValueError, KeyError, OSError) as exc:
        log.warning("Ignoring unreadable checkpoint %s (%s)", CHECKPOINT_FILE, exc)
        return 0, 0


def save_checkpoint(replay_cycle: int, next_row: int) -> None:
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"replay_cycle": replay_cycle, "next_row": next_row}))
    tmp.replace(CHECKPOINT_FILE)


# --------------------------------------------------------------------------- #
# Streaming
# --------------------------------------------------------------------------- #
def create_producer():
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "kaggle-transaction-replayer",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "zstd",
        }
    )


def wait_for_broker(producer, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            producer.list_topics(timeout=5)
            log.info("Connected to Redpanda at %s", KAFKA_BOOTSTRAP)
            return
        except Exception as exc:  # noqa: BLE001
            log.info("Waiting for broker (%s) ...", exc)
            time.sleep(3)
    raise RuntimeError(f"Broker {KAFKA_BOOTSTRAP} not reachable after {timeout_s}s")


def _delivery_report(err, _msg):
    if err is not None:
        log.error("Delivery failed: %s", err)


def stream(csv_path: Path) -> None:
    producer = create_producer()
    wait_for_broker(producer)

    interval = 1.0 / MSG_RATE if MSG_RATE > 0 else 0.0
    sent = 0
    cycle, start_row = load_checkpoint()
    next_row = start_row
    started = last_report = time.monotonic()
    log.info(
        "Streaming %s -> topic '%s' at %.0f msg/s (cycle=%d, starting at row %d)",
        csv_path.name,
        TOPIC,
        MSG_RATE,
        cycle,
        start_row,
    )

    while _running:
        for event in iter_events(csv_path, cycle, start_row):
            if not _running or (MAX_MESSAGES and sent >= MAX_MESSAGES):
                break
            producer.produce(
                TOPIC,
                key=event["account_id"].encode(),
                value=json.dumps(event).encode(),
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            sent += 1
            next_row = event["source_row_id"] + 1

            # Pace against the wall clock so throughput stays at MSG_RATE.
            if interval:
                sleep_for = started + sent * interval - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)

            now = time.monotonic()
            if now - last_report >= 10:
                producer.flush(10)  # checkpoint only rows the broker has acknowledged
                save_checkpoint(cycle, next_row)
                log.info("sent=%d  rate=%.1f msg/s  cycle=%d  row=%d", sent, sent / (now - started), cycle, next_row)
                last_report = now

        if not _running or (MAX_MESSAGES and sent >= MAX_MESSAGES) or not LOOP_FOREVER:
            break
        cycle, start_row, next_row = cycle + 1, 0, 0
        log.info("Dataset exhausted; starting replay cycle %d", cycle)

    producer.flush(30)
    save_checkpoint(cycle, next_row)
    log.info("Producer stopped after %d messages", sent)


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    stream(ensure_dataset())
    return 0


if __name__ == "__main__":
    sys.exit(main())
