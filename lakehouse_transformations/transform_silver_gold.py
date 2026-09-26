"""
Medallion transformation job: Bronze -> Silver -> Gold (DuckDB + Delta Lake on MinIO).

Every TRANSFORM_INTERVAL_SECONDS the job:
  1. Loads the Bronze Delta table (respecting the Delta transaction log).
  2. Runs `models/silver_cleansed_transactions.sql` in DuckDB and overwrites
     `s3://silver/cleansed_transactions`.
  3. Runs each named statement in `models/gold_fraud_metrics.sql` and overwrites
     `s3://gold/<name>`.

Bronze file compaction is owned by the streaming engine (the table's single writer).

Run once with `--once` (useful for CI or ad-hoc backfills).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | transformer | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).parent / "models"
BRONZE_URI = os.getenv("BRONZE_URI", "s3://bronze") + "/transactions"
SILVER_URI = os.getenv("SILVER_URI", "s3://silver") + "/cleansed_transactions"
GOLD_ROOT = os.getenv("GOLD_URI", "s3://gold")
INTERVAL = int(os.getenv("TRANSFORM_INTERVAL_SECONDS", "30"))


def storage_options() -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


# --------------------------------------------------------------------------- #
# SQL model loading
# --------------------------------------------------------------------------- #
def load_sql(name: str) -> str:
    return (MODELS_DIR / name).read_text()


def parse_named_queries(sql: str) -> dict[str, str]:
    """Split a SQL file into {name: statement} using `-- name: <model>` markers."""
    parts = re.split(r"^--\s*name:\s*(\w+)\s*$", sql, flags=re.MULTILINE)
    queries = {}
    for name, body in zip(parts[1::2], parts[2::2], strict=False):
        statement = body.strip().rstrip(";").strip()
        if statement:
            queries[name] = statement
    return queries


# --------------------------------------------------------------------------- #
# Pure transformation (no I/O) -- unit-testable
# --------------------------------------------------------------------------- #
def build_silver(con: duckdb.DuckDBPyConnection, bronze: pa.Table) -> pa.Table:
    con.register("bronze_transactions", bronze)
    try:
        return con.execute(load_sql("silver_cleansed_transactions.sql")).to_arrow_table()
    finally:
        con.unregister("bronze_transactions")


def build_gold(con: duckdb.DuckDBPyConnection, silver: pa.Table) -> dict[str, pa.Table]:
    con.register("silver_cleansed_transactions", silver)
    try:
        return {
            name: con.execute(query).to_arrow_table()
            for name, query in parse_named_queries(load_sql("gold_fraud_metrics.sql")).items()
        }
    finally:
        con.unregister("silver_cleansed_transactions")


# --------------------------------------------------------------------------- #
# Delta I/O
# --------------------------------------------------------------------------- #
def _normalise(table: pa.Table) -> pa.Table:
    """Delta Lake has no unsigned/interval types; DuckDB may emit them for counts."""
    fields = []
    for f in table.schema:
        t = f.type
        if pa.types.is_unsigned_integer(t) or (pa.types.is_integer(t) and t.bit_width > 64):
            t = pa.int64()
        elif pa.types.is_decimal(t):
            t = pa.int64() if t.scale == 0 else pa.float64()
        fields.append(pa.field(f.name, t))
    return table.cast(pa.schema(fields))


def read_bronze(opts: dict) -> pa.Table | None:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        return DeltaTable(BRONZE_URI, storage_options=opts).to_pyarrow_table()
    except TableNotFoundError:
        return None


def write_overwrite(uri: str, table: pa.Table, opts: dict) -> None:
    from deltalake import write_deltalake

    write_deltalake(
        uri,
        _normalise(table),
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=opts,
    )


def run_once(con: duckdb.DuckDBPyConnection, opts: dict) -> bool:
    started = time.monotonic()
    bronze = read_bronze(opts)
    if bronze is None or bronze.num_rows == 0:
        log.info("Bronze table not available yet; waiting for streaming engine")
        return False

    silver = build_silver(con, bronze)
    write_overwrite(SILVER_URI, silver, opts)

    gold = build_gold(con, silver)
    for name, table in gold.items():
        write_overwrite(f"{GOLD_ROOT}/{name}", table, opts)

    flagged = pc.sum(silver["is_flagged"]).as_py() or 0
    log.info(
        "bronze=%d -> silver=%d (flagged=%d) -> gold=%s in %.1fs",
        bronze.num_rows,
        silver.num_rows,
        flagged,
        {k: v.num_rows for k, v in gold.items()},
        time.monotonic() - started,
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run a single transformation cycle")
    args = parser.parse_args()

    opts = storage_options()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    while True:
        try:
            run_once(con, opts)
        except Exception:  # noqa: BLE001 - keep the service alive, surface the error
            log.exception("Transformation cycle failed")
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
