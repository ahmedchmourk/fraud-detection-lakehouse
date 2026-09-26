from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pytest
import streaming_consumer as sc
import transaction_generator as tg
import transform_silver_gold as tsg
from conftest import FIXTURE_CSV, FRAUD_ROWS


@pytest.fixture(scope="module")
def bronze() -> pa.Table:
    rows = []
    for offset, event in enumerate(tg.iter_events(FIXTURE_CSV)):
        record = sc.validate_event(event)
        now = datetime.now(UTC)
        record.update(ingested_at=now, ingest_date=now.strftime("%Y-%m-%d"), kafka_partition=0, kafka_offset=offset)
        rows.append(record)
    rows.extend(dict(r, kafka_offset=r["kafka_offset"] + 10_000) for r in rows[:25])  # redelivered duplicates
    return pa.Table.from_pylist(rows, schema=sc.BRONZE_SCHEMA)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    return c


def test_silver_deduplicates_and_scores(con, bronze):
    silver = tsg.build_silver(con, bronze).to_pandas()
    assert bronze.num_rows == 325
    assert len(silver) == 300
    assert silver["transaction_id"].is_unique
    assert silver["risk_score"].between(0, 100).all()
    assert set(silver["risk_tier"]) <= {"LOW", "MEDIUM", "HIGH", "CRITICAL"}


def test_risk_model_separates_real_fraud_from_legit(con, bronze):
    silver = tsg.build_silver(con, bronze).to_pandas()
    fraud_mean = silver.loc[silver.is_fraud == 1, "risk_score"].mean()
    legit_mean = silver.loc[silver.is_fraud == 0, "risk_score"].mean()
    assert fraud_mean > 5 * legit_mean
    flagged = silver[silver.is_flagged]
    assert len(flagged) > 0 and flagged.is_fraud.mean() > 0.8  # precision on the real sample


def test_gold_models_are_built(con, bronze):
    silver = tsg.build_silver(con, bronze)
    gold = tsg.build_gold(con, silver)
    assert set(gold) == {"daily_merchant_risk_summary", "hourly_fraud_rate_metrics", "high_risk_account_leaderboard"}
    hourly = gold["hourly_fraud_rate_metrics"].to_pandas()
    assert hourly["txn_count"].sum() == 300
    assert hourly["confirmed_fraud_count"].sum() == FRAUD_ROWS
    merchants = gold["daily_merchant_risk_summary"].to_pandas()
    assert merchants["txn_count"].sum() == 300
    board = gold["high_risk_account_leaderboard"].to_pandas()
    assert list(board["risk_rank"]) == sorted(board["risk_rank"])
    for table in gold.values():
        tsg._normalise(table)  # Delta-compatible types


def test_named_query_parser():
    sql = "-- name: a\nSELECT 1;\n\n-- name: b\nSELECT 2;"
    assert tsg.parse_named_queries(sql) == {"a": "SELECT 1", "b": "SELECT 2"}
