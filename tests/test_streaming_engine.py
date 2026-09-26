import json

import pytest
import streaming_consumer as sc
import transaction_generator as tg
from conftest import FIXTURE_CSV


def _event(**overrides):
    event = next(tg.iter_events(FIXTURE_CSV))
    event.update(overrides)
    return event


def test_valid_event_passes_and_is_normalised():
    record = sc.validate_event(_event())
    assert record["event_time"].tzinfo is not None
    assert isinstance(record["amount"], float)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"amount": -5.0}, "non-negative"),
        ({"is_fraud": 3}, "0 or 1"),
        ({"V14": "abc"}, "invalid type"),
        ({"account_id": None}, "missing"),
        ({"event_time": "not-a-date"}, "timestamp"),
        ({"amount": float("nan")}, "finite"),
        ({"is_fraud": True}, "invalid type"),
    ],
)
def test_invalid_events_are_rejected(overrides, message):
    with pytest.raises(sc.SchemaError, match=message):
        sc.validate_event(_event(**overrides))


def test_rolling_window_evicts_events_older_than_window():
    w = sc.RollingWindow(seconds=60)
    w.add(1000.0, 1, 10.0)
    w.add(1030.0, 0, 20.0)
    w.add(1030.0, valid=False)
    snap = w.snapshot(1065.0)  # first event is now 65s old
    assert snap["events_in_window"] == 1
    assert snap["fraud_in_window"] == 0
    assert snap["amount_in_window"] == 20.0
    assert snap["invalid_in_window"] == 1
    assert snap["total_events"] == 2


class FakeMsg:
    def __init__(self, value, offset):
        self._value, self._offset = value, offset

    def value(self):
        return self._value

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    commits = 0

    def commit(self, asynchronous=True):
        self.commits += 1


class MemorySink:
    def __init__(self):
        self.tables = {}

    def append(self, uri, rows, schema, partition_by=None):
        if rows:
            table = sc.pa.Table.from_pylist(rows, schema=schema)  # enforces the Arrow contract
            self.tables.setdefault(uri, []).append(table)


def test_engine_routes_valid_and_invalid_records_and_commits_after_write():
    consumer, sink = FakeConsumer(), MemorySink()
    engine = sc.StreamingEngine(consumer, sink)
    engine.handle(FakeMsg(json.dumps(_event()).encode(), 1))
    engine.handle(FakeMsg(b"{not json", 2))
    engine.handle(FakeMsg(json.dumps(_event(amount=-1)).encode(), 3))
    engine.flush()

    assert sink.tables[sc.TRANSACTIONS_URI][0].num_rows == 1
    assert sink.tables[sc.QUARANTINE_URI][0].num_rows == 2
    assert consumer.commits == 1
    assert engine.buffer == [] and engine.quarantine == []
