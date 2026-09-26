import json

import transaction_generator as tg
from conftest import FIXTURE_CSV, FRAUD_ROWS


def test_events_carry_real_kaggle_fields():
    events = list(tg.iter_events(FIXTURE_CSV))
    assert len(events) == 300
    first = events[0]
    for key in ("transaction_id", "event_time", "amount", "is_fraud", "account_id", "merchant_id", "V1", "V28"):
        assert key in first
    assert sum(e["is_fraud"] for e in events) == FRAUD_ROWS
    json.dumps(first)  # must be JSON serialisable


def test_event_ids_are_deterministic_and_unique_per_replay_cycle():
    row = next(iter(__import__("csv").DictReader(open(FIXTURE_CSV))))
    a, b = tg.build_event(7, row, 0), tg.build_event(7, row, 0)
    c = tg.build_event(7, row, 1)
    assert a["transaction_id"] == b["transaction_id"]
    assert a["transaction_id"] != c["transaction_id"]
    assert a["account_id"] == c["account_id"]  # surrogate keys depend on row id only


def test_event_time_is_anchored_to_dataset_epoch():
    row = next(iter(__import__("csv").DictReader(open(FIXTURE_CSV))))
    row = {**row, "Time": "3600"}
    assert tg.build_event(0, row)["event_time"].startswith("2013-09-01T01:00:00")


def test_cached_dataset_is_used_without_download(tmp_path, monkeypatch):
    cached = tmp_path / "creditcard.csv"
    cached.write_text("Time,Amount,Class\n0,1,0\n")
    monkeypatch.setattr(tg, "DATASET_FILE", cached)
    monkeypatch.setattr(tg, "_download_via_kagglehub", lambda: (_ for _ in ()).throw(AssertionError))
    assert tg.ensure_dataset() == cached


def test_checkpoint_roundtrip_and_resume(tmp_path, monkeypatch):
    monkeypatch.setattr(tg, "CHECKPOINT_FILE", tmp_path / "ckpt.json")
    monkeypatch.setattr(tg, "RESUME", True)
    assert tg.load_checkpoint() == (0, 0)
    tg.save_checkpoint(2, 150)
    assert tg.load_checkpoint() == (2, 150)
    events = list(tg.iter_events(FIXTURE_CSV, replay_cycle=2, start_row=150))
    assert len(events) == 150 and events[0]["source_row_id"] == 150
