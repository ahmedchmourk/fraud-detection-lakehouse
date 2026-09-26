import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for sub in ("producer", "streaming_engine", "lakehouse_transformations"):
    sys.path.insert(0, str(ROOT / sub))

FIXTURE_CSV = ROOT / "tests" / "fixtures" / "creditcard_sample.csv"

with open(FIXTURE_CSV) as _fh:
    FRAUD_ROWS = sum(1 for line in _fh.readlines()[1:] if line.rstrip().endswith(("1", '"1"')))
