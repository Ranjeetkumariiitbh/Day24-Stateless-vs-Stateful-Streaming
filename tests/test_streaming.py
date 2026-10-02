"""Spark tests on the small sample data (skipped automatically if pyspark is missing)."""
import pytest

pytest.importorskip("pyspark")

from src.main import create_spark, run_demo  # noqa: E402
from src.transformations import clean_transactions  # noqa: E402
from src.config import TRANSACTION_SCHEMA  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = create_spark()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_clean_transactions_drops_invalid_rows(spark):
    data = [(1, "A", 10.0, "deposit", "2026-10-01 09:00:00"),
            (2, "A", -5.0, "DEPOSIT", "2026-10-01 09:00:00"),   # negative amount
            (3, None, 5.0, "DEPOSIT", "2026-10-01 09:00:00"),   # no account
            (4, "B", 5.0, "REFUND", "2026-10-01 09:00:00")]     # unknown type
    out = clean_transactions(spark.createDataFrame(data, TRANSACTION_SCHEMA)).collect()
    assert [r["txn_id"] for r in out] == [1]
    assert out[0]["txn_type"] == "DEPOSIT"


def test_stateless_vs_stateful_counts(spark, tmp_path):
    rows, consistent = run_demo(spark, work_dir=tmp_path / "work")
    assert consistent
    final = {r["account_id"]: r["running_count"] for r in rows if r["batch_id"] == 2}
    assert final == {"ACC001": 5, "ACC002": 3, "ACC003": 2, "ACC004": 1, "ACC005": 1}
    last_batch = {r["account_id"]: r["batch_count"] for r in rows if r["batch_id"] == 2}
    assert last_batch["ACC001"] == 2 and last_batch["ACC002"] == 0
