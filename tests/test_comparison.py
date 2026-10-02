"""Spark-free tests of the comparison helpers."""
from src.comparison import (build_comparison, format_table,
                            verify_running_matches_cumulative)

BATCH = {0: {"ACC001": 2, "ACC002": 1, "ACC003": 1},
         1: {"ACC002": 2, "ACC001": 1, "ACC004": 1},
         2: {"ACC001": 2, "ACC003": 1, "ACC005": 1}}
RUNNING = {0: {"ACC001": 2, "ACC002": 1, "ACC003": 1},
           1: {"ACC001": 3, "ACC002": 3, "ACC003": 1, "ACC004": 1},
           2: {"ACC001": 5, "ACC002": 3, "ACC003": 2, "ACC004": 1, "ACC005": 1}}


def test_inactive_account_has_zero_batch_count_but_keeps_running_count():
    rows = build_comparison(BATCH, RUNNING)
    row = next(r for r in rows if r["batch_id"] == 2 and r["account_id"] == "ACC002")
    assert row["batch_count"] == 0 and row["running_count"] == 3


def test_consistency_check_passes_and_detects_mismatch():
    assert verify_running_matches_cumulative(BATCH, RUNNING)
    bad = {**RUNNING, 2: {**RUNNING[2], "ACC001": 99}}
    assert not verify_running_matches_cumulative(BATCH, bad)


def test_format_table_has_header_and_all_rows():
    rows = build_comparison(BATCH, RUNNING)
    assert len(format_table(rows).splitlines()) > len(rows)
