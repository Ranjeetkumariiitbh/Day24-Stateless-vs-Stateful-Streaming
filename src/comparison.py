"""Pure-Python helpers (no Spark needed) to compare stateless vs stateful results.

Input shape for both arguments:  {batch_id: {account_id: count}}
"""
from typing import Dict, List

Counts = Dict[int, Dict[str, int]]


def build_comparison(batch: Counts, running: Counts) -> List[dict]:
    """One row per (batch_id, account_id) with current-batch vs running counts.

    Accounts that were active earlier but not in this batch show
    batch_count = 0 while their running_count is carried forward - that is
    exactly what state buys you.
    """
    rows = []
    for batch_id in sorted(running):
        accounts = set(running[batch_id]) | set(batch.get(batch_id, {}))
        for account in sorted(accounts):
            rows.append({
                "batch_id": batch_id,
                "account_id": account,
                "batch_count": batch.get(batch_id, {}).get(account, 0),
                "running_count": running[batch_id].get(account, 0),
            })
    return rows


def verify_running_matches_cumulative(batch: Counts, running: Counts) -> bool:
    """Self-check: running count == sum of all stateless batch counts so far."""
    cumulative: Dict[str, int] = {}
    for batch_id in sorted(running):
        for account, cnt in batch.get(batch_id, {}).items():
            cumulative[account] = cumulative.get(account, 0) + cnt
        if cumulative != running[batch_id]:
            return False
    return True


def format_table(rows: List[dict]) -> str:
    header = f"{'batch':<7}{'account':<10}{'batch_count':>12}{'running_count':>15}"
    lines = [header, "-" * len(header)]
    last_batch = None
    for r in rows:
        if last_batch is not None and r["batch_id"] != last_batch:
            lines.append("")
        last_batch = r["batch_id"]
        lines.append(f"{r['batch_id']:<7}{r['account_id']:<10}"
                     f"{r['batch_count']:>12}{r['running_count']:>15}")
    return "\n".join(lines)
