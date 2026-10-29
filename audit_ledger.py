"""Audit the SQLite ledger against what the engine reported.

    python audit_ledger.py                       # uses trades.db + run_summary.json
    python audit_ledger.py --db trades.db --expect 123456

Checks: (1) row count == trades the engine matched (minus any it had to drop),
(2) sequence numbers are contiguous - no gaps, no duplicates, (3) no bad values.
"""
import argparse
import json
import os
import sqlite3
import sys

from config import DEFAULT_DB


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--summary", default="run_summary.json")
    ap.add_argument("--expect", type=int, default=None, help="expected number of trades")
    ap.add_argument("--run", type=int, default=None, help="run_id (default: latest run)")
    a = ap.parse_args()

    con = sqlite3.connect(a.db)
    run_id = a.run or con.execute("SELECT MAX(run_id) FROM trades").fetchone()[0]
    if run_id is None:
        raise SystemExit("ledger is empty")
    rows, lo, hi = con.execute("SELECT COUNT(*), MIN(seq), MAX(seq) FROM trades WHERE run_id=?", (run_id,)).fetchone()
    bad = con.execute("SELECT COUNT(*) FROM trades WHERE run_id=? AND (qty<=0 OR price<0 OR taker_id=maker_id)", (run_id,)).fetchone()[0]
    vol, notional = con.execute("SELECT SUM(qty), SUM(qty*price) FROM trades WHERE run_id=?", (run_id,)).fetchone()
    lat = con.execute("SELECT AVG(match_ns - order_ts_ns) FROM trades WHERE run_id=? AND match_ns>=order_ts_ns AND order_ts_ns>0", (run_id,)).fetchone()[0]

    expect, dropped = a.expect, 0
    if expect is None and os.path.exists(a.summary):
        s = json.load(open(a.summary))
        expect, dropped = s["trades_count"], s.get("trades_dropped", 0)

    print(f"run {run_id}: {rows:,} trades in ledger, total volume {vol:,}, notional {notional:,} tick-units")
    if lat is not None:
        print(f"average wait before matching (producer -> engine): {lat / 1000:,.1f} us")
    ok = True

    def check(name, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")

    check("sequence contiguous (no gaps, no duplicates)", lo == 0 and hi - lo + 1 == rows,
          f"(seq {lo}..{hi}, rows {rows:,})")
    check("no invalid rows", bad == 0, f"({bad} bad)")
    if expect is not None:
        check("ledger rows == engine trades", rows == expect - dropped,
              f"(engine {expect:,} - dropped {dropped:,} = {expect - dropped:,}; ledger {rows:,})")
    print("AUDIT", "PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
