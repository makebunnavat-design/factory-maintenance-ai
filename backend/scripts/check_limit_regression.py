#!/usr/bin/env python3
"""
Lightweight regression checks for SQL LIMIT and excluded-tech recovery.

Run:
    python backend/scripts/check_limit_regression.py
"""

from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_import_path() -> None:
    script_dir = Path(__file__).resolve().parent
    backend_dir = script_dir.parent
    sys.path.insert(0, str(backend_dir))


_bootstrap_import_path()

from main import (  # noqa: E402
    WORK_DB_PATH,
    ensure_limit_5,
    execute_sql_safe,
    get_tech_exclude_for_answer,
)


BASE_SQL = (
    "SELECT Tech, SUM(CAST(RepairMinutes AS FLOAT)) AS TotalMinutes, "
    "COUNT(*) AS TotalItems "
    "FROM repairs_enriched "
    "WHERE Date BETWEEN '2026-01-01' AND '2026-01-31' "
    "GROUP BY Tech "
    "ORDER BY TotalMinutes DESC, TotalItems DESC LIMIT 1;"
)


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"PASS: {label}")


def check_limit_parsing() -> None:
    sql_top_32 = ensure_limit_5(BASE_SQL, user_msg="top 32 \u0e23\u0e32\u0e22\u0e01\u0e32\u0e23")
    sql_top_1 = ensure_limit_5(BASE_SQL, user_msg="top technician by repair minutes")
    sql_override = ensure_limit_5(BASE_SQL, user_msg="top technician by repair minutes", limit_override=50)

    assert_equal("LIMIT 32" in sql_top_32.upper(), True, "explicit list size updates LIMIT")
    assert_equal("LIMIT 1" in sql_top_1.upper(), True, "top query keeps generated LIMIT 1")
    assert_equal("LIMIT 50" in sql_override.upper(), True, "frontend limit_override wins")


def check_no_limit_execution() -> None:
    sql = "SELECT Tech, COUNT(*) AS CallCount FROM repairs_enriched GROUP BY Tech ORDER BY CallCount DESC"
    df_default, sql_default = execute_sql_safe(sql)
    df_nolimit, sql_nolimit = execute_sql_safe(sql, skip_limit_enforcement=True)

    assert_equal(len(df_default), 10, "default execution still enforces LIMIT 10")
    assert_equal("LIMIT 10" in sql_default.upper(), True, "default SQL shows LIMIT 10")
    assert_equal("LIMIT" in sql_nolimit.upper(), False, "skip_limit_enforcement keeps SQL unlimited")
    assert_equal(len(df_nolimit) > len(df_default), True, "unlimited execution returns more rows")


def check_excluded_tech_recovery() -> None:
    sql = "SELECT Tech, COUNT(*) AS CallCount FROM repairs_enriched GROUP BY Tech ORDER BY CallCount DESC LIMIT 10"
    df_limited, _ = execute_sql_safe(sql, skip_limit_enforcement=True)

    exclude_upper = {name.strip().upper() for name in get_tech_exclude_for_answer()}
    filtered = df_limited[~df_limited["Tech"].astype(str).str.strip().str.upper().isin(exclude_upper)].copy()

    sql_no_limit = "SELECT Tech, COUNT(*) AS CallCount FROM repairs_enriched GROUP BY Tech ORDER BY CallCount DESC"
    df_all, _ = execute_sql_safe(sql_no_limit, skip_limit_enforcement=True)
    recovered = df_all[~df_all["Tech"].astype(str).str.strip().str.upper().isin(exclude_upper)].head(10).copy()

    assert_equal(len(filtered) < 10, True, "top-10 query can underfill after excluded tech filter")
    assert_equal(len(recovered), 10, "unlimited recovery can backfill excluded-tech gaps")


def main() -> None:
    if not Path(WORK_DB_PATH).exists():
        raise SystemExit(f"Database not found: {WORK_DB_PATH}")

    print("Running limit regression checks...")
    check_limit_parsing()
    check_no_limit_execution()
    check_excluded_tech_recovery()
    print("All regression checks passed.")


if __name__ == "__main__":
    main()
