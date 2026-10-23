"""
PHASE 1 TEST — Extract only

Runs ONLY the extract stage for one day and checks that the raw data
came back in the expected shape. Nothing is transformed or loaded.

For each extractor it checks:
  1. all expected columns are present
  2. the primary key column (id) has no NULL values
  3. the primary key column (id) has no duplicate values

Run for yesterday (default):
    python -m pipeline.run_extract

Run for a specific day:
    python -m pipeline.run_extract 2026-10-05

Exit code is 0 if every extractor passed, 1 if any failed.
"""

import sys
from datetime import datetime, timedelta

import pandas as pd

from config.source_db import check_source_env
from extract.extract_sales import extract_sales
from extract.extract_expenses import extract_waste, extract_inventory_costs
from utils.logger import get_logger

log = get_logger(__name__)

# Columns each extractor must return. If the source schema changes or a
# query is edited wrongly, this is where it shows up first.
# "id" is the primary key of order_items, waste_logs and inventory_logs.
EXPECTED_COLUMNS = {
    "sales (order_items)": ["id", "quantity", "total_price", "updated_at", "status"],
    "waste_logs": [
        "id", "waste_type", "item_name", "quantity", "unit", "cost", "logged_at", "voided_at",
    ],
    "inventory_logs": [
        "id", "item_type", "item_name", "transaction_type", "cost", "created_at", "voided_at",
    ],
}


def _check(name: str, df: pd.DataFrame) -> bool:
    """Print a short report for one extractor and return True if it passed."""
    print(f"\n--- {name} ---")

    missing = [c for c in EXPECTED_COLUMNS[name] if c not in df.columns]
    if missing:
        print(f"FAIL: missing columns {missing}")
        return False

    print(f"PASS: {len(df)} row(s), all expected columns present")

    ok = True

    null_ids = int(df["id"].isna().sum())
    if null_ids:
        print(f"FAIL: primary key (id) has {null_ids} NULL value(s)")
        ok = False
    else:
        print("PASS: primary key (id) has no NULL values")

    duplicate_ids = int(df["id"].duplicated().sum())
    if duplicate_ids:
        print(f"FAIL: primary key (id) has {duplicate_ids} duplicate value(s)")
        ok = False
    else:
        print("PASS: primary key (id) is unique")

    if df.empty:
        print("NOTE: 0 rows for this day (fine if the shop had no activity)")
        return ok

    print(df.head(5).to_string(index=False))
    return ok


def run_extract_test(target_date: str | None = None) -> bool:
    check_source_env()

    if target_date:
        day = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        day = datetime.now() - timedelta(days=1)

    start_date = day.strftime("%Y-%m-%d 00:00:00")
    end_date = day.strftime("%Y-%m-%d 23:59:59")

    log.info("EXTRACT TEST START | data_start=%s | data_end=%s", start_date, end_date)

    results = {
        "sales (order_items)": extract_sales(start_date, end_date),
        "waste_logs": extract_waste(start_date, end_date),
        "inventory_logs": extract_inventory_costs(start_date, end_date),
    }

    print(f"\nEXTRACT TEST for {day.strftime('%Y-%m-%d')}")
    passed = [_check(name, df) for name, df in results.items()]

    ok = all(passed)
    log.info("EXTRACT TEST END | status=%s", "SUCCESS" if ok else "FAILED")
    print("\nRESULT:", "ALL EXTRACTORS PASSED" if ok else "SOMETHING FAILED — check the log")
    return ok


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(0 if run_extract_test(date_arg) else 1)