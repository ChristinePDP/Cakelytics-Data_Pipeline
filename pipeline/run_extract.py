"""
STAGE 1 TEST — Extract only

Runs ONLY the extract stage for one business day (Asia/Manila) and checks
that the raw data came back in the expected shape. Nothing is transformed
or loaded.

For each extractor it checks:
  1. all expected columns are present
  2. the primary key column (id) has no NULL values
  3. the primary key column (id) has no duplicate values

Every PASS / FAIL result is written to the log file (and shown on screen).
The first rows of each result are shown on screen only.

Run for yesterday (default):
    python -m pipeline.run_extract

Run for a specific day:
    python -m pipeline.run_extract 2026-10-05

Exit code is 0 if every extractor passed, 1 if any failed.
"""

import sys

import pandas as pd

from config.source_db import check_source_env
from extract.extract_sales import extract_sales
from extract.extract_expenses import extract_waste, extract_inventory_costs
from utils.batch import make_batch
from utils.logger import get_logger

log = get_logger(__name__)

# Columns each extractor must return. If the source schema changes or a
# query is edited wrongly, this is where it shows up first.
# "id" is the primary key of order_items, waste_logs and inventory_logs.
EXPECTED_COLUMNS = {
    "sales (order_items)": ["id", "order_id", "quantity", "total_price", "updated_at", "status"],
    "waste_logs": [
        "id", "waste_type", "item_name", "quantity", "unit", "cost", "logged_at", "voided_at",
    ],
    "inventory_logs": [
        "id", "item_type", "item_name", "transaction_type", "cost", "created_at", "voided_at",
    ],
}


def _check(name: str, df: pd.DataFrame) -> bool:
    """Log the result of each check for one extractor and return True if it passed."""
    missing = [c for c in EXPECTED_COLUMNS[name] if c not in df.columns]
    if missing:
        log.error("[%s] FAIL: missing columns %s", name, missing)
        return False

    log.info("[%s] PASS: %d row(s), all expected columns present", name, len(df))

    ok = True

    null_ids = int(df["id"].isna().sum())
    if null_ids:
        log.error("[%s] FAIL: primary key (id) has %d NULL value(s)", name, null_ids)
        ok = False
    else:
        log.info("[%s] PASS: primary key (id) has no NULL values", name)

    duplicate_ids = int(df["id"].duplicated().sum())
    if duplicate_ids:
        log.error("[%s] FAIL: primary key (id) has %d duplicate value(s)", name, duplicate_ids)
        ok = False
    else:
        log.info("[%s] PASS: primary key (id) is unique", name)

    if df.empty:
        log.info("[%s] NOTE: 0 rows for this day (fine if the shop had no activity)", name)
    else:
        # sample rows are for the screen only, so they are printed and not logged
        print(f"\n--- {name}: first rows ---")
        print(df.head(5).to_string(index=False))

    return ok


def run_extract_test(target_date: str | None = None) -> bool:
    check_source_env()

    batch = make_batch(target_date)

    log.info(
        "EXTRACT TEST START | run_id=%s | batch_id=%s | data_start=%s | data_end=%s",
        batch.extraction_run_id, batch.batch_id,
        batch.window_start.isoformat(), batch.window_end.isoformat(),
    )

    results = {
        "sales (order_items)": extract_sales(batch.window_start, batch.window_end),
        "waste_logs": extract_waste(batch.window_start, batch.window_end),
        "inventory_logs": extract_inventory_costs(batch.window_start, batch.window_end),
    }

    passed = [_check(name, df) for name, df in results.items()]

    ok = all(passed)
    log.info(
        "EXTRACT TEST END | run_id=%s | status=%s",
        batch.extraction_run_id, "SUCCESS" if ok else "FAILED",
    )
    print("\nRESULT:", "ALL EXTRACTORS PASSED" if ok else "SOMETHING FAILED — check the log")
    return ok


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(0 if run_extract_test(date_arg) else 1)
