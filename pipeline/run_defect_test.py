"""
STAGE 2 DEFECT TEST — Synthetic Data Quality and Edge Case Test

Feeds synthetic data containing known defects (missing IDs, duplicate keys,
negative amounts, invalid quantities, orphan items, voided entries, out-of-window
timestamps, and conversion failures) into transform_batch() to verify that all
cleaning (C), standardization (S), and business (B) rules trigger as expected.
"""

from datetime import datetime
import pandas as pd

from transform.quarantine import write_quarantine
from transform.reporting import (
    build_metadata, profile_to_markdown, write_metadata, write_profile_report,
)
from transform.transform_batch import transform_batch
from utils.batch import Batch, PH_TZ


def run_defect_test():
    batch = Batch(
        batch_id="BATCH_20261020_DEFECT_TEST",
        batch_date="2026-10-20",
        window_start=datetime(2026, 10, 20, 0, 0, 0, tzinfo=PH_TZ),
        window_end=datetime(2026, 10, 20, 23, 59, 59, 999999, tzinfo=PH_TZ),
        extraction_run_id="EXT_20261020_DEFECT_TEST",
        run_id="TRN_20261020_DEFECT_TEST",
    )

    order_items = pd.DataFrame([
        # Valid
        {"id": "11111111-1111-1111-1111-111111111111", "order_id": "o1", "quantity": 2, "total_price": 200.0, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Completed"},
        # C-01: missing id
        {"id": None, "order_id": "o2", "quantity": 1, "total_price": 100.0, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Completed"},
        # C-02: duplicate id
        {"id": "11111111-1111-1111-1111-111111111111", "order_id": "o1", "quantity": 2, "total_price": 200.0, "updated_at": "2026-10-20 10:05:00+08:00", "status": "Completed"},
        # C-03: missing amount
        {"id": "22222222-2222-2222-2222-222222222222", "order_id": "o3", "quantity": 1, "total_price": None, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Completed"},
        # C-04: negative amount
        {"id": "33333333-3333-3333-3333-333333333333", "order_id": "o4", "quantity": 1, "total_price": -50.0, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Completed"},
        # C-05: invalid quantity
        {"id": "44444444-4444-4444-4444-444444444444", "order_id": "o5", "quantity": 0, "total_price": 150.0, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Completed"},
        # C-06: orphan line item
        {"id": "55555555-5555-5555-5555-555555555555", "order_id": None, "quantity": 1, "total_price": 120.0, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Completed"},
        # B-01: cancelled order
        {"id": "66666666-6666-6666-6666-666666666666", "order_id": "o6", "quantity": 1, "total_price": 180.0, "updated_at": "2026-10-20 10:00:00+08:00", "status": "Cancelled"},
    ])

    waste = pd.DataFrame([
        # Valid
        {"id": "w1", "waste_type": "spoilage", "item_name": "Eggs", "quantity": 1.5, "unit": "kg", "cost": 75.0, "logged_at": "2026-10-20 11:00:00+08:00", "voided_at": None},
        # C-07: whitespace
        {"id": "w2", "waste_type": "spoilage", "item_name": "  Flour  ", "quantity": 2.0, "unit": "kg", "cost": 90.0, "logged_at": "2026-10-20 11:30:00+08:00", "voided_at": None},
        # B-02: voided waste
        {"id": "w3", "waste_type": "spoilage", "item_name": "Milk", "quantity": 1.0, "unit": "L", "cost": 85.0, "logged_at": "2026-10-20 11:45:00+08:00", "voided_at": "2026-10-20 12:00:00+08:00"},
    ])

    inventory = pd.DataFrame([
        # Valid
        {"id": "i1", "item_type": "raw", "item_name": "Sugar", "transaction_type": "IN", "cost": 500.0, "created_at": "2026-10-20 09:00:00+08:00", "voided_at": None},
        # S-04: lowercase transaction_type
        {"id": "i2", "item_type": "raw", "item_name": "Butter", "transaction_type": "in", "cost": 350.0, "created_at": "2026-10-20 09:15:00+08:00", "voided_at": None},
        # S-02: out of window
        {"id": "i3", "item_type": "raw", "item_name": "Yeast", "transaction_type": "IN", "cost": 120.0, "created_at": "2026-10-19 23:59:58+08:00", "voided_at": None},
        # B-03: voided inventory
        {"id": "i4", "item_type": "raw", "item_name": "Vanilla", "transaction_type": "IN", "cost": 200.0, "created_at": "2026-10-20 09:30:00+08:00", "voided_at": "2026-10-20 10:00:00+08:00"},
        # S-03: conversion failure
        {"id": "i5", "item_type": "raw", "item_name": "Cocoa", "transaction_type": "IN", "cost": "INVALID_NUM", "created_at": "2026-10-20 09:45:00+08:00", "voided_at": None},
    ])

    extracted = {"order_items": order_items, "waste_logs": waste, "inventory_logs": inventory}
    handover = {t: len(df) for t, df in extracted.items()}
    res = transform_batch(extracted, batch, handover)

    quarantine_path = write_quarantine(res.quarantine, batch)
    profile_path = write_profile_report(
        profile_to_markdown(batch, res.profile, res.distributions, handover), batch
    )
    metadata_path = write_metadata(build_metadata(res, handover), batch)

    print(f"\nTRANSFORM TEST for {batch.batch_date}  ({batch.batch_id})")
    print(f"Extraction run: {batch.extraction_run_id}   Transformation run: {batch.run_id}\n")
    print(f"{'table':<16}{'read':>6}{'output':>8}{'rejected':>10}{'excluded':>10}")
    for t, r in res.tables.items():
        print(f"{t:<16}{r.records_read:>6}{r.records_output:>8}{r.records_rejected:>10}{r.records_excluded:>10}")

    m = res.measures
    print(f"\nTotals: total_sales={m['total_sales']}  total_expenses={m['total_expenses']}  "
          f"profit={m['profit']}  (items sold: {m['total_items_sold']})")

    print("\nValidation checks")
    for c in res.checks:
        detail_suffix = f": {c.detail}" if c.status != "PASS" else ""
        print(f"  {c.check_id}  {c.status:<8} {c.name} ({c.severity}){detail_suffix}")

    print(f"\nQuarantine : {quarantine_path}  ({len(res.quarantine)} record(s))")
    print(f"Profile    : {profile_path}")
    print(f"Metadata   : {metadata_path}")
    print(f"\nDECISION: {res.decision}")

    return res


if __name__ == "__main__":
    res = run_defect_test()
    print("Decision:", res.decision)
