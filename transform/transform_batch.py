"""
TRANSFORM — Batch orchestration

Receives the extraction handover (the three raw DataFrames, the extraction run
ID, the batch window, and the row counts) and returns the business-ready output
tables together with the quarantine, the metadata, the validation results, and
the acceptance decision.

Steps: profile -> clean/standardize/filter each table -> derive outputs ->
validate -> decide.

This stage never reads from the source database and never loads the warehouse.

RERUNS are idempotent: the output is a pure function of the extracted batch,
and the quarantine file is overwritten by batch ID (see quarantine.py).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import pandas as pd

from transform.compute_profit import build_output_tables
from transform.engine import QUARANTINE_COLUMNS, TableResult, process_table
from transform.profile_data import profile_data, profile_distributions
from transform.rules import (
    CODE_VERSION, TABLE_INVENTORY, TABLE_SALES, TABLE_WASTE, TABLES, build_rules,
)
from transform.validate import CheckResult, decide, run_checks
from utils.batch import PH_TZ, Batch
from utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class TransformResult:
    batch: Batch
    started_at: datetime
    ended_at: datetime
    tables: dict[str, TableResult]
    outputs: dict[str, pd.DataFrame]
    quarantine: pd.DataFrame
    checks: list[CheckResult]
    decision: str
    profile: list[dict]
    distributions: dict[str, pd.DataFrame]
    measures: dict[str, Decimal | int]


def transform_batch(extracted: dict[str, pd.DataFrame], batch: Batch,
                    handover_counts: dict[str, int]) -> TransformResult:
    started = datetime.now(PH_TZ)
    log.info(
        "TRANSFORM START | run_id=%s | parent_run_id=%s | batch_id=%s | code_version=%s",
        batch.run_id, batch.extraction_run_id, batch.batch_id, CODE_VERSION,
    )

    rules_by_table = build_rules(batch.batch_date)

    # 1. profile the raw data before touching it
    profile = profile_data(extracted, rules_by_table)
    distributions = profile_distributions(extracted)

    # 2. clean, standardize, and filter each table
    tables: dict[str, TableResult] = {}
    for table in TABLES:
        result = process_table(extracted[table], table, rules_by_table[table], batch)
        tables[table] = result
        log.info(
            "TABLE DONE | run_id=%s | parent_run_id=%s | table=%s | read=%d | output=%d | "
            "rejected=%d | excluded=%d | rules_applied=%s",
            batch.run_id, batch.extraction_run_id, table, result.records_read, result.records_output,
            result.records_rejected, result.records_excluded,
            {k: v for k, v in result.rules_applied.items() if v},
        )

    # 3. business-ready output tables (rules B-04 to B-09)
    sales_df = tables[TABLE_SALES].output_df
    measures = {
        "total_sales": tables[TABLE_SALES].output_sum,
        "waste_cost": tables[TABLE_WASTE].output_sum,
        "inventory_cost": tables[TABLE_INVENTORY].output_sum,
        "total_items_sold": int(sales_df["quantity"].sum()) if len(sales_df) else 0,
    }
    outputs = build_output_tables(
        batch, measures["total_sales"], measures["waste_cost"], measures["inventory_cost"]
    )
    fact = outputs["fact_daily_summary"].iloc[0]
    measures["total_expenses"] = fact["total_expenses"]
    measures["profit"] = fact["profit"]

    # 4. validate and decide
    checks = run_checks(tables, outputs, handover_counts)
    decision = decide(checks)
    for c in checks:
        level = log.info if c.status == "PASS" else (log.error if c.status == "FAIL" else log.warning)
        level("[%s] %s %s (%s): %s", c.check_id, c.status, c.name, c.severity, c.detail)

    parts = [t.quarantine for t in tables.values() if not t.quarantine.empty]
    quarantine = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=QUARANTINE_COLUMNS)

    ended = datetime.now(PH_TZ)
    log.info(
        "TRANSFORM END | run_id=%s | parent_run_id=%s | decision=%s | total_sales=%s | total_expenses=%s | "
        "profit=%s | quarantined=%d",
        batch.run_id, batch.extraction_run_id, decision, measures["total_sales"],
        measures["total_expenses"], measures["profit"], len(quarantine),
    )

    return TransformResult(
        batch=batch, started_at=started, ended_at=ended, tables=tables, outputs=outputs,
        quarantine=quarantine, checks=checks, decision=decision, profile=profile,
        distributions=distributions, measures=measures,
    )
