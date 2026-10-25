"""
TRANSFORM — Rule engine

Applies the rules from rules.py to one table, in order, and keeps count of
everything that happens:

  * records read
  * records modified or removed by each rule (rules_applied)
  * records rejected (data-quality problems) and excluded (business rules),
    every one of them stored in the quarantine table with its rule ID and reason
  * the amounts that were read, rejected, excluded, and kept, so the
    aggregate check can prove that no money disappeared

Nothing is dropped silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import pandas as pd

from transform.rules import MEASURE, TIMESTAMP_COL, Rule, _b, sum_money
from utils.batch import PH_TZ, Batch

QUARANTINE_COLUMNS = [
    "batch_id", "run_id", "table_name", "record_id", "rule_id",
    "category", "reason", "amount", "quarantined_at",
]

ZERO = Decimal("0.00")


@dataclass
class TableResult:
    table: str
    started_at: datetime
    ended_at: datetime
    records_read: int
    records_output: int        # records that contribute to the daily totals
    records_rejected: int      # data-quality rejects (category REJECTED)
    records_excluded: int      # business exclusions (category EXCLUDED)
    rules_applied: dict        # rule_id -> records modified or removed by that rule
    raw_sum: Decimal           # sum of valid amounts read
    rejected_sum: Decimal
    excluded_sum: Decimal
    output_sum: Decimal
    output_df: pd.DataFrame
    quarantine: pd.DataFrame


def _quarantine_rows(removed: pd.DataFrame, table: str, rule: Rule, batch: Batch, measure: str) -> pd.DataFrame:
    return pd.DataFrame({
        "batch_id": batch.batch_id,
        "run_id": batch.run_id,
        "table_name": table,
        "record_id": removed["id"].astype("string").reset_index(drop=True),
        "rule_id": rule.rule_id,
        "category": rule.category,
        "reason": rule.reason,
        "amount": removed[measure].astype("string").reset_index(drop=True),
        "quarantined_at": datetime.now(PH_TZ).isoformat(timespec="seconds"),
    })


def process_table(raw_df: pd.DataFrame, table: str, rules: list[Rule], batch: Batch) -> TableResult:
    started = datetime.now(PH_TZ)
    measure = MEASURE[table]

    df = raw_df.reset_index(drop=True).copy()
    ts_col = TIMESTAMP_COL.get(table)
    if ts_col and ts_col in df.columns and "id" in df.columns:
        df = df.sort_values(by=[ts_col, "id"], na_position="last").reset_index(drop=True)
    elif "id" in df.columns:
        df = df.sort_values(by=["id"], na_position="last").reset_index(drop=True)

    records_read = len(df)
    raw_sum = sum_money(df[measure]) if records_read else ZERO

    applied: dict[str, int] = {}
    parts: list[pd.DataFrame] = []
    totals = {"REJECTED": [0, ZERO], "EXCLUDED": [0, ZERO]}   # category -> [count, amount]

    def remove(mask: pd.Series, rule: Rule) -> int:
        nonlocal df
        removed = df[mask]
        parts.append(_quarantine_rows(removed, table, rule, batch, measure))
        totals[rule.category][0] += len(removed)
        totals[rule.category][1] += sum_money(removed[measure])
        df = df[~mask].reset_index(drop=True)
        return len(removed)

    for rule in rules:
        affected = 0

        if df.empty:
            applied[rule.rule_id] = 0
            continue

        # STANDARDIZE rules: values that cannot be converted are rejected first
        if rule.detect_fail is not None:
            fail = _b(rule.detect_fail(df))
            if fail.any():
                affected += remove(fail, rule)

        matches = _b(rule.detect(df))

        if rule.action in ("REJECT", "EXCLUDE"):
            if matches.any():
                affected += remove(matches, rule)
        elif rule.action == "MODIFY" and rule.fix is not None:
            count = int(matches.sum())
            if count:
                df = rule.fix(df)
                affected += count
        elif rule.action == "STANDARDIZE" and rule.fix is not None:
            # All remaining rows are converted to the standard format
            df = rule.fix(df)
            affected += len(df)

        applied[rule.rule_id] = applied.get(rule.rule_id, 0) + affected

    quarantine = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame(columns=QUARANTINE_COLUMNS)
    )

    ended = datetime.now(PH_TZ)
    return TableResult(
        table=table,
        started_at=started,
        ended_at=ended,
        records_read=records_read,
        records_output=len(df),
        records_rejected=totals["REJECTED"][0],
        records_excluded=totals["EXCLUDED"][0],
        rules_applied=applied,
        raw_sum=raw_sum,
        rejected_sum=totals["REJECTED"][1],
        excluded_sum=totals["EXCLUDED"][1],
        output_sum=sum_money(df[measure]) if len(df) else ZERO,
        output_df=df,
        quarantine=quarantine,
    )
