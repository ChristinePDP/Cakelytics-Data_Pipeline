"""
TRANSFORM — Data profiling

Runs BEFORE any cleaning. For every rule in rules.py it counts how many raw
records match the rule's condition, and shows a few sample values. The result
is the data-quality issue list of the batch, and each issue is linked to the
rule that treats it (traceability from issue to rule).

It also reports value distributions (status, waste type, transaction type)
so that unexpected codes are visible before they reach the totals.
"""

from __future__ import annotations

import pandas as pd

from transform.rules import Rule, _b
from utils.logger import get_logger

log = get_logger(__name__)


def _samples(df: pd.DataFrame, mask: pd.Series, col: str | None, limit: int = 3) -> str:
    if col is None or col not in df.columns:
        return "-"
    values = df.loc[mask, col].dropna().astype(str).unique()[:limit]
    if len(values) == 0:
        return "-"
    return ", ".join(v[:40] for v in values)


def profile_data(raw: dict[str, pd.DataFrame], rules_by_table: dict[str, list[Rule]]) -> list[dict]:
    """One row per (table, rule): the issue, an example, and how many records it affects."""
    rows = []
    for table, rules in rules_by_table.items():
        df = raw[table]
        total = len(df)
        for rule in rules:
            affected, example = 0, "-"
            if total:
                try:
                    mask = _b(rule.detect(df))
                    affected = int(mask.sum())
                    example = _samples(df, mask, rule.sample_col)
                except Exception as exc:   # profiling must never stop the batch
                    log.warning("Profiling failed for %s on %s: %s", rule.rule_id, table, exc)
                    example = "(profiling error)"
            rows.append({
                "table": table,
                "column": rule.column,
                "issue_type": rule.issue_type,
                "issue": rule.name,
                "example": example,
                "affected": affected,
                "total": total,
                "pct": (affected / total * 100) if total else 0.0,
                "treatment": f"{rule.treatment.split('. ')[0]} (Rule {rule.rule_id})",
                "rule_id": rule.rule_id,
            })
    return rows


def _distribution(df: pd.DataFrame, group_col: str, amount_col: str) -> pd.DataFrame:
    """Records and total amount per code value. Amounts that are not numbers count as missing."""
    data = df[[group_col, "id", amount_col]].copy()
    data[amount_col] = pd.to_numeric(data[amount_col], errors="coerce")
    return (
        data.groupby(group_col, dropna=False)
        .agg(records=("id", "size"), **{amount_col: (amount_col, "sum")})
        .reset_index()
    )


def profile_distributions(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Value counts of the code columns, to spot unexpected values early."""
    spec = [
        ("order_items", "status", "total_price"),
        ("waste_logs", "waste_type", "cost"),
        ("inventory_logs", "transaction_type", "cost"),
    ]
    out: dict[str, pd.DataFrame] = {}
    for table, group_col, amount_col in spec:
        df = raw[table]
        if not len(df):
            continue
        try:
            out[f"{table}.{group_col}"] = _distribution(df, group_col, amount_col)
        except Exception as exc:   # profiling must never stop the batch
            log.warning("Distribution of %s.%s failed: %s", table, group_col, exc)
    return out
