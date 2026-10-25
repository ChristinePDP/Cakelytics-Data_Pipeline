"""
TRANSFORM — Validation checks

Verifies that the TRANSFORMED data is correct, complete, and safe to hand
over to the Load stage. It does not repeat extraction checks (connectivity,
query execution). Extraction counts are used only as a reference value for
reconciliation.

Severity
  Critical      a FAIL stops the pipeline (decision: REJECTED)
  Non-critical  a problem is a WARNING (decision: MANUAL REVIEW)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from transform.engine import TableResult
from transform.rules import REJECTION_THRESHOLD, TABLES
from utils.batch import Batch

CRITICAL = "Critical"
NON_CRITICAL = "Non-critical"

# check_id: (name, target, purpose, criteria, severity)
CHECK_CATALOG = {
    "V-01": ("Record reconciliation", "All three input tables",
             "Detect records that were lost or created by the transformation",
             "records_read = records_output + records_rejected + records_excluded, for every table",
             CRITICAL),
    "V-02": ("Extraction count reference", "All three input tables",
             "Confirm that Stage 2 received exactly the records that Stage 1 handed over",
             "records_read equals the row count in the extraction handover", CRITICAL),
    "V-03": ("Key uniqueness of retained records", "order_items.id, waste_logs.id, inventory_logs.id",
             "Prevent double counting in the totals",
             "Retained records have no NULL id and duplicate count = 0", CRITICAL),
    "V-04": ("Output key uniqueness", "dim_date.full_date, fact_daily_summary.full_date",
             "Prevent duplicate rows in the output tables",
             "date_key and full_date are unique for each output table", CRITICAL),
    "V-05": ("Required columns not null", "All columns of dim_date and fact_daily_summary",
             "Ensure the Load stage never receives an incomplete row",
             "NULL count = 0 in every output column", CRITICAL),
    "V-06": ("Referential integrity", "fact_daily_summary.date_key -> dim_date.date_key",
             "Prevent a fact row without its date",
             "100% of fact rows have a matching dim_date row by date_key and full_date", CRITICAL),
    "V-07": ("Data type and format conformance", "Output tables",
             "Ensure the output matches the warehouse column types",
             "full_date is YYYY-MM-DD; date_key is integer; money columns have at most 2 decimal places; day, month, year are integers",
             CRITICAL),
    "V-08": ("Business rule check", "fact_daily_summary",
             "Catch impossible or inconsistent totals",
             "total_sales >= 0, total_expenses >= 0, profit = total_sales - total_expenses",
             CRITICAL),
    "V-09": ("Aggregate reconciliation", "Amounts of the three input tables and the output totals",
             "Prove that no money was lost or invented",
             "Sum read = sum output + sum rejected + sum excluded, and output totals equal the sums of the retained records",
             CRITICAL),
    "V-10": ("Join cardinality (row multiplication)", "order_items.id after the extraction join",
             "Detect a join that multiplied rows",
             "Number of duplicate ids rejected by C-02 = 0", NON_CRITICAL),
    "V-11": ("Rejection rate within threshold", "Each input table",
             "Detect unusually poor data quality",
             f"records_rejected / records_read <= {REJECTION_THRESHOLD:.0%}", NON_CRITICAL),
}


@dataclass
class CheckResult:
    check_id: str
    name: str
    target: str
    purpose: str
    criteria: str
    severity: str
    status: str      # PASS | FAIL | WARNING
    detail: str


def _result(check_id: str, ok: bool, detail: str) -> CheckResult:
    name, target, purpose, criteria, severity = CHECK_CATALOG[check_id]
    status = "PASS" if ok else ("FAIL" if severity == CRITICAL else "WARNING")
    return CheckResult(check_id, name, target, purpose, criteria, severity, status, detail)


def run_checks(
    tables: dict[str, TableResult],
    outputs: dict[str, pd.DataFrame],
    handover_counts: dict[str, int],
) -> list[CheckResult]:
    dim, fact = outputs["dim_date"], outputs["fact_daily_summary"]
    results: list[CheckResult] = []

    # V-01 record reconciliation
    bad = [t for t, r in tables.items()
           if r.records_read != r.records_output + r.records_rejected + r.records_excluded]
    results.append(_result("V-01", not bad, "; ".join(
        f"{t}: read={r.records_read}, output={r.records_output}, rejected={r.records_rejected}, "
        f"excluded={r.records_excluded}" for t, r in tables.items())))

    # V-02 extraction count reference
    bad = [t for t in TABLES if tables[t].records_read != handover_counts.get(t)]
    results.append(_result("V-02", not bad, "; ".join(
        f"{t}: handover={handover_counts.get(t)}, read={tables[t].records_read}" for t in TABLES)))

    # V-03 key uniqueness of retained records
    bad = [t for t, r in tables.items()
           if r.records_output and (r.output_df["id"].isna().any() or not r.output_df["id"].is_unique)]
    results.append(_result("V-03", not bad, "duplicate or NULL ids in: " + ", ".join(bad) if bad
                           else "all retained ids are unique and not NULL"))

    # V-04 output key uniqueness
    bad = []
    for n, df in (("dim_date", dim), ("fact_daily_summary", fact)):
        if not df["full_date"].is_unique or not df["date_key"].is_unique:
            bad.append(n)
    results.append(_result("V-04", not bad, "duplicate keys in: " + ", ".join(bad) if bad
                           else "date_key and full_date are unique in both output tables"))

    # V-05 required columns not null
    nulls = {f"{n}.{c}": int(df[c].isna().sum())
             for n, df in (("dim_date", dim), ("fact_daily_summary", fact)) for c in df.columns
             if df[c].isna().any()}
    results.append(_result("V-05", not nulls, f"NULL counts: {nulls}" if nulls else "no NULL values in the output"))

    # V-06 referential integrity
    orphans = int((~fact["date_key"].isin(dim["date_key"])).sum()) + int((~fact["full_date"].isin(dim["full_date"])).sum())
    results.append(_result("V-06", orphans == 0, f"{orphans} fact row(s) without a matching dim_date row"))

    # V-07 data type and format conformance
    problems = []
    for df in (dim, fact):
        if not df["full_date"].astype(str).map(lambda v: bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v))).all():
            problems.append("full_date format")
        if not df["date_key"].map(lambda v: isinstance(v, (int, int)) and v > 0).all():
            problems.append("date_key type or value")
    for col in ("total_sales", "total_expenses", "profit"):
        if not fact[col].map(lambda v: isinstance(v, Decimal) and v.is_finite() and v.as_tuple().exponent >= -2).all():
            problems.append(f"{col} type or precision")
    for col in ("day", "month", "year"):
        if not dim[col].map(lambda v: isinstance(v, (int, int))).all():
            problems.append(f"{col} type")
    results.append(_result("V-07", not problems, ", ".join(problems) if problems else "types and formats conform"))

    # V-08 business rules
    row = fact.iloc[0]
    problems = []
    if row["total_sales"] < 0:
        problems.append("total_sales is negative")
    if row["total_expenses"] < 0:
        problems.append("total_expenses is negative")
    if row["profit"] != row["total_sales"] - row["total_expenses"]:
        problems.append("profit is not sales - expenses")
    results.append(_result("V-08", not problems, ", ".join(problems) if problems else "totals are consistent"))

    # V-09 aggregate reconciliation
    problems = [t for t, r in tables.items()
                if r.raw_sum != r.output_sum + r.rejected_sum + r.excluded_sum]
    if row["total_sales"] != tables["order_items"].output_sum:
        problems.append("fact total_sales")
    if row["total_expenses"] != tables["waste_logs"].output_sum + tables["inventory_logs"].output_sum:
        problems.append("fact total_expenses")
    results.append(_result("V-09", not problems, "mismatch in: " + ", ".join(problems) if problems else
                           "; ".join(f"{t}: read={r.raw_sum}, output={r.output_sum}, rejected={r.rejected_sum}, "
                                     f"excluded={r.excluded_sum}" for t, r in tables.items())))

    # V-10 join cardinality (non-critical)
    dup = tables["order_items"].rules_applied.get("C-02", 0)
    results.append(_result("V-10", dup == 0, f"{dup} duplicate order_items id(s) found after the join"))

    # V-11 rejection rate (non-critical)
    over = []
    rates = []
    for t, r in tables.items():
        rate = (r.records_rejected / r.records_read) if r.records_read else 0.0
        rates.append(f"{t}: {rate:.2%}")
        if rate > REJECTION_THRESHOLD:
            over.append(t)
    results.append(_result("V-11", not over, "rejection rate -> " + "; ".join(rates)))

    return results


def decide(checks: list[CheckResult]) -> str:
    """APPROVED -> proceed to Load, MANUAL REVIEW -> hold the batch, REJECTED -> stop."""
    if any(c.status == "FAIL" for c in checks):
        return "REJECTED"
    if any(c.status == "WARNING" for c in checks):
        return "MANUAL REVIEW"
    return "APPROVED"
