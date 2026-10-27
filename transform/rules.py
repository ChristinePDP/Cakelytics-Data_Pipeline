"""
TRANSFORM — Rule registry

Every cleaning (C), standardization (S), and business (B) rule is defined here
with its ID, target, condition, action, and justification.

The SAME rule objects are used to:
  1. profile the raw extracted data (how many records does each rule affect?),
  2. clean / standardize / filter the data, and
  3. generate the rule tables for the documentation,
so the documentation and the code cannot drift apart.

Rule actions
  REJECT       the record is removed and stored in quarantine (category REJECTED)
  EXCLUDE      a business exclusion; stored in quarantine (category EXCLUDED)
  MODIFY       the record is corrected and kept
  STANDARDIZE  the value is converted to the standard format; if the conversion
               fails the record is rejected (the rule's "on failure" behaviour)

Nothing is ever dropped silently: every removed record is counted, logged, and
written to the quarantine file with its rule ID and reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Callable

import pandas as pd

from utils.batch import PH_TZ

# Bump whenever any rule changes. It is written to the run metadata.
CODE_VERSION = "2.0.0"

# Acceptance rule, defined in advance (not chosen after seeing the results):
# if more than 1% of the records READ from a table are rejected for
# data-quality reasons, the batch needs manual review.
# Justification: with a few hundred rows per day, 1% is about a handful of
# rows, which is a small but visible amount of bad data for a single-shop system.
REJECTION_THRESHOLD = 0.01

MONEY_STEP = Decimal("0.01")

TABLE_SALES = "order_items"
TABLE_WASTE = "waste_logs"
TABLE_INVENTORY = "inventory_logs"
TABLES = (TABLE_SALES, TABLE_WASTE, TABLE_INVENTORY)

# The amount that each table contributes to the daily totals.
MEASURE = {TABLE_SALES: "total_price", TABLE_WASTE: "cost", TABLE_INVENTORY: "cost"}

# The timestamp column that decides which business day a record belongs to.
TIMESTAMP_COL = {TABLE_SALES: "updated_at", TABLE_WASTE: "logged_at", TABLE_INVENTORY: "created_at"}


# --------------------------------------------------------------------------
# Small helpers (work on both raw and already-converted data)
# --------------------------------------------------------------------------

def txt(s: pd.Series) -> pd.Series:
    return s.astype("string")


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def to_ph(s: pd.Series) -> pd.Series:
    """Parse timestamps (UTC, naive, or text) and convert them to Asia/Manila."""
    return pd.to_datetime(s, utc=True, errors="coerce").dt.tz_convert(PH_TZ)


def money(v) -> Decimal:
    """Round to 2 decimal places, half up."""
    return Decimal(str(v)).quantize(MONEY_STEP, rounding=ROUND_HALF_UP)


def sum_money(s: pd.Series) -> Decimal:
    return sum((money(v) for v in num(s).dropna()), Decimal("0.00"))


def _b(mask: pd.Series) -> pd.Series:
    """Plain boolean mask (missing values count as False)."""
    return mask.fillna(False).astype(bool)


# --------------------------------------------------------------------------
# Rule definition
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Rule:
    rule_id: str
    name: str
    table: str
    column: str          # target column(s)
    issue_type: str      # label used in the profiling report
    action: str          # REJECT | EXCLUDE | MODIFY | STANDARDIZE
    condition: str
    treatment: str
    justification: str
    detect: Callable[[pd.DataFrame], pd.Series]        # rows that match the condition
    fix: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    detect_fail: Callable[[pd.DataFrame], pd.Series] | None = None  # STANDARDIZE: cannot convert
    category: str = "REJECTED"                         # REJECTED | EXCLUDED
    reason: str = ""                                   # text stored in quarantine
    sample_col: str | None = None                      # column shown as example in profiling


# --------------------------------------------------------------------------
# Cleaning rules (C)
# --------------------------------------------------------------------------

def _c01(table: str) -> Rule:
    return Rule(
        "C-01", "Missing primary key", table, "id", "Missing values", "REJECT",
        "id is NULL or blank",
        "Reject the record to quarantine",
        "A record without an id cannot be traced, deduplicated, or reported",
        detect=lambda df: _b(txt(df["id"]).isna() | (txt(df["id"]).str.strip() == "")),
        reason="Missing primary key", sample_col="id",
    )


def _c02(table: str) -> Rule:
    return Rule(
        "C-02", "Duplicate primary key", table, "id", "Duplicates", "REJECT",
        "The same id appears more than once in the batch",
        "Keep the first occurrence; reject the other copies to quarantine",
        "id is the primary key, so a repeated id is a copy created by a faulty join or retry; "
        "keeping one copy prevents double counting",
        detect=lambda df: _b(df["id"].duplicated(keep="first") & df["id"].notna()),
        reason="Duplicate id (first occurrence kept)", sample_col="id",
    )


def _c03(table: str) -> Rule:
    col = MEASURE[table]
    return Rule(
        "C-03", "Missing amount", table, col, "Missing values", "REJECT",
        f"{col} is NULL",
        "Reject the record to quarantine",
        "An amount cannot be guessed, and filling it with 0 would hide the problem and "
        "understate sales or expenses",
        detect=lambda df: _b(df[col].isna()),
        reason=f"Missing {col}", sample_col=col,
    )


def _c04(table: str) -> Rule:
    col = MEASURE[table]
    return Rule(
        "C-04", "Negative amount", table, col, "Invalid values", "REJECT",
        f"{col} is less than 0",
        "Reject the record to quarantine",
        "The business has no negative sales or costs, and a negative value would "
        "wrongly reduce the daily totals",
        detect=lambda df: _b(num(df[col]) < 0),
        reason=f"Negative {col}", sample_col=col,
    )


def _c05(table: str) -> Rule:
    return Rule(
        "C-05", "Invalid quantity", table, "quantity", "Invalid values", "REJECT",
        "quantity is NULL or not greater than 0",
        "Reject the record to quarantine",
        "The source requires quantity > 0, so any other value is an invalid record",
        detect=lambda df: _b(num(df["quantity"]).isna() | (num(df["quantity"]) <= 0)),
        reason="Invalid quantity", sample_col="quantity",
    )


def _c06() -> Rule:
    return Rule(
        "C-06", "Orphan line item", TABLE_SALES, "order_id", "Orphan records", "REJECT",
        "order_id is NULL or blank (no parent order)",
        "Reject the record to quarantine",
        "A line item without a parent order cannot be tied to a sale, so it is not counted as revenue",
        detect=lambda df: _b(txt(df["order_id"]).isna() | (txt(df["order_id"]).str.strip() == "")),
        reason="Orphan record: no parent order", sample_col="order_id",
    )


def _c07(table: str, cols: tuple[str, ...]) -> Rule:
    def detect(df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(False, index=df.index)
        for c in cols:
            s = txt(df[c])
            mask = mask | _b(s.notna() & (s != s.str.strip()))
        return mask

    def fix(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for c in cols:
            df[c] = txt(df[c]).str.strip()
        return df

    return Rule(
        "C-07", "Leading/trailing whitespace", table, ", ".join(cols), "Inconsistent formats", "MODIFY",
        "Text value has leading or trailing spaces",
        "Trim the spaces and keep the record",
        "Prevents false mismatches when grouping or comparing names and codes",
        detect=detect, fix=fix, sample_col=cols[0],
    )


# --------------------------------------------------------------------------
# Standardization rules (S)
# --------------------------------------------------------------------------

def _s01(table: str) -> Rule:
    ts = TIMESTAMP_COL[table]

    def fix(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        parsed = to_ph(df[ts])
        df[ts] = parsed
        df["business_date"] = parsed.dt.strftime("%Y-%m-%d")
        return df

    return Rule(
        "S-01", "Timestamp standardization", table, ts, "Inconsistent formats", "STANDARDIZE",
        f"{ts} is stored as UTC (+00:00); source format timestamptz",
        "Convert to ISO 8601 in Asia/Manila (UTC+8) and derive business_date (YYYY-MM-DD). "
        "On failure: reject to quarantine",
        "The shop's business day follows Philippine time, not UTC",
        detect=lambda df: _b(to_ph(df[ts]).notna()),
        detect_fail=lambda df: _b(to_ph(df[ts]).isna()),
        fix=fix, reason=f"Missing or invalid {ts}", sample_col=ts,
    )


def _s02(table: str, batch_date: str) -> Rule:
    ts = TIMESTAMP_COL[table]

    def detect(df: pd.DataFrame) -> pd.Series:
        parsed = to_ph(df[ts])
        return _b(parsed.notna() & (parsed.dt.strftime("%Y-%m-%d") != batch_date))

    return Rule(
        "S-02", "Batch window conformance", table, ts, "Invalid values", "REJECT",
        "business_date (Asia/Manila) is not the batch date",
        "Reject the record to quarantine",
        "Guarantees that each batch contains only its own business day, so a rerun never mixes days",
        detect=detect, reason="Outside the batch window", sample_col=ts,
    )


def _s03(table: str, amount_cols=(), int_cols=(), plain_cols=()) -> Rule:
    numeric = tuple(amount_cols) + tuple(int_cols) + tuple(plain_cols)
    cols_text = ", ".join(("id",) + numeric)

    def detect(df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(False, index=df.index)
        for c in amount_cols:
            cents = num(df[c]) * 100
            mask = mask | _b((cents - cents.round()).abs() > 1e-6)
        return mask

    def detect_fail(df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(False, index=df.index)
        for c in numeric:
            mask = mask | _b(df[c].notna() & num(df[c]).isna())
        for c in int_cols:
            n = num(df[c])
            mask = mask | _b(n.notna() & ((n % 1) != 0))
        return mask

    def fix(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for c in ("id", "order_id"):
            if c in df.columns:
                df[c] = txt(df[c]).str.strip().str.lower()
        for c in amount_cols:
            df[c] = num(df[c]).map(lambda v: float(money(v)) if pd.notna(v) else v)
        for c in int_cols:
            df[c] = num(df[c]).astype("Int64")
        for c in plain_cols:
            df[c] = num(df[c])
        return df

    detail = []
    if amount_cols:
        detail.append(f"{', '.join(amount_cols)} to a number with 2 decimal places (round half up)")
    if int_cols:
        detail.append(f"{', '.join(int_cols)} to an integer")
    if plain_cols:
        detail.append(f"{', '.join(plain_cols)} to a number")
    detail.append("id to lowercase text (UUID)")

    return Rule(
        "S-03", "Data types and numeric precision", table, cols_text, "Inconsistent formats", "STANDARDIZE",
        "Value is not yet in the target data type or has more than 2 decimal places",
        "Convert " + "; ".join(detail) + ". On failure: reject to quarantine",
        "Consistent types and 2-decimal money make sums exact and match the warehouse NUMERIC(10,2) columns",
        detect=detect, detect_fail=detect_fail, fix=fix,
        reason="Cannot convert to the target data type",
        sample_col=(amount_cols or int_cols or plain_cols or ("id",))[0],
    )


def _s04() -> Rule:
    def fix(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["transaction_type"] = txt(df["transaction_type"]).str.strip().str.upper()
        return df

    return Rule(
        "S-04", "Transaction type code mapping", TABLE_INVENTORY, "transaction_type",
        "Inconsistent formats", "MODIFY",
        "transaction_type is not uppercase (for example: in, In)",
        "Convert to uppercase (for example: in -> IN)",
        "Codes must have one standard spelling so that the same transaction type is never split into two groups",
        detect=lambda df: _b(txt(df["transaction_type"]).notna()
                             & (txt(df["transaction_type"]) != txt(df["transaction_type"]).str.upper())),
        fix=fix, sample_col="transaction_type",
    )


# --------------------------------------------------------------------------
# Business exclusion rules (B-01 to B-03)
# --------------------------------------------------------------------------

def _b01() -> Rule:
    return Rule(
        "B-01", "Exclude cancelled orders", TABLE_SALES, "status", "Business exclusion", "EXCLUDE",
        "status = 'Cancelled'",
        "Exclude from the sales total; store in quarantine as EXCLUDED with the reason",
        "A cancelled order is not revenue",
        detect=lambda df: _b(txt(df["status"]).str.strip().str.casefold() == "cancelled"),
        category="EXCLUDED", reason="Cancelled order is not revenue", sample_col="status",
    )


def _b02() -> Rule:
    return Rule(
        "B-02", "Exclude voided waste logs", TABLE_WASTE, "voided_at", "Business exclusion", "EXCLUDE",
        "voided_at is not NULL",
        "Exclude from the waste cost; store in quarantine as EXCLUDED with the reason",
        "A voided entry was cancelled in the operational system, so counting it would double count",
        detect=lambda df: _b(df["voided_at"].notna()),
        category="EXCLUDED", reason="Voided waste log is not an expense", sample_col="voided_at",
    )


def _b03() -> Rule:
    return Rule(
        "B-03", "Exclude voided inventory logs", TABLE_INVENTORY, "voided_at", "Business exclusion", "EXCLUDE",
        "voided_at is not NULL",
        "Exclude from the inventory cost; store in quarantine as EXCLUDED with the reason",
        "A voided entry was cancelled in the operational system, so counting it would double count",
        detect=lambda df: _b(df["voided_at"].notna()),
        category="EXCLUDED", reason="Voided inventory log is not an expense", sample_col="voided_at",
    )


# --------------------------------------------------------------------------
# Rule order per table
#
# Order matters: identity checks first, then business exclusions (so cancelled
# or voided records never count toward the data-quality rejection rate), then
# standardization, then value checks.
# --------------------------------------------------------------------------

def build_rules(batch_date: str) -> dict[str, list[Rule]]:
    return {
        TABLE_SALES: [
            _c01(TABLE_SALES), _c02(TABLE_SALES),
            _b01(),
            _s01(TABLE_SALES), _s02(TABLE_SALES, batch_date),
            _s03(TABLE_SALES, amount_cols=("total_price",), int_cols=("quantity",)),
            _c03(TABLE_SALES), _c04(TABLE_SALES), _c05(TABLE_SALES), _c06(),
        ],
        TABLE_WASTE: [
            _c01(TABLE_WASTE), _c02(TABLE_WASTE),
            _b02(),
            _c07(TABLE_WASTE, ("item_name", "unit")),
            _s01(TABLE_WASTE), _s02(TABLE_WASTE, batch_date),
            _s03(TABLE_WASTE, amount_cols=("cost",), plain_cols=("quantity",)),
            _c03(TABLE_WASTE), _c04(TABLE_WASTE), _c05(TABLE_WASTE),
        ],
        TABLE_INVENTORY: [
            _c01(TABLE_INVENTORY), _c02(TABLE_INVENTORY),
            _b03(),
            _c07(TABLE_INVENTORY, ("item_name", "transaction_type")),
            _s04(),
            _s01(TABLE_INVENTORY), _s02(TABLE_INVENTORY, batch_date),
            _s03(TABLE_INVENTORY, amount_cols=("cost",)),
            _c03(TABLE_INVENTORY), _c04(TABLE_INVENTORY),
        ],
    }


# --------------------------------------------------------------------------
# Derivation / aggregation rules (B-04 to B-10). These produce the output
# columns; they are documented here and executed in compute_profit.py.
# --------------------------------------------------------------------------

DERIVATION_RULES = [
    {"rule_id": "B-04", "inputs": "order_items.total_price (records retained by all rules)",
     "logic": "SUM(total_price)", "output": "fact_daily_summary.total_sales",
     "reason": "Revenue for the day. Cancelled orders are already excluded by B-01"},
    {"rule_id": "B-05", "inputs": "waste_logs.cost (records retained by all rules)",
     "logic": "SUM(cost)", "output": "waste_cost (intermediate measure, in the run metadata)",
     "reason": "Cost of wasted ingredients and materials"},
    {"rule_id": "B-06", "inputs": "inventory_logs.cost (records retained by all rules)",
     "logic": "SUM(cost); every transaction_type is included",
     "output": "inventory_cost (intermediate measure, in the run metadata)",
     "reason": "Cost of inventory transactions recorded for the day"},
    {"rule_id": "B-07", "inputs": "waste_cost, inventory_cost",
     "logic": "waste_cost + inventory_cost", "output": "fact_daily_summary.total_expenses",
     "reason": "All expenses of the day"},
    {"rule_id": "B-08", "inputs": "total_sales, total_expenses",
     "logic": "total_sales - total_expenses", "output": "fact_daily_summary.profit",
     "reason": "Daily profit, the main figure of the dashboard"},
    {"rule_id": "B-09", "inputs": "batch date",
     "logic": "Take the day, month, and year of the batch date",
     "output": "dim_date.full_date, day, month, year",
     "reason": "Supports reporting by day, month, and year. The surrogate key date_key is generated by the warehouse at load"},
    {"rule_id": "B-10", "inputs": "order_items.quantity (records retained by all rules)",
     "logic": "SUM(quantity)", "output": "total_items_sold (measure in the run metadata)",
     "reason": "Number of items sold, used to monitor the batch"},
]
