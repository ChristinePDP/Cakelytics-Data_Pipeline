# Stage 2: Data Transformation Documentation

**Project:** CakeLytics ETL Pipeline (Aileen & Niculus Cake Shop)  
**Stage:** 2 of 3 (Extraction → **Transformation** → Load)  
**Implementation:** `transform/` (rules, engine, validation) and `pipeline/run_transform.py` (test runner)  
**Code / rules version:** `2.0.0` (`CODE_VERSION` in `transform/rules.py`)  

This document describes the transformation pipeline implemented in the repository. The rule tables in Sections 3 to 5 and the validation catalog in Section 7 originate directly from the code's central rule registry (`transform/rules.py`, `transform/validate.py`), ensuring complete alignment between documentation and code execution.

The source database schema and extraction queries are specified in the **Stage 1 Extraction Documentation** and are not repeated here.

---

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

| Item | Description |
|---|---|
| **Input Source** | The **extraction handover** from Stage 1: raw extracted datasets of `order_items` (sales), `waste_logs`, and `inventory_logs`. |
| **Format and Location** | In-memory Pandas DataFrames (`order_items_df`, `waste_df`, `inventory_df`) passed directly from extraction functions to `transform_batch()`. There are no intermediate staging tables or physical staging files. |
| **Extraction Run ID** | `extraction_run_id` (e.g., `EXT_20261023_100546`), preserved in transformation metadata as `parent_run_id` for end-to-end data lineage. |
| **Batch ID** | `batch_id` (e.g., `BATCH_20261019`), deterministic across reruns of the same business day. |
| **Batch Window** | One full business day in Asia/Manila time (UTC+8): `2026-10-19T00:00:00+08:00` to `2026-10-19T23:59:59.999999+08:00`. |
| **Handover Counts** | Exact row count per table captured at extraction (`handover_counts`). Stage 2 uses these counts as a reference value for reconciliation (Check V-02). |

> **Note:** Stage 2 is strictly decoupled from the source database. The transformation engine (`transform_batch()`) never issues SQL queries or reads directly from Supabase; it operates solely on the in-memory DataFrames handed over by Stage 1. The test runner `pipeline/run_transform.py` serves as the development test harness that simulates extraction handover before triggering transformation.

### 1.2 Batch Processing Method

| Item | Description |
|---|---|
| **Tool / Technology** | Python 3 with Pandas for tabular transformations. Financial aggregations utilize Python `Decimal` (`ROUND_HALF_UP`) to ensure exact monetary precision without floating-point inaccuracies. |
| **Batch Frequency & Schedule** | Daily at 12:00 AM Asia/Manila time (PHT, UTC+8) for the previous business day. In Phase 2, runs are invoked via test harness (`python -m pipeline.run_transform [YYYY-MM-DD]`). Full orchestration via Prefect is scheduled in Phase 3. |
| **Batch Window Size** | Exactly 24 hours of operational activity (one business day in Philippine time). |
| **Execution Entry Point** | `python -m pipeline.run_transform [YYYY-MM-DD]` (extracts the batch, transforms in-memory, executes validation checks; nothing is loaded into the warehouse). |
| **Rule Execution Order** | Fixed pipeline order per table: Identity checks (`C-01`, `C-02`) → Business exclusions (`B-01` to `B-03`) → Text cleaning (`C-07`, `S-04`) → Timestamp & type standardization (`S-01` to `S-03`) → Value validation (`C-03` to `C-06`). The order is fixed in `build_rules()`. |

**Rerun of a failed batch:** A run that fails writes execution metadata with `status = FAILED`, logs the specific error message, exits with code `1`, and produces no output tables. Because the handover is in-memory, a rerun executes Stage 1 extraction followed by Stage 2 for the same `batch_id`. Same extracted input → same output. No database rollback or manual cleanup is required.

**Idempotency:** Re-executing the same batch date yields identical results without duplicate records:

| Mechanism | Implementation Detail |
|---|---|
| **Pure Transformation** | Output tables are pure deterministic functions of the input DataFrames. No state is persisted across runs. |
| **Overwrite by Batch ID** | The quarantine file is named `quarantine_<batch_id>.csv` and is completely overwritten per batch run. A clean rerun with zero defects replaces an earlier file containing rejects. |
| **Unique Run Metadata** | Every production run writes a unique timestamped file (`transform_<run_id>.json`), preserving historical audit records (standalone test suites such as `run_defect_test` use a fixed test label for predictable test output). |
| **Warehouse Deduplication** | In the subsequent Load stage, the warehouse enforces uniqueness on `date_key`, replacing existing records upon rerun. |

### 1.3 Transformation Scope

| Input Table | Cleaned Fields | Standardized Fields | Derived / Aggregated Measures |
|---|---|---|---|
| `order_items` | `id`, `order_id`, `quantity`, `total_price`, `status` | `updated_at`, `total_price`, `quantity`, `id` | `total_sales`, `total_items_sold` |
| `waste_logs` | `id`, `item_name`, `unit`, `quantity`, `cost`, `voided_at` | `logged_at`, `cost`, `quantity`, `id` | `waste_cost` |
| `inventory_logs` | `id`, `item_name`, `transaction_type`, `cost`, `voided_at` | `created_at`, `cost`, `transaction_type`, `id` | `inventory_cost` |

**Transformed Outputs:** `dim_date` (Date Dimension) and `fact_daily_summary` (Daily Fact Summary).

### 1.4 Dependencies and Assumptions

**Processing Order:**
1. Dimension generation occurs first: `dim_date` is constructed from the batch date.
2. Individual input tables (`order_items`, `waste_logs`, `inventory_logs`) are cleaned, filtered, and aggregated independently.
3. Daily summary fact table (`fact_daily_summary`) is constructed, linking to `dim_date`.
4. Automated validation suite (V-01 to V-11) evaluates the completed output tables prior to handover.

**Key Assumptions:**
- **Business Day Alignment:** The bake shop operates under Philippine Standard Time (UTC+8, no DST). Timestamps are converted from UTC to UTC+8 so early morning sales (12:00 AM – 7:59 AM PHT) align with the proper business day.
- **Order Attribution Date:** Sales are attributed to the date of last update (`orders.updated_at`).
- **Inventory Costs:** All inventory log movements contribute to daily accounting. Usage logs (`OUT`) carry ₱0.00 cost in the operational system as usage is tracked by volume, while restocks (`IN`) carry cost. Every transaction type is included in summation to maintain mathematical reconciliation against raw reads.
- **Currency:** All financial figures are strictly in Philippine Peso (PHP, ₱); no multi-currency conversion is needed.
- **Lookup Tables:** No external dimension lookups are required; code mappings are standardized programmatically via rule S-04.

---

## 2. Data Profiling and Issue Identification

Data profiling runs **prior to data cleaning** (`transform/profile_data.py`). The profiler evaluates raw extracted records against every rule condition in the registry, quantifying defect frequency and capturing representative sample values.

### 2.1 Production Batch Profiling (`BATCH_20261019`)
Profiling of operational data from October 19, 2026 (Run `TRN_20261023_100546`), where business exclusions (cancelled orders and voided inventory) were encountered:

| Table | Column | Issue Type | Example | Affected Records | Planned Treatment |
|---|---|---|---|---|---|
| `order_items` | `id` | Missing values: Missing primary key | — | 0 of 2 (0.0%) | Reject the record to quarantine (Rule C-01) |
| `order_items` | `id` | Duplicates: Duplicate primary key | — | 0 of 2 (0.0%) | Keep the first occurrence; reject the other copies to quarantine (Rule C-02) |
| `order_items` | `status` | Business exclusion: Exclude cancelled orders | `Cancelled` | 2 of 2 (100.0%) | Exclude from the sales total; store in quarantine as EXCLUDED with the reason (Rule B-01) |
| `order_items` | `updated_at` | Inconsistent formats: Timestamp standardization | `2026-10-19 10:52:39.842182+00:00` | 2 of 2 (100.0%) | Convert to ISO 8601 in Asia/Manila (UTC+8) and derive business_date (YYYY-MM-DD) (Rule S-01) |
| `order_items` | `updated_at` | Invalid values: Batch window conformance | — | 0 of 2 (0.0%) | Reject the record to quarantine (Rule S-02) |
| `order_items` | `id`, `total_price`, `quantity` | Inconsistent formats: Data types and numeric precision | — | 0 of 2 (0.0%) | Convert total_price to a number with 2 decimal places (round half up); quantity to an integer; id to lowercase text (UUID) (Rule S-03) |
| `order_items` | `total_price` | Missing values: Missing amount | — | 0 of 2 (0.0%) | Reject the record to quarantine (Rule C-03) |
| `order_items` | `total_price` | Invalid values: Negative amount | — | 0 of 2 (0.0%) | Reject the record to quarantine (Rule C-04) |
| `order_items` | `quantity` | Invalid values: Invalid quantity | — | 0 of 2 (0.0%) | Reject the record to quarantine (Rule C-05) |
| `order_items` | `order_id` | Orphan records: Orphan line item | — | 0 of 2 (0.0%) | Reject the record to quarantine (Rule C-06) |
| `waste_logs` | `(all)` | Empty dataset | — | 0 of 0 (0.0%) | No waste logged for this business day |
| `inventory_logs` | `id` | Missing values: Missing primary key | — | 0 of 19 (0.0%) | Reject the record to quarantine (Rule C-01) |
| `inventory_logs` | `id` | Duplicates: Duplicate primary key | — | 0 of 19 (0.0%) | Keep the first occurrence; reject the other copies to quarantine (Rule C-02) |
| `inventory_logs` | `voided_at` | Business exclusion: Exclude voided inventory logs | `2026-10-19 12:20:19.135000+00:00, 2026-10-19 13:31:07.094000+00:00, 2026-10-19 12:53:32.964000+00:00` | 3 of 19 (15.8%) | Exclude from the inventory cost; store in quarantine as EXCLUDED with the reason (Rule B-03) |
| `inventory_logs` | `item_name`, `transaction_type` | Inconsistent formats: Leading/trailing whitespace | — | 0 of 19 (0.0%) | Trim the spaces and keep the record (Rule C-07) |
| `inventory_logs` | `transaction_type` | Inconsistent formats: Transaction type code mapping | — | 0 of 19 (0.0%) | Convert to uppercase (for example: in -> IN) (Rule S-04) |
| `inventory_logs` | `created_at` | Inconsistent formats: Timestamp standardization | `2026-10-19 13:17:49.568758+00:00, 2026-10-19 11:10:42.942805+00:00, 2026-10-19 13:21:26.877190+00:00` | 19 of 19 (100.0%) | Convert to ISO 8601 in Asia/Manila (UTC+8) and derive business_date (YYYY-MM-DD) (Rule S-01) |
| `inventory_logs` | `created_at` | Invalid values: Batch window conformance | — | 0 of 19 (0.0%) | Reject the record to quarantine (Rule S-02) |
| `inventory_logs` | `id`, `cost` | Inconsistent formats: Data types and numeric precision | — | 0 of 19 (0.0%) | Convert cost to a number with 2 decimal places (round half up); id to lowercase text (UUID) (Rule S-03) |
| `inventory_logs` | `cost` | Missing values: Missing amount | — | 0 of 19 (0.0%) | Reject the record to quarantine (Rule C-03) |
| `inventory_logs` | `cost` | Invalid values: Negative amount | — | 0 of 19 (0.0%) | Reject the record to quarantine (Rule C-04) |

**Value Distributions (`BATCH_20261019`):**

`order_items.status`:
| status | records | total_price |
|---|---|---|
| Cancelled | 2 | ₱680.00 |

`inventory_logs.transaction_type` (Raw extracted distribution prior to voided exclusion):
| transaction_type | records | cost |
|---|---|---|
| IN | 16 | ₱3,007.00 |
| OUT | 3 | ₱0.00 |

### 2.2 Synthetic Edge-Case & Defect Profiling (Test Suite Validation)
To thoroughly verify that every cleaning rule functions when defects occur, a synthetic defect test batch (`BATCH_20261020_DEFECT_TEST`) was profiled through the engine:

| Table | Column | Issue Type | Example Value | Affected Records | Planned Treatment |
|---|---|---|---|---|---|
| `order_items` | `id` | Missing values: Missing primary key | `None` | 1 of 8 (12.5%) | Reject the record to quarantine (Rule C-01) |
| `order_items` | `id` | Duplicates: Duplicate primary key | `11111111-1111...` | 1 of 8 (12.5%) | Retain first occurrence; reject copies (Rule C-02) |
| `order_items` | `total_price` | Missing values: Missing amount | `NaN` | 1 of 8 (12.5%) | Reject the record to quarantine (Rule C-03) |
| `order_items` | `total_price` | Invalid values: Negative amount | `-50.00` | 1 of 8 (12.5%) | Reject the record to quarantine (Rule C-04) |
| `order_items` | `quantity` | Invalid values: Invalid quantity | `0` | 1 of 8 (12.5%) | Reject the record to quarantine (Rule C-05) |
| `order_items` | `order_id` | Orphan records: Orphan line item | `None` | 1 of 8 (12.5%) | Reject the record to quarantine (Rule C-06) |
| `waste_logs` | `voided_at` | Business exclusion: Exclude voided waste | `2026-10-20 12:00:00+08:00` | 1 of 3 (33.3%) | Exclude from waste cost; quarantine as EXCLUDED (Rule B-02) |
| `waste_logs` | `item_name` | Inconsistent formats: Whitespace padding | `"  Flour  "` | 1 of 3 (33.3%) | Trim whitespace and retain (Rule C-07) |
| `inventory_logs` | `transaction_type` | Inconsistent formats: Code casing | `'in'` | 1 of 5 (20.0%) | Standardize to uppercase `'IN'` (Rule S-04) |
| `inventory_logs` | `created_at` | Invalid values: Out of batch window | `2026-10-19 23:59:58+08:00` | 1 of 5 (20.0%) | Reject the record to quarantine (Rule S-02) |
| `inventory_logs` | `cost` | Inconsistent formats: Conversion failure | `"INVALID_NUM"` | 1 of 5 (20.0%) | Reject the record to quarantine (Rule S-03) |

---

## 3. Data Cleaning Rules

Every rule specifies an ID, target field, detection condition, pipeline action, and business justification. Profiling (Section 2) identifies the defect; cleaning rules define the response.

> **Important:** No data is dropped silently. Records removed due to data quality defects are categorized as `REJECTED`, while valid records excluded by business criteria are categorized as `EXCLUDED`. All removed records are written to `quarantine/quarantine_<batch_id>.csv`.

| Rule ID | Name | Target | Condition | Action | Justification |
|---|---|---|---|---|---|
| **C-01** | Missing primary key | `order_items.id`, `waste_logs.id`, `inventory_logs.id` | `id` is NULL, empty, or whitespace | Reject the record to quarantine | A record lacking an ID cannot be traced, deduplicated, or reconciled. |
| **C-02** | Duplicate primary key | `order_items.id`, `waste_logs.id`, `inventory_logs.id` | The same `id` appears more than once in the batch | **Keep the first occurrence**; reject other copies to quarantine | `id` is the primary key; duplicate entries inflate counts and financial totals. First occurrence is retained deterministically. |
| **C-03** | Missing amount | `order_items.total_price`, `waste_logs.cost`, `inventory_logs.cost` | The amount is NULL / NaN | Reject the record to quarantine | Monetary amounts cannot be guessed; replacing with 0 hides problems and understates revenue/expenses. |
| **C-04** | Negative amount | `order_items.total_price`, `waste_logs.cost`, `inventory_logs.cost` | The amount is less than 0 | Reject the record to quarantine | Operational sales and costs cannot be negative; negative values improperly reduce daily totals. |
| **C-05** | Invalid quantity | `order_items.quantity`, `waste_logs.quantity` | Quantity is NULL or not greater than 0 | Reject the record to quarantine | Transactions require positive physical quantities (`quantity > 0`). |
| **C-06** | Orphan line item | `order_items.order_id` | `order_id` is NULL or blank (no parent order) | Reject the record to quarantine | A line item without an order cannot be attributed to valid customer revenue. |
| **C-07** | Leading/trailing whitespace | `item_name`, `unit`, `transaction_type` | Text value has leading or trailing spaces | Trim the spaces and keep the record (`MODIFY`) | Cleans formatting inconsistencies without discarding valid records. |

### Cleaning Considerations Coverage

| Consideration | Handling Rules |
|---|---|
| **Missing or Null Values** | Rejected to quarantine (C-01, C-03, C-05, C-06); never imputed with default values. |
| **Duplicate Detection & Removal Logic** | C-02 deduplication. To guarantee deterministic results across reruns, the dataset is sorted by chronological timestamp (`updated_at` for sales, `logged_at` for waste, `created_at` for inventory) and `id` prior to deduplication, deterministically preserving the earliest record. |
| **Whitespace & Invalid Characters** | Leading and trailing spaces trimmed (C-07); alphanumeric content preserved. |
| **Invalid or Out-of-Range Values** | Negative amounts (C-04), invalid quantities (C-05), and timestamps outside batch date (S-02) are rejected. |
| **Orphan Records** | Enforced at extraction via inner join and defended in Stage 2 via C-06 (NULL parent `order_id`). |

A record that is cleaned and kept (C-07, S-04) is **modified**, not rejected, and is not a subtraction term in the reconciliation equation.

---

## 4. Standardization and Data Type Rules

### 4.1 Data Types Specification

| Column | Source Type | Standardized Output Type | Rule ID |
|---|---|---|---|
| `id`, `order_id` | UUID / String | Lowercase text (UUID) | S-03 |
| `order_items.quantity` | INTEGER | Python `int` (Integer) | S-03 |
| `waste_logs.quantity` | NUMERIC | Python `float` (Fractional quantity) | S-03 |
| `total_price`, `cost` | NUMERIC | Python `Decimal` (2 decimal places, `ROUND_HALF_UP`) | S-03 |
| `updated_at`, `logged_at`, `created_at` | TIMESTAMPTZ (UTC) | ISO 8601 string in Asia/Manila (`+08:00`) | S-01 |
| `business_date` | Derived | Text `YYYY-MM-DD` | S-01 |
| `transaction_type` | TEXT | Uppercase text (`IN`, `OUT`) | S-04 |

### 4.2 Standardization Rules

| Rule ID | Target Column | Source Format | Standardized Format | Action on Failure |
|---|---|---|---|---|
| **S-01** | `updated_at`, `logged_at`, `created_at` | `2026-10-12 08:15:37+00:00` (UTC) | ISO 8601 in Asia/Manila (`2026-10-12T16:15:37+08:00`), plus `business_date` = `2026-10-12` | Reject and log to quarantine |
| **S-02** | `business_date` | Date derived by S-01 | Must equal target `batch_date` | Reject and log (outside batch window) |
| **S-03** | `total_price`, `cost` | Any scale, e.g., `50.255` | 2 decimal places, **round half up** (`50.26`) | Reject and log to quarantine |
| **S-03** | `order_items.quantity` | Integer or numeric text | Python integer (`int`) | Reject and log to quarantine |
| **S-04** | `inventory_logs.transaction_type` | `in`, `In`, `IN` | `IN` (uppercase) | Coerce to uppercase string |

### 4.3 Formats, Precision, and Codes

| Topic | Rule Specification |
|---|---|
| **Date and Time** | ISO 8601 format. UTC timestamps from PostgreSQL are converted to Philippine Standard Time (Asia/Manila, UTC+8). Business date is derived as `YYYY-MM-DD`. |
| **Numeric Precision & Rounding** | Currency amounts are standardized to 2 decimal places using Python `Decimal` with `ROUND_HALF_UP` (e.g., `50.255` rounds to `50.26`). Totals are computed using `Decimal` summation to prevent floating-point inaccuracies. |
| **Quantities** | `order_items.quantity` is strictly an integer. `waste_logs.quantity` is stored as a floating-point number because ingredient waste is measured by weight or volume (e.g., `1.5` kg of flour, `0.75` L of milk). |
| **Text Casing** | `transaction_type` is coerced to uppercase (`IN`, `OUT`). Names such as `item_name` preserve original casing from the source system and are only trimmed of leading/trailing spaces (C-07). |
| **Reference Codes & Mappings** | Code spelling is standardized (S-04). Codes are not merged or remapped; distinct values are audited in profiling reports. |
| **Currency, Units, Regions** | Strictly Philippine Peso (PHP, ₱); no currency conversion is performed. Measurement units (`unit`) are preserved as text strings. No regional or postal codes are used. |

### 4.4 Handling of Conversion Failures
A value that cannot be converted to target format (S-01, S-03) is **never** filled with default values. The record is rejected to quarantine with rule ID and reason (`Cannot convert to the target data type` or `Missing or invalid <timestamp>`), contributing to `records_rejected`.

---

## 5. Business Transformation Rules

### 5.1 Business Exclusions (Filtering Logic)
Extraction applies only a **technical** filter (the batch date window). Business exclusions are applied in Stage 2 so that excluded volume remains auditable:

| Rule ID | Name | Target | Condition | Action | Business Reason |
|---|---|---|---|---|---|
| **B-01** | Exclude cancelled orders | `order_items.status` | `status = 'Cancelled'` (case and spaces ignored) | Exclude from sales total; quarantine as `EXCLUDED` | A cancelled order produces no realized revenue. |
| **B-02** | Exclude voided waste logs | `waste_logs.voided_at` | `voided_at IS NOT NULL` | Exclude from waste cost; quarantine as `EXCLUDED` | A voided entry was invalidated in the operational system; counting it would duplicate or inflate costs. |
| **B-03** | Exclude voided inventory logs | `inventory_logs.voided_at` | `voided_at IS NOT NULL` | Exclude from inventory cost; quarantine as `EXCLUDED` | A voided restock was cancelled; counting it would overstate operating expenses. |

### 5.2 Derived Columns and Aggregations

| Rule ID | Input Columns | Logic / Formula | Output Column | Business Reason |
|---|---|---|---|---|
| **B-04** | `order_items.total_price` (kept rows) | `SUM(total_price)` | `fact_daily_summary.total_sales` | Total gross sales realized for the business day. |
| **B-05** | `waste_logs.cost` (kept rows) | `SUM(cost)` | `waste_cost` (measure in metadata) | Total direct cost of wasted raw ingredients and materials. |
| **B-06** | `inventory_logs.cost` (kept rows) | `SUM(cost)` across all transaction types | `inventory_cost` (measure in metadata) | Total cost of inventory restocks for the day (usage logs carry ₱0.00). |
| **B-07** | `waste_cost`, `inventory_cost` | `waste_cost + inventory_cost` | `fact_daily_summary.total_expenses` | Total operating expenses of the day. |
| **B-08** | `total_sales`, `total_expenses` | `total_sales - total_expenses` | `fact_daily_summary.profit` | Net daily operating profit or loss for executive dashboards. |
| **B-09** | `batch_date` | Extract Day, Month, Year, and Date Key | `dim_date.date_key`, `full_date`, `day`, `month`, `year` | Supports reporting by calendar day, month, and year. |
| **B-10** | `order_items.quantity` (kept rows) | `SUM(quantity)` | `total_items_sold` (measure in metadata) | Operational volume metric tracking total baked items sold. |

**Aggregation Grain:** Exactly **one row per business date**, containing total sales, total expenses, and profit.

### 5.3 Joins and Lookups

| Topic | Description |
|---|---|
| **Join Tables and Keys** | `order_items` is joined with `orders` on `order_items.order_id = orders.id`. |
| **Join Type** | Inner join executed during Stage 1 extraction. Stage 2 performs no additional SQL joins; it processes the handed-over joined dataset. |
| **Handling Unmatched Records** | An order item without a parent order cannot appear after the inner join. Defensive rule C-06 further protects against any NULL `order_id`. |
| **Row Multiplication** | Detected by C-02 and monitored by validation check V-10. |
| **Lookups** | None required; codes are standardized via programmatic rules (S-04). |

### 5.4 Dimensional Modeling and Changing Attributes
The transformed data conforms to a dimensional star schema:
- **Tables:** `dim_date` (Dimension) and `fact_daily_summary` (Fact).
- **Surrogate Keys:** `date_key` is generated in Stage 2 as an integer in `YYYYMMDD` format (e.g., `20261019`) to link fact to dimension. The surrogate key `summary_key` is generated by the analytics warehouse (`SERIAL`) upon load.
- **Handling of Changing Attributes (SCD):** Date dimension attributes (`day`, `month`, `year`) are mathematically derived and immutable, conforming to **SCD Type 0** (no historical change tracking needed).

---

## 6. Transformed Data Contract and Naming Convention

### 6.1 Required Output Schema

#### Dimension Table: `dim_date` (one row per business date)
| Column | Type at Handover | Nullable | Key / Constraint | Description |
|---|---|---|---|---|
| `date_key` | Integer | No | **Surrogate Key (PK)** | Smart date surrogate key (`YYYYMMDD`, e.g., `20261019`). |
| `full_date` | Text `YYYY-MM-DD` | No | **Natural Key (Unique)** | ISO calendar date string. Loaded as DATE. |
| `day` | Integer | No | Attribute | Calendar day of month (1–31). |
| `month` | Integer | No | Attribute | Calendar month (1–12). |
| `year` | Integer | No | Attribute | Calendar year (e.g., 2026). |

#### Fact Table: `fact_daily_summary` (one row per business date)
| Column | Type at Handover | Nullable | Key / Constraint | Description |
|---|---|---|---|---|
| `date_key` | Integer | No | **Primary Key at Handover / FK** | Unique grain identifier at handover; foreign key to `dim_date.date_key`. |
| `full_date` | Text `YYYY-MM-DD` | No | **Natural Foreign Key / Ref** | References `dim_date.full_date`. |
| `total_sales` | Decimal (2 dp) | No | Measure (≥ 0.00) | Net daily sales revenue. Loaded as NUMERIC(10,2). |
| `total_expenses` | Decimal (2 dp) | No | Measure (≥ 0.00) | Total daily operating expenses. Loaded as NUMERIC(10,2). |
| `profit` | Decimal (2 dp) | No | Derived Measure | Daily operating profit (`total_sales - total_expenses`). |

> **Note on `date_key` and `summary_key`:** In the in-memory handover schema, `date_key` acts as the primary key of `fact_daily_summary` because the table grain is exactly one record per calendar date. When loaded into the analytics warehouse in Stage 3, an auto-increment surrogate primary key `summary_key` (`SERIAL`) is generated by PostgreSQL, and `date_key` serves as the foreign key referencing `dim_date.date_key`. Because `summary_key` is created at load time, it is intentionally omitted from the in-memory handover DataFrame and does not trigger null validation errors.

### 6.2 Naming Convention

| Topic | Convention |
|---|---|
| **Format** | `snake_case` for all table and column names. |
| **Table Prefixes** | `dim_` for dimension tables, `fact_` for fact tables. The `stg_` prefix is not used because the handover occurs entirely in-memory. |
| **Keys** | Natural key uses `full_date`. Surrogate keys consistently use the `_key` suffix (`date_key`, `summary_key`). Foreign keys share the exact name of the referenced primary key (`date_key`). |
| **New Columns** | `business_date` (internal transformation date), `date_key` (smart integer surrogate). |

### 6.3 Source-to-Target Mapping

| Source Column | Target Column | Source Type | Target Type | Applied Rules |
|---|---|---|---|---|
| `order_items.total_price` | `fact_daily_summary.total_sales` | NUMERIC | Decimal (2 dp) | C-03, C-04, S-03, B-01, B-04 (sum) |
| `waste_logs.cost` | `fact_daily_summary.total_expenses` (part) | NUMERIC | Decimal (2 dp) | C-03, C-04, S-03, B-02, B-05 (sum), B-07 |
| `inventory_logs.cost` | `fact_daily_summary.total_expenses` (part) | NUMERIC | Decimal (2 dp) | C-03, C-04, S-03, B-03, B-06 (sum), B-07 |
| *(Derived)* | `fact_daily_summary.profit` | — | Decimal (2 dp) | B-08 |
| `orders.updated_at`, `waste_logs.logged_at`, `inventory_logs.created_at` | `dim_date.date_key`, `fact_daily_summary.date_key` | TIMESTAMPTZ | Integer | S-01, B-09 (`YYYYMMDD`) |
| `orders.updated_at`, `waste_logs.logged_at`, `inventory_logs.created_at` | `dim_date.full_date`, `fact_daily_summary.full_date` | TIMESTAMPTZ | Text `YYYY-MM-DD` | S-01, S-02, B-09 |
| `order_items.quantity` | `total_items_sold` (metadata) | INTEGER | Integer | C-05, S-03, B-10 |

**Distinction from Extraction Contract:**  
The extraction contract describes raw record-level columns with source schema names (`order_items.quantity`, `order_items.total_price`, with `id` and `order_id` preserved for traceability). In contrast, the transformed data contract describes analytical structures delivered to the Load stage: aggregated business measures (`total_expenses`, `profit`), a conformed date dimension, and dimensional surrogate keys (`date_key`) that did not exist in the source system.

### 6.4 Schema Consistency Handling

| Situation | Pipeline Handling |
|---|---|
| **Required input column missing or renamed** | The transformation engine raises an error, the run is marked `FAILED` (exit code 1), **no output tables are produced**, and the error is stored in metadata. (Stage 1 expected-column checks normally intercept this earlier). |
| **Unexpected extra input column** | Ignored. Only columns specified in rule definitions are processed. |
| **New status or transaction type** | Permitted through the pipeline; appears in profiling value distributions for developer review. |

---

## 7. Transformation Validation and Data Quality Checks

Validation checks evaluate the integrity, correctness, completeness, and business-rule compliance of the **transformed output**. They do not repeat extraction-stage connectivity or query checks. Extraction counts are referenced solely for reconciliation.

**Severity:** *Critical* (a FAIL immediately halts the pipeline) or *Non-critical* (a WARNING flags the batch for manual review).

### 7.1 Validation Catalog

| Check ID | Check Name | Target | Purpose | Validation Criteria | Severity |
|---|---|---|---|---|---|
| **V-01** | Record reconciliation | All three input tables | Detect records that were lost or created by transformation | `records_read = records_output + records_rejected + records_excluded`, for every table | Critical |
| **V-02** | Extraction count reference | All three input tables | Confirm Stage 2 received exactly the records that Stage 1 handed over (reference value for reconciliation) | `records_read` equals the row count in the extraction handover metadata | Critical |
| **V-03** | Key uniqueness of retained records | `order_items.id`, `waste_logs.id`, `inventory_logs.id` | Prevent double counting in the totals | Retained records have no NULL `id` and duplicate count = 0 | Critical |
| **V-04** | Output key uniqueness | `dim_date`, `fact_daily_summary` | Prevent duplicate rows in the output tables | `date_key` and `full_date` are unique for each output table | Critical |
| **V-05** | Required columns not null | All columns of `dim_date` and `fact_daily_summary` | Ensure the Load stage never receives an incomplete row | NULL count = 0 in every output column | Critical |
| **V-06** | Referential integrity | `fact_daily_summary.date_key -> dim_date.date_key` | Prevent a fact row without its date | 100% of fact rows have a matching `dim_date` row by `date_key` and `full_date` | Critical |
| **V-07** | Data type and format conformance | Output tables | Ensure output matches warehouse column types | `full_date` is `YYYY-MM-DD`; `date_key` is integer; money columns have at most 2 decimal places; day, month, year are integers | Critical |
| **V-08** | Business rule check | `fact_daily_summary` | Catch impossible or inconsistent totals | `total_sales >= 0`, `total_expenses >= 0`, `profit = total_sales - total_expenses` | Critical |
| **V-09** | Aggregate reconciliation | Amounts of the three input tables and output totals | Prove that no money was lost or invented | Sum read = sum output + sum rejected + sum excluded, and output totals equal the sums of retained records | Critical |
| **V-10** | Join cardinality (row multiplication) | `order_items.id` after extraction join | Detect a join that multiplied rows | Number of duplicate IDs rejected by C-02 = 0 | Non-critical |
| **V-11** | Rejection rate within threshold | Each input table | Detect unusually poor data quality | `records_rejected / records_read <= 1%` | Non-critical |

### 7.2 Reconciliation Rules and Mathematical Equations

| Dimension | Rule & Mathematical Formulation |
|---|---|
| **Row-Level Conservation** | Every extracted record read accounts for exactly one outcome: retained in output, rejected for defects, or excluded by business rules. Formulation: `records_read = records_output + records_rejected + records_excluded` (equivalent to standard template formula: `records_read = records_output + records_quarantined`, where `records_quarantined = records_rejected + records_excluded`). |
| **Modified Records** | A record cleaned and retained (C-07, S-04) is modified, not rejected. The count of modified records is logged in `rules_applied` and is not a subtraction term in the reconciliation equation. |
| **Aggregate Financial Reconciliation** | Monetary values must balance: `Gross Amount Read = Net Output Amount + Rejected Amount + Excluded Amount`. |
| **Filtering Exclusions** | Rules B-01 to B-03 count excluded records and quarantine them with category `EXCLUDED` and reason; no exclusions are dropped silently. |

---

## 8. Transformation Metadata and Log Specification

Every transformation execution produces a structured JSON metadata file (`logs/transform_<run_id>.json`) and detailed event logs in `logs/etl_<date>.log`.

### 8.1 Metadata Fields Specification

| Field | Purpose | Illustrative Example |
|---|---|---|
| `run_id` | Unique ID of the transformation run | `TRN_20261023_100546` |
| `parent_run_id` | Extraction run this batch came from (referenced, not duplicated) | `EXT_20261023_100546` |
| `batch_id` | ID of the business day processed (deterministic on rerun) | `BATCH_20261019` |
| `batch_window` | Date range processed in Philippine Standard Time | `2026-10-19T00:00:00+08:00 to 2026-10-19T23:59:59.999999+08:00` |
| `table_name` | Source table transformed | `order_items` |
| `started_at` / `ended_at` | Start and end timestamp of table processing and overall run | `2026-10-23T10:05:46+08:00` |
| `duration_seconds` | Total processing runtime in seconds | `0.062` |
| `status` | Overall execution status (`SUCCESS` or `FAILED`) | `SUCCESS` |
| `error_message` | Error text if the run failed; null otherwise | `null` |
| `records_read` | Rows received from extraction handover | `2` |
| `records_output` | Rows retained (contributing to daily totals) | `0` |
| `records_rejected` | Rows rejected to quarantine due to data-quality defects | `0` |
| `records_excluded` | Rows excluded due to business rules | `2` |
| `rules_applied` | Records affected by each rule (modified or removed) | `{"B-01": 2, "S-01": 2}` |
| `validation_results` | Pass/Fail/Warning status of checks V-01 to V-11 | `[{"check_id": "V-01", "status": "PASS"}, ...]` |
| `decision` | Automated pipeline verdict | `APPROVED` |
| `code_version` | Version of the transformation rules registry | `2.0.0` |
| `measures` | Daily totals produced (sales, expenses, profit, item counts) | `{"total_sales": "0.00", "total_expenses": "2882.00", "profit": "-2882.00"}` |

### 8.2 Operational Use of Metadata and Logs

| Use Case | Implementation Mechanism |
|---|---|
| **Monitoring** | `decision`, `status`, and check results V-01 to V-11 show batch health and acceptance at a glance. |
| **Troubleshooting** | `error_message`, log lines, and the quarantine file (`record_id`, `rule_id`, `reason`) isolate the exact record and rule causing pipeline failures. |
| **Auditing** | `code_version`, `rules_applied`, and table reconciliation counts provide a complete audit trail of data modifications. |
| **Data Lineage** | `parent_run_id` and `batch_id` establish an unbroken link between transformed summaries and raw extraction batches. |
| **Recovery** | `batch_id` and the batch window identify target dates for idempotent reprocessing without data loss. |

---

## 9. Log Retention, Rejected Data, and Access

Extraction and transformation records are maintained **separately**, establishing distinct ownership and retention schedules.

### 9.1 Separation of Records and Ownership

| Record Category | Content Description | Stage Owner |
|---|---|---|
| **Extraction Logs** | Retrieval errors, query execution times, row counts, source-to-staging checks (see Stage 1 doc) | ETL Developer |
| **Transformation Logs** | Data cleaning actions, format conversions, rule applications, check results V-01 to V-11 | ETL Developer |
| **Quarantine Files** | Rejected and excluded records with record ID, batch ID, rule ID, category, reason, amount | ETL Developer |

> **Operational Logs vs. Audit Records:** Operational logs (retained for 90 days) track daily runtime execution details and debugging traces. Audit records (`transform_<run_id>.json`, retained for 1 year) preserve immutable, long-term compliance evidence of approved financial summaries and active rule versions.

### 9.2 Storage Location and Access Permissions

| Storage Category | Physical Directory | Access Permissions | Security & Git Status |
|---|---|---|---|
| **Operational Logs** | Project root `logs/` directory (`logs/etl_<date>.log`) | **Read/Write:** ETL Developer only; **Delete:** ETL Developer only; **End-users:** No access | Excluded from git repository via `.gitignore`. Not exposed to web endpoints. |
| **Audit Metadata** | Project root `logs/` directory (`logs/transform_<run_id>.json`) | **Read/Write:** ETL Developer only; **Delete:** ETL Developer only; **End-users:** No access | Excluded from git repository via `.gitignore`. Stored as JSON audit evidence. |
| **Quarantine Files** | Project root `quarantine/` directory (`quarantine/quarantine_<batch_id>.csv`) | **Read/Write:** ETL Developer only; **Delete:** ETL Developer only; **End-users:** No access | Excluded from git repository via `.gitignore`. Contains sanitized defect records. |
| **Profiling Reports** | Project root `reports/` directory (`reports/profile_<batch_id>.md`) | **Read/Write:** ETL Developer only; **Delete:** ETL Developer only; **End-users:** No access | Excluded from git repository via `.gitignore`. Internal documentation asset. |

> **Security & Privacy:** Quarantine files store only technical and financial attributes (`record_id`, `rule_id`, `reason`, `amount`). Customer personally identifiable information (PII) is never written to logs or quarantine files.

### 9.3 Retention Periods and Justifications

| Record Type | Retention Schedule | Technical Justification | Implementation Status |
|---|---|---|---|
| **Operational Logs** (`etl_<date>.log`) | Retain **90 Days** | Covers a full quarterly reporting and debugging cycle; old logs can be cleared after quarterly sign-off. | Retention is managed manually; automatic log rotation is not implemented in Phase 2. |
| **Audit Run Records** (`transform_<run_id>.json`) | Retain **1 Year** | Provides permanent evidence of approved financial summaries and rule versions for annual financial audits. | Stored on disk; manual pruning after 1 year. |
| **Quarantine Files** (`quarantine_<batch_id>.csv`) | Retain **180 Days** | Allows store management sufficient operational runway (6 months) to investigate, correct, and reconcile rejected transactions with operational staff. | Overwritten by `batch_id` upon rerun; manual cleanup after 180 days. |
| **Profiling Reports** (`profile_<batch_id>.md`) | Retain **90 Days** | Tracks data quality trends and defect frequencies across developmental and operational sprints. | Stored per batch; manual deletion. |

### 9.4 Archiving and Deletion Governance
- **Deletion Restrictions:** Operational logs, audit records, and quarantine files **must not be deleted** prior to the expiration of their defined retention periods, or while an active data investigation or recovery operation is open.
- **Archiving Requirements:** When operational logs exceed 90 days but remain relevant to an active investigation, they are manually archived to offline cold storage. Automatic archiving is not implemented.
- **Responsible Person:** File deletion is performed exclusively by the **ETL Developer** following quarterly operational reviews.

---

## 10. Transformation Acceptance and Handover Rules

### 10.1 Rejection-Rate Threshold and Justification
The automated acceptance gate enforces a **1% rejection threshold** (`REJECTION_THRESHOLD = 0.01`):

`Rejection Rate = records_rejected / records_read <= 1.0%`

> **Justification:** As an artisanal bake shop, daily transaction volume is small (typically 2 to 50 records per table depending on the operational day). In this operational scale, even a single invalid transaction represents 2.0% to 50.0% of an individual table's batch. Therefore, a strict 1% threshold was intentionally configured in advance: any data defect (≥ 1 record) automatically escalates the batch to **MANUAL REVIEW**, preventing corrupted figures from being loaded into business intelligence dashboards without developer oversight.

### 10.2 Decision Gate Matrix

| Validation Condition | Acceptance Decision | Pipeline Action | Decision Type | Responsible Role |
|---|---|---|---|---|
| All critical checks (V-01 to V-09) PASS and non-critical checks (V-10, V-11) PASS | **APPROVED** | Hand over to Load stage automatically | Automatic | ETL Pipeline |
| All critical checks PASS, but one or more non-critical checks WARN (e.g., rejection rate > 1%) | **MANUAL REVIEW** | Hold batch; exit code 2; log warnings to console | Manual | ETL Developer |
| One or more critical checks FAIL, or runtime error occurs | **REJECTED** | Halt pipeline execution immediately with exit code 1; log error | Automatic | ETL Developer |

**Operating Procedures for Decisions:**
- **APPROVED:** The in-memory output DataFrames (`dim_date`, `fact_daily_summary`) proceed automatically to the Load stage for warehouse loading.
- **MANUAL REVIEW:** In Phase 2 development, `run_transform.py` returns exit code `2`, holding automatic progression. The developer inspects `reports/profile_<batch_id>.md` and `quarantine/quarantine_<batch_id>.csv`. Because the handover is currently in-memory, manual authorization is procedural during Phase 2 testing. In Phase 3 (Prefect orchestration), this gate is wired into a formal Prefect pause/approval state where the developer can confirm handover or reject the run. If defects indicate legitimate corruption, the developer corrects the issue in the source database and reruns the batch.
- **REJECTED:** The run halts with exit code `1`. The developer inspects `error_message` in the metadata JSON and console logs, addresses the root cause, and re-executes the batch date. Because Stage 2 is idempotent, the rerun cleanly overwrites earlier quarantine files without producing duplicate records.

### 10.3 Handover to the Load Stage
When a batch is `APPROVED` (or authorized following review), the following artifacts are handed over:

| Handover Component | Content Description |
|---|---|
| **Output DataFrames** | In-memory `dim_date` and `fact_daily_summary` DataFrames conforming to Section 6. |
| **Lineage Metadata** | `run_id`, `parent_run_id`, `batch_id`, `batch_window`, row counts, and `code_version`. |
| **Validation Results** | Complete list of check results (V-01 to V-11) and the `APPROVED` decision. |
| **Quarantine Manifest** | File path and count of quarantined records (`quarantine/quarantine_<batch_id>.csv`). |

### 10.4 Execution Evidence

#### Production Execution Evidence (`BATCH_20261019`)
Screen output from `python -m pipeline.run_transform 2026-10-19` (Run `TRN_20261023_100546`):

```
TRANSFORM TEST for 2026-10-19  (BATCH_20261019)
Extraction run: EXT_20261023_100546   Transformation run: TRN_20261023_100546

table             read  output  rejected  excluded
order_items          2       0         0         2
waste_logs           0       0         0         0
inventory_logs      19      16         0         3

Totals: total_sales=0.00  total_expenses=2882.00  profit=-2882.00  (items sold: 0)

Validation checks
  V-01  PASS     Record reconciliation (Critical)
  V-02  PASS     Extraction count reference (Critical)
  V-03  PASS     Key uniqueness of retained records (Critical)
  V-04  PASS     Output key uniqueness (Critical)
  V-05  PASS     Required columns not null (Critical)
  V-06  PASS     Referential integrity (Critical)
  V-07  PASS     Data type and format conformance (Critical)
  V-08  PASS     Business rule check (Critical)
  V-09  PASS     Aggregate reconciliation (Critical)
  V-10  PASS     Join cardinality (row multiplication) (Non-critical)
  V-11  PASS     Rejection rate within threshold (Non-critical)

Quarantine : quarantine/quarantine_BATCH_20261019.csv  (5 record(s))
Profile    : reports/profile_BATCH_20261019.md
Metadata   : logs/transform_TRN_20261023_100546.json

DECISION: APPROVED
```

**Quarantine Manifest (`quarantine/quarantine_BATCH_20261019.csv`):**

| batch_id | table_name | record_id | rule_id | category | reason | amount |
|---|---|---|---|---|---|---|
| BATCH_20261019 | order_items | `bfbbdf67-7ff7-4f2d-bd54-26a327b822a9` | B-01 | EXCLUDED | Cancelled order is not revenue | 160.00 |
| BATCH_20261019 | order_items | `8118c042-80a2-43c6-b2d6-8eecfc7a2317` | B-01 | EXCLUDED | Cancelled order is not revenue | 520.00 |
| BATCH_20261019 | inventory_logs | `f9c27276-55b4-4b94-98ba-43b5d003904a` | B-03 | EXCLUDED | Voided inventory log is not an expense | 1.00 |
| BATCH_20261019 | inventory_logs | `cf444eae-aeec-46ab-a906-e5809b83cec3` | B-03 | EXCLUDED | Voided inventory log is not an expense | 123.00 |
| BATCH_20261019 | inventory_logs | `f3a8cd6d-1b1d-433b-a926-f88f62980171` | B-03 | EXCLUDED | Voided inventory log is not an expense | 1.00 |

#### Defect Test Execution Evidence (`BATCH_20261020_DEFECT_TEST`)
Execution output from `python -m pipeline.run_defect_test` verifying data cleaning rejection rules (C-01 to C-06, S-02, S-03):

```
TRANSFORM TEST for 2026-10-20  (BATCH_20261020_DEFECT_TEST)
Extraction run: EXT_20261020_DEFECT_TEST   Transformation run: TRN_20261020_DEFECT_TEST

table             read  output  rejected  excluded
order_items          8       1         6         1
waste_logs           3       2         0         1
inventory_logs       5       2         2         1

Totals: total_sales=200.00  total_expenses=1015.00  profit=-815.00  (items sold: 2)

Validation checks
  V-01  PASS     Record reconciliation (Critical)
  V-02  PASS     Extraction count reference (Critical)
  V-03  PASS     Key uniqueness of retained records (Critical)
  V-04  PASS     Output key uniqueness (Critical)
  V-05  PASS     Required columns not null (Critical)
  V-06  PASS     Referential integrity (Critical)
  V-07  PASS     Data type and format conformance (Critical)
  V-08  PASS     Business rule check (Critical)
  V-09  PASS     Aggregate reconciliation (Critical)
  V-10  WARNING  Join cardinality (row multiplication) (Non-critical): 1 duplicate order_items id(s) found after the join
  V-11  WARNING  Rejection rate within threshold (Non-critical): rejection rate -> order_items: 75.00%; waste_logs: 0.00%; inventory_logs: 40.00%

Quarantine : quarantine/quarantine_BATCH_20261020_DEFECT_TEST.csv  (11 record(s))
Profile    : reports/profile_BATCH_20261020_DEFECT_TEST.md
Metadata   : logs/transform_TRN_20261020_DEFECT_TEST.json

DECISION: MANUAL REVIEW
```

**Quarantine Manifest for Defect Batch:**

| batch_id | table_name | record_id | rule_id | category | reason | amount |
|---|---|---|---|---|---|---|
| BATCH_20261020_DEFECT_TEST | order_items | | C-01 | REJECTED | Missing primary key | 100.00 |
| BATCH_20261020_DEFECT_TEST | order_items | `11111111-1111-1111-1111-111111111111` | C-02 | REJECTED | Duplicate id (first occurrence kept) | 200.00 |
| BATCH_20261020_DEFECT_TEST | order_items | `66666666-6666-6666-6666-666666666666` | B-01 | EXCLUDED | Cancelled order is not revenue | 180.00 |
| BATCH_20261020_DEFECT_TEST | order_items | `22222222-2222-2222-2222-222222222222` | C-03 | REJECTED | Missing total_price | |
| BATCH_20261020_DEFECT_TEST | order_items | `33333333-3333-3333-3333-333333333333` | C-04 | REJECTED | Negative total_price | -50.00 |
| BATCH_20261020_DEFECT_TEST | order_items | `44444444-4444-4444-4444-444444444444` | C-05 | REJECTED | Invalid quantity | 150.00 |
| BATCH_20261020_DEFECT_TEST | order_items | `55555555-5555-5555-5555-555555555555` | C-06 | REJECTED | Orphan record: no parent order | 120.00 |
| BATCH_20261020_DEFECT_TEST | waste_logs | `w3` | B-02 | EXCLUDED | Voided waste log is not an expense | 85.00 |
| BATCH_20261020_DEFECT_TEST | inventory_logs | `i4` | B-03 | EXCLUDED | Voided inventory log is not an expense | 200.00 |
| BATCH_20261020_DEFECT_TEST | inventory_logs | `i3` | S-02 | REJECTED | Outside the batch window | 120.00 |
| BATCH_20261020_DEFECT_TEST | inventory_logs | `i5` | S-03 | REJECTED | Cannot convert to the target data type | INVALID_NUM |

---

## 11. Common Transformation Problems and Mitigation

| # | Problem Scenario | Root Cause | Impact | Engineering Mitigation |
|---|---|---|---|---|
| 1 | **Duplicate totals after a rerun** | The same batch date is reprocessed without idempotent safeguards | Double-counted sales and expenses in reporting | Transformation is a pure function of extracted batch; quarantine CSV is overwritten by `batch_id`; warehouse enforces uniqueness on `date_key`. |
| 2 | **Order lands on wrong business day** | Server UTC timestamps evaluated without local offset | Sales between 12:00 AM and 8:00 AM PHT counted on previous day | Extraction window localizes boundaries to Asia/Manila (+08:00); Rule S-01 converts timestamps; S-02 rejects out-of-window timestamps. |
| 3 | **Late-arriving updates** | An order is updated after its original batch was processed | Historical reporting missing updated order total | Orders are attributed to date of last update (`updated_at`). Affected historical days can be reprocessed idempotently. |
| 4 | **Cancelled or voided records counted** | Operational exclusions included in summation | Overstated sales or operating expenses | Rules B-01 to B-03 explicitly exclude them, logging count, reason, and amount to quarantine. Profiling distribution audits spelling variants. |
| 5 | **Inventory purchases and usage both added to cost** | Every `transaction_type` in `inventory_logs` summed into cost | Possible double-counting of material expenses | Handled intentionally by assumption: usage logs (`OUT`) carry ₱0.00 cost in the operational system; all transaction types are summed to preserve aggregate mathematical reconciliation (V-09). |
| 6 | **Unexpected source schema change** | A source column is renamed or dropped in production | Pipeline fails or silently corrupts data | Stage 1 halts on expected column checks; Stage 2 fails fast with `status = FAILED`, producing no output tables, and logs `error_message`. |
| 7 | **Row multiplication after extraction join** | The join key between `orders` and `order_items` produces duplicate rows | Inflated sales revenue | Rule C-02 detects duplicate primary keys; earliest row is kept deterministically; Check V-10 issues a non-critical warning. |
| 8 | **Poor data quality from manual data entry** | Staff enter negative or missing amounts at POS register | Understated revenue or inaccurate net margins | Rules C-03 and C-04 reject defective rows to quarantine; Check V-11 warns if rejection rate exceeds 1%. |

---

## Expected Output

The transformation delivers in-memory DataFrames for `dim_date` and `fact_daily_summary` for one business day, validated by checks V-01 to V-11, accompanied by the quarantine file (`quarantine/quarantine_<batch_id>.csv`), profiling report (`reports/profile_<batch_id>.md`), and run metadata (`logs/transform_<run_id>.json`). Direct database insertion into the analytics warehouse is executed in the subsequent **Stage 3: Load**.

## Implementation Reference

| Documentation Section | Code Reference |
|---|---|
| Section 1 (Input, Batch, Idempotency) | `utils/batch.py`, `pipeline/run_transform.py`, `transform/transform_batch.py`, `transform/quarantine.py` |
| Section 2 (Profiling) | `transform/profile_data.py` |
| Sections 3 to 5 (Rules Engine & Logic) | `transform/rules.py` (Rule Registry), `transform/engine.py` (Engine), `transform/compute_profit.py` (B-04 to B-09) |
| Section 6 (Data Contract) | `transform/compute_profit.py` |
| Section 7 (Validation Suite) | `transform/validate.py` |
| Section 8 (Metadata Logging) | `transform/reporting.py` |
| Section 10 (Acceptance Gates) | `transform/validate.py` (`decide`), `pipeline/run_transform.py` (exit codes), `pipeline/run_defect_test.py` |
