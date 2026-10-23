# Stage 1: Data Extraction Documentation

This document extends the pipeline design from Lab 2 (*Technical Metadata Documentation*) by specifying, in full technical detail, how the Extraction stage of the CakeLytics ETL pipeline is implemented, validated, logged, and handed over to the Transformation stage.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Field | Detail |
| :--- | :--- |
| **Name** | CakeLytics Operational System |
| **Purpose** | A web-based Decision Support System built for Aileen and Niculus Bake Shop (Calaca City, Batangas). It manages online ordering, walk-in Point-of-Sale (POS) transactions, stock inventory (raw ingredients, celebration materials, recipes, produced products), and waste logging. |
| **Role in the Pipeline** | This system is the source — the ETL pipeline only ever reads from it, and never writes back to it. |

### 1.2 Source Database

| Field | Detail |
| :--- | :--- |
| **Database Management System** | PostgreSQL, hosted and managed through Supabase. |

### 1.3 Extraction Method

The pipeline uses **incremental extraction**. Each run retrieves data for one target calendar day instead of extracting the entire operational database.

#### Incremental Field and Logic (per table)

| Table | Incremental Field | Logic |
| :--- | :--- | :--- |
| `orders` / `order_items` | `orders.updated_at` | Row is included if `updated_at` falls within the target day's window |
| `waste_logs` | `logged_at` | Row is included if `logged_at` falls within the target day's window |
| `inventory_logs` | `created_at` | Row is included if `created_at` falls within the target day's window |

The extraction functions connect to the source database through the shared SQLAlchemy engine and execute parameterized SQL `SELECT` queries. The retrieved data is loaded into Pandas DataFrames for use by the Transformation stage.

| Field | Detail |
| :--- | :--- |
| **Extraction Tool** | Python extraction functions using SQLAlchemy and Pandas (`extract/extract_sales.py` and `extract/extract_expenses.py`), tested with the extraction test script `pipeline/run_extract.py`. Orchestration with Prefect is part of a later phase. |
| **Extraction Schedule** | In Phase 1, the extraction is run manually. The full pipeline is designed for daily execution at 12:00 AM, but no scheduler is part of the extraction code and the automatic schedule is not currently enabled. |
| **Extraction Window** | One full calendar day, from 12:00:00 AM to 11:59:59 PM of the target date. |
| **Default Target Date** | The previous day, based on the computer's local date and time, when `run_extract.py` is run without a date. |
| **Historical Reprocessing** | A specific past date can be provided as a command-line argument in `YYYY-MM-DD` format, for example: `python -m pipeline.run_extract 2026-10-05` |

### 1.4 Extraction Scope

The Extraction stage retrieves sales and expense-related data from the CakeLytics operational database. Only records within the target date are included, subject to the conditions listed below.

| Table | Data Extracted |
| :--- | :--- |
| `orders` and `order_items` | Order line items, including quantity and total price, for each order |
| `waste_logs` | Cost of spoiled, unsold, or damaged items |
| `inventory_logs` | Cost of stock movements involving ingredients and celebration materials |

#### Date Range
* Each run covers one full calendar day, from 12:00:00 AM to 11:59:59 PM of the target date.
* By default, the target date is the previous day because the pipeline is designed to run at 12:00 AM after the business day has ended.
* A specific past date may also be provided as a command-line argument of `run_extract.py` (`YYYY-MM-DD`) to extract the data for that day.

The extraction stage only retrieves the required source data. Data cleaning, standardization, and business calculations (such as total sales, total expenses, and profit) are handled in the Transformation stage.

### 1.5 Source Limitations and Assumptions

* **Schema Dependency:** The extraction queries depend on specific table and column names in the CakeLytics operational database, such as `total_price`, `cost`, `logged_at`, and `created_at`. Changes to the source database schema may cause extraction queries to fail and must be communicated to the ETL maintainer.
* **Database Connection Limits:** The Supabase database has connection limits. The pipeline uses a shared SQLAlchemy engine to reduce unnecessary database connections during extraction.
* **Limited Historical Data:** The pipeline can only extract records that exist in the CakeLytics operational database. It cannot produce historical records for dates before the system began recording data.
* **Order Date Assumption:** The pipeline uses `orders.updated_at` to determine the extraction date for sales-related records. It assumes that this field corresponds to the relevant order update for sales reporting. If an order is updated for another reason, its records may be included in a different day's extraction.

---

## 2. Source Tables and Column Specification

### 2.1 `orders` Table

| Field | Detail |
| :--- | :--- |
| **Purpose** | Provides order information used together with the `order_items` table to retrieve sales-related records for the target date. |
| **Important Columns** | `id` (UUID), `updated_at` (TIMESTAMPTZ), `status` (ENUM `order_status`) |
| **Primary Key** | The `id` column serves as the primary key of the `orders` table and identifies each order. |
| **Relationships** | Joined with `order_items` using `orders.id = order_items.order_id`. This allows the query to retrieve the quantity and total price associated with each order. |
| **Reason for Inclusion** | Provides the date and status information needed by the sales extraction query. Only order items associated with orders within the specified date range and with a status other than `Cancelled` are retrieved. The `orders.id` column is used for the join but is not included in the extracted result. |

### 2.2 `order_items` Table

| Field | Detail |
| :--- | :--- |
| **Purpose** | Contains the individual items included in customer orders. It provides the quantity and total price needed for sales reporting. |
| **Important Columns** | `id` (UUID, PK), `order_id` (UUID, FK), `quantity` (INTEGER), `total_price` (NUMERIC) |
| **Primary Key** | The `id` column (UUID) is the primary key. The extraction query selects it so that primary key presence and uniqueness can be checked. |
| **Relationships** | `order_items.order_id` is a foreign key to `orders.id`. The sales query joins the two tables through `order_items.order_id = orders.id`. |
| **Reason for Inclusion** | Provides the quantity and total price used by the Transformation stage to calculate sales-related totals. The SQL query retrieves these values together with the corresponding order's date and status information. |

### 2.3 `waste_logs` Table

| Field | Detail |
| :--- | :--- |
| **Purpose** | Provides records of wasted items and their associated costs. These records are used by the Transformation stage to calculate the total waste cost for the target date. |
| **Important Columns** | `id` (UUID, PK), `waste_type` (ENUM), `item_name` (TEXT), `quantity` (NUMERIC), `unit` (TEXT), `cost` (NUMERIC), `logged_at` (TIMESTAMPTZ), `voided_at` (TIMESTAMPTZ, nullable) |
| **Primary Key** | The `id` column (UUID) is the primary key. The extraction query selects it so that primary key presence and uniqueness can be checked. |
| **Relationships** | Reads directly from `waste_logs` without joining another table. Has optional foreign keys to `raw_ingredients` (`raw_ingredient_id`) and `celebration_materials` (`material_id`), which extraction does not use. |
| **Reason for Inclusion** | Provides cost data needed for waste expenses. The SQL query retrieves records whose `logged_at` values fall within the specified date range and whose `voided_at` values are `NULL`. Extracted records are passed unchanged to Transformation. |

### 2.4 `inventory_logs` Table

| Field | Detail |
| :--- | :--- |
| **Purpose** | Provides records of inventory transactions and their associated costs. These records are used by the Transformation stage to calculate inventory-related expenses for the target date. |
| **Important Columns** | `id` (UUID, PK), `item_type` (ENUM), `item_name` (TEXT), `transaction_type` (TEXT), `cost` (NUMERIC), `created_at` (TIMESTAMPTZ), `voided_at` (TIMESTAMPTZ, nullable) |
| **Primary Key** | The `id` column (UUID) is the primary key. The extraction query selects it so that primary key presence and uniqueness can be checked. |
| **Relationships** | Reads directly from `inventory_logs` without joining another table. Has optional foreign keys to `raw_ingredients` (`raw_ingredient_id`) and `celebration_materials` (`material_id`), which extraction does not use. |
| **Reason for Inclusion** | Provides cost data needed for inventory-related expenses. Retrieves records whose `created_at` values fall within the target date range and whose `voided_at` values are `NULL`. Every transaction type is extracted without filtering. |

---

## 3. Extraction Validation and Data Quality Checks

The extraction stage defines eight validation checks. Each check has a name, target, purpose, validation criteria, and Phase 1 implementation status.

| # | Check Name | Target | Purpose | Validation Criteria & Phase 1 Implementation Status |
| :-: | :--- | :--- | :--- | :--- |
| **1** | Source connectivity | Source database connection | Confirm the source database is reachable before any query runs | **Criteria:** The `check_source_env()` function attempts to open a source database connection and closes it after the check. A connection error interrupts the pipeline.<br>**Phase 1 Status:** *Implemented.* `check_source_env()` performs this check, and `run_extract.py` calls it before any query. |
| **2** | Required table/column existence | `orders`, `order_items`, `waste_logs`, `inventory_logs` | Detect schema drift before relying on query results | **Criteria:** All columns referenced in the extraction queries exist with expected names.<br>**Phase 1 Status:** *Partially implemented.* A missing table/column makes the SQL query raise an error and stops the run. `run_extract.py` reports FAIL if an extracted DataFrame lacks an expected column. No separate pre-query schema check exists. |
| **3** | Primary key presence | `order_items.id`, `waste_logs.id`, `inventory_logs.id` | Ensure every extracted row has a usable identifier | **Criteria:** No NULLs in the identifier column of any extracted row.<br>**Phase 1 Status:** *Implemented.* `run_extract.py` checks that the `id` column of every extracted DataFrame has no NULL values and reports FAIL otherwise. |
| **4** | Primary key uniqueness | `order_items.id`, `waste_logs.id`, `inventory_logs.id` | Detect duplicate rows caused by a faulty join or query | **Criteria:** Count of distinct `id` values equals total row count.<br>**Phase 1 Status:** *Implemented.* `run_extract.py` checks that `id` values are unique and reports FAIL if duplicates are found. |
| **5** | Required field completeness | `total_price` (`order_items`), `cost` (`waste_logs`, `inventory_logs`) | Prevent silently undercounting sales/expenses due to missing values | **Criteria:** No NULLs in `total_price` or `cost` for extracted rows; NULLs found are flagged, not silently dropped.<br>**Phase 1 Status:** *Not implemented.* NULL values are extracted as-is and are not flagged during extraction. |
| **6** | Record count logging | Each table's extraction result | Catch unexpectedly empty or abnormally large extractions | **Criteria:** Record count $\ge 0$ and below a configurable upper bound.<br>**Phase 1 Status:** *Partially implemented.* Each extraction function logs its row count, and `run_extract.py` reports it and notes a count of 0. No upper bound is configured. |
| **7** | Referential integrity | `order_items.order_id` $\rightarrow$ `orders.id` | Ensure no orphaned line items are summed into sales | **Criteria:** Every extracted `order_items.order_id` has a matching row in the extracted `orders` set.<br>**Phase 1 Status:** *Enforced by query design.* The sales query uses an inner JOIN, so only `order_items` with a matching order are extracted. No separate check exists. |
| **8** | Extraction completeness | Returned sales and expense records | Detect partial extraction from a dropped connection mid-query | **Criteria:** Rows extracted match rows read from the source for the same query.<br>**Phase 1 Status:** *Not implemented.* A connection failure raises an error and stops the run, but extracted rows are not compared with a source row count. |

> **Note:** Data cleaning, including duplicate-row removal and handling missing `total_price` or `cost` values, belongs to the Transformation stage and is not part of the Extraction stage.

### Extraction Test Script (`pipeline/run_extract.py`)
Runs only the extraction stage for one day. For each extractor (`sales`, `waste_logs`, and `inventory_logs`), it checks that:
1. Expected columns are present.
2. The primary key column (`id`) has no NULL values and no duplicates.
3. It writes the PASS or FAIL result of each check and the row count to the log, printing first rows on screen.
4. A day with 0 rows still passes (a day without activity is valid).
5. The script exits with code `0` when every extractor passes and with code `1` when a check fails.

---

## 4. Extraction Metadata and Log Specification

The Extraction stage records log messages when retrieving data from the source database. These include extraction activity, target date range, rows returned, timestamp, log level, and module name.

| Logged Information | Purpose | Illustrative Example | Logged in Phase 1? |
| :--- | :--- | :--- | :--- |
| **Table Name** | Identifies which source table a log line refers to. | `order_items` | *Partly* (appears inside message text) |
| **Timestamp** | Records when the extraction message was logged. | `2026-10-13 18:59:22` | **Yes** |
| **Run Start & End Timestamps** | Shows when a run began/ended and execution time. | `Start 2026-10-16 16:49:35, end 2026-10-16 16:49:39` | *Partly* (START and END lines carry timestamps; not separate fields) |
| **Log Level** | Indicates the type of log message. | `INFO` | **Yes** |
| **Module Name** | Identifies the function that generated the message. | `CakelyticsETL.extract.extract_sales` | **Yes** |
| **Extraction Activity** | Shows which source data is being retrieved. | `Extracting sales (order_items) from 2026-10-12 00:00:00 to 2026-10-12 23:59:59` | **Yes** |
| **Extraction Date Range** | Shows start and end of target date window. | `2026-10-12 00:00:00 to 2026-10-12 23:59:59` | **Yes** |
| **Status** | Shows run or table extraction status. | `SUCCESS` | *Partly* (run-level status only, not per table) |
| **Records Read** | Number of rows returned by source query. | `30` | *Partly* (only extracted count is logged) |
| **Records Extracted** | Records actual number of rows returned. | `Extracted 30 order_items rows` | **Yes** |
| **Validation Results** | PASS/FAIL result for each check in Section 3. | `[sales (order_items)] PASS: primary key (id) is unique` | **Yes** (FAIL logged at ERROR level) |
| **Error Message** | Text of error when run fails for troubleshooting. | `Missing SOURCE_DB_URL in .env` | **No** (stops run and shows in terminal only) |

### Use of Extraction Logs
* **Monitoring:** Shows when each extraction query starts and how many rows it returns.
* **Troubleshooting:** Helps identify which extraction function was running and which date range was used.
* **Record Tracking:** Provides a record of the number of rows returned by each extraction query.
* **Auditing:** Run ID, table name, and data window show which data was extracted, when, and with what result.
* **Recovery:** Data window and status of a failed run show which day must be extracted again.

---

## 5. Log Retention and Access

| Aspect | Policy | Current Implementation |
| :--- | :--- | :--- |
| **Storage Location** | Store operational log files in the project's `logs` directory. | **Implemented.** Logs are written to `logs/etl_YYYY-MM-DD.log` (one file per day). `*.log` files are excluded from the repository via `.gitignore`. |
| **Retention Period** | Retain operational logs for 90 days. Retain separately maintained audit records for one year (if introduced). | No automatic retention period is configured. |
| **Access Permissions** | Limit access to log files to authorized project developers and maintainers. | Separate role-based access permissions are not configured by logging code. |
| **Archiving** | Archive operational logs older than 90 days when needed for auditing or troubleshooting. | Automatic archiving is not implemented. |
| **Deletion Rules** | Delete logs only after retention period and confirming they are no longer needed. | Automatic log deletion is not implemented. |

### Retention and Access Considerations
* **Troubleshooting:** Recent logs should remain available to investigate failed extraction runs and database connection problems.
* **Auditing:** Logs containing useful run details should be retained to trace target dates, extracted record counts, and processing results.
* **Recovery:** Logs should not be deleted while needed to identify or investigate a date requiring reprocessing.
* **Security:** Database credentials and sensitive configuration values must not be included in log messages. Access must be limited to authorized personnel.

---

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Schema (as extracted)

| Source Table | Column | Expected Type | Purpose | Required? |
| :--- | :--- | :--- | :--- | :---: |
| `orders` | `orders.id` | UUID | Joins an order to its order items. | **Yes** |
| `orders` | `orders.updated_at` | TIMESTAMPTZ | Determines whether an order falls within the target date range. | **Yes** |
| `orders` | `orders.status` | ENUM (`order_status`) | Excludes cancelled orders from sales extraction. | **Yes** |
| `order_items` | `order_items.id` | UUID | Primary key. Used for PK presence and uniqueness checks. | **Yes** |
| `order_items` | `order_items.order_id` | UUID | Links each order item to its order. | **Yes** |
| `order_items` | `order_items.quantity` | INTEGER | Provides quantity used in sales processing. | **Yes** |
| `order_items` | `order_items.total_price` | NUMERIC | Provides item total used in sales processing. | **Yes** |
| `waste_logs` | `waste_logs.id` | UUID | Primary key. Used for PK presence and uniqueness checks. | **Yes** |
| `waste_logs` | `waste_logs.cost` | NUMERIC | Provides cost used in expense processing. | **Yes** |
| `waste_logs` | `waste_logs.logged_at` | TIMESTAMPTZ | Determines whether a waste record falls within the target date range. | **Yes** |
| `waste_logs` | `waste_logs.voided_at` | TIMESTAMPTZ, nullable | Allows query to retrieve records that have not been voided. | **Yes** |
| `inventory_logs` | `inventory_logs.id` | UUID | Primary key. Used for PK presence and uniqueness checks. | **Yes** |
| `inventory_logs` | `inventory_logs.cost` | NUMERIC | Provides cost used in expense processing. | **Yes** |
| `inventory_logs` | `inventory_logs.created_at`| TIMESTAMPTZ | Determines whether an inventory record falls within target date range. | **Yes** |
| `inventory_logs` | `inventory_logs.voided_at` | TIMESTAMPTZ, nullable | Allows query to retrieve records that have not been voided. | **Yes** |

> **Note:** The columns listed above are required by the documented extraction logic; they do not mean every column is included in the returned DataFrame (e.g., `orders.id` and `order_items.order_id` are used for joining in SQL).

### 6.2 Naming Convention

* **Column Naming:** Source column names are retained as used in SQL queries. No separate renaming step occurs during extraction (`snake_case` retained: `updated_at`, `logged_at`, `total_price`).
* **Abbreviations:** Existing source column names are kept without introducing new abbreviations.
* **Identifiers:** `orders.id` and `order_items.order_id` match order records. Each query selects `id` as the row identifier.
* **Consistency:** Documentation mirrors source table and column names for clear traceability.

### 6.3 Schema Consistency Handling

If an expected column is renamed or removed in the source system, the SQL query raises a database error and the extraction run stops immediately. If the query runs but a returned DataFrame lacks an expected column, `run_extract.py` reports FAIL and exits with code `1`. No partial data is handed over, and mismatches must be resolved with documented mapping before re-running.

### 6.4 Source-to-Standardized Field Mapping

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
| :--- | :--- | :--- | :--- | :--- |
| `orders.updated_at` | `updated_at` | TIMESTAMPTZ | `datetime64` (timezone-aware, UTC) | Type mapping |
| `orders.status` | `status` | ENUM (`order_status`) | `string` | Type mapping |
| `order_items.id` | `id` | UUID (PK) | `object` (UUID) | Type mapping |
| `order_items.quantity` | `quantity` | INTEGER | `int64` | Type mapping |
| `order_items.total_price` | `total_price` | NUMERIC | `float64` | Type mapping |
| `waste_logs.id` | `id` | UUID (PK) | `object` (UUID) | Type mapping |
| `waste_logs.waste_type` | `waste_type` | ENUM | `string` | Type mapping |
| `waste_logs.item_name` | `item_name` | TEXT | `string` | Type mapping |
| `waste_logs.quantity` | `quantity` | NUMERIC | `float64` | Type mapping |
| `waste_logs.unit` | `unit` | TEXT | `string` | Type mapping |
| `waste_logs.cost` | `cost` | NUMERIC | `float64` | Type mapping |
| `waste_logs.logged_at` | `logged_at` | TIMESTAMPTZ | `datetime64` (timezone-aware, UTC) | Type mapping |
| `waste_logs.voided_at` | `voided_at` | TIMESTAMPTZ, nullable | `datetime64` (timezone-aware, UTC), NULL kept as null | Type mapping |
| `inventory_logs.id` | `id` | UUID (PK) | `object` (UUID) | Type mapping |
| `inventory_logs.item_type` | `item_type` | ENUM | `string` | Type mapping |
| `inventory_logs.item_name` | `item_name` | TEXT | `string` | Type mapping |
| `inventory_logs.transaction_type` | `transaction_type` | TEXT | `string` | Type mapping |
| `inventory_logs.cost` | `cost` | NUMERIC | `float64` | Type mapping |
| `inventory_logs.created_at` | `created_at` | TIMESTAMPTZ | `datetime64` (timezone-aware, UTC) | Type mapping |
| `inventory_logs.voided_at` | `voided_at` | TIMESTAMPTZ, nullable | `datetime64` (timezone-aware, UTC), NULL kept as null | Type mapping |

---

## 7. Extraction Acceptance and Handover Rules

### 7.1 Required Decisions

1. **Automatic Approval**
   * **Condition:** All Section 3 checks pass for every extracted table.
   * **Action:** Proceed automatically to Transformation.
   * **Decision Type:** Automatic.
   * **Owner:** ETL developer (monitors run).
   * **Phase 1 Status:** *Implemented.* `run_extract.py` exits with code `0` when every extractor passes.

2. **Manual Review**
   * **Condition:** Non-critical warning occurs (e.g., unusually high record count, small number of rows missing cost/price).
   * **Action:** Marked `MANUAL REVIEW`; hold pipeline for developer inspection.
   * **Decision Type:** Manual.
   * **Owner:** ETL developer.
   * **Phase 1 Status:** *Not implemented.* Code does not raise non-critical warnings.

3. **Rejection or Failure**
   * **Condition:** Source connectivity fails, required table/column missing, or PK duplicates found.
   * **Action:** Retries automatically (2 attempts with short delay). If failing, marked `REJECTED`; no data handed to Transformation.
   * **Decision Type:** Automatic.
   * **Owner:** ETL developer.
   * **Phase 1 Status:** *Partially implemented.* Stops with error or exits with code `1`. Retries come from Prefect in later phases.

4. **Handover**
   * **Data Passed:** Pandas DataFrames from `extract_sales()`, `extract_waste()`, and `extract_inventory_costs()` (`order_items_df`, `waste_df`, `inventory_df`) in memory.
   * In Phase 1, these are tested; Transformation consumes them in Phase 2. Logs are saved to `logs/` for failure traceability.

### 7.2 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action | Phase 1 Status |
| :--- | :--- | :--- | :--- |
| **All required tables PASS** | APPROVED | Proceed to Transformation | Implemented (exit code `0`) |
| **One or more non-critical WARNING** | MANUAL REVIEW | Hold pipeline, notify developer | Not implemented |
| **One or more critical FAIL** | REJECTED | Retry (2x) $\rightarrow$ quarantine/stop if failing | Run stops or exits with code `1` |

### 7.3 Example Validation Summary (`run_extract.py`, target date 2026-10-12)

| Validation Summary | Result |
| :--- | :--- |
| `order_items` validation | **PASS** (30 rows) |
| `waste_logs` validation | **PASS** (0 rows) |
| `inventory_logs` validation | **PASS** (27 rows) |
| **Overall Extraction** | **PASS** (all extractors passed) |

---

## 8. Common Extraction Problems and Mitigation

| # | Problem | Possible Cause | Impact | Mitigation & Phase 1 Status |
| :-: | :--- | :--- | :--- | :--- |
| **1** | Supabase connection pool exhaustion | New connection opened for every call instead of reusing across run | Extraction fails with "too many clients" error | **Mitigation:** Maintain single reusable pool with health checks.<br>**Phase 1 Status:** *Implemented.* `config/source_db.py` creates one shared engine (pool size 5, `pool_pre_ping`). |
| **2** | Schema change in source system | Source column renamed or removed without notification | Query fails or returns invalid/empty data | **Mitigation:** Run pre-extraction schema checks; update contract before release.<br>**Phase 1 Status:** *Partially implemented.* Query fails on missing column; `run_extract.py` checks result structure. |
| **3** | Network interruption mid-extraction | Unstable network between host machine and Supabase cloud | Partial extraction (some rows read before connection drops) | **Mitigation:** Task retries + completeness check.<br>**Phase 1 Status:** *Not in Phase 1.* Connection error stops run for manual retry. |
| **4** | Missing cost/price values from manual entry | Staff logs waste/inventory without filling cost field | Total expenses understated without detection | **Mitigation:** Completeness check flags NULL cost/price for review.<br>**Phase 1 Status:** *Not implemented.* NULLs passed through as-is. |

---

## Expected Output

The extracted datasets (`order_items`, `waste_logs`, and `inventory_logs`, scoped to the target date and verified by `run_extract.py`) are handed over directly to the Transformation stage. **No cleaning, standardization, or business computation (such as total sales, total expenses, or net profit) occurs during Extraction itself — those remain the sole responsibility of the Transformation stage.**
