# Stage 1: Data Extraction Documentation

**Implementation:** `extract/` (queries), `config/source_db.py` (connection), and `pipeline/run_extract.py` (test script)

This document extends the pipeline design from Lab 2 (Technical Metadata Documentation) by specifying, in full technical detail, how the Extraction stage of the CakeLytics ETL pipeline is implemented, validated, logged, and handed over to the Transformation stage. This version covers the Extraction stage only, as pushed in Phase 1 of the project repository; the Transformation and Load stages are documented separately.

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Field | Detail |
|---|---|
| **Name** | CakeLytics Operational System |
| **Purpose** | A web-based Decision Support System built for Aileen and Niculus Bake Shop (Calaca City, Batangas). It manages online ordering, walk-in Point-of-Sale (POS) transactions, stock inventory (raw ingredients, celebration materials, recipes, produced products), and waste logging. |
| **Role in the Pipeline** | This system is the source — the ETL pipeline only ever reads from it, and never writes back to it. |

### 1.2 Source Database

| Field | Detail |
|---|---|
| **Database Management System** | PostgreSQL, hosted and managed through Supabase. |

### 1.3 Extraction Method

The pipeline uses incremental extraction. Each run retrieves data for one target business day (Asia/Manila time) instead of extracting the entire operational database.

Incremental field and logic, per table:

| Table | Incremental Field | Logic |
|---|---|---|
| orders / order_items | orders.updated_at | Row is included if updated_at falls within the target day's window |
| waste_logs | logged_at | Row is included if logged_at falls within the target day's window |
| inventory_logs | created_at | Row is included if created_at falls within the target day's window |

The extraction functions connect to the source database through the shared SQLAlchemy engine and execute parameterized SQL SELECT queries. The retrieved data is loaded into Pandas DataFrames for use by the Transformation stage.

| Field | Detail |
|---|---|
| **Extraction Tool** | Python extraction functions using SQLAlchemy and Pandas (extract/extract_sales.py and extract/extract_expenses.py), tested with the extraction test script pipeline/run_extract.py. Orchestration with Prefect is part of a later phase. |
| **Extraction Schedule** | In Phase 1 the extraction is run manually. The full pipeline is designed for daily execution at 12:00 AM, but no scheduler is part of the extraction code and the automatic schedule is not currently enabled. |
| **Extraction Window** | One full business day in Asia/Manila time (UTC+8), from 12:00:00 AM to 11:59:59.999999 PM of the target date. The window is built with an explicit +08:00 offset because the source stores timestamps in UTC. The only filter applied in extraction is this window; business exclusions are applied in the Transformation stage. |
| **Default Target Date** | The previous business day in Asia/Manila time, when run_extract.py is run without a date. |
| **Historical Reprocessing** | A specific past date can be provided as a command-line argument in YYYY-MM-DD format, for example: python -m pipeline.run_extract 2026-10-05 |

### 1.4 Extraction Scope

The Extraction stage retrieves sales and expense-related data from the CakeLytics operational database. Only records within the target date are included, subject to the conditions listed below.

| Table | Data Extracted |
|---|---|
| orders and order_items | Order line items, including quantity and total price, for each order |
| waste_logs | Cost of spoiled, unsold, or damaged items |
| inventory_logs | Cost of stock movements involving ingredients and celebration materials |

#### Date Range

- Each run covers one full business day in Asia/Manila time (UTC+8), from 12:00:00 AM to 11:59:59 PM of the target date.
- By default, the target date is the previous business day because the pipeline is designed to run at 12:00 AM after the business day has ended.
- A specific past date may also be provided as a command-line argument of run_extract.py (YYYY-MM-DD) to extract the data for that day.

The extraction stage only retrieves the required source data. Data cleaning, standardization, and business calculations, such as total sales, total expenses, and profit, are handled in the Transformation stage.

### 1.5 Source Limitations and Assumptions

- **Schema Dependency:** The extraction queries depend on specific table and column names in the CakeLytics operational database, such as total_price, cost, logged_at, and created_at. Changes to the source database schema may cause extraction queries to fail and must be communicated to the ETL maintainer.
- **Database Connection Limits:** The Supabase database has connection limits. The pipeline uses a shared SQLAlchemy engine to reduce unnecessary database connections during extraction.
- **Limited Historical Data:** The pipeline can only extract records that exist in the CakeLytics operational database. It cannot produce historical records for dates before the system began recording data.
- **Order Date Assumption:** The pipeline uses orders.updated_at to determine the extraction date for sales-related records. It assumes that this field corresponds to the relevant order update for sales reporting. If an order is updated for another reason, its records may be included in a different day's extraction.
- **Time Zone of the Extraction Window:** The source timestamp columns are TIMESTAMPTZ and are returned in UTC (+00:00). The extraction window is built in Asia/Manila time (UTC+8) with an explicit +08:00 offset, so records created close to midnight in Manila time fall on the correct business day instead of the previous UTC day.

## 2. Source Tables and Column Specification

### 2.1 orders Table

| Field | Detail |
|---|---|
| **Purpose** | The orders table provides order information used together with the order_items table to retrieve sales-related records for the target date. |
| **Important Columns** | id (UUID), updated_at (TIMESTAMPTZ), status (ENUM order_status) |
| **Primary Key** | The id column serves as the primary key of the orders table and identifies each order. |
| **Relationships** | The orders table is joined with order_items using orders.id = order_items.order_id. This allows the query to retrieve the quantity and total price associated with each order. |
| **Reason for Inclusion** | The orders table provides the date and status information needed by the sales extraction query. Order items associated with orders within the specified date range are retrieved, including cancelled orders, which are excluded later in the Transformation stage. The orders.id column is used for the join but is not included in the extracted result. |

### 2.2 order_items Table

| Field | Detail |
|---|---|
| **Purpose** | The order_items table contains the individual items included in customer orders. It provides the quantity and total price needed for sales reporting. |
| **Important Columns** | id (UUID, PK), order_id (UUID, FK), quantity (INTEGER), total_price (NUMERIC) |
| **Primary Key** | The id column (UUID) is the primary key of the order_items table. The extraction query selects it so that primary key presence and uniqueness can be checked. |
| **Relationships** | order_items.order_id is a foreign key to orders.id. The sales query joins the two tables through order_items.order_id = orders.id. |
| **Reason for Inclusion** | The order_items table provides the quantity and total price used by the Transformation stage to calculate sales-related totals. The SQL query retrieves these values together with the corresponding order's date and status information. |

### 2.3 waste_logs

| Field | Detail |
|---|---|
| **Purpose** | The waste_logs table provides records of wasted items and their associated costs. These records are used by the Transformation stage to calculate the total waste cost for the target date. |
| **Important Columns** | id (UUID, PK), waste_type (ENUM), item_name (TEXT), quantity (NUMERIC), unit (TEXT), cost (NUMERIC), logged_at (TIMESTAMPTZ), voided_at (TIMESTAMPTZ, nullable) |
| **Primary Key** | The id column (UUID) is the primary key of the waste_logs table. The extraction query selects it so that primary key presence and uniqueness can be checked. |
| **Relationships** | The extraction query reads directly from waste_logs and does not join another table. The table has optional foreign keys to raw_ingredients (raw_ingredient_id) and celebration_materials (material_id), which the extraction does not use. |
| **Reason for Inclusion** | The waste_logs table provides the cost data needed for waste expenses. The SQL query retrieves records whose logged_at values fall within the specified date range. Voided records (voided_at is not NULL) are also extracted and are excluded later in the Transformation stage. The extracted records are passed unchanged to the Transformation stage. |

### 2.4 inventory_logs

| Field | Detail |
|---|---|
| **Purpose** | The inventory_logs table provides records of inventory transactions and their associated costs. These records are used by the Transformation stage to calculate inventory-related expenses for the target date. |
| **Important Columns** | id (UUID, PK), item_type (ENUM), item_name (TEXT), transaction_type (TEXT), cost (NUMERIC), created_at (TIMESTAMPTZ), voided_at (TIMESTAMPTZ, nullable) |
| **Primary Key** | The id column (UUID) is the primary key of the inventory_logs table. The extraction query selects it so that primary key presence and uniqueness can be checked. |
| **Relationships** | The extraction query reads directly from inventory_logs and does not join another table. The table has optional foreign keys to raw_ingredients (raw_ingredient_id) and celebration_materials (material_id), which the extraction does not use. |
| **Reason for Inclusion** | The inventory_logs table provides the cost data needed for inventory-related expenses. The SQL query retrieves records whose created_at values fall within the specified date range. Voided records (voided_at is not NULL) are also extracted and are excluded later in the Transformation stage. The query does not filter by transaction_type, so every transaction type is extracted. The extracted records are passed unchanged to the Transformation stage. |

## 3. Extraction Validation and Data Quality Checks

The extraction stage defines the eight validation checks below. Each check has a name, a target, a purpose, and measurable pass/fail criteria, followed by its Phase 1 implementation status, so that checks that are implemented can be told apart from checks that are defined but not yet automated. Together the checks cover source connectivity, required tables and columns, primary key uniqueness, and record counts.

| # | Check Name | Target | Purpose | Validation Criteria |
|---|---|---|---|---|
| 1 | Source connectivity | Source database connection | Confirm the source database is reachable before any query runs | The check_source_env() function attempts to open a source database connection and closes it after the connection check. A connection error interrupts the pipeline<br>**Phase 1 status:** Implemented. check_source_env() performs this check, and run_extract.py calls it before any query. |
| 2 | Required table/column existence | orders, order_items, waste_logs, inventory_logs | Detect schema drift before relying on query results | All columns referenced in the extraction queries exist with the expected names<br>**Phase 1 status:** Partially implemented. A missing table or column makes the SQL query raise an error and stops the run, and run_extract.py reports FAIL if an extracted DataFrame lacks an expected column. There is no separate schema check before the queries. |
| 3 | Primary key presence | order_items.id, waste_logs.id, inventory_logs.id | Ensure every extracted row has a usable identifier | No NULLs in the identifier column of any extracted row<br>**Phase 1 status:** Implemented. run_extract.py checks that the id column of every extracted DataFrame has no NULL values and reports FAIL otherwise. |
| 4 | Primary key uniqueness | order_items.id, waste_logs.id, inventory_logs.id | Detect duplicate rows caused by a faulty join or query | Count of distinct id values equals row count<br>**Phase 1 status:** Implemented. run_extract.py checks that the id values of every extracted DataFrame are unique and reports FAIL if duplicates are found. |
| 5 | Required field completeness | total_price (order_items), cost (waste_logs, inventory_logs) | Prevent silently undercounting sales/expenses due to missing values | No NULLs in total_price or cost for extracted rows; NULLs found are flagged, not silently dropped<br>**Phase 1 status:** Not implemented. NULL values are extracted as they are and are not flagged during extraction. |
| 6 | Record count logging | Each table's extraction result | Catch unexpectedly empty or abnormally large extractions | Record count >= 0 and below a configurable upper bound<br>**Phase 1 status:** Partially implemented. Each extraction function logs its row count, and run_extract.py reports it and notes a count of 0. No upper bound is configured. |
| 7 | Referential integrity | order_items.order_id -> orders.id | Ensure no orphaned line items are summed into sales | Every extracted order_items.order_id has a matching row in the extracted orders set<br>**Phase 1 status:** Enforced by the query design. The sales query uses an inner JOIN, so only order_items with a matching order are extracted. There is no separate check. |
| 8 | Extraction completeness | Returned sales and expense records | Detect partial extraction from a dropped connection mid-query | Rows extracted matches rows read from the source for the same query<br>**Phase 1 status:** Not implemented. A connection failure during a query raises an error and stops the run, but the extracted rows are not compared with a row count from the source. |

Data cleaning, including duplicate-row removal and handling missing total_price or cost values, belongs to the Transformation stage and is not part of the Extraction stage.

Extraction test script: pipeline/run_extract.py runs only the extraction stage for one day. For each extractor (sales, waste_logs, and inventory_logs) it checks that the expected columns are present and that the primary key column (id) has no NULL values and no duplicates, writes the PASS or FAIL result of each check and the row count to the log, and prints the first rows on screen. A day with 0 rows still passes, because a day without activity is valid. The script exits with code 0 when every extractor passes and with code 1 when a check fails.

## 4. Extraction Metadata and Log Specification

The Extraction stage records log messages when retrieving data from the source database. These messages include the extraction activity, the target date range, and the number of rows returned by each extraction query. Each log entry also contains a timestamp, log level, and module name. The table below is the log specification. The last column shows whether the Phase 1 code already writes each field; "No" or "Partly" means the field is part of the specification but is not yet logged.

| Logged Information | Purpose | Illustrative Example | Logged in Phase 1? |
|---|---|---|---|
| Run ID | Identifies one extraction run so that every log line of that run can be traced together. It is handed to the Transformation stage as parent_run_id. | EXT_20261020_101639 | Yes (in the EXTRACT TEST START and END lines) |
| Table Name | Identifies which source table a log line refers to. | order_items | Partly (appears inside the message text) |
| Timestamp | Records when the extraction message was logged. | 2026-10-13 18:59:22 | Yes |
| Run Start and End Timestamps | Shows when a run began and ended, and how long it took. | Start 2026-10-16 16:49:35, end 2026-10-16 16:49:39 | Partly (the START and END lines carry a timestamp; they are not separate fields) |
| Log Level | Indicates the type of log message. | INFO | Yes |
| Module Name | Identifies the extraction function that generated the message. | CakelyticsETL.extract.extract_sales | Yes |
| Extraction Activity | Shows which source data is being retrieved. | Extracting sales (order_items) from 2026-10-12T00:00:00+08:00 to 2026-10-12T23:59:59.999999+08:00 | Yes |
| Extraction Date Range | Shows the start and end of the target date window. | 2026-10-12T00:00:00+08:00 to 2026-10-12T23:59:59.999999+08:00 | Yes |
| Status | Shows whether the run, or the extraction of one table, succeeded or failed. | SUCCESS | Partly (run-level status only, not per table) |
| Records Read | Number of rows returned by the source query. | 30 | Partly (only the extracted count is logged) |
| Records Extracted | Records the number of rows returned by the query. | Extracted 30 order_items rows | Yes |
| Records Rejected | Number of rows removed by an extraction check. The extraction does not remove rows, so the expected value is 0. | 0 | No |
| Validation Results | PASS or FAIL result of each check in Section 3. | [sales (order_items)] PASS: primary key (id) is unique | Yes (each check result is written to the log; a FAIL is logged at ERROR level) |
| Error Message | Text of the error when a run fails, used for troubleshooting. | Missing SOURCE_DB_URL in .env | No (the error stops the run and is shown in the terminal only) |

#### Use of Extraction Logs

- **Monitoring:** Shows when each extraction query starts and how many rows it returns.
- **Troubleshooting:** Helps identify which extraction function was running and which date range it used.
- **Record Tracking:** Provides a record of the number of rows returned by each extraction query.
- **Auditing:** The run ID, table name, and data window show which data was extracted, when, and with what result.
- **Recovery:** The data window and status of a failed run show which day must be extracted again.

## 5. Log Retention and Access

| Aspect | Policy | Current Implementation |
|---|---|---|
| Storage Location | Store operational log files in the project's logs directory. | Implemented. Logs are written to logs/etl_YYYY-MM-DD.log, one file per day, and *.log files are excluded from the repository through .gitignore. |
| Retention Period | Retain operational logs for 90 days. Retain any separately maintained audit records for one year, if such records are introduced. | No automatic retention period is configured. |
| Access Permissions | Limit access to log files to authorized project developers and personnel responsible for maintaining the pipeline. | Separate role-based access permissions are not configured by the logging code. |
| Archiving | Archive operational logs older than 90 days when they are still needed for troubleshooting or auditing. | Automatic archiving is not implemented. |
| Deletion Rules | Delete operational logs only after the proposed retention period and after confirming that they are no longer needed for troubleshooting, auditing, or recovery. | Automatic log deletion is not implemented. |

#### Retention and Access Considerations

- Troubleshooting: Recent logs should remain available to help developers investigate failed extraction runs and database connection problems.
- Auditing: Logs containing useful run details should be retained when needed to trace the target date, extracted record counts, and processing results.
- Recovery: Logs should not be deleted while they are needed to identify or investigate a date that requires reprocessing.
- Security: Database credentials and other sensitive configuration values should not be included in log messages. Access to stored logs should be limited to authorized personnel.

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Schema (as extracted)

| Source Table | Column | Expected Type | Purpose | Required? |
|---|---|---|---|---|
| orders | orders.id | UUID | Joins an order to its order items. | Yes |
| orders | orders.updated_at | TIMESTAMPTZ | Determines whether an order falls within the target date range. | Yes |
| orders | orders.status | ENUM (order_status) | Returned with each order so that cancelled orders can be excluded in the Transformation stage. | Yes |
| order_items | order_items.id | UUID | Primary key. Used for the primary key presence and uniqueness checks. | Yes |
| order_items | order_items.order_id | UUID | Links each order item to its order. It is returned so that every item can be traced to its order. | Yes |
| order_items | order_items.quantity | INTEGER | Provides the quantity used in sales processing. | Yes |
| order_items | order_items.total_price | NUMERIC | Provides the item total used in sales processing. | Yes |
| waste_logs | waste_logs.id | UUID | Primary key. Used for the primary key presence and uniqueness checks. | Yes |
| waste_logs | waste_logs.cost | NUMERIC | Provides the cost used in expense processing. | Yes |
| waste_logs | waste_logs.logged_at | TIMESTAMPTZ | Determines whether a waste record falls within the target date range. | Yes |
| waste_logs | waste_logs.voided_at | TIMESTAMPTZ, nullable | Returned so that voided records can be excluded in the Transformation stage. | Yes |
| inventory_logs | inventory_logs.id | UUID | Primary key. Used for the primary key presence and uniqueness checks. | Yes |
| inventory_logs | inventory_logs.cost | NUMERIC | Provides the cost used in expense processing. | Yes |
| inventory_logs | inventory_logs.created_at | TIMESTAMPTZ | Determines whether an inventory record falls within the target date range. | Yes |
| inventory_logs | inventory_logs.voided_at | TIMESTAMPTZ, nullable | Returned so that voided records can be excluded in the Transformation stage. | Yes |

The query also depends on the relevant source tables being available. The columns listed above are required by the documented extraction logic; they do not mean that every column is included in the returned DataFrame. For example, orders.id is used for joining records in the sales query but is not returned.

### 6.2 Naming Convention

The extraction process retains the source column names used in its SQL queries. It does not introduce a separate column-renaming step during extraction.

- **Column naming:** Source names use the existing CakeLytics naming convention, including snake_case names such as updated_at, logged_at, and total_price.
- **Abbreviations:** Existing source column names are retained instead of introducing new abbreviations.
- **Identifiers:** The sales query uses orders.id and order_items.order_id to match order records with their corresponding items. The sales query also returns order_items.order_id so that every item can be traced to its order. Each extraction query also selects the id column of its own table as the row identifier.
- **Consistency:** The documentation uses the same table and column names as the source SQL queries to make the extraction logic easier to trace.

### 6.3 Schema Consistency Handling

If an expected column is renamed or removed in the source system, the SQL query raises a database error and the extraction run stops before any data is handed over. If the query still runs but a returned DataFrame lacks an expected column, run_extract.py reports FAIL and exits with code 1. In both cases no partial data is handed to the next stage, and the mismatch must be resolved with an explicit, documented mapping before the extraction is run again for that table.

### 6.4 Source-to-Standardized Field Mapping

Since the extraction layer intentionally keeps field names identical to the source (no renaming), the table below documents, for every extracted column, the type mapping applied as data moves from PostgreSQL into Python/Pandas:

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
|---|---|---|---|---|
| orders.updated_at | updated_at | TIMESTAMPTZ | datetime64 (timezone-aware, UTC) | Type mapping |
| orders.status | status | ENUM (order_status) | string | Type mapping |
| order_items.id | id | UUID (PK) | object (UUID) | Type mapping |
| order_items.order_id | order_id | UUID (FK) | object (UUID) | Type mapping |
| order_items.quantity | quantity | INTEGER | int64 | Type mapping |
| order_items.total_price | total_price | NUMERIC | float64 | Type mapping |
| waste_logs.id | id | UUID (PK) | object (UUID) | Type mapping |
| waste_logs.waste_type | waste_type | ENUM | string | Type mapping |
| waste_logs.item_name | item_name | TEXT | string | Type mapping |
| waste_logs.quantity | quantity | NUMERIC | float64 | Type mapping |
| waste_logs.unit | unit | TEXT | string | Type mapping |
| waste_logs.cost | cost | NUMERIC | float64 | Type mapping |
| waste_logs.logged_at | logged_at | TIMESTAMPTZ | datetime64 (timezone-aware, UTC) | Type mapping |
| waste_logs.voided_at | voided_at | TIMESTAMPTZ, nullable | datetime64 (timezone-aware, UTC), NULL kept as null | Type mapping |
| inventory_logs.id | id | UUID (PK) | object (UUID) | Type mapping |
| inventory_logs.item_type | item_type | ENUM | string | Type mapping |
| inventory_logs.item_name | item_name | TEXT | string | Type mapping |
| inventory_logs.transaction_type | transaction_type | TEXT | string | Type mapping |
| inventory_logs.cost | cost | NUMERIC | float64 | Type mapping |
| inventory_logs.created_at | created_at | TIMESTAMPTZ | datetime64 (timezone-aware, UTC) | Type mapping |
| inventory_logs.voided_at | voided_at | TIMESTAMPTZ, nullable | datetime64 (timezone-aware, UTC), NULL kept as null | Type mapping |

## 7. Extraction Acceptance and Handover Rules

### 7.1 Required Decisions

#### Automatic Approval

Condition: All checks in Section 3 pass for every extracted table. Action: proceed automatically to Transformation. Decision type: automatic. Owner: ETL developer (monitors the run).

**Phase 1 status:** Implemented. run_extract.py exits with code 0 when every extractor passes.

#### Manual Review

Condition: a non-critical warning occurs — e.g., an unusually high record count, or a small number of rows missing cost/total_price. Action: the run is marked MANUAL REVIEW and does not proceed automatically; the developer inspects the flagged rows first. Decision type: manual. Owner: ETL developer.

**Phase 1 status:** Not implemented. The extraction code does not raise non-critical warnings.

#### Rejection or Failure

Condition: source connectivity fails, a required table/column is missing, or primary key duplicates are found. Action: the extraction step retries automatically (2 attempts, with a short delay in between). If all attempts fail, the run is marked REJECTED and recorded as a failed run; no data is handed to Transformation. Decision type: automatic. Owner: ETL developer (investigates the cause and runs the extraction again).

**Phase 1 status:** Partially implemented. The run stops with an error, or run_extract.py reports FAIL and exits with code 1. Automatic retries come from Prefect in the full pipeline and are not part of Phase 1.

#### Handover

What is passed to Transformation: the Pandas DataFrames returned by extract_sales(), extract_waste(), and extract_inventory_costs() (order_items_df, waste_df, inventory_df), passed in memory. In Phase 1 these are only tested; the Transformation stage uses them in Phase 2. The extraction log lines for each run are written to the logs folder, so a later failure can be traced back to the extraction that produced the data.

### 7.2 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
|---|---|---|
| All required tables PASS | APPROVED | Proceed to Transformation<br>**Phase 1 status:** Implemented (exit code 0). |
| One or more non-critical WARNING | MANUAL REVIEW | Hold pipeline, notify developer<br>**Phase 1 status:** Not implemented. |
| One or more critical FAIL | REJECTED | Retry (2x) -> quarantine/stop if still failing<br>**Phase 1 status:** The run stops or exits with code 1. Retries and quarantine are not part of Phase 1. |

### 7.3 Example Validation Summary (run_extract.py, target date 2026-10-12)

| Validation Summary | Result |
|---|---|
| **order_items validation** | PASS (30 rows) |
| **waste_logs validation** | PASS (0 rows) |
| **inventory_logs validation** | PASS (27 rows) |
| **Overall Extraction** | PASS (all extractors passed) |

## 8. Common Extraction Problems and Mitigation

| # | Problem | Possible Cause | Impact | Mitigation |
|---|---|---|---|---|
| 1 | Supabase connection pool exhaustion | A new connection opened for every extraction call instead of being reused across a run | Extraction fails with a "too many clients" error | Maintain a single reusable connection pool for the whole run, with automatic health checks to detect and replace stale connections<br>**Phase 1 status:** Implemented. config/source_db.py creates one shared SQLAlchemy engine (pool of 5 connections, with pool_pre_ping health checks) that every extraction function reuses. |
| 2 | Schema change in the source system | The development team renames or removes a column without notifying the ETL maintainer | Extraction query fails outright, or silently returns incorrect/empty data | Run the schema existence check before every extraction; require the data contract to be updated before any schema change ships<br>**Phase 1 status:** Partially implemented. A missing column makes the query fail, and run_extract.py checks the expected columns of each result. A separate schema check before extraction is not implemented. |
| 3 | Timezone misalignment | The extraction window is computed in server time (possibly UTC) instead of Asia/Manila time | Late-night orders get attributed to the wrong day's sales total | Explicitly localize extraction window boundaries to Asia/Manila before building the SQL BETWEEN filter<br>**Phase 1 status:** Implemented. The extraction window is built in Asia/Manila time with an explicit +08:00 offset (utils/batch.py), so the database compares it correctly with its UTC timestamps. |
| 4 | Network interruption mid-extraction | Unstable internet connection between the pipeline's host machine and the Supabase cloud database | Partial extraction — some rows read, connection drops before the full result set returns | Prefect's built-in task retries (2 retries, 60s delay); the extraction completeness check ensures a partial result is never silently accepted as a full extraction<br>**Phase 1 status:** Not part of Phase 1. A failed query raises an error and stops the run, and the extraction is run again manually. Automatic retries come from Prefect in the full pipeline, and a separate completeness check is not implemented. |
| 5 | Missing cost/price values from manual data entry | Staff occasionally log a waste or inventory entry without filling in a cost value | total_expenses is understated without anyone noticing | The completeness check flags rows with NULL cost/total_price for manual review instead of silently dropping or zero-filling them<br>**Phase 1 status:** Not yet implemented. The extraction passes NULL values through unchanged and does not flag them. |

#### Expected Output

The extracted datasets (order items, waste logs, and inventory logs, scoped to the target day and checked by run_extract.py) are what the Transformation stage receives. No cleaning, standardization, or business computation (such as total sales, total expenses, or profit) occurs during Extraction itself — that is the responsibility of the Transformation stage.
