# Stage 3: Data Load

**Project:** CakeLytics ETL Pipeline  
**Stage:** 3 — Load  
**Target:** Separate CakeLytics analytics warehouse on Supabase PostgreSQL  
**Load mode:** Daily batch  
**Status:** Implemented as a daily batch loader and read-only CustomTkinter warehouse display.

This document replaces the previous Stage 3 document and follows the requirements in Lab 5. It describes the load contract using the supplied warehouse schema and the outputs handed over by Stage 2. Stage 3 must load only that handover: it must not query the operational source, clean records, or recompute sales, expenses, or profit. The provided schema SQL must be run once in the **new warehouse Supabase project**, not in the source project. This document does not run it.

## 1. Load source and target

### Input

Stage 2 currently writes a JSON handover at `outputs/<batch_id>/transform/transform_<transformation_run_id>.json`. It contains `decision`, `run_id`, `parent_run_id`, `batch_id`, `outputs.dim_date`, and `outputs.fact_daily_summary`. Stage 3 reads this artifact and its metadata; it does not re-extract or re-transform data.

The normal handover contains one date row (`full_date`, `day`, `month`, `year`) and one summary row (`full_date`, `total_sales`, `total_expenses`, `profit`). Amounts are the already-calculated Stage 2 values. The warehouse generates `date_key` and `summary_key`; `date_key` must not be supplied from a YYYYMMDD transformation key because the target column is `SERIAL`.

A batch represents one business day in Asia/Manila (UTC+8). Its extraction window and IDs are carried in the handover metadata: `batch_id`, transformation `run_id`, extraction `parent_run_id`, and batch date/window. Only `APPROVED` handovers are eligible for automatic load. `MANUAL REVIEW`, `REJECTED`, missing, or malformed handovers are held without writes.

### Target and connection

The target is a separate Supabase PostgreSQL warehouse for reporting and dashboard queries. Its default schema is `public`, matching the supplied SQL; no `warehouse` schema is created by that SQL. Configure the warehouse connection separately from `SOURCE_DB_URL` as `WAREHOUSE_DB_URL` in `.env`. The loader checks that the warehouse host/port/user do not exactly match the source connection before connecting. Do not put credentials in source code, Markdown, logs, or version control. The loader role needs `SELECT`, `INSERT`, and `UPDATE` on only the two warehouse tables, with no DDL rights. The dashboard prefers a separate read-only `WAREHOUSE_READONLY_DB_URL` role with `SELECT` only; when absent, it falls back to `WAREHOUSE_DB_URL` and issues only `SELECT` queries. The source connection remains read-only for this pipeline.

### Method, schedule, and scope

The Python batch loader at `load_stage.py` uses SQLAlchemy/pg8000 and parameterized PostgreSQL operations. Run it after an approved transformation with `python load_stage.py` (yesterday) or `python load_stage.py YYYY-MM-DD`. It runs on demand; automatic scheduling is not configured. This is batch only, not streaming or real-time. Set `WAREHOUSE_DB_URL` separately from `SOURCE_DB_URL` in `.env` using the new warehouse project's URI. `.env.example` documents both the load and optional read-only dashboard variables.

Load `dim_date` first, then `fact_daily_summary`, in one database transaction. The normal batch has one row for each table (two rows total), and the intended completion time is within 60 seconds. The destination must be reachable and have sufficient storage and write permission. The CustomTkinter owner display is `display/dashboard.py`, run with `python -m display.dashboard`. It has preset and calendar date filters, sales/expense/profit KPI cards, a chart with hover details, and a daily summary table. It queries committed rows and does not calculate or write the measures. Install its `customtkinter`, `tkcalendar`, and `matplotlib` dependencies from `requirements.txt`. Automatic scheduling remains unconfigured.

## 2. Target tables and column specification

The target schema is exactly the user-provided schema:

```sql
CREATE TABLE dim_date (
    date_key    SERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    day         INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL
);

CREATE TABLE fact_daily_summary (
    summary_key     SERIAL PRIMARY KEY,
    date_key        INTEGER NOT NULL UNIQUE REFERENCES dim_date(date_key),
    total_sales     NUMERIC(10,2) NOT NULL DEFAULT 0,
    total_expenses  NUMERIC(10,2) NOT NULL DEFAULT 0,
    profit          NUMERIC(10,2) NOT NULL DEFAULT 0
);
```

| Table | Type and purpose | Key, relationship, and relevant columns |
|---|---|---|
| `dim_date` | Date dimension for day, month, and year reporting. | `date_key SERIAL PRIMARY KEY`; `full_date DATE NOT NULL UNIQUE`; `day`, `month`, and `year` are `INTEGER NOT NULL`. `full_date` is the natural match key. |
| `fact_daily_summary` | One daily aggregate of sales, expenses, and profit. | `summary_key SERIAL PRIMARY KEY`; `date_key INTEGER NOT NULL UNIQUE` references `dim_date(date_key)`; measures are `NUMERIC(10,2) NOT NULL DEFAULT 0`. `date_key` is the upsert match key and enforces one summary per date. |

The `UNIQUE` constraint on fact `date_key` makes the daily fact idempotent. The schema has no batch, run, load timestamp, or load-log columns; those lineage and operational details belong in load metadata/log artifacts, not in these two tables. No additional check constraints, indexes, partitions, or SCD history columns are defined by the supplied SQL. The date dimension uses overwrite-free reuse of an existing date (static calendar attributes); the fact uses Type 1 overwrite/upsert of the daily measures.

## 3. Load strategy and rules

| Table | Load type | Write logic and match key | Dependency and failure handling |
|---|---|---|---|
| `dim_date` | Incremental | Insert the date attributes if `full_date` is absent; reuse the existing row on rerun. Natural match key: `full_date`. | Parent table. A failed batch rolls back its transaction. |
| `fact_daily_summary` | Incremental | Upsert measures on unique `date_key`; update the existing daily totals on rerun. | Child table. Requires a matching `dim_date`; missing parent rejects the batch. |

The loader obtains the warehouse-generated `date_key` by inserting or selecting the `dim_date` row matched by `full_date`, then uses that key in the fact upsert. It passes through Stage 2 amounts without recalculation. Negative `profit` is valid. Late-arriving batches for an existing date overwrite that date's measures (Type 1); the schema does not preserve historical versions. There are no source deletions to propagate. A handover schema mismatch is rejected before writing. Facts with no date dimension row are rejected; no Unknown member exists in this schema.

Both writes are one transaction. Any write or critical validation failure rolls back both tables so a dimension-only partial batch is not left behind. A rerun is safe because `full_date` and fact `date_key` are unique and writes use conflict-aware insert/upsert logic. Concurrent loads for the same date rely on those unique constraints and should be serialized/retried on a recoverable conflict.

## 4. Pre-load validation

| Check | Target | Pass criteria | Severity |
|---|---|---|---|
| Warehouse connection and write access | Warehouse | Connection succeeds within configured timeout and role can write both target tables. | Critical |
| Target schema | `dim_date`, `fact_daily_summary` | Both tables and required columns exist with compatible PostgreSQL types and constraints. | Critical |
| Handover status | Stage 2 metadata | Decision is exactly `APPROVED`. | Critical |
| Handover completeness | JSON artifact | Required metadata and both output tables are present; each contains the expected date and measures. | Critical |
| Input count reconciliation | Handover metadata and rows | Parsed rows/counts agree with the artifact; expected daily input is one date and one fact row. | Critical |
| Date and key consistency | Both input outputs | Same valid `full_date`; day/month/year agree with that date. Loader does not require or trust a transformation `date_key`. | Critical |
| Measure validity | Fact output | Values are finite decimal amounts representable as `NUMERIC(10,2)`; preserve Stage 2 values and allow negative profit. | Critical |
| Duplicate/replay condition | Warehouse date/fact keys | Existing rows are handled by the documented idempotent upsert; no second summary can be created. | Critical |
| Storage availability | Warehouse | Loader records `pg_database_size` before and after the transaction; the target connection and writes must succeed. Monitor Supabase quota/headroom in the project dashboard because the supplied SQL exposes no quota limit. Capacity/connection failures roll back the transaction. | Critical |

A failed critical check stops the batch before writes. The database size measurement is included in load metadata. Supabase's project storage quota is not queryable through this table schema, so the maintainer must monitor provider quota before backfills; a database storage error causes rollback.

## 5. Post-load validation and reconciliation

The loader checks the connection, target tables/keys/relationships, handover completeness and measures before writing. It validates target row counts, key uniqueness, referential integrity, required values, and exact amount reconciliation before commit; a critical failure rolls back the batch.

| Check | Target | Pass criteria | Severity |
|---|---|---|---|
| Record reconciliation | Batch input and target | For each table, `received = inserted + updated + unchanged + rejected`; normal expected target cardinality is one date and one fact for the business date. Rejected means load-stage rows not written, not rows already removed by Stage 2. | Critical |
| Row count | Both tables | Exactly one matching `dim_date.full_date` and one `fact_daily_summary.date_key` for that date. | Critical |
| Primary/unique key integrity | Both tables | No duplicate primary keys, `dim_date.full_date`, or fact `date_key`; unique constraints remain satisfied. | Critical |
| Referential integrity | Fact to dimension | The fact `date_key` resolves to the date row for the same `full_date`. | Critical |
| Required values | Both tables | All required fields are non-null; measures equal the Stage 2 input at two-decimal precision. | Critical |
| Amount reconciliation | Fact measures | Loaded `total_sales`, `total_expenses`, and `profit` equal the corresponding transformed values. No new calculation is performed. | Critical |
| Load errors | Transaction | No unhandled database errors or constraint violations; transaction remains committable. | Critical |

## 6. Load metadata and logs

Record one load result per attempted batch as JSON and operational log entries. Suggested artifact location: `outputs/<batch_id>/load/load_<load_run_id>.json`; operational messages go to the project `logs/` directory. Do not log credentials or full connection strings.

| Field | Purpose / example |
|---|---|
| `run_id`, `transformation_run_id`, `extraction_run_id` | Link the load to pipeline stages, e.g. `LOAD_20261111_120001`, `TRN_20261111_120001`, `EXT_20261111_120001`. |
| `batch_id`, `batch_window` | Business batch and Philippine date/time range, e.g. `BATCH_20261110`, one day in UTC+8. |
| `target_tables`, `tables[].table_name`, `load_type` | Target tables, per-table results, and mode, e.g. `fact_daily_summary`, `INCREMENTAL`. |
| `load_start_ts`, `load_end_ts`, `load_duration_sec` | Timing and elapsed duration in ISO 8601 / seconds. |
| `status` | `SUCCESS`, `MANUAL_REVIEW`, `REJECTED`, or `FAILED`. |
| `records_received`, `records_inserted`, `records_updated`, `records_unchanged`, `records_rejected` | Reconciliation counts per table. |
| `pre_load_result`, `post_load_result`, `validation_results` | Named check outcomes and pass/fail details. |
| `error_code`, `error_message` | Database or validation failure, when applicable; redact sensitive values. |
| `load_code_version` | Git revision or release identifier for the loader. |

The loader writes this metadata to `outputs/<batch_id>/load/load_<load_run_id>.json`; `logs/load.log` records operations and failures. These records support monitoring, incident diagnosis, audit, lineage, and safe recovery. The database tables themselves do not store this metadata, so a missing audit artifact must be treated as an operational issue.

## 7. Retention, rejected data, and access

Retain load logs, metadata, and rejected load records for at least 90 days, or longer when required for audit or an open incident. Preserve them while needed for troubleshooting, audit, or recovery. The loader writes to protected local `outputs/<batch_id>/load/` and `logs/` locations; the maintainer is responsible for access controls, archival, and deletion after the retention period. Exclude artifacts and `.env` from version control. `logs/load.log` is append-only; no automatic deletion job is configured.

Rejected load reasons are recorded in the load artifact with batch/run references, without storing secrets. Do not confuse load rejections with Stage 2 quarantined records. A target backup/snapshot is recommended before a major historical backfill; the normal one-day idempotent load does not require a table replacement or truncate.

## 8. Acceptance, rollback, and completion

| Condition | Decision | Pipeline action |
|---|---|---|
| Handover is approved; all critical pre/post checks pass; transaction commits; counts and measures reconcile. | `APPROVED` / `SUCCESS` | Mark complete and make the committed rows available to authorized reporting/dashboard readers. |
| The latest transformation handover is `MANUAL REVIEW`. | `MANUAL_REVIEW` | Do not write to the warehouse; record the reason and hold for maintainer review. |
| Any critical check fails, handover is not approved, schema differs, or transaction fails. | `REJECTED` or `FAILED` | Roll back the whole transaction, record the reason, and stop the batch. |

Retry transient database connection failures at most three times, with waits of 5 seconds and 15 seconds before attempts two and three. Do not retry schema, permission, handover, or data-contract failures automatically. After retries are exhausted, record failure in the load artifact and operational log; a separate notification integration is not configured.

A completion record includes final status, all linked IDs, batch window when present in Stage 2 metadata, tables, row counts, validation outcomes, start/end timestamps, duration, and code version. Loads over 60 seconds are marked with a performance warning in metadata and logs; this timing target does not change data acceptance. Downstream readers should use committed warehouse data. The dashboard queries the committed warehouse rows; notifications are not configured, so the maintainer reviews held and failed load artifacts/logs.

## 9. Common load problems and mitigation

| Problem | Cause and impact | Mitigation |
|---|---|---|
| Duplicate daily fact on rerun | Plain insert would violate uniqueness or inflate reporting totals. | Unique `date_key`; upsert the existing daily summary and record inserted/updated counts. |
| Foreign-key violation | Fact write precedes date dimension or uses the wrong key. | Load/retrieve dimension first in the same transaction; verify date-key relationship before commit. |
| Partial load after interruption | Dimension write succeeds but fact write fails without transaction control. | Put both table writes and post-load checks in one transaction; roll back on any critical failure. |
| Schema/type mismatch | Warehouse schema or handover differs from the contract. | Validate schema and payload before writing; reject and report the mismatch. |
| Warehouse unavailable or timeout | Network interruption or Supabase service outage delays the batch. | Bounded retries for transient errors; then fail with metadata and resume safely by rerunning the same batch. |
| Insufficient privileges | ETL role lacks table insert/update rights. | Grant only required table privileges to the dedicated warehouse role; never use source credentials for warehouse writes. |
| Incorrect financial values | Conversion/rounding or unintended recomputation changes Stage 2 totals. | Use decimal-safe parameter binding and compare stored amounts with Stage 2 values before commit. |
| Concurrent same-date load | Two jobs write the same date simultaneously. | Serialize batches where possible; rely on unique constraints and retry only transient conflicts safely. |
| Missing trace information | Load succeeds but metadata is absent or incomplete. | Require run/batch IDs and write the completion artifact; flag missing metadata as an operational failure. |
| Dashboard shows stale totals | Display reads before commit or does not refresh warehouse data. | Read only committed rows and refresh/requery after a successful load; report load status and date displayed. |

## Expected output

A successful Stage 3 run leaves one date row and one daily fact row for the target date, with the fact measures matching the approved Stage 2 handover, plus a traceable load result and operational logs. It does not alter the source database or add extraction, cleaning, or business transformation logic. The CustomTkinter dashboard reads warehouse tables after commit; it does not independently calculate or rewrite daily business measures.
