# CakeLytics Sales, Expense, and Profit ETL Pipeline

Batch ETL pipeline for the CakeLytics bake shop: extracts sales and expense
data from the operational database (Supabase), cleans and transforms it into
daily totals and profit, loads the result into a separate analytics warehouse,
and displays it in a desktop dashboard.

The pipeline has three stages:

| Stage | What it does | Status |
|-------|--------------|--------|
| 1. Extract | Pull raw sales, waste, and inventory data from the source database | Implemented |
| 2. Transform | Profile, clean, standardize, validate, and build the daily summary | Implemented |
| 3. Load and display | Idempotently write approved daily summaries to the separate warehouse and display them with CustomTkinter | Implemented; manual run |

## Folder Structure

```
config/       -> separate source and warehouse database connections (read .env)
utils/        -> logger, batch window and run IDs
extract/      -> pulls raw data from the SOURCE database (window filter only)
transform/    -> rules, rule engine, profiling, validation, quarantine, reports
pipeline/     -> run_extract.py and run_transform.py
load_stage.py -> approved Stage 2 handoff loader for the separate warehouse
display/     -> CustomTkinter owner dashboard with date filters, KPI cards, chart, and daily table
docs/         -> Stage 1, Stage 2, and Stage 3 documentation
logs/         -> ETL run logs (generated, not committed)
metadata/     -> transformation run metadata (generated, not committed)
quarantine/   -> rejected and excluded records per batch (generated, not committed)
reports/      -> data profiling report per batch (generated, not committed)
outputs/      -> physical JSON handoffs and load results per batch and stage
```

## 1. Requirements

- Python 3.10 or newer
- CakeLytics Supabase source project
- A separate Supabase PostgreSQL warehouse with the `dim_date` and `fact_daily_summary` tables from `docs/Stage3-DataLoad.md`

## 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
pip install -r requirements.txt

.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## 3. Configure environment variables

```bash
cp .env.example .env
```

Set `SOURCE_DB_URL` to the CakeLytics operational/source project's connection URI.
Set `WAREHOUSE_DB_URL` to the **separate warehouse project's** URI. The loader
checks that it does not point to the same host, port, and user as the source.
For the dashboard, `WAREHOUSE_READONLY_DB_URL` is optional but recommended with
a role that has `SELECT` access only. If it is omitted, the dashboard uses
`WAREHOUSE_DB_URL` and only issues `SELECT` queries.
Find each URI in that project's Supabase dashboard under Project Settings ->
Database -> Connection string -> URI. Never use the source URI as the warehouse
connection.

**Never commit your `.env` file.** It is already listed in `.gitignore`.

## 4. Business day and batch IDs

One batch is **one business day in Philippine time (Asia/Manila, UTC+8)**.
The source stores timestamps in UTC, so the extraction window is built with an
explicit `+08:00` offset. Every run gets IDs:

- `batch_id`: `BATCH_20261012` (the same on every rerun of that day)
- extraction run ID: `EXT_20261020_101639`
- transformation run ID: `TRN_20261020_101639`

## 5. Test Stage 1: Extract only

```bash
python -m pipeline.run_extract              # yesterday
python -m pipeline.run_extract 2026-10-12   # a specific day
```

Checks that every query returns the expected columns and that the primary key
(`id`) has no NULLs and no duplicates. Exit code `0` = passed, `1` = failed.

## 6. Test Stage 2: Extract, then Transform (nothing is loaded)

```bash
python -m pipeline.run_transform              # yesterday
python -m pipeline.run_transform 2026-10-12   # a specific day
```

It prints, per table, the records read, kept, rejected, and excluded; the
daily totals; the result of the 11 validation checks; and the decision.

| Decision | Meaning | Exit code |
|----------|---------|-----------|
| APPROVED | All checks pass; the batch can go to Load | 0 |
| MANUAL REVIEW | A non-critical warning (for example, more than 1% rejected); the batch is held | 2 |
| REJECTED | A critical check failed or the run failed; the batch is stopped | 1 |

Files written by a run:

| File | Content |
|------|---------|
| `quarantine/quarantine_<batch_id>.csv` | Every rejected or excluded record: id, rule ID, category, reason, amount |
| `reports/profile_<batch_id>.md` | Data profiling report (issue table for the documentation) |
| `metadata/transform_<run_id>.json` | Run metadata: run IDs, counts, rules applied, validation results, errors |
| `logs/etl_<date>.log` | Log lines (also shown on screen) |
| `outputs/<batch_id>/extract/extract_<run_id>.json` | The exact raw records returned by each extractor; this is the physical handoff into Transform |
| `outputs/<batch_id>/transform/transform_<run_id>.json` | The cleaned records per table plus final output tables, decision, and detailed failed/quarantined records; this is the physical handoff for Load |
| `outputs/<batch_id>/load/load_<load_run_id>.json` | Load status, linked run IDs, counts, validation results, timing, and failure details |
| `logs/load.log` | Load-stage operational log; do not commit generated log files |

## 7. Load an approved batch

Run the loader after Stage 2. It reads the latest handoff for that date and
writes only when its decision is `APPROVED`. `MANUAL REVIEW` is recorded and
held without warehouse writes; rejected handoffs and critical load failures
stop the run. The date and fact writes are one transaction, and reruns use the
warehouse's unique date keys and conflict-aware writes.

```bash
python load_stage.py              # yesterday in Asia/Manila
python load_stage.py 2026-10-12   # a specific business date
```

The loader writes to `dim_date` first, obtains its database-generated
`date_key`, and then upserts `fact_daily_summary`. It does not re-extract or
recalculate Stage 2 measures. Configure `WAREHOUSE_DB_URL` before running.
The database schema must already exist in the separate warehouse project; the
loader does not create tables or modify the source project.

Exit code `0` means successful load, `2` means held for manual review, and `1`
means rejected or failed. Inspect the load JSON and `logs/load.log` for results.

## 8. Open Dashboard

Install dependencies with `python -m pip install -r requirements.txt`, make
sure `WAREHOUSE_DB_URL` is set (or optionally set the read-only
`WAREHOUSE_READONLY_DB_URL`), then run:

```bash
python -m display.dashboard
```

The dashboard displays committed daily sales, expenses, and profit for the
selected date. It reads warehouse values and does not write or recalculate them.

Reruns are idempotent: the quarantine file is overwritten by batch ID, so
running the same day twice never creates duplicates.

The JSON artifact marks a rejected record with `category: "REJECTED"` and
lists it in `failed_records` and counts it in `failed_record_count`. A rejected record does not automatically make
the whole batch fail: the current acceptance policy makes the batch
`MANUAL REVIEW` when the rejection rate is above 1%, and `REJECTED` when a
critical validation check fails or the run itself errors.

Review examples:

- `2026-10-12`: `APPROVED` production batch
- `2026-11-11`: `MANUAL REVIEW` database defect batch
- `2026-11-12`: `REJECTED` database critical-validation failure

## 9. The rules

All cleaning (C), standardization (S), and business (B) rules are defined in
`transform/rules.py` with an ID, target, condition, action, and justification.
The same rule objects are used to profile the data, to clean it, and to write
the documentation tables, so code and docs cannot drift apart.

| Group | Rules |
|-------|-------|
| Cleaning | C-01 missing id, C-02 duplicate id (first kept), C-03 missing amount, C-04 negative amount, C-05 invalid quantity, C-06 orphan line item, C-07 trim whitespace |
| Standardization | S-01 timestamps to Asia/Manila (ISO 8601), S-02 batch-window conformance, S-03 types and 2-decimal money, S-04 transaction type uppercase |
| Business | B-01 exclude cancelled orders, B-02 exclude voided waste, B-03 exclude voided inventory, B-04 to B-10 totals, profit, and date dimension |

Nothing is dropped silently: every removed record is counted, logged, and
stored in the quarantine file.

## 10. Output tables (handed over to Load)

- `dim_date`: `full_date`, `day`, `month`, `year`
- `fact_daily_summary`: `full_date`, `total_sales`, `total_expenses`, `profit`

The warehouse creates the surrogate keys (`date_key`, `summary_key`) at load.

## 11. Documentation

- `docs/Stage1-Extraction.md`: Stage 1 documentation
- `docs/Stage2-DataTransformation.md`: Stage 2 documentation
- `docs/Stage3-DataLoad.md`: Stage 3 load design, schema, validations, operations, and recovery

The pipeline only ever **reads** from the source database. It never writes
back to the operational database. The warehouse load uses its own connection
variable and points to the separate analytics project.


python -m pipeline.run_transform 2026-11-10

# 1. Pagsusuri sa Extraction checks:
python -m pipeline.run_extract 2026-11-11

# 2. Pagsusuri sa Transformation at Quarantine:
python -m pipeline.run_transform 2026-11-11


2026-11-10: Success / Approved
2026-11-11: Manual Review
2026-11-12: Rejected

& "C:\Program Files\CakelyticsVenv\Scripts\Activate.ps1"        