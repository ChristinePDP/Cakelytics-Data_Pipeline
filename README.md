# CakeLytics Sales, Expense, and Profit ETL Pipeline

Batch ETL pipeline for the CakeLytics bake shop: extracts sales and expense
data from the operational database (Supabase), cleans and transforms it into
daily totals and profit, loads the result into a separate analytics warehouse,
and displays it in a desktop dashboard.

The project is pushed in three parts:

| Phase | What it adds | Status |
|-------|--------------|--------|
| 1. Extract | Pull raw sales, waste, and inventory data from the source database | Done |
| 2. Transform | Profile, clean, standardize, and validate the data; build the daily summary | **This commit** |
| 3. Load | Write the daily summary into the warehouse, plus the Prefect flow and dashboard | Next |

## Folder Structure

```
config/       -> source database connection (reads .env)
utils/        -> logger, batch window and run IDs
extract/      -> pulls raw data from the SOURCE database (window filter only)
transform/    -> rules, rule engine, profiling, validation, quarantine, reports
pipeline/     -> run_extract.py (Stage 1 test) and run_transform.py (Stage 2 test)
docs/         -> Lab 3 (Extraction) and Lab 4 (Transformation) documentation
logs/         -> run logs and run metadata (generated, not committed)
quarantine/   -> rejected and excluded records per batch (generated, not committed)
reports/      -> data profiling report per batch (generated, not committed)
```

## 1. Requirements

- Python 3.10 or newer
- Your existing CakeLytics Supabase project (the source database)

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

Fill in `SOURCE_DB_URL` with your CakeLytics database connection string
(Supabase dashboard -> Project Settings -> Database -> Connection string -> URI).

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
| `logs/transform_<run_id>.json` | Run metadata: run IDs, counts, rules applied, validation results, errors |
| `logs/etl_<date>.log` | Log lines (also shown on screen) |

Reruns are idempotent: the quarantine file is overwritten by batch ID, so
running the same day twice never creates duplicates.

## 7. The rules

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

## 8. Output tables (handed over to Load)

- `dim_date`: `full_date`, `day`, `month`, `year`
- `fact_daily_summary`: `full_date`, `total_sales`, `total_expenses`, `profit`

The warehouse creates the surrogate keys (`date_key`, `summary_key`) at load.

## 9. Documentation

- `docs/Lab4-Transformation.md`: Stage 2 documentation (Markdown)
- `docs/Lab3-Extraction.docx`: Stage 1 documentation

The pipeline only ever **reads** from the source database. It never writes
back to the operational database.
