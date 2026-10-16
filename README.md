# CakeLytics Sales, Expense, and Profit ETL Pipeline

Batch ETL pipeline for the CakeLytics bake shop: extracts sales and expense
data from the operational database (Supabase), computes daily totals and
profit, loads the result into a separate analytics warehouse, and displays
it in a desktop dashboard.

The project is pushed in three parts:

| Phase | What it adds | Status |
|-------|--------------|--------|
| 1. Extract | Pull raw sales, waste, and inventory data from the source database | **This commit** |
| 2. Transform | Clean the raw data and compute daily totals and profit | Next |
| 3. Load | Write the daily summary into the warehouse, plus the Prefect flow and dashboard | After that |

## Folder Structure (Phase 1)

```
config/       -> source database connection (reads .env)
utils/        -> shared logger
extract/      -> pulls raw data from the SOURCE database
pipeline/     -> run_extract.py, the test script for this phase
logs/         -> run logs (generated, not committed)
```

## 1. Requirements

- Python 3.10 or newer
- Your existing CakeLytics Supabase project (the source database)

## 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Configure environment variables

```bash
cp .env.example .env
```

Fill in `SOURCE_DB_URL` with your CakeLytics database connection string
(Supabase dashboard -> Project Settings -> Database -> Connection string -> URI).

**Never commit your `.env` file.** It is already listed in `.gitignore`.

## 4. Test the extract stage

```bash
python -m pipeline.run_extract
```

This runs **only** the extract stage for yesterday's data and checks that
each query returned the expected columns. To test a specific day:

```bash
python -m pipeline.run_extract 2026-10-05
```

For every extractor it prints PASS or FAIL, the row count, and the first
few rows. The exit code is `0` when everything passed and `1` when
something failed. A day with 0 rows still passes, because a day without
activity is valid.

## 5. View the logs

Every run writes a log file to `logs/`, named after the computer date:

```text
logs/etl_2026-10-01.log
```

From PowerShell:

```powershell
Get-Content .\logs\etl_2026-10-01.log -Tail 40
```

## What gets extracted

- **Sales**: `order_items` joined with `orders`, excluding orders with
  status `Cancelled` (same rule as `orderItems.model.js`).
- **Waste**: `waste_logs` with `voided_at IS NULL`.
- **Inventory costs**: `inventory_logs` with `voided_at IS NULL`.

The `voided_at IS NULL` filters mirror the Node.js models
(`waste.model.js`, `inventoryLog.model.js`) so voided entries are never
double-counted.

The pipeline only ever **reads** from the source database. It never
writes back to the operational database.




# kapag running script disabled 

Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned

# para ma activate ang (venv)
.\venv\Scripts\Activate.ps1