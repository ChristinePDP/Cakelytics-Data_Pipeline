"""
EXTRACT — Expenses

Pulls raw waste_logs and inventory_logs from the source database.
Mirrors wasteLogs.model.js (getRecent) and inventoryLog.model.js
(getByDateRange) — both use .is('voided_at', null) in the JS models,
so voided_at IS NULL is applied here too, to avoid double-counting
entries that were cancelled/voided in the operational system.
"""

import pandas as pd
from sqlalchemy import text
from config.source_db import get_source_engine
from utils.logger import get_logger

log = get_logger(__name__)


def extract_waste(start_date: str, end_date: str) -> pd.DataFrame:
    log.info("Extracting waste_logs from %s to %s", start_date, end_date)

    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT waste_type, item_name, quantity, unit, cost, logged_at, voided_at
            FROM waste_logs
            WHERE logged_at BETWEEN :start AND :end
              AND voided_at IS NULL
            """),
            conn,
            params={"start": start_date, "end": end_date},
        )

    log.info("Extracted %d waste_logs rows", len(df))
    return df


def extract_inventory_costs(start_date: str, end_date: str) -> pd.DataFrame:
    log.info("Extracting inventory_logs from %s to %s", start_date, end_date)

    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT item_type, item_name, transaction_type, cost, created_at, voided_at
            FROM inventory_logs
            WHERE created_at BETWEEN :start AND :end
              AND voided_at IS NULL
            """),
            conn,
            params={"start": start_date, "end": end_date},
        )

    log.info("Extracted %d inventory_logs rows", len(df))
    return df