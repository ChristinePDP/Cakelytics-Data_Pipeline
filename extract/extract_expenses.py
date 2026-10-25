"""
EXTRACT — Expenses

Pulls raw waste_logs and inventory_logs from the source database.

Only a TECHNICAL filter is applied here: the batch date window. Voided
entries (voided_at is not NULL) are NOT filtered in extraction. They are
extracted with their voided_at value, and the Transformation stage excludes
them (rules B-02 and B-03) and counts them, so they are never dropped silently.
"""

from datetime import datetime

import pandas as pd
from sqlalchemy import text
from config.source_db import get_source_engine
from utils.logger import get_logger

log = get_logger(__name__)


def extract_waste(window_start: datetime, window_end: datetime) -> pd.DataFrame:
    log.info(
        "Extracting waste_logs from %s to %s",
        window_start.isoformat(), window_end.isoformat(),
    )

    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT id, waste_type, item_name, quantity, unit, cost, logged_at, voided_at
            FROM waste_logs
            WHERE logged_at BETWEEN :start AND :end
            """),
            conn,
            params={"start": window_start, "end": window_end},
        )

    log.info("Extracted %d waste_logs rows", len(df))
    return df


def extract_inventory_costs(window_start: datetime, window_end: datetime) -> pd.DataFrame:
    log.info(
        "Extracting inventory_logs from %s to %s",
        window_start.isoformat(), window_end.isoformat(),
    )

    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT id, item_type, item_name, transaction_type, cost, created_at, voided_at
            FROM inventory_logs
            WHERE created_at BETWEEN :start AND :end
            """),
            conn,
            params={"start": window_start, "end": window_end},
        )

    log.info("Extracted %d inventory_logs rows", len(df))
    return df
