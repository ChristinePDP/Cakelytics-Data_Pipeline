"""
EXTRACT — Sales

Pulls raw order_items joined with orders from the source database.
Mirrors the join pattern in orderItems.model.js (getByOrderDateRange),
excluding cancelled orders the same way excludeCancelled does there.
"""

import pandas as pd
from sqlalchemy import text
from config.source_db import get_source_engine
from utils.logger import get_logger

log = get_logger(__name__)


def extract_sales(start_date: str, end_date: str) -> pd.DataFrame:
    log.info("Extracting sales (order_items) from %s to %s", start_date, end_date)

    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT oi.quantity, oi.total_price, o.updated_at, o.status
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            WHERE o.updated_at BETWEEN :start AND :end
              AND o.status != 'Cancelled'
            """),
            conn,
            params={"start": start_date, "end": end_date},
        )

    log.info("Extracted %d order_items rows", len(df))
    return df