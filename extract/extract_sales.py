"""
EXTRACT — Sales

Pulls raw order_items joined with orders from the source database.
Mirrors the join pattern in orderItems.model.js (getByOrderDateRange).

Only a TECHNICAL filter is applied here: the batch date window. Business
exclusions (such as cancelled orders) are NOT applied in extraction. Every
order in the window is extracted with its status, and the Transformation
stage excludes cancelled orders (rule B-01) and counts them.
"""

from datetime import datetime

import pandas as pd
from sqlalchemy import text
from config.source_db import get_source_engine
from utils.logger import get_logger

log = get_logger(__name__)


def extract_sales(window_start: datetime, window_end: datetime) -> pd.DataFrame:
    log.info(
        "Extracting sales (order_items) from %s to %s",
        window_start.isoformat(), window_end.isoformat(),
    )

    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT oi.id, oi.order_id, oi.quantity, oi.total_price, o.updated_at, o.status
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            WHERE o.updated_at BETWEEN :start AND :end
            """),
            conn,
            params={"start": window_start, "end": window_end},
        )

    log.info("Extracted %d order_items rows", len(df))
    return df
