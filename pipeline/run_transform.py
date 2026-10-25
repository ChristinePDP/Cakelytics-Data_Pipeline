"""
STAGE 2 TEST — Extract -> Transform (nothing is loaded)

Runs the extraction for one business day (Asia/Manila), hands the extracted
records to the Transformation stage, and shows what happened:

  * counts per table: read, output, rejected, excluded
  * the daily totals produced (total sales, total expenses, profit)
  * the result of every validation check
  * the acceptance decision: APPROVED, MANUAL REVIEW, or REJECTED

Files written:
  quarantine/quarantine_<batch_id>.csv   rejected and excluded records
  reports/profile_<batch_id>.md          data profiling report
  logs/transform_<run_id>.json           run metadata
  logs/etl_<date>.log                    log lines (also shown on screen)

Run for yesterday (default):
    python -m pipeline.run_transform

Run for a specific day:
    python -m pipeline.run_transform 2026-10-12

Exit code: 0 = APPROVED, 2 = MANUAL REVIEW, 1 = REJECTED or the run failed.
"""

import sys
from datetime import datetime

from config.source_db import check_source_env
from extract.extract_sales import extract_sales
from extract.extract_expenses import extract_waste, extract_inventory_costs
from transform.quarantine import write_quarantine
from transform.reporting import (
    build_metadata, failed_metadata, profile_to_markdown, write_metadata, write_profile_report,
)
from transform.transform_batch import transform_batch
from utils.batch import PH_TZ, make_batch
from utils.logger import get_logger

log = get_logger(__name__)

EXIT_CODES = {"APPROVED": 0, "MANUAL REVIEW": 2, "REJECTED": 1}