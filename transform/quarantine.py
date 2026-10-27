"""
TRANSFORM — Quarantine file

Every rejected or excluded record is written here with its batch ID, rule ID,
category (REJECTED or EXCLUDED), and reason. Only the record id and the amount
are stored, not the full row, to avoid keeping unnecessary source data.

IDEMPOTENT: the file name contains the batch ID, and it is overwritten on every
run of the same batch (delete-and-insert by batch ID). Running the same batch
twice therefore never leaves duplicate quarantine rows, and a rerun with no
rejects replaces an older file that had some.
"""

from __future__ import annotations

import os

import pandas as pd

from transform.engine import QUARANTINE_COLUMNS
from utils.batch import Batch

QUARANTINE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "quarantine")


def write_quarantine(quarantine: pd.DataFrame, batch: Batch) -> str:
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    path = os.path.join(QUARANTINE_DIR, f"quarantine_{batch.batch_id}.csv")
    quarantine.reindex(columns=QUARANTINE_COLUMNS).to_csv(path, index=False)
    return path
