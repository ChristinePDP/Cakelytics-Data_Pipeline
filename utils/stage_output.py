"""Write one physical JSON artifact per ETL stage and batch."""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from utils.batch import Batch

ROOT = os.path.dirname(os.path.dirname(__file__))
OUTPUTS_DIR = os.path.join(ROOT, "outputs")


def _json_value(value: Any) -> Any:
    if pd.isna(value) is True:
        return None
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def dataframe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {str(key): _json_value(value) for key, value in row.items()}
        for row in df.to_dict(orient="records")
    ]


def stage_directory(batch: Batch, stage: str) -> str:
    path = os.path.join(OUTPUTS_DIR, batch.batch_id, stage)
    os.makedirs(path, exist_ok=True)
    return path


def write_stage_output(
    batch: Batch,
    stage: str,
    run_id: str,
    payload: dict[str, Any],
) -> str:
    """Write a stage artifact and return its absolute path."""
    path = os.path.join(stage_directory(batch, stage), f"{stage}_{run_id}.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, default=_json_value)
    return path


def ensure_stage_directories(batch: Batch) -> None:
    """Create the three stage directories, including the future load folder."""
    for stage in ("extract", "transform", "load"):
        stage_directory(batch, stage)
