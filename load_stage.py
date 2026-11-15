"""Stage 3: idempotently load approved daily summaries into the warehouse."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


# Use SQLAlchemy's pure-Python implementation when endpoint policy blocks the
# optional compiled extensions on Windows.
os.environ["DISABLE_SQLALCHEMY_CEXT"] = "1"

from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError, OperationalError

from config.warehouse_db import get_warehouse_engine

ROOT = Path(__file__).resolve().parent
OUTPUTS = ROOT / "outputs"
LOGS = ROOT / "logs"
PH_TZ = timezone(timedelta(hours=8), "Asia/Manila")
CODE_VERSION = "1.0.0"
MAX_ATTEMPTS = 3
RETRY_DELAYS = (5, 15)
MONEY_COLUMNS = ("total_sales", "total_expenses", "profit")

LOGS.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOGS / "load.log", encoding="utf-8")],
)
log = logging.getLogger("load")


class LoadRejected(Exception):
    """A permanent handover or target-contract failure; do not retry."""


class LoadHeld(Exception):
    """The transformation requires human review; keep the warehouse untouched."""


def _now() -> datetime:
    return datetime.now(PH_TZ)


def _json_default(value: Any) -> str:
    if isinstance(value, (Decimal, date, datetime)):
        return str(value)
    return repr(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")
    temporary.replace(path)


def _prior_success_exists(batch_id: str) -> bool:
    for path in (OUTPUTS / batch_id / "load").glob("load_*.json"):
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if previous.get("status") == "SUCCESS":
            return True
    return False


def _load_handover(batch_id: str) -> tuple[Path, dict[str, Any]]:
    if not re.fullmatch(r"BATCH_\d{8}", batch_id):
        raise LoadRejected("batch_id must use BATCH_YYYYMMDD format")
    directory = OUTPUTS / batch_id / "transform"
    candidates = sorted(directory.glob("transform_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise LoadRejected(f"No transformation handover found for {batch_id}")
    path = candidates[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoadRejected(f"Latest transformation handover is unreadable: {path.name}") from exc
    if payload.get("batch_id") != batch_id or payload.get("decision") not in {"APPROVED", "MANUAL REVIEW"}:
        raise LoadRejected(f"Latest transformation handover is not loadable: {path.name}")
    return path, payload


def _one_record(payload: dict[str, Any], table: str, required: set[str]) -> dict[str, Any]:
    rows = payload.get("outputs", {}).get(table)
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise LoadRejected(f"{table} must contain exactly one record for a daily batch")
    row = rows[0]
    missing = required - row.keys()
    if missing:
        raise LoadRejected(f"{table} handover is missing columns: {', '.join(sorted(missing))}")
    return row


def _validate_handover(payload: dict[str, Any], batch_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if payload.get("decision") == "MANUAL REVIEW":
        raise LoadHeld("Transformation handover requires manual review; no warehouse writes were made")
    if payload.get("decision") != "APPROVED":
        raise LoadRejected("Only APPROVED transformation handovers can be loaded")
    if payload.get("stage") != "transform" or payload.get("status") != "SUCCESS":
        raise LoadRejected("Transformation handover stage/status is not successful")
    if payload.get("batch_id") != batch_id or not payload.get("run_id") or not payload.get("parent_run_id"):
        raise LoadRejected("Handover batch or run lineage is missing or inconsistent")
    dim = _one_record(payload, "dim_date", {"full_date", "day", "month", "year"})
    fact = _one_record(payload, "fact_daily_summary", {"full_date", *MONEY_COLUMNS})
    try:
        full_date = date.fromisoformat(str(dim["full_date"]))
    except (TypeError, ValueError) as exc:
        raise LoadRejected("dim_date.full_date must be a valid ISO date") from exc
    if str(fact["full_date"]) != full_date.isoformat():
        raise LoadRejected("dim_date and fact_daily_summary full_date values differ")
    if "date_key" in dim and "date_key" in fact and dim["date_key"] != fact["date_key"]:
        raise LoadRejected("Stage 2 dim_date and fact date_key values differ")
    if batch_id != f"BATCH_{full_date:%Y%m%d}":
        raise LoadRejected("Handover date does not match batch_id")
    expected = (full_date.day, full_date.month, full_date.year)
    actual = (dim["day"], dim["month"], dim["year"])
    if actual != expected:
        raise LoadRejected("dim_date day/month/year do not match full_date")
    for column in MONEY_COLUMNS:
        try:
            amount = Decimal(str(fact[column]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise LoadRejected(f"{column} is not a valid decimal amount") from exc
        if not amount.is_finite() or abs(amount) >= Decimal("100000000"):
            raise LoadRejected(f"{column} cannot be represented as NUMERIC(10,2)")
        if amount != amount.quantize(Decimal("0.01")):
            raise LoadRejected(f"{column} has more than two decimal places")
        fact[column] = amount
    return {"full_date": full_date, "day": expected[0], "month": expected[1], "year": expected[2]}, fact


def _check_schema(connection) -> int:
    inspector = inspect(connection)
    required = {
        "dim_date": {"date_key", "full_date", "day", "month", "year"},
        "fact_daily_summary": {"summary_key", "date_key", *MONEY_COLUMNS},
    }
    expected_types = {
        "dim_date": {"date_key": "INTEGER", "full_date": "DATE", "day": "INTEGER", "month": "INTEGER", "year": "INTEGER"},
        "fact_daily_summary": {"summary_key": "INTEGER", "date_key": "INTEGER", "total_sales": "NUMERIC(10,2)",
                                "total_expenses": "NUMERIC(10,2)", "profit": "NUMERIC(10,2)"},
    }
    for table, columns in required.items():
        if not inspector.has_table(table, schema="public"):
            raise LoadRejected(f"Required target table public.{table} does not exist")
        definitions = {column["name"]: column for column in inspector.get_columns(table, schema="public")}
        available = set(definitions)
        if not columns.issubset(available):
            raise LoadRejected(f"public.{table} is missing required columns: {', '.join(sorted(columns - available))}")
        for name, expected in expected_types[table].items():
            actual = str(definitions[name]["type"]).upper().replace(" ", "")
            if actual != expected:
                raise LoadRejected(f"public.{table}.{name} has type {actual}; expected {expected}")
            if name not in ("date_key", "summary_key") and definitions[name]["nullable"]:
                raise LoadRejected(f"public.{table}.{name} must be NOT NULL")
        if table == "dim_date" and not definitions["date_key"].get("default"):
            raise LoadRejected("dim_date.date_key must be generated by its SERIAL sequence")
        if table == "fact_daily_summary" and not definitions["summary_key"].get("default"):
            raise LoadRejected("fact_daily_summary.summary_key must be generated by its SERIAL sequence")
        if table == "fact_daily_summary":
            for name in MONEY_COLUMNS:
                default = str(definitions[name].get("default") or "").lower().replace("::numeric", "").replace(" ", "")
                if default.strip("()'") != "0":
                    raise LoadRejected(f"fact_daily_summary.{name} must have a database default")
    if not inspector.get_pk_constraint("dim_date", schema="public").get("constrained_columns") == ["date_key"]:
        raise LoadRejected("dim_date.date_key must be the primary key")
    if not inspector.get_pk_constraint("fact_daily_summary", schema="public").get("constrained_columns") == ["summary_key"]:
        raise LoadRejected("fact_daily_summary.summary_key must be the primary key")
    dim_unique = {tuple(item.get("column_names") or ()) for item in inspector.get_unique_constraints("dim_date", schema="public")}
    fact_unique = {tuple(item.get("column_names") or ()) for item in inspector.get_unique_constraints("fact_daily_summary", schema="public")}
    if ("full_date",) not in dim_unique or ("date_key",) not in fact_unique:
        raise LoadRejected("Target requires UNIQUE dim_date.full_date and fact_daily_summary.date_key")
    foreign_keys = inspector.get_foreign_keys("fact_daily_summary", schema="public")
    if not any(fk.get("constrained_columns") == ["date_key"] and fk.get("referred_table") == "dim_date"
               and fk.get("referred_columns") == ["date_key"] for fk in foreign_keys):
        raise LoadRejected("fact_daily_summary.date_key must reference dim_date.date_key")
    permissions = connection.execute(text(
        "SELECT has_table_privilege(current_user, 'public.dim_date', 'SELECT'), "
        "has_table_privilege(current_user, 'public.dim_date', 'INSERT'), "
        "has_table_privilege(current_user, 'public.dim_date', 'UPDATE'), "
        "has_table_privilege(current_user, 'public.fact_daily_summary', 'SELECT'), "
        "has_table_privilege(current_user, 'public.fact_daily_summary', 'INSERT'), "
        "has_table_privilege(current_user, 'public.fact_daily_summary', 'UPDATE')"
    )).one()
    if not all(permissions):
        raise LoadRejected("Warehouse role needs SELECT, INSERT, and UPDATE on both target tables")
    return int(connection.execute(text("SELECT pg_database_size(current_database())")).scalar_one())


def _run_transaction(engine, dim: dict[str, Any], fact: dict[str, Any]) -> dict[str, Any]:
    counts = {"dim_date": {"inserted": 0, "updated": 0, "unchanged": 0},
              "fact_daily_summary": {"inserted": 0, "updated": 0, "unchanged": 0}}
    with engine.begin() as connection:
        size_before = _check_schema(connection)
        prior_dim = connection.execute(text(
            "SELECT date_key, day, month, year FROM public.dim_date WHERE full_date=:full_date FOR UPDATE"
        ), {"full_date": dim["full_date"]}).mappings().first()
        if prior_dim:
            date_key = prior_dim["date_key"]
            if (prior_dim["day"], prior_dim["month"], prior_dim["year"]) != (dim["day"], dim["month"], dim["year"]):
                raise LoadRejected("Existing date dimension attributes conflict with the handover")
            counts["dim_date"]["unchanged"] = 1
        else:
            inserted_key = connection.execute(text(
                "INSERT INTO public.dim_date (full_date, day, month, year) VALUES (:full_date, :day, :month, :year) "
                "ON CONFLICT (full_date) DO NOTHING RETURNING date_key"
            ), dim).scalar_one_or_none()
            if inserted_key is None:
                prior_dim = connection.execute(text(
                    "SELECT date_key, day, month, year FROM public.dim_date WHERE full_date=:full_date FOR UPDATE"
                ), {"full_date": dim["full_date"]}).mappings().one()
                if (prior_dim["day"], prior_dim["month"], prior_dim["year"]) != (dim["day"], dim["month"], dim["year"]):
                    raise LoadRejected("Concurrent date dimension row conflicts with the handover")
                date_key = prior_dim["date_key"]
                counts["dim_date"]["unchanged"] = 1
            else:
                date_key = inserted_key
                counts["dim_date"]["inserted"] = 1

        prior_fact = connection.execute(text(
            "SELECT total_sales, total_expenses, profit FROM public.fact_daily_summary WHERE date_key=:date_key FOR UPDATE"
        ), {"date_key": date_key}).mappings().first()
        values = {"date_key": date_key, **{column: fact[column] for column in MONEY_COLUMNS}}
        if prior_fact and all(Decimal(str(prior_fact[c])).quantize(Decimal("0.01")) == values[c] for c in MONEY_COLUMNS):
            counts["fact_daily_summary"]["unchanged"] = 1
        else:
            result = connection.execute(text(
                "INSERT INTO public.fact_daily_summary (date_key, total_sales, total_expenses, profit) "
                "VALUES (:date_key, :total_sales, :total_expenses, :profit) "
                "ON CONFLICT (date_key) DO UPDATE SET total_sales=EXCLUDED.total_sales, "
                "total_expenses=EXCLUDED.total_expenses, profit=EXCLUDED.profit "
                "RETURNING (xmax = 0) AS inserted"
            ), values).mappings().one()
            counts["fact_daily_summary"]["inserted" if result["inserted"] else "updated"] = 1

        date_rows = connection.execute(text(
            "SELECT count(*) FROM public.dim_date WHERE full_date=:full_date"
        ), {"full_date": dim["full_date"]}).scalar_one()
        fact_rows = connection.execute(text(
            "SELECT count(*) FROM public.fact_daily_summary WHERE date_key=:date_key"
        ), {"date_key": date_key}).scalar_one()
        loaded = connection.execute(text(
            "SELECT d.full_date, f.date_key, f.total_sales, f.total_expenses, f.profit "
            "FROM public.fact_daily_summary f JOIN public.dim_date d ON d.date_key=f.date_key "
            "WHERE f.date_key=:date_key"
        ), {"date_key": date_key}).mappings().first()
        if date_rows != 1 or fact_rows != 1 or not loaded:
            raise RuntimeError("Post-load row-count or referential-integrity check failed")
        if loaded["full_date"] != dim["full_date"]:
            raise RuntimeError("Post-load date reconciliation failed")
        for table, table_counts in counts.items():
            received_rows = 1
            completed_rows = sum(table_counts[key] for key in ("inserted", "updated", "unchanged"))
            if received_rows != completed_rows:
                raise RuntimeError(f"Record reconciliation failed for {table}")
        for column in MONEY_COLUMNS:
            if loaded[column] is None or Decimal(str(loaded[column])).quantize(Decimal("0.01")) != fact[column]:
                raise RuntimeError(f"Post-load amount reconciliation failed for {column}")
        db_size = connection.execute(text("SELECT pg_database_size(current_database())")).scalar_one()
        counts["warehouse_size_bytes_before"] = size_before
        counts["warehouse_size_bytes_after"] = int(db_size)
        counts["post_load_checks"] = {
            "record_reconciliation": "PASS", "row_count": "PASS", "primary_and_unique_keys": "PASS",
            "referential_integrity": "PASS", "required_values": "PASS", "amount_reconciliation": "PASS",
        }
        counts["duplicate_batch_check"] = {
            "result": "PASS",
            "detail": "Unique date keys and conflict-aware upserts make this batch replay idempotent.",
        }
    return counts


def load_batch(batch_id: str) -> dict[str, Any]:
    load_id = f"LOAD_{_now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    started = _now()
    artifact = OUTPUTS / batch_id / "load" / f"load_{load_id}.json"
    record: dict[str, Any] = {
        "stage": "load", "run_id": load_id, "batch_id": batch_id, "load_type": "INCREMENTAL",
        "load_start_ts": started.isoformat(), "status": "FAILED", "load_code_version": CODE_VERSION,
        "records_received": {}, "records_inserted": {}, "records_updated": {},
        "records_unchanged": {}, "records_rejected": {}, "pre_load_result": "FAIL",
        "post_load_result": "NOT_RUN", "validation_results": [], "error_code": None,
        "error_message": None, "target_tables": ["dim_date", "fact_daily_summary"], "tables": [],
    }
    engine = None
    try:
        source_path, handover = _load_handover(batch_id)
        received = handover.get("outputs", {})
        record["records_received"] = {
            table: len(received.get(table, [])) if isinstance(received.get(table), list) else 0
            for table in ("dim_date", "fact_daily_summary")
        }
        record["validation_results"].append({
            "check": "duplicate_batch_check", "result": "PASS",
            "previous_success_exists": _prior_success_exists(batch_id),
            "action": "apply idempotent conflict-aware writes",
        })
        record.update({
            "transformation_run_id": handover["run_id"],
            "extraction_run_id": handover["parent_run_id"],
            "batch_window": handover.get("batch_window"),
            "handover_path": str(source_path.relative_to(ROOT)),
        })
        transform_metadata = ROOT / "metadata" / f"transform_{handover['run_id']}.json"
        if transform_metadata.exists():
            try:
                record["batch_window"] = json.loads(transform_metadata.read_text(encoding="utf-8")).get("batch_window")
            except (OSError, json.JSONDecodeError):
                pass
        dim, fact = _validate_handover(handover, batch_id)
        record["pre_load_result"] = "PASS"
        record["validation_results"].append({"check": "approved_handover_completeness_and_amounts", "result": "PASS"})
        try:
            engine = get_warehouse_engine()
        except (EnvironmentError, ValueError) as exc:
            raise LoadRejected(str(exc)) from exc
        record["validation_results"].append({"check": "target_connection_permissions_schema_and_constraints", "result": "PENDING"})

        outcome = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                outcome = _run_transaction(engine, dim, fact)
                break
            except LoadRejected:
                raise
            except (OperationalError, DBAPIError) as exc:
                transient = isinstance(exc, OperationalError) or getattr(exc, "connection_invalidated", False)
                if attempt == MAX_ATTEMPTS or not transient:
                    raise
                delay = RETRY_DELAYS[attempt - 1]
                log.warning("Transient warehouse error; retry %s/%s in %ss", attempt + 1, MAX_ATTEMPTS, delay)
                time.sleep(delay)
        if outcome is None:
            raise RuntimeError("Warehouse transaction did not complete")
        record["validation_results"][-1]["result"] = "PASS"
        record["pre_load_result"] = "PASS"
        record["post_load_result"] = "PASS"
        record["validation_results"].extend(
            {"check": key, "result": value} for key, value in outcome.pop("post_load_checks").items()
        )
        duplicate_check = outcome.pop("duplicate_batch_check")
        record["validation_results"].append({"check": "duplicate_batch_idempotency", **duplicate_check})
        record["warehouse_size_bytes_before"] = outcome.pop("warehouse_size_bytes_before")
        record["warehouse_size_bytes_after"] = outcome.pop("warehouse_size_bytes_after")
        for table, table_counts in outcome.items():
            for verb in ("inserted", "updated", "unchanged"):
                record[f"records_{verb}"][table] = table_counts[verb]
            record["records_rejected"][table] = 0
            record["tables"].append({"table_name": table, **table_counts})
        record["status"] = "SUCCESS"
        log.info("Load succeeded batch=%s run_id=%s", batch_id, load_id)
    except Exception as exc:
        is_held = isinstance(exc, LoadHeld)
        record["status"] = "MANUAL_REVIEW" if is_held else "REJECTED" if isinstance(exc, LoadRejected) else "FAILED"
        if is_held:
            record["pre_load_result"] = "MANUAL REVIEW"
            record["error_message"] = str(exc)
            log.warning("Load held for manual review batch=%s: %s", batch_id, exc)
        else:
            record["error_code"] = type(exc).__name__
            record["error_message"] = str(exc)
        if record["validation_results"] and record["validation_results"][-1].get("result") == "PENDING":
            record["validation_results"][-1]["result"] = "FAIL"
            record["pre_load_result"] = "FAIL"
        if not is_held:
            record["post_load_result"] = "FAIL"
            if record["records_received"]:
                record["records_rejected"] = dict(record["records_received"])
            record["rejected_records"] = [{"batch_id": batch_id, "reason": str(exc)}]
            log.exception("Load failed batch=%s run_id=%s", batch_id, load_id)
    finally:
        if engine is not None:
            engine.dispose()
        ended = _now()
        record["load_end_ts"] = ended.isoformat()
        record["load_duration_sec"] = round((ended - started).total_seconds(), 3)
        if record["load_duration_sec"] > 60:
            record.setdefault("validation_results", []).append({
                "check": "load_duration_target_60_seconds", "result": "WARNING",
                "duration_sec": record["load_duration_sec"],
            })
            log.warning("Load exceeded the 60-second target (%.3fs)", record["load_duration_sec"])
        _write_json(artifact, record)
        log.info("Load result status=%s metadata=%s", record["status"], artifact)
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description="Load one approved daily Stage 2 batch into the warehouse")
    parser.add_argument("date", nargs="?", help="Business date YYYY-MM-DD (defaults to yesterday, Asia/Manila)")
    args = parser.parse_args()
    try:
        target = date.fromisoformat(args.date) if args.date else (_now().date() - timedelta(days=1))
        result = load_batch(f"BATCH_{target:%Y%m%d}")
    except (ValueError, LoadRejected) as exc:
        log.error("Load was not started: %s", exc)
        return 1
    print(json.dumps({k: result.get(k) for k in ("run_id", "batch_id", "status", "error_message", "load_duration_sec")}, indent=2))
    return 0 if result["status"] == "SUCCESS" else 2 if result["status"] == "MANUAL_REVIEW" else 1


if __name__ == "__main__":
    sys.exit(main())
