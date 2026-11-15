"""Prefect orchestration for the CakeLytics daily ETL pipeline.

Run once (defaults to yesterday in Asia/Manila):
    python -m pipeline.etl_flow

Backfill a business date:
    python -m pipeline.etl_flow 2026-10-12

To serve a daily Prefect deployment, replace the __main__ invocation with:
    cakelytics_etl.serve(name="cakelytics-daily", cron="0 0 * * *")
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter

import pandas as pd
from prefect import flow, task

from config.source_db import check_source_env
from extract.extract_expenses import extract_inventory_costs, extract_waste
from extract.extract_sales import extract_sales
from load_stage import load_batch
from transform.quarantine import write_quarantine
from transform.reporting import (
    build_metadata,
    failed_metadata,
    profile_to_markdown,
    write_metadata,
    write_profile_report,
)
from transform.transform_batch import transform_batch
from utils.batch import PH_TZ, Batch, make_batch
from utils.logger import get_logger
from utils.stage_output import (
    dataframe_records,
    ensure_stage_directories,
    stage_directory,
    write_stage_output,
)

log = get_logger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def open_dashboard() -> None:
    """Start the desktop dashboard without opening a second console window."""
    python_executable = sys.executable
    options = {
        "cwd": PROJECT_ROOT,
        "close_fds": True,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if sys.platform == "win32":
        # pythonw.exe runs Tkinter without creating a command prompt window.
        gui_executable = Path(sys.executable).with_name("pythonw.exe")
        if gui_executable.exists():
            python_executable = str(gui_executable)
        options["creationflags"] = subprocess.CREATE_NO_WINDOW
    else:
        options["start_new_session"] = True
    subprocess.Popen([python_executable, "-m", "display.dashboard"], **options)


class PipelineRejected(RuntimeError):
    """The transformation rejected the batch; downstream loading must stop."""


@task(retries=2, retry_delay_seconds=60, name="Check source database")
def check_source() -> None:
    check_source_env()
    log.info("SOURCE CONNECTION | PASS")


@task(retries=2, retry_delay_seconds=60, name="Stage 1: Extract")
def extract_stage(batch: Batch) -> dict[str, pd.DataFrame]:
    """Extract all three source tables and write the Stage 1 handoff."""
    log.info("STAGE 1/3 START | EXTRACT | batch_id=%s", batch.batch_id)
    ensure_stage_directories(batch)
    try:
        extracted = {
            "order_items": extract_sales(batch.window_start, batch.window_end),
            "waste_logs": extract_waste(batch.window_start, batch.window_end),
            "inventory_logs": extract_inventory_costs(batch.window_start, batch.window_end),
        }
        row_counts = {name: len(frame) for name, frame in extracted.items()}
        output_path = write_stage_output(batch, "extract", batch.extraction_run_id, {
            "stage": "extract",
            "run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id,
            "batch_date": batch.batch_date,
            "window": [batch.window_start.isoformat(), batch.window_end.isoformat()],
            "status": "SUCCESS",
            "tables": {
                name: {"records_read": len(frame), "records": dataframe_records(frame)}
                for name, frame in extracted.items()
            },
        })
    except Exception as exc:
        write_stage_output(batch, "extract", batch.extraction_run_id, {
            "stage": "extract", "run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id, "batch_date": batch.batch_date,
            "status": "FAILED", "error_message": str(exc), "tables": {},
        })
        log.exception("STAGE 1/3 END | EXTRACT | FAILED | batch_id=%s", batch.batch_id)
        raise

    log.info("STAGE 1/3 END | EXTRACT | PASS | rows=%s | handoff=%s", row_counts, output_path)
    return extracted


@task(retries=0, name="Stage 2: Transform and validate")
def transform_stage(batch: Batch, extracted: dict[str, pd.DataFrame]) -> dict:
    """Transform, validate, publish artifacts, and return the acceptance decision."""
    log.info("STAGE 2/3 START | TRANSFORM | batch_id=%s", batch.batch_id)
    started = datetime.now(PH_TZ)
    handover_counts = {name: len(frame) for name, frame in extracted.items()}
    extract_path = str(
        Path(stage_directory(batch, "extract")) / f"extract_{batch.extraction_run_id}.json"
    )

    try:
        result = transform_batch(extracted, batch, handover_counts)
        quarantine_path = write_quarantine(result.quarantine, batch)
        profile_path = write_profile_report(
            profile_to_markdown(batch, result.profile, result.distributions, handover_counts), batch
        )
        metadata_path = write_metadata(build_metadata(result, handover_counts), batch)
        failed_records = result.quarantine[result.quarantine["category"].eq("REJECTED")].copy()
        output_path = write_stage_output(batch, "transform", batch.run_id, {
            "stage": "transform",
            "run_id": batch.run_id,
            "parent_run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id,
            "status": "SUCCESS" if result.decision == "APPROVED" else result.decision.replace(" ", "_"),
            "decision": result.decision,
            "handover_counts": handover_counts,
            "failed_record_count": len(failed_records),
            "failed_records": dataframe_records(failed_records),
            "quarantine": dataframe_records(result.quarantine),
            "input_extract_output": extract_path,
            "transformed_records": {
                name: dataframe_records(table.output_df)
                for name, table in result.tables.items()
            },
            "outputs": {name: dataframe_records(frame) for name, frame in result.outputs.items()},
        })
    except Exception as exc:
        ended = datetime.now(PH_TZ)
        write_metadata(failed_metadata(batch, handover_counts, started, ended, str(exc)), batch)
        write_stage_output(batch, "transform", batch.run_id, {
            "stage": "transform", "run_id": batch.run_id,
            "parent_run_id": batch.extraction_run_id, "batch_id": batch.batch_id,
            "status": "FAILED", "decision": "REJECTED", "error_message": str(exc),
            "handover_counts": handover_counts, "input_extract_output": extract_path,
        })
        log.exception("STAGE 2/3 END | TRANSFORM | FAILED | batch_id=%s", batch.batch_id)
        raise

    measures = {name: str(value) for name, value in result.measures.items()}
    log.info(
        "STAGE 2/3 END | TRANSFORM | decision=%s | sales=%s | expenses=%s | profit=%s | "
        "quarantine=%d | profile=%s | metadata=%s | handoff=%s",
        result.decision, measures.get("total_sales"), measures.get("total_expenses"),
        measures.get("profit"), len(result.quarantine), profile_path, metadata_path, output_path,
    )
    print(f"\nTRANSFORM DECISION: {result.decision} | batch={batch.batch_id}")
    for check in result.checks:
        print(f"  {check.check_id} {check.status:<8} {check.name} ({check.severity})")
    print(
        f"  sales={measures.get('total_sales')} expenses={measures.get('total_expenses')} "
        f"profit={measures.get('profit')} quarantine={len(result.quarantine)}"
    )
    print(f"  quarantine={quarantine_path}\n  profile={profile_path}\n  handoff={output_path}")
    return {"decision": result.decision, "measures": measures, "output_path": output_path}


@task(retries=0, name="Stage 3: Load warehouse summary")
def load_stage_task(batch_id: str) -> dict:
    """Load an approved handoff; surface loader failure as a failed Prefect task."""
    log.info("STAGE 3/3 START | LOAD | batch_id=%s", batch_id)
    result = load_batch(batch_id)
    status = result.get("status", "FAILED")
    log.info("STAGE 3/3 END | LOAD | status=%s | batch_id=%s", status, batch_id)
    print(f"LOAD RESULT: {status} | batch={batch_id}")
    if status != "SUCCESS":
        raise RuntimeError(result.get("error_message") or f"Warehouse load ended with status {status}")
    return result


@flow(name="CakeLytics Sales, Expense, and Profit ETL", log_prints=True)
def cakelytics_etl(target_date: str | None = None) -> dict:
    """Run Extract -> Transform -> Load for a date, defaulting to Manila yesterday.

    APPROVED advances to Load. MANUAL REVIEW holds without warehouse writes.
    REJECTED fails the flow and stops downstream tasks.
    """
    batch = make_batch(target_date)
    started = perf_counter()
    run_started = datetime.now(PH_TZ)
    log.info(
        "ETL RUN START | started_at=%s | batch_id=%s | window=%s to %s",
        run_started.isoformat(timespec="seconds"), batch.batch_id,
        batch.window_start.isoformat(), batch.window_end.isoformat(),
    )
    print(f"ETL START: batch={batch.batch_id} window={batch.window_start} to {batch.window_end}")

    try:
        check_source()
        extracted = extract_stage(batch)
        transformed = transform_stage(batch, extracted)
        decision = transformed["decision"]

        if decision == "REJECTED":
            print(f"ETL STOPPED: REJECTED | batch={batch.batch_id} | Load was not run")
            raise PipelineRejected(f"Transformation rejected {batch.batch_id}; load stopped")
        if decision == "MANUAL REVIEW":
            elapsed = round(perf_counter() - started, 2)
            print(f"ETL HELD: MANUAL REVIEW | batch={batch.batch_id} | Load was not run")
            log.warning(
                "ETL RUN END | status=MANUAL_REVIEW | batch_id=%s | duration_seconds=%.2f",
                batch.batch_id, elapsed,
            )
            return {
                "batch_id": batch.batch_id, "batch_date": batch.batch_date,
                "decision": decision, "status": "MANUAL_REVIEW",
                "duration_seconds": elapsed, "transform": transformed,
            }
        if decision != "APPROVED":
            raise RuntimeError(f"Unknown transformation decision {decision!r}; load stopped")

        print(f"DECISION APPROVED: advancing to warehouse Load | batch={batch.batch_id}")
        loaded = load_stage_task(batch.batch_id)
        try:
            open_dashboard()
            print("DASHBOARD: opened after successful warehouse load")
        except Exception:
            log.exception("Dashboard launch failed after successful warehouse load")
            print("DASHBOARD: could not open automatically; the warehouse load succeeded")
        elapsed = round(perf_counter() - started, 2)
        log.info("ETL RUN END | status=SUCCESS | batch_id=%s | duration_seconds=%.2f", batch.batch_id, elapsed)
        print(f"ETL COMPLETE: SUCCESS | batch={batch.batch_id} | duration={elapsed:.2f}s")
        return {
            "batch_id": batch.batch_id, "batch_date": batch.batch_date,
            "decision": decision, "status": "SUCCESS", "duration_seconds": elapsed,
            "transform": transformed, "load": loaded,
        }
    except Exception as exc:
        elapsed = round(perf_counter() - started, 2)
        log.exception(
            "ETL RUN END | status=FAILED | batch_id=%s | duration_seconds=%.2f",
            batch.batch_id, elapsed,
        )
        if not isinstance(exc, PipelineRejected):
            print(f"ETL FAILED: {type(exc).__name__}: {exc} | batch={batch.batch_id} | downstream stages stopped")
        raise


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    cakelytics_etl(date_arg)
