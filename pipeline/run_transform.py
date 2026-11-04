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
from utils.stage_output import dataframe_records, ensure_stage_directories, write_stage_output

log = get_logger(__name__)

EXIT_CODES = {"APPROVED": 0, "MANUAL REVIEW": 2, "REJECTED": 1}


def run_transform_test(target_date: str | None = None) -> str:
    check_source_env()

    batch = make_batch(target_date)
    ensure_stage_directories(batch)
    started = datetime.now(PH_TZ)
    handover_counts: dict[str, int] = {}
    extract_output_path: str | None = None

    log.info(
        "TRANSFORM TEST START | run_id=%s | parent_run_id=%s | batch_id=%s | batch_window=%s to %s",
        batch.run_id, batch.extraction_run_id, batch.batch_id,
        batch.window_start.isoformat(), batch.window_end.isoformat(),
    )

    try:
        # ---- Stage 1: extraction (technical window filter only) ----
        extracted = {
            "order_items": extract_sales(batch.window_start, batch.window_end),
            "waste_logs": extract_waste(batch.window_start, batch.window_end),
            "inventory_logs": extract_inventory_costs(batch.window_start, batch.window_end),
        }
        handover_counts = {t: len(df) for t, df in extracted.items()}
        extract_output_path = write_stage_output(batch, "extract", batch.extraction_run_id, {
            "stage": "extract",
            "run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id,
            "batch_date": batch.batch_date,
            "window": [batch.window_start.isoformat(), batch.window_end.isoformat()],
            "status": "SUCCESS",
            "tables": {
                name: {"records_read": len(df), "records": dataframe_records(df)}
                for name, df in extracted.items()
            },
        })
        log.info(
            "EXTRACTION HANDOVER | extraction_run_id=%s | batch_id=%s | rows=%s",
            batch.extraction_run_id, batch.batch_id, handover_counts,
        )

        # ---- Stage 2: transformation ----
        result = transform_batch(extracted, batch, handover_counts)

        quarantine_path = write_quarantine(result.quarantine, batch)
        profile_path = write_profile_report(
            profile_to_markdown(batch, result.profile, result.distributions, handover_counts), batch
        )
        metadata_path = write_metadata(build_metadata(result, handover_counts), batch)
        output_path = write_stage_output(batch, "transform", batch.run_id, {
            "stage": "transform",
            "run_id": batch.run_id,
            "parent_run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id,
            "status": "SUCCESS" if result.decision == "APPROVED" else result.decision.replace(" ", "_"),
            "decision": result.decision,
            "handover_counts": handover_counts,
            "failed_records": int(result.quarantine["category"].eq("REJECTED").sum()),
            "quarantine": dataframe_records(result.quarantine),
            "input_extract_output": extract_output_path,
            "transformed_records": {
                name: dataframe_records(table.output_df)
                for name, table in result.tables.items()
            },
            "outputs": {
                name: dataframe_records(df) for name, df in result.outputs.items()
            },
        })

    except Exception as exc:
        ended = datetime.now(PH_TZ)
        log.exception("TRANSFORM TEST END | run_id=%s | status=FAILED", batch.run_id)
        if extract_output_path is None:
            extract_output_path = write_stage_output(batch, "extract", batch.extraction_run_id, {
                "stage": "extract",
                "run_id": batch.extraction_run_id,
                "batch_id": batch.batch_id,
                "batch_date": batch.batch_date,
                "status": "FAILED",
                "error_message": str(exc),
                "tables": {},
            })
        write_metadata(failed_metadata(batch, handover_counts, started, ended, str(exc)), batch)
        output_path = write_stage_output(batch, "transform", batch.run_id, {
            "stage": "transform",
            "run_id": batch.run_id,
            "parent_run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id,
            "status": "FAILED",
            "decision": "REJECTED",
            "error_message": str(exc),
            "handover_counts": handover_counts,
            "input_extract_output": extract_output_path,
        })
        print(f"\nRESULT: REJECTED — the run failed ({exc}). See the log.")
        print(f"JSON output: {output_path}")
        return "REJECTED"

    # ---- screen summary ----
    print(f"\nTRANSFORM TEST for {batch.batch_date}  ({batch.batch_id})")
    print(f"Extraction run: {batch.extraction_run_id}   Transformation run: {batch.run_id}\n")
    print(f"{'table':<16}{'read':>6}{'output':>8}{'rejected':>10}{'excluded':>10}")
    for t, r in result.tables.items():
        print(f"{t:<16}{r.records_read:>6}{r.records_output:>8}{r.records_rejected:>10}{r.records_excluded:>10}")

    m = result.measures
    print(f"\nTotals: total_sales={m['total_sales']}  total_expenses={m['total_expenses']}  "
          f"profit={m['profit']}  (items sold: {m['total_items_sold']})")

    print("\nValidation checks")
    for c in result.checks:
        print(f"  {c.check_id}  {c.status:<8} {c.name} ({c.severity})")

    print(f"\nQuarantine : {quarantine_path}  ({len(result.quarantine)} record(s))")
    print(f"Profile    : {profile_path}")
    print(f"Metadata   : {metadata_path}")
    print(f"JSON output: {output_path}")
    print(f"\nDECISION: {result.decision}")
    return result.decision


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(EXIT_CODES[run_transform_test(date_arg)])
