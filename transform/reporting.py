"""
TRANSFORM — Reports and run metadata

  * profiling report   (Markdown, same columns as the documentation, Section 2)
  * run metadata       (JSON: run_id, parent_run_id, batch window, counts per
                        table, rules applied, validation results, errors, duration)
  * rules table helper (Markdown generated from the rule registry)
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd

from transform.rules import CODE_VERSION, Rule
from utils.batch import Batch

ROOT = os.path.dirname(os.path.dirname(__file__))
REPORTS_DIR = os.path.join(ROOT, "reports")
LOGS_DIR = os.path.join(ROOT, "logs")


def _cell(value) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _md_table(headers: list[str], rows: list[list]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(_cell(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def profile_to_markdown(batch: Batch, rows: list[dict], distributions: dict[str, pd.DataFrame],
                        handover_counts: dict[str, int]) -> str:
    out = [
        f"# Data Profiling Report — {batch.batch_id}",
        "",
        f"- Extraction run ID (parent_run_id): `{batch.extraction_run_id}`",
        f"- Transformation run ID: `{batch.run_id}`",
        f"- Batch window: `{batch.window_start.isoformat()}` to `{batch.window_end.isoformat()}`",
        "- Records received: " + ", ".join(f"{t} = {n}" for t, n in handover_counts.items()),
        "",
        "## Issues found (paste into Section 2 of the documentation)",
        "",
        _md_table(
            ["Table", "Column", "Issue Type", "Example", "Affected Records", "Planned Treatment"],
            [[r["table"], r["column"], f'{r["issue_type"]}: {r["issue"]}', r["example"],
              f'{r["affected"]} of {r["total"]} ({r["pct"]:.1f}%)', r["treatment"]] for r in rows],
        ),
        "",
        "## Value distributions",
    ]
    if not distributions:
        out += ["", "No records in this batch."]
    for name, df in distributions.items():
        out += ["", f"### {name}", "", _md_table(list(df.columns), df.values.tolist())]
    return "\n".join(out) + "\n"


def write_profile_report(text: str, batch: Batch) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, f"profile_{batch.batch_id}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def build_metadata(result, handover_counts: dict[str, int], error: str | None = None) -> dict:
    """result is a TransformResult (see transform_batch.py)."""
    batch = result.batch
    checks = [{"check_id": c.check_id, "name": c.name, "severity": c.severity,
               "status": c.status, "detail": c.detail} for c in result.checks]

    tables = []
    for t, r in result.tables.items():
        tables.append({
            "run_id": batch.run_id,
            "parent_run_id": batch.extraction_run_id,
            "batch_id": batch.batch_id,
            "batch_window": [batch.window_start.isoformat(), batch.window_end.isoformat()],
            "table_name": t,
            "started_at": r.started_at.isoformat(timespec="seconds"),
            "ended_at": r.ended_at.isoformat(timespec="seconds"),
            "duration_seconds": round((r.ended_at - r.started_at).total_seconds(), 3),
            "records_read": r.records_read,
            "records_output": r.records_output,
            "records_rejected": r.records_rejected,
            "records_excluded": r.records_excluded,
            "rules_applied": r.rules_applied,
            "status": "SUCCESS",
        })

    return {
        "run_id": batch.run_id,
        "parent_run_id": batch.extraction_run_id,
        "batch_id": batch.batch_id,
        "batch_window": [batch.window_start.isoformat(), batch.window_end.isoformat()],
        "code_version": CODE_VERSION,
        "started_at": result.started_at.isoformat(timespec="seconds"),
        "ended_at": result.ended_at.isoformat(timespec="seconds"),
        "duration_seconds": round((result.ended_at - result.started_at).total_seconds(), 3),
        "decision": result.decision,
        "status": "FAILED" if error else "SUCCESS",
        "error_message": error,
        "handover_counts": handover_counts,
        "measures": {k: str(v) for k, v in result.measures.items()},
        "tables": tables,
        "validation_results": checks,
    }


def write_metadata(metadata: dict, batch: Batch) -> str:
    os.makedirs(LOGS_DIR, exist_ok=True)
    path = os.path.join(LOGS_DIR, f"transform_{batch.run_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    return path


def failed_metadata(batch: Batch, handover_counts: dict[str, int], started: datetime,
                    ended: datetime, error: str) -> dict:
    return {
        "run_id": batch.run_id,
        "parent_run_id": batch.extraction_run_id,
        "batch_id": batch.batch_id,
        "batch_window": [batch.window_start.isoformat(), batch.window_end.isoformat()],
        "code_version": CODE_VERSION,
        "started_at": started.isoformat(timespec="seconds"),
        "ended_at": ended.isoformat(timespec="seconds"),
        "duration_seconds": round((ended - started).total_seconds(), 3),
        "decision": "REJECTED",
        "status": "FAILED",
        "error_message": error,
        "handover_counts": handover_counts,
    }


def rules_table_markdown(rules_by_table: dict[str, list[Rule]], prefix: str) -> str:
    """Rules whose ID starts with prefix (C-, S-, B-), merged across tables."""
    merged: dict[str, dict] = {}
    for table, rules in rules_by_table.items():
        for r in rules:
            if not r.rule_id.startswith(prefix):
                continue
            m = merged.setdefault(r.rule_id, {"name": r.name, "targets": [], "conditions": [],
                                              "treatments": [], "justification": r.justification})
            m["targets"] += [f"{table}.{c.strip()}" for c in r.column.split(", ")]
            for key, val in (("conditions", r.condition), ("treatments", r.treatment)):
                if val not in m[key]:
                    m[key].append(val)
    rows = [[rid, m["name"], ", ".join(m["targets"]), "; ".join(m["conditions"]),
             "; ".join(m["treatments"]), m["justification"]] for rid, m in sorted(merged.items())]
    return _md_table(["Rule ID", "Name", "Target", "Condition", "Action", "Justification"], rows)
