"""
Batch window and run-ID helpers shared by the Extract and Transform stages.

A batch is ONE business day in Philippine time (Asia/Manila, UTC+8, no DST).
The source database stores timestamps in UTC, so the window is built with an
explicit +08:00 offset. That way an order placed at 7:00 AM in Manila lands
on the correct business day instead of the previous UTC day.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

PH_TZ = timezone(timedelta(hours=8), "Asia/Manila")


@dataclass(frozen=True)
class Batch:
    batch_id: str            # BATCH_20261012  (same id on every rerun of the same day)
    batch_date: str          # 2026-10-12      (business date, Asia/Manila)
    window_start: datetime   # 2026-10-12 00:00:00+08:00
    window_end: datetime     # 2026-10-12 23:59:59.999999+08:00
    extraction_run_id: str   # EXT_20261019_121602  (Stage 1 run, handed to Stage 2)
    run_id: str              # TRN_20261019_121602  (Stage 2 run)


def make_batch(target_date: str | None = None, now: datetime | None = None) -> Batch:
    """Build the batch for target_date (YYYY-MM-DD), or yesterday in Manila time."""
    now = now or datetime.now(PH_TZ)

    if target_date:
        day = datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        day = (now - timedelta(days=1)).date()

    start = datetime(day.year, day.month, day.day, tzinfo=PH_TZ)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    stamp = now.strftime("%Y%m%d_%H%M%S")

    return Batch(
        batch_id=f"BATCH_{day.strftime('%Y%m%d')}",
        batch_date=day.strftime("%Y-%m-%d"),
        window_start=start,
        window_end=end,
        extraction_run_id=f"EXT_{stamp}",
        run_id=f"TRN_{stamp}",
    )
