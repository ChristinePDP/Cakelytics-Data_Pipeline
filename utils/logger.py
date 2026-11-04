import os
import logging
from datetime import datetime, timedelta

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y-%m-%d')}.log")

logger = logging.getLogger("CakelyticsETL")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    return logger.getChild(name)


def cleanup_old_logs(log_dir: str = LOG_DIR, retention_days: int = 90) -> int:
    """
    Automatically prune operational log files (*.log) older than retention_days.
    Safely preserves non-log files such as .gitkeep.
    """
    if not os.path.exists(log_dir):
        return 0

    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted_count = 0

    for file_name in os.listdir(log_dir):
        # Only evaluate log files; protect .gitkeep and other non-log entries
        if file_name.endswith(".log"):
            file_path = os.path.join(log_dir, file_name)
            try:
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_mtime < cutoff:
                    os.remove(file_path)
                    deleted_count += 1
            except OSError as err:
                logger.warning(f"Could not remove expired log {file_name}: {err}")

    if deleted_count > 0:
        logger.info(f"Automatic retention: pruned {deleted_count} log file(s) older than {retention_days} days.")

    return deleted_count


# Execute automatic cleanup upon logger module initialization
cleanup_old_logs(retention_days=90)
