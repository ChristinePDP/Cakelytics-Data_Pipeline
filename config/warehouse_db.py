"""Connection helpers for the separate CakeLytics analytics warehouse."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def get_warehouse_url(readonly: bool = False):
    """Return the warehouse URL, preferring the read-only URL when requested."""
    variable = "WAREHOUSE_READONLY_DB_URL" if readonly else "WAREHOUSE_DB_URL"
    raw = os.getenv(variable)
    if readonly and not raw:
        variable = "WAREHOUSE_DB_URL"
        raw = os.getenv(variable)
    if not raw:
        raise EnvironmentError(f"Missing {variable}; configure the separate warehouse project")

    target = make_url(raw)
    source_raw = os.getenv("SOURCE_DB_URL")
    if source_raw:
        source = make_url(source_raw)
        if (target.host, target.port, target.username) == (source.host, source.port, source.username):
            raise EnvironmentError(f"{variable} points to the source database; use the separate warehouse project")
    return target.set(drivername="postgresql+pg8000")


def get_warehouse_engine(readonly: bool = False):
    """Create an engine for the warehouse (optionally using read-only credentials)."""
    options = {"pool_pre_ping": True}
    if readonly:
        options["connect_args"] = {"timeout": 10}
    return create_engine(get_warehouse_url(readonly=readonly), **options)
