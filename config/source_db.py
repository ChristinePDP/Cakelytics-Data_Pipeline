"""Connection to the CakeLytics operational (SOURCE) database."""

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from dotenv import load_dotenv

load_dotenv()

# Reused across the whole process instead of creating a new engine (and a
# new pool of open connections) every time get_source_engine() is called.
# Creating one per call was leaking connections during long backfill runs
# and eventually exhausted the Supabase pooler's client connection limit.
_engine = None


def _database_url():
    url = os.getenv("SOURCE_DB_URL")
    if not url:
        raise EnvironmentError("Missing SOURCE_DB_URL in .env")
    return make_url(url).set(drivername="postgresql+pg8000")


def get_source_connection():
    return get_source_engine().raw_connection()


def get_source_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(_database_url(), pool_size=5, max_overflow=5, pool_pre_ping=True)
    return _engine


def check_source_env():
    get_source_connection().close()