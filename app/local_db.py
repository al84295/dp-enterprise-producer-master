"""Local demo backend: DuckDB file loaded from the sample CSVs.

Used only when MDM_BACKEND=local, so you can try the app without Azure.
The app's SQL uses :name parameters (Databricks style); DuckDB uses $name.
"""
import os
import re
from pathlib import Path

import duckdb
import pandas as pd

DATA_DIR = Path(os.getenv("MDM_SAMPLE_DIR", Path(__file__).resolve().parent.parent / "data"))
DB_PATH = os.getenv("MDM_LOCAL_DB", str(Path(__file__).resolve().parent / "local_mdm.duckdb"))

_DDL = {
    "producer_golden": """
        SELECT producer_id, producer_name, npn, email, phone, agency_name, segment, office,
               licensed_states, status, steward_notes, CAST(version AS INTEGER) AS version,
               updated_by, CAST(updated_at AS TIMESTAMP) AS updated_at
        FROM read_csv('{f}', all_varchar = true, header = true)""",
    "producer_source_records": """
        SELECT source_system, source_key, producer_name, npn, email, agency_name, matched_producer_id,
               CAST(match_confidence AS DOUBLE) AS match_confidence, match_status, reviewed_by,
               CAST(reviewed_at AS TIMESTAMP) AS reviewed_at, CAST(loaded_at AS TIMESTAMP) AS loaded_at
        FROM read_csv('{f}', all_varchar = true, header = true)""",
    "producer_change_log": """
        SELECT change_id, producer_id, action, field, old_value, new_value, reason, changed_by,
               CAST(changed_at AS TIMESTAMP) AS changed_at
        FROM read_csv('{f}', all_varchar = true, header = true)""",
}

_conn = None


def connect():
    global _conn
    if _conn is None:
        fresh = not Path(DB_PATH).exists()
        _conn = duckdb.connect(DB_PATH)
        if fresh:
            for table, select in _DDL.items():
                _conn.execute(f"CREATE TABLE {table} AS " + select.format(f=DATA_DIR / f"{table}.csv"))
    return _conn


def _convert(stmt, params):
    params = params or {}
    stmt = re.sub(r"(?<![:\w]):(\w+)", lambda m: "$" + m.group(1), stmt)
    used = set(re.findall(r"\$(\w+)", stmt))
    clean = {}
    for k, v in params.items():
        if k in used:
            if hasattr(v, "tzinfo") and v.tzinfo is not None:  # store UTC as naive TIMESTAMP
                v = v.replace(tzinfo=None)
            clean[k] = v
    return stmt, clean


def query(stmt, params=None):
    stmt, p = _convert(stmt, params)
    return connect().execute(stmt, p).df()


def execute(stmt, params=None):
    stmt, p = _convert(stmt, params)
    connect().execute(stmt, p)
