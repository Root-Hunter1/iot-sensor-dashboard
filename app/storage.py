"""SQLite storage: schema, batch writes, and read queries.

SQLite is used so the project runs with zero setup. The SQL below is
standard enough that moving to PostgreSQL later is mostly a driver change.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

from app.protocol import Reading

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT    NOT NULL,
    temp_c    REAL    NOT NULL,
    humidity  REAL    NOT NULL,
    ts_ms     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_device_ts ON readings (device_id, ts_ms);
"""


def connect(path: str) -> sqlite3.Connection:
    """Open a connection (one per process/thread) and make sure the schema exists."""
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    # WAL lets readers (the dashboard) run while the writer is inserting.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


def insert_many(conn: sqlite3.Connection, readings: Iterable[Reading]) -> int:
    rows = [(r.device_id, r.temp_c, r.humidity, r.ts_ms) for r in readings]
    if not rows:
        return 0
    with conn:  # one transaction per batch
        conn.executemany(
            "INSERT INTO readings (device_id, temp_c, humidity, ts_ms) VALUES (?, ?, ?, ?)",
            rows,
        )
    return len(rows)


def devices(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute("SELECT DISTINCT device_id FROM readings ORDER BY device_id")
    return [row["device_id"] for row in cur]


def latest_per_device(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        """
        SELECT r.device_id, r.temp_c, r.humidity, r.ts_ms
        FROM readings r
        JOIN (
            SELECT device_id, MAX(ts_ms) AS max_ts
            FROM readings
            GROUP BY device_id
        ) m ON r.device_id = m.device_id AND r.ts_ms = m.max_ts
        GROUP BY r.device_id
        ORDER BY r.device_id
        """
    )
    return [dict(row) for row in cur]


def history(
    conn: sqlite3.Connection, device_id: str, since_ms: int, limit: int = 2000
) -> list[dict]:
    """Return the newest `limit` readings since `since_ms`, oldest first."""
    cur = conn.execute(
        """
        SELECT ts_ms, temp_c, humidity
        FROM readings
        WHERE device_id = ? AND ts_ms >= ?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (device_id, since_ms, limit),
    )
    return [dict(row) for row in reversed(cur.fetchall())]
