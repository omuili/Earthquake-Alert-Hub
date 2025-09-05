# app/db.py
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, Mapping, Any, List, Dict

DB_PATH = (Path(__file__).resolve().parent.parent / "quakes.db").as_posix()

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS quakes (
            id        TEXT PRIMARY KEY,
            time_ms   INTEGER NOT NULL,
            mag       REAL    NOT NULL,
            place     TEXT    NOT NULL,
            lon       REAL    NOT NULL,
            lat       REAL    NOT NULL,
            depth_km  REAL    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rules (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT NOT NULL,
            min_mag REAL NOT NULL,
            bbox    TEXT  -- 'lon1,lat1,lon2,lat2'
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            quake_id   TEXT NOT NULL,
            rule_id    INTEGER NOT NULL,
            created_ms INTEGER NOT NULL,
            FOREIGN KEY(quake_id) REFERENCES quakes(id),
            FOREIGN KEY(rule_id)  REFERENCES rules(id)
        );

        CREATE INDEX IF NOT EXISTS idx_quakes_time ON quakes(time_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_quakes_mag  ON quakes(mag DESC);

        -- prevent duplicate alerts for the same (quake, rule)
        CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_quake_rule ON alerts(quake_id, rule_id);
        """
    )
    conn.commit()
    conn.close()

def upsert_quake_record(q: Mapping[str, Any]) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT OR IGNORE INTO quakes(id, time_ms, mag, place, lon, lat, depth_km)
        VALUES (:id, :time_ms, :mag, :place, :lon, :lat, :depth_km)
        """,
        q,
    )
    conn.commit()
    conn.close()

def bulk_upsert_quakes(quakes: Iterable[Mapping[str, Any]]) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR IGNORE INTO quakes(id, time_ms, mag, place, lon, lat, depth_km)
        VALUES (:id, :time_ms, :mag, :place, :lon, :lat, :depth_km)
        """,
        list(quakes),
    )
    conn.commit()
    count = cur.rowcount if cur.rowcount is not None else 0
    conn.close()
    return count

def list_recent_quakes(limit: int = 20) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, time_ms, mag, place, lon, lat, depth_km FROM quakes ORDER BY time_ms DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def list_quakes_since(since_ms: int) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT id, time_ms, mag, place, lon, lat, depth_km
        FROM quakes
        WHERE time_ms >= ?
        ORDER BY time_ms DESC
        """,
        (since_ms,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------- rules ----------
def create_rule(name: str, min_mag: float, bbox: str | None) -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO rules(name, min_mag, bbox) VALUES (?,?,?)",
        (name, float(min_mag), bbox),
    )
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return int(rid)

def list_rules() -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("SELECT id, name, min_mag, bbox FROM rules ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def delete_rule(rule_id: int) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
    conn.commit()
    conn.close()

# ---------- alerts ----------
def add_alert(quake_id: str, rule_id: int, created_ms: int) -> bool:
    """
    Returns True if inserted; False if duplicate (same quake_id, rule_id).
    """
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO alerts(quake_id, rule_id, created_ms) VALUES (?,?,?)",
            (quake_id, rule_id, created_ms),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def list_alerts(limit: int = 50) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT a.id, a.quake_id, a.rule_id, a.created_ms,
               q.mag, q.place, q.time_ms, r.name as rule_name, r.min_mag, r.bbox
        FROM alerts a
        JOIN quakes q ON q.id = a.quake_id
        JOIN rules r  ON r.id = a.rule_id
        ORDER BY a.created_ms DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
