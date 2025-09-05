# app/main.py
from __future__ import annotations
import time, json, sys, subprocess  # <-- added sys, subprocess
from pathlib import Path
from typing import List, Dict, Optional

from fastapi import FastAPI, Request, Form, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from app.db import (
    init_db, get_conn, create_rule, list_rules, bulk_upsert_quakes,
    add_alert
)
from app.rules import Rule, quake_matches_rule
from app.usgs import fetch_quakes
from app.events import bus

app = FastAPI(title="Earthquake Alert Hub")

# robust paths
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent  # <-- tests live here
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

init_db()

# ---------- metrics ----------
INGEST_COUNT   = Counter("quakes_ingested_total", "Total quakes ingested")
ALERT_COUNT    = Counter("alerts_emitted_total",  "Total alerts emitted")
LAST_INGEST_TS = Gauge(  "last_ingest_timestamp",  "Last ingest epoch millis")
INGEST_LATENCY = Histogram("ingest_duration_seconds", "Ingest duration")

def get_daily_report(limit_days: int = 7) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT date(time_ms/1000,'unixepoch') AS day,
               COUNT(*) AS n,
               ROUND(AVG(mag),2) AS avg_mag,
               ROUND(MAX(mag),2) AS max_mag
        FROM quakes
        GROUP BY day
        ORDER BY day DESC
        LIMIT ?
        """,
        (limit_days,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def list_recent_alerts(limit: int = 25) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
          a.id, a.created_ms,
          datetime(a.created_ms/1000, 'unixepoch', 'localtime') AS created_at,
          a.quake_id, a.rule_id,
          q.time_ms, q.mag, q.place, q.lon, q.lat, q.depth_km,
          r.name AS rule_name, r.min_mag, r.bbox
        FROM alerts a
        JOIN quakes q ON q.id = a.quake_id
        JOIN rules  r ON r.id = a.rule_id
        ORDER BY a.created_ms DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "rules": list_rules(), "report": get_daily_report(7), "alerts": list_recent_alerts(25)},
    )

@app.post("/rules")
def add_rule_endpoint(name: str = Form(...), min_mag: float = Form(...), bbox: Optional[str] = Form(None)):
    rid = create_rule(name, float(min_mag), bbox if bbox else None)
    return {"created": rid}

@app.get("/rules")
def get_rules():
    return list_rules()

@app.post("/ingest")
def ingest(feed: str = Form("all_hour")):
    start = time.time()
    quakes = [q.to_dict() for q in fetch_quakes(feed)]
    bulk_upsert_quakes(quakes)
    INGEST_COUNT.inc(len(quakes))

    rules = [Rule(**{**r, "id": r["id"]}) for r in list_rules()]
    now_ms = int(time.time() * 1000)
    alerts = []

    for q in quakes:
        for r in rules:
            if quake_matches_rule(q, r):
                if add_alert(q["id"], r.id, now_ms):
                    bus.publish({"type": "QuakeDetected", "rule": {"id": r.id, "name": r.name}, "quake": q})
                    ALERT_COUNT.inc()
                    alerts.append({"quake_id": q["id"], "rule_id": r.id, "mag": q["mag"], "place": q["place"]})

    LAST_INGEST_TS.set(int(time.time() * 1000))
    INGEST_LATENCY.observe(time.time() - start)
    bus.publish({"type": "IngestCompleted", "feed": feed, "ingested": len(quakes), "alerts": len(alerts)})

    return JSONResponse({"ingested": len(quakes), "alerts": alerts})

@app.get("/reports/daily")
def reports_daily():
    return get_daily_report(7)

@app.get("/alerts")
def get_alerts():
    return list_recent_alerts(50)

# ---------- events ----------
@app.get("/events/tail")
def events_tail(n: int = 50):
    return bus.tail(n)

@app.get("/events/stream")
def events_stream():
    def gen():
        last = None
        while True:
            events = bus.tail(1)
            if events and events[-1] is not last:
                last = events[-1]
                yield "data: " + json.dumps(last) + "\n\n"
            time.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.post("/events/test")
def events_test():
    bus.publish({"type": "TestEvent", "message": "Hello from /events/test"})
    return {"published": True}

# ---------- run tests ----------
@app.post("/run-tests")
def run_tests():
    """
    Launch pytest in a subprocess so the web server stays alive.
    Requires pytest.ini with `pythonpath = .` so imports work.
    """
    cmd = [sys.executable, "-m", "pytest", "-q", "--maxfail=1", "--disable-warnings", "--color=no"]
    try:
        proc = subprocess.run(
            cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=180
        )
        ok = proc.returncode == 0
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        # keep payload small
        tail = "\n".join((stdout + ("\n" + stderr if stderr else "")).splitlines()[-200:])
        # publish an event for the UI stream
        bus.publish({"type": "TestsRun", "ok": ok})
        return {"ok": ok, "returncode": proc.returncode, "output": tail}
    except subprocess.TimeoutExpired:
        bus.publish({"type": "TestsRun", "ok": False})
        return JSONResponse({"ok": False, "error": "timeout running tests"}, status_code=500)

# ---------- metrics ----------
@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
