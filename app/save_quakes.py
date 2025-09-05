# app/save_quakes.py
from __future__ import annotations
import sys, json
from app.usgs import fetch_quakes, FEEDS
from app.db import init_db, bulk_upsert_quakes

def main():
    feed = sys.argv[1] if len(sys.argv) > 1 else "all_hour"
    if feed in FEEDS:
        print(f"Using USGS feed key: {feed} -> {FEEDS[feed]}")
    else:
        print(f"Using custom URL: {feed}")

    init_db()
    quakes = fetch_quakes(feed)
    payload = [q.to_dict() for q in quakes]
    n = bulk_upsert_quakes(payload)

    print(f"Fetched {len(quakes)} quakes; upserted (attempted) {n}.")
    for q in payload[:3]:
        print(json.dumps(q, indent=2))

if __name__ == "__main__":
    main()
