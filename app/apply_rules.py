# app/apply_rules.py
from __future__ import annotations
import time, argparse
from typing import List
from app.db import init_db, list_rules, list_quakes_since, add_alert
from app.rules import Rule, quake_matches_rule

def main():
    ap = argparse.ArgumentParser(description="Apply rules to stored quakes")
    ap.add_argument("--hours", type=float, default=24.0, help="How far back to scan (hours)")
    args = ap.parse_args()

    init_db()
    since_ms = int((time.time() - args.hours * 3600) * 1000)

    rules_dicts = list_rules()
    rules: List[Rule] = [Rule(**{**r, "id": r["id"]}) for r in rules_dicts]
    if not rules:
        print("No rules found. Add one with: python -m app.rules_cli add --name 'USA West 3+' --min-mag 3.0 --bbox '-125,32,-114,42'")
        return

    quakes = list_quakes_since(since_ms)
    print(f"Scanning {len(quakes)} quakes across {len(rules)} rules (since {args.hours}h)...")

    total_matches = 0
    for q in quakes:
        for r in rules:
            if quake_matches_rule(q, r):
                inserted = add_alert(q["id"], r.id, int(time.time() * 1000))
                if inserted:
                    total_matches += 1
                    print(f"[ALERT] Rule#{r.id}({r.name}) matched {q['id']}  M{q['mag']}  {q['place']}")

    print(f"Done. New alerts inserted: {total_matches}")

if __name__ == "__main__":
    main()
