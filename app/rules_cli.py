from __future__ import annotations
import argparse, json
from app.db import init_db, create_rule, list_rules, delete_rule

def main():
    p = argparse.ArgumentParser(description="Rules CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    addp = sub.add_parser("add", help="Add a rule")
    addp.add_argument("--name", required=True)
    addp.add_argument("--min-mag", type=float, required=True)
    addp.add_argument("--bbox", type=str, default=None, help="lon1,lat1,lon2,lat2")

    listp = sub.add_parser("list", help="List rules")

    delp = sub.add_parser("del", help="Delete rule")
    delp.add_argument("--id", type=int, required=True)

    args = p.parse_args()
    init_db()

    if args.cmd == "add":
        rid = create_rule(args.name, args.min_mag, args.bbox)
        print(f"Created rule #{rid}")
    elif args.cmd == "list":
        print(json.dumps(list_rules(), indent=2))
    elif args.cmd == "del":
        delete_rule(args.id)
        print(f"Deleted rule #{args.id}")

if __name__ == "__main__":
    main()
