#!/usr/bin/env python3
"""Generate JSON-lines event files (one JSON message per line).

  gen_events.py --rows 1 --out data/one.json            # the "one JSON message" case
  gen_events.py --rows 1000000 --out data/big.json      # ~150 MB
  gen_events.py --rows 5 --bad 2 --out data/mixed.json  # 2 deliberately broken lines

event_id is deterministic (prefix + row number): generating the same file twice
produces the same ids, which is how the idempotency test works.
"""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone

TYPES = ["click", "view", "purchase", "refund", "login"]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--prefix", default="evt")
    p.add_argument("--bad", type=int, default=0, help="append N malformed lines")
    a = p.parse_args()

    rnd = random.Random(42)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with open(a.out, "w") as f:
        for i in range(a.rows):
            f.write(json.dumps({
                "event_id": f"{a.prefix}-{i:09d}",
                "user_id": rnd.randint(1, 50_000),
                "event_type": rnd.choice(TYPES),
                "amount": round(rnd.uniform(1, 500), 2),
                "ts": (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }) + "\n")
        for j in range(a.bad):
            # half not-JSON-at-all, half valid JSON missing event_id
            f.write("{this is not json" + "\n" if j % 2 == 0
                    else json.dumps({"user_id": 1, "event_type": "click"}) + "\n")
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
