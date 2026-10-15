#!/usr/bin/env bash
# Spark standalone cluster in docker (compose project `sparkflock`, profile `cluster`):
# 1 master + N workers (2 cores / 2g each).
#   standalone.sh up N | down | status
# The master never creates workers itself. N is whatever you start here.
set -euo pipefail
. "$(dirname "$0")/env.sh"
DC=(docker compose -f "$ROOT/docker-compose.yml" --profile cluster)
alive() {
  curl -s "http://$SA_MASTER_HOST:$SA_MASTER_UI/json/" | "$ROOT/.venv/bin/python" -c \
    'import sys,json;print(len([w for w in json.load(sys.stdin)["workers"] if w["state"]=="ALIVE"]))' 2>/dev/null || echo 0
}
case "${1:-}" in
  up)
    n="${2:-1}"
    "${DC[@]}" up -d --wait --scale spark-worker="$n" spark-master spark-worker
    # Containers running is not workers registered: wait until the master reports N ALIVE.
    for _ in $(seq 1 60); do [ "$(alive)" = "$n" ] && break; sleep 1; done
    [ "$(alive)" = "$n" ] || { echo "only $(alive) of $n workers ALIVE after 60s" >&2; exit 1; }
    "$0" status ;;
  down)
    "${DC[@]}" rm -sf spark-master spark-worker >/dev/null 2>&1
    echo "standalone cluster stopped" ;;
  status)
    curl -s "http://$SA_MASTER_HOST:$SA_MASTER_UI/json/" | "$ROOT/.venv/bin/python" -c \
      'import sys,json;d=json.load(sys.stdin);print("workers:",len([w for w in d["workers"] if w["state"]=="ALIVE"]),"cores:",d["cores"],"apps:",len(d["activeapps"]))' ;;
  *) echo "usage: $0 up N | down | status" >&2; exit 2 ;;
esac
