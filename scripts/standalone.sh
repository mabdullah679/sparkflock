#!/usr/bin/env bash
# Spark standalone cluster on this Mac: 1 master + N workers (2 cores / 2g each).
#   standalone.sh up N | down | status
# The master never creates workers itself. N is whatever you start here.
set -euo pipefail
. "$(dirname "$0")/env.sh"
RUN="$ROOT/.spark"; mkdir -p "$RUN/logs" "$RUN/pids" "$RUN/work"
CLASS="$SPARK_HOME/bin/spark-class"
start() { # name logfile -- command...
  local name=$1; shift
  nohup "$@" > "$RUN/logs/$name.log" 2>&1 &
  echo $! > "$RUN/pids/$name.pid"
}
case "${1:-}" in
  up)
    n="${2:-1}"
    # Each app copies a ~280 MB SDK jar into the worker dir; without cleanup 60 runs = 15 GB.
    export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=60 -Dspark.worker.cleanup.appDataTtl=120"
    start master "$CLASS" org.apache.spark.deploy.master.Master \
      --host "$SA_MASTER_HOST" --port "$SA_MASTER_PORT" --webui-port "$SA_MASTER_UI"
    sleep 3
    for i in $(seq 1 "$n"); do
      start "worker$i" "$CLASS" org.apache.spark.deploy.worker.Worker \
        --host "$SA_MASTER_HOST" --port $((27100 + i)) --webui-port $((28080 + i)) \
        --cores 2 --memory 2g --work-dir "$RUN/work$i" "spark://$SA_MASTER_HOST:$SA_MASTER_PORT"
    done
    sleep 4; "$0" status ;;
  down)
    for f in "$RUN"/pids/*.pid; do
      [ -e "$f" ] || continue
      kill "$(cat "$f")" 2>/dev/null || true
    done
    rm -f "$RUN"/pids/*.pid; sleep 1; rm -rf "$RUN"/work*; echo "standalone cluster stopped" ;;
  status)
    curl -s "http://$SA_MASTER_HOST:$SA_MASTER_UI/json/" | "$ROOT/.venv/bin/python" -c \
      'import sys,json;d=json.load(sys.stdin);print("workers:",len([w for w in d["workers"] if w["state"]=="ALIVE"]),"cores:",d["cores"],"apps:",len(d["activeapps"]))' ;;
  *) echo "usage: $0 up N | down | status" >&2; exit 2 ;;
esac
