#!/usr/bin/env bash
# Run the ingest job natively on this Mac.
#   submit.sh local  --mode once|stream     # single JVM, local[*]
#   submit.sh cluster --mode once|stream    # against the standalone master (standalone.sh up N)
# Extra spark-submit flags go in SPARK_SUBMIT_EXTRA (e.g. "--conf spark.cores.max=2").
set -euo pipefail
. "$(dirname "$0")/env.sh"
target="${1:?local|cluster}"; shift
case "$target" in
  local)   master="local[*]" ;;
  cluster) master="spark://$SA_MASTER_HOST:$SA_MASTER_PORT" ;;
  *) echo "unknown target $target" >&2; exit 2 ;;
esac
# shellcheck disable=SC2086
exec "$SPARK_HOME/bin/spark-submit" --master "$master" --packages "$PACKAGES" \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.ui.showConsoleProgress=false ${SPARK_SUBMIT_EXTRA:-} \
  "$ROOT/spark/job.py" "$@"
