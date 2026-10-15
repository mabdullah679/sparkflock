#!/usr/bin/env bash
# Run the ingest job.
#   submit.sh local  --mode once|stream     # natively on this Mac, single JVM, local[*]
#   submit.sh cluster --mode once|stream    # driver container sparkflock-spark-driver against the
#                                           # docker standalone master (standalone.sh up N)
# Extra spark-submit flags go in SPARK_SUBMIT_EXTRA (e.g. "--conf spark.cores.max=2").
set -euo pipefail
. "$(dirname "$0")/env.sh"
target="${1:?local|cluster}"; shift
case "$target" in
  local) ;;
  cluster)
    # In-network: MinIO/Postgres by service name (docker-compose.yml `spark-submit`), not .env's localhost.
    export SPARK_SUBMIT_EXTRA="${SPARK_SUBMIT_EXTRA:-}"
    exec docker compose -f "$ROOT/docker-compose.yml" --profile cluster \
      run --rm -T --name sparkflock-spark-driver spark-submit "$@" ;;
  *) echo "unknown target $target" >&2; exit 2 ;;
esac
# shellcheck disable=SC2086
exec "$SPARK_HOME/bin/spark-submit" --master "local[*]" --packages "$PACKAGES" \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.ui.showConsoleProgress=false ${SPARK_SUBMIT_EXTRA:-} \
  "$ROOT/spark/job.py" "$@"
