#!/usr/bin/env bash
# Submit the ingest job to the minikube cluster with dynamic allocation.
#   MAX_EXECUTORS=4 k8s/submit.sh stream|once
# These env vars ARE the scaling configuration:
#   MIN_EXECUTORS (default 1)   floor: pods kept alive when idle
#   MAX_EXECUTORS (default 4)   ceiling: Spark never asks K8s for more
#   IDLE_TIMEOUT  (default 20s) how long an idle executor lives before removal
set -euo pipefail
. "$(dirname "$0")/../scripts/env.sh"
MODE="${1:-stream}"
CTX=sparkflock
API="$(kubectl --context $CTX config view --minify --context $CTX -o jsonpath='{.clusters[0].cluster.server}')"
IMAGE="${IMAGE:-sparkflock-spark:3.5.6}"
MIN="${MIN_EXECUTORS:-1}"; MAX="${MAX_EXECUTORS:-4}"; IDLE="${IDLE_TIMEOUT:-20s}"
HOST_SVC="host.minikube.internal"   # how a pod reaches MinIO/Postgres published on the Mac

kubectl --context $CTX -n spark delete pod sparkflock-driver --ignore-not-found --wait=true >/dev/null

unset PYSPARK_PYTHON   # the Mac venv path must not leak into the pods
exec "$SPARK_HOME/bin/spark-submit" \
  --master "k8s://$API" --deploy-mode cluster --name sparkflock-ingest \
  --conf spark.kubernetes.namespace=spark \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image="$IMAGE" \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.driver.pod.name=sparkflock-driver \
  --conf spark.kubernetes.submission.waitAppCompletion=$([ "$MODE" = once ] && echo true || echo false) \
  --conf spark.driver.memory=768m --conf spark.executor.memory=640m \
  --conf spark.executor.cores=1 --conf spark.kubernetes.executor.request.cores=0.5 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.timeout=30s \
  --conf spark.dynamicAllocation.minExecutors="$MIN" \
  --conf spark.dynamicAllocation.initialExecutors="$MIN" \
  --conf spark.dynamicAllocation.maxExecutors="$MAX" \
  --conf spark.dynamicAllocation.executorIdleTimeout="$IDLE" \
  --conf spark.dynamicAllocation.schedulerBacklogTimeout=1s \
  --conf spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=1s \
  --conf spark.kubernetes.driverEnv.SPARK_JARS_PACKAGES= \
  --conf spark.pyspark.python=python3 --conf spark.pyspark.driver.python=python3 \
  --conf spark.kubernetes.driverEnv.S3_ENDPOINT="http://$HOST_SVC:29000" \
  --conf spark.kubernetes.driverEnv.S3_BUCKET="$S3_BUCKET" \
  --conf spark.kubernetes.driverEnv.DB_HOST="$HOST_SVC" \
  --conf spark.kubernetes.driverEnv.DB_PORT=25432 \
  --conf spark.kubernetes.driverEnv.DB_NAME="$DB_NAME" \
  --conf spark.kubernetes.driverEnv.DB_USER="$DB_USER" \
  --conf spark.kubernetes.driverEnv.MAX_PARTITION_MB="$MAX_PARTITION_MB" \
  --conf spark.kubernetes.driverEnv.JDBC_PARTITIONS="$JDBC_PARTITIONS" \
  --conf spark.kubernetes.driverEnv.TRIGGER_SECONDS="$TRIGGER_SECONDS" \
  --conf spark.kubernetes.driver.secretKeyRef.S3_ACCESS_KEY=sparkflock-creds:S3_ACCESS_KEY \
  --conf spark.kubernetes.driver.secretKeyRef.S3_SECRET_KEY=sparkflock-creds:S3_SECRET_KEY \
  --conf spark.kubernetes.driver.secretKeyRef.DB_PASSWORD=sparkflock-creds:DB_PASSWORD \
  local:///opt/app/job.py --mode "$MODE"
