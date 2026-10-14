#!/usr/bin/env bash
# Wipe pipeline state: incoming files, Spark checkpoint, all tables. Dev only.
set -uo pipefail
. "$(dirname "$0")/env.sh"
S3="aws --endpoint-url $S3_ENDPOINT"
$S3 s3 rm "s3://$S3_BUCKET/incoming/" --recursive --quiet 2>/dev/null
$S3 s3 rm "s3://$S3_BUCKET/_checkpoints/" --recursive --quiet 2>/dev/null
psql_q "TRUNCATE events, events_rejects, events_stage, events_rejects_stage" >/dev/null 2>&1 || true
echo "reset done"
