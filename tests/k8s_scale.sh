#!/usr/bin/env bash
# Proves Spark-on-K8s scaling with the real cluster: executor PODS appear when a
# big file lands and disappear when idle, bounded by MAX_EXECUTORS.
#   tests/k8s_scale.sh 4      # ceiling 4
#   tests/k8s_scale.sh 2      # ceiling 2  (same file, different config)
set -uo pipefail
. "$(dirname "$0")/../scripts/env.sh"
MAX="${1:-4}"; ROWS="${BIG_ROWS:-600000}"; FAILS=0
K() { kubectl --context sparkflock -n spark "$@"; }
S3="aws --endpoint-url $S3_ENDPOINT"
check() { if [ "$2" = "$3" ]; then echo "  PASS  $1 (= $3)"; else echo "  FAIL  $1: expected $2, got $3"; FAILS=$((FAILS+1)); fi; }
execs() { K get pods -l spark-role=executor --no-headers 2>/dev/null | grep -c -E "Running|Pending|ContainerCreating" || true; }
cd "$ROOT"

echo "== K8s scaling test, MAX_EXECUTORS=$MAX"
scripts/reset.sh >/dev/null
python3 scripts/gen_events.py --rows "$ROWS" --prefix "k$MAX" --out "data/k8s_big_$MAX.json" >/dev/null
MAX_EXECUTORS=$MAX k8s/submit.sh stream > .spark/k8s_submit_$MAX.log 2>&1

t0=$SECONDS
until K logs sparkflock-driver 2>/dev/null | grep -q "^watching"; do
  [ $((SECONDS-t0)) -gt 240 ] && { echo "  FAIL driver never started"; K logs sparkflock-driver | tail -20; exit 1; }
  sleep 3
done
echo "  driver up after $((SECONDS-t0))s; executors idle = $(execs)"
sleep 8
BASE=$(execs)

: > .spark/pods_$MAX.txt
( while true; do echo "$(date +%s) $(execs)" >> .spark/pods_$MAX.txt; sleep 2; done ) & SAMPLER=$!

$S3 s3 cp "data/k8s_big_$MAX.json" "s3://$S3_BUCKET/incoming/big.json" --quiet
t1=$SECONDS
while [ "$(psql_q 'select count(*) from events')" -lt "$ROWS" ]; do
  [ $((SECONDS-t1)) -gt 300 ] && break; sleep 2
done
echo "  ingest of $ROWS rows took $((SECONDS-t1))s"
PEAK=$(sort -k2 -n .spark/pods_$MAX.txt | tail -1 | cut -d' ' -f2)
check "rows landed" "$ROWS" "$(psql_q 'select count(*) from events')"
check "executors before the file" "1" "$BASE"
check "peak executor pods (ceiling is $MAX)" "$MAX" "$PEAK"

t2=$SECONDS
while [ "$(execs)" -gt 1 ] && [ $((SECONDS-t2)) -lt 180 ]; do sleep 3; done
check "scaled back down to minExecutors" "1" "$(execs)"
echo "  scale-down took $((SECONDS-t2))s after ingest finished"
echo "$(date +%s) $(execs)" >> .spark/pods_$MAX.txt
kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null
echo "  timeline (sec since upload : pods):"
awk -v t=$(head -1 .spark/pods_$MAX.txt | cut -d' ' -f1) '{ if ($2!=p) printf "    t+%3ds  %s executors\n", $1-t, $2; p=$2 }' .spark/pods_$MAX.txt
K logs sparkflock-driver | grep "^\[batch"
echo "  -- pretty-printed message through the pods (Python 3.8 runtime) --"
cat > data/k8s_pretty.json <<'JSON'
{
  "event_id": "k8s-pretty-001",
  "user_id": 7,
  "event_type": "purchase",
  "amount": 19.99,
  "ts": "2026-10-12T08:00:00Z"
}
JSON
$S3 s3 cp data/k8s_pretty.json "s3://$S3_BUCKET/incoming/k8s_pretty.json" --quiet
t3=$SECONDS
while [ "$(psql_q "select count(*) from events where event_id='k8s-pretty-001'")" != "1" ] && [ $((SECONDS-t3)) -lt 90 ]; do sleep 2; done
check "pretty-printed message ingested by the K8s driver" "1" "$(psql_q "select count(*) from events where event_id='k8s-pretty-001'")"
check "and produced no rejects" "0" "$(psql_q 'select count(*) from events_rejects')"
K delete pod sparkflock-driver --wait=false >/dev/null
[ $FAILS -eq 0 ] && echo "ALL PASS" || { echo "$FAILS FAILED"; exit 1; }
