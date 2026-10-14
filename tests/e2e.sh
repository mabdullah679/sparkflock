#!/usr/bin/env bash
# End-to-end proof against the REAL stack (MinIO + Postgres + Spark standalone).
# Each check prints PASS/FAIL; exit code is non-zero if any FAIL.
#   tests/e2e.sh            all stages
#   BIG_ROWS=600000 tests/e2e.sh
set -uo pipefail
. "$(dirname "$0")/../scripts/env.sh"
BIG_ROWS="${BIG_ROWS:-600000}"
FAILS=0
want() { [[ " ${STAGES:-0 1 2 3 4 5} " == *" $1 "* ]]; }
S3="aws --endpoint-url $S3_ENDPOINT"
check() { # name expected actual
  if [ "$2" = "$3" ]; then echo "  PASS  $1 (= $3)"; else echo "  FAIL  $1: expected $2, got $3"; FAILS=$((FAILS+1)); fi
}
reset() { "$ROOT/scripts/reset.sh" >/dev/null; }
n_events()  { psql_q "SELECT count(*) FROM events"; }
n_rejects() { psql_q "SELECT count(*) FROM events_rejects"; }
wait_rows() { # expected timeout_s -> echoes seconds waited or TIMEOUT
  local t0=$SECONDS
  while [ $((SECONDS - t0)) -lt "$2" ]; do
    [ "$(n_events)" -ge "$1" ] && { echo $((SECONDS - t0)); return; }
    sleep 1
  done; echo TIMEOUT
}
summary_field() { grep SUMMARY "$1" | tail -1 | sed 's/^SUMMARY //' | .venv/bin/python -c "import sys,json;print(json.load(sys.stdin)['$2'])"; }
mkdir -p "$ROOT/.spark" "$ROOT/data"
cd "$ROOT"

if want 0; then
echo "== 0. stack reachable"
check "minio bucket exists" "ok" "$($S3 s3 ls "s3://$S3_BUCKET" >/dev/null 2>&1 && echo ok || echo missing)"
check "postgres reachable"  "1"  "$(psql_q 'select 1')"
scripts/standalone.sh down >/dev/null 2>&1; reset
fi

if want 1; then
echo "== 1. AUTOMATIC: streaming job running, then a file is uploaded, nobody runs anything"
scripts/standalone.sh up 1
scripts/submit.sh cluster --mode stream > .spark/stream.log 2>&1 &
STREAM_PID=$!
sleep 25   # let the driver register and the first (empty) trigger pass
check "rows before upload" "0" "$(n_events)"
python3 scripts/gen_events.py --rows 1 --prefix auto --out data/one.json >/dev/null
$S3 s3 cp data/one.json "s3://$S3_BUCKET/incoming/one.json" --quiet
w="$(wait_rows 1 60)"
check "row appeared without any manual step (latency ${w}s)" "1" "$(n_events)"
pkill -P $STREAM_PID 2>/dev/null; kill $STREAM_PID 2>/dev/null; sleep 3; pkill -f "spark/job.py" 2>/dev/null || true
fi

if want 2; then
echo "== 2. IDEMPOTENT: same event_ids uploaded again under a new file name"
cp data/one.json data/one_again.json
$S3 s3 cp data/one_again.json "s3://$S3_BUCKET/incoming/one_again.json" --quiet
scripts/submit.sh cluster --mode once > .spark/once.log 2>&1
check "still 1 row after replay" "1" "$(n_events)"
check "replay was its own batch, 1 row in / 0 inserted / 1 skipped" "1" "$(grep -c 'rows_in=1 inserted=0 dup_skipped=1' .spark/once.log)"
fi

if want 3; then
echo "== 3. BAD DATA: 5 good + 2 broken lines"
python3 scripts/gen_events.py --rows 5 --bad 2 --prefix mixed --out data/mixed.json >/dev/null
$S3 s3 cp data/mixed.json "s3://$S3_BUCKET/incoming/mixed.json" --quiet
scripts/submit.sh cluster --mode once > .spark/once.log 2>&1
check "events = 1 + 5" "6" "$(n_events)"
check "rejects = 2 (job did not crash)" "2" "$(n_rejects)"
check "reject reasons" "missing_event_id,unparseable_json" "$(psql_q "select string_agg(distinct reason, ',' order by reason) from events_rejects")"

echo "== 3b. PRETTY-PRINTED: one message over many lines, and a multi-line JSON array"
cat > data/pretty.json <<'JSON'
{
  "event_id": "pretty-001",
  "user_id": 7,
  "event_type": "purchase",
  "amount": 19.99,
  "ts": "2026-10-12T08:00:00Z"
}
JSON
cat > data/pretty_array.json <<'JSON'
[
  {"event_id": "arr-1", "user_id": 1, "event_type": "click", "amount": 1.5, "ts": "2026-10-12T08:00:00Z"},
  {"event_id": "arr-2", "user_id": 2, "event_type": "view",  "amount": 2.5, "ts": "2026-10-12T08:00:01Z"}
]
JSON
$S3 s3 cp data/pretty.json "s3://$S3_BUCKET/incoming/pretty.json" --quiet
$S3 s3 cp data/pretty_array.json "s3://$S3_BUCKET/incoming/pretty_array.json" --quiet
scripts/submit.sh cluster --mode once > .spark/once.log 2>&1
check "events = 6 + 1 object + 2 array items" "9" "$(n_events)"
check "pretty files produced no new rejects" "2" "$(n_rejects)"
fi

if want 4; then
echo "== 4. SCALING: ${BIG_ROWS}-row file, 1 worker, master must NOT add workers by itself"
python3 scripts/gen_events.py --rows "$BIG_ROWS" --prefix bigA --out data/big_a.json >/dev/null
SZ=$(du -m data/big_a.json | cut -f1); echo "  file size: ${SZ} MB"
before=$(n_events)
$S3 s3 cp data/big_a.json "s3://$S3_BUCKET/incoming/big_a.json" --quiet
scripts/submit.sh cluster --mode once > .spark/big1.log 2>&1
check "all rows landed" "$((before + BIG_ROWS))" "$(n_events)"
check "executors used with 1 worker" "1" "$(summary_field .spark/big1.log max_executors)"
check "workers still alive" "workers: 1 cores: 2 apps: 0" "$(scripts/standalone.sh status)"
grep "batch" .spark/big1.log
fi

if want 5; then
echo "== 5. SCALING: add a 2nd worker by hand, same size file with fresh ids"
scripts/standalone.sh down >/dev/null; sleep 2; scripts/standalone.sh up 2
python3 scripts/gen_events.py --rows "$BIG_ROWS" --prefix bigB --out data/big_b.json >/dev/null
before=$(n_events)
$S3 s3 cp data/big_b.json "s3://$S3_BUCKET/incoming/big_b.json" --quiet
scripts/submit.sh cluster --mode once > .spark/big2.log 2>&1
check "all rows landed" "$((before + BIG_ROWS))" "$(n_events)"
check "executors used with 2 workers" "2" "$(summary_field .spark/big2.log max_executors)"
grep "batch" .spark/big2.log
fi

scripts/standalone.sh down >/dev/null
echo; [ $FAILS -eq 0 ] && echo "ALL PASS" || { echo "$FAILS FAILED"; exit 1; }
