# sparkflock — agent guide

JSON files land in S3-compatible storage → Spark (Structured Streaming) → Postgres.
Local stack: MinIO (S3) + Postgres (RDS stand-in) + native Spark 3.5.6 + minikube.
Same `spark/job.py` targets real S3 + RDS by env vars only.

Human-facing guide: `README.txt` (plain-English twin: **update both in the same step**).
Walkthrough with file:line refs: `presentation/index.html`.
Live 2.5D scaling view: `make topo-up` → http://localhost:29090 (see [Topology dashboard](#topology-dashboard)).

## Status in one breath (2026-10-15)

Done and proven: ingest pipeline, replay safety, bad-data routing, K8s executor scaling 1→4→1.
Done, data path proven, **screen never seen**: the topology dashboard.
Not done: anything on real AWS; K8s pods → real S3.
Next: reconnect the Chrome extension and run the [GUI test](#testing-the-gui-in-a-browser).

## Verified state (2026-10-12, this Mac, M1 Max / Docker Desktop 23 GiB)

| Claim | Evidence | Command |
|---|---|---|
| File upload → row, no manual step (9–10 s with 5 s trigger) | `tests/e2e.sh` stage 1 | `make test` |
| Replay of same `event_id` never duplicates | stage 2; mutation-proven (remove `ON CONFLICT` → red) | |
| Malformed lines → `events_rejects`, job survives | stage 3; mutation-proven (`is_bad := False` → red) | |
| Standalone master does **not** add workers; executors == workers started | stages 4–5 | |
| K8s dynamic allocation: pods 1→2→4 up, →1 idle; ceiling configurable | `tests/k8s_scale.sh 4` and `2`; mutation-proven (hard-coded ceiling → red) | `make k8s-test MAX=4` |
| Empty-prefix startup (S3 has no folders) | stage 1; mutation-proven (remove `ensure_input_prefix` → red) | |
| Pretty-printed object / multi-line array ingested, no rejects (also via K8s pods, Python 3.8) | stage 3b + `k8s_scale.sh`; mutation-proven (recovery off → red; validity check off → red) | |
| Dashboard feed reports real pod phases (Pending→Running→Terminating→gone) and the event log matches | polled `/api/state` during a 71 MB upload: 1→4→2 executors (2026-10-15) | `curl -s localhost:29090/api/state` |
| Pre-signed browser upload lands in the bucket from the dashboard origin | `curl -X PUT` with `Origin` header → 200, object listed (2026-10-15) | `topology/console-trigger.js` |

**NOT verified (do not claim otherwise):**
- **The dashboard's canvas has never been seen rendered.** Chrome extension was not connected; only the JSON feed and `node --check` on the JS were exercised. Layout, animation, label overlap, and the standalone platform are unobserved. Not mutation-tested.
- `console-trigger.js` was never executed in a real browser (600k-row generation may freeze the tab).
- Dashboard behaviour when minikube is stopped / token expired is untested beyond the code path (`k8s.ok=false` → red pill + banner).
- `terraform/envs/staging` passes `terraform validate` only. No AWS credentials existed on this machine: never planned, never applied, job never run against real S3/RDS.
- K8s pods → real AWS S3 is not built (needs IRSA or a creds Secret; `k8s/submit.sh` hard-wires MinIO via `host.minikube.internal`).
- Spark standalone dynamic allocation not tested (not needed for the answer: master never creates workers).
- RDS-specific behaviour (forced SSL, t4g.micro throughput, param groups) unobserved. `DB_SSLMODE=require` is wired but unexercised.

## Measured, not assumed

80 MB / 600k rows, one batch: ~7.4 s total = 1.4 s stage write + 4.1 s `pg_merge` (single server-side
`INSERT … SELECT … ON CONFLICT`) + parse/count. `JDBC_PARTITIONS` 1/2/4/8 → 7.3/7.4/7.5/7.5 s, so
JDBC fan-out is not the limit. Executor ceiling 4 vs 2: 17–23 s vs 18–19 s, no consistent gain.
Extra executors only parallelise the Spark half; the merge is serial in Postgres.

## Layout

```
spark/job.py            the pipeline. Config.from_env → build_spark → readStream.json → foreachBatch
spark/schema.sql        events, events_rejects, *_stage; run by the job on every start
scripts/env.sh          sourced by all scripts; ENV_FILE=… switches local/staging
scripts/submit.sh       local|cluster → spark-submit with --packages (REQUIRED, see gotchas)
scripts/standalone.sh   master + N workers via spark-class (pip pyspark has no sbin/start-*.sh)
scripts/gen_events.py   deterministic JSONL generator (--rows --prefix --bad)
scripts/reset.sh        wipe incoming/, _checkpoints/, tables
k8s/Dockerfile          apache/spark:3.5.6-python3 + 3 jars + job.py
k8s/rbac.yaml           ns spark, SA spark, Role to manage pods
k8s/submit.sh           cluster-mode submit; MIN/MAX_EXECUTORS, IDLE_TIMEOUT
terraform/modules/pipeline   bucket (+ RDS when create_rds)
terraform/envs/local    aws provider → MinIO
terraform/envs/staging  real AWS; allowed_cidr validated != 0.0.0.0/0
tests/e2e.sh            STAGES="0 1 2 3" selects stages
tests/k8s_scale.sh N    scaling proof with pod timeline
topology/server.py      stdlib feed: polls K8s pods+conf, standalone master, TCP probes → SSE /events, /api/state
topology/static/index.html  canvas 2.5D SPA (iso projection, no libs)
topology/compose.yml    own compose project `sparkflock-topology`, joins minikube network `sparkflock`
k8s/topology-rbac.yaml  SA `topology`: get/list/watch pods+configmaps in ns spark, nothing else
scripts/presign_console.py  writes topology/console-trigger.js (stdlib SigV4 pre-signed PUTs)
```

## Commands

`make help`. Order: `make init up cluster-up stream`, then `make gen-one upload FILE=data/one.json count`.
K8s: `make k8s-start`, `make k8s-test MAX=4`. Proof: `make test`.
Dashboard: `make topo-up` / `make topo-down` (needs `k8s-start` first).
Live scale demo without a terminal: paste `topology/console-trigger.js` into DevTools on the dashboard tab.
**After a cold `minikube start`, wait for CoreDNS Ready before `k8s/submit.sh`** (`kubectl -n kube-system wait --for=condition=Ready pod -l k8s-app=kube-dns`); otherwise the driver dies with `UnknownHostException: kubernetes.default.svc`.

## Invariants — do not break

1. **Idempotency is `ON CONFLICT (event_id) DO NOTHING`** in `MERGE_EVENTS_SQL` (`spark/job.py:47-51`) on a `PRIMARY KEY`. Spark's JDBC writer cannot do upserts; the stage-table + merge pattern exists for that reason.
2. **`is_bad` routing** (`spark/job.py:224`) must stay ahead of the events write; a null `event_id` hits the PK and kills the batch.
3. **Checkpoint on S3**, not pod disk (`Config.checkpoint_path`), so a restarted driver resumes.
4. **`JDBC_PARTITIONS` caps DB connections.** Raising executors must not raise connections; RDS free tier has few.
5. **Secrets stay out of git.** `.env.example` holds `CHANGE_ME` placeholders only; real values live in `.env` / `.env.staging` (git-ignored). Terraform local gets the MinIO secret via `TF_VAR_s3_secret_key` from `.env`, no default.
6. **The dashboard is read-only.** `topology-rbac.yaml` grants get/list/watch only; do not add verbs, and do not mount the Docker socket. A "control" button that no RBAC rule permits would be a control that was never code.
7. **The dashboard may only claim what it measures.** It shows pod phase, standalone worker state and TCP reachability. It has no throughput/CPU signal (no metrics-server); link particles are decoration and are labelled so.

## Gotchas that already cost time

- `spark.jars.packages` in the script is ignored under `spark-submit` (JVM already up). Pass `--packages` (`scripts/submit.sh:15`). The script's own setting only serves `python spark/job.py`.
- `DriverManager` cannot see `--packages` jars. `run_sql` instantiates the driver via the context classloader (`spark/job.py:131`).
- `apache/spark:3.5.6` has no Python; use `-python3`. The pod's Python is 3.8: no `X | Y` type syntax, no 3.9+ stdlib.
- Mac `PYSPARK_PYTHON` leaks into the driver pod → `error=2`. `k8s/submit.sh:19` unsets it and pins `spark.pyspark.python=python3`.
- Java 26 is the system default; Spark 3.5 needs 21 (`env.sh` sets `JAVA_HOME`).
- Empty S3 prefix = nonexistent path = `PATH_NOT_FOUND`. `ensure_input_prefix` creates the marker.
- `minikube start` switches the kubectl **default context**. Always pass `--context sparkflock`; `make k8s-start` restores the previous one.
- Changing `spark/job.py` requires `docker build` + `minikube image load` (job is baked into the image).
- Hadoop 3.3.4 + AWS SDK v1 1.12.262 is the pairing for Spark 3.5.x. Do not bump one alone. Spark 4 / Hadoop 3.4 uses SDK v2 and needs different MinIO settings (untested).

## Shared machine — contention

Other projects' containers run here (floci on 4566, many `nfl-*`, `claimsdemo-*`, Postgres on 5432/5435/5440/15432…).
This project uses only `sparkflock-*` containers and ports 29000, 29001, 25432, 27077, 28080+, 29090, minikube profile `sparkflock`.
Never `docker rm`, `docker compose down` outside this folder, or `minikube delete` without `-p sparkflock`.

## Staging (unverified path)

```
cd terraform/envs/staging && cp terraform.tfvars.example terraform.tfvars   # edit
terraform init && terraform plan        # READ IT. RDS bills if left running.
terraform apply
cp .env.staging.example .env.staging    # fill from `terraform output`
ENV_FILE=.env.staging scripts/submit.sh local --mode once
terraform destroy
```
Credentials: env vars or profile via the default AWS chain (`S3_ENDPOINT` empty). `aws login` SSO
tokens are not understood by SDK v1.12.262; export temporary keys instead (`aws configure export-credentials --format env`) — unverified.
`publicly_accessible = true` on RDS is a staging convenience bounded by a `/32` security group.

## Extending

- New fields: `EVENT_SCHEMA` + `EVENT_COLS` + both `schema.sql` tables + both `MERGE_*` SQL strings.
- Parallelism per file: `MAX_PARTITION_MB` (default 128 in code, 8 in `.env.example` so an 80 MB file splits).
- Compressed JSON (`.gz`) is **not splittable**: one file = one task regardless of size.
- Pretty-printed JSON (object or array) is recovered by `recover_multiline` (`spark/job.py:176`): only files with corrupt lines are re-read whole, accepted only if `json.loads` accepts the entire text. Do NOT swap in Spark's `multiLine` reader: it silently truncated a mixed file (1 of 6 rows, no error). Files > 32 MB or concatenated multi-object files stay rejected.
- `spark.read.text(files, wholetext=True)` must use the kwarg; `.option("wholetext", …)` is reset by `.text()`.

## Topology dashboard

**What it is.** One container (`sparkflock-topology`, :29090) serving a canvas SPA fed by SSE once a second.
Platforms: object store (MinIO) → driver → **executors** (K8s pods) → database (Postgres); a second
platform shows the native standalone master + workers when `make cluster-up` is running.

| Visual | Meaning | Source |
|---|---|---|
| dashed ghost cube, pulsing | executor pod `Pending`/`Starting` | pod phase + Ready condition |
| solid cube rising | `Running` and Ready | pod list |
| red cube sinking, then gone | `Terminating` / removed | `deletionTimestamp` / absent from list |
| header pills | k8s API, MinIO, Postgres TCP, standalone worker count | server probes |
| `/ N max`, `min · max · idle` | the driver's real dynamic-allocation conf | driver `spark.properties` configmap |
| timeline | executor count, last 5 min | server ring buffer (1 sample/s) |

**Credentials.** Token + CA are written to `.spark/topology/` (git-ignored) by `make topo-up` and mounted read-only.
The container reaches the API at `https://$(minikube ip):8443` over the `sparkflock` docker network.

**Trigger a scale-up.** `make gen-big upload FILE=data/big.json`, or paste `topology/console-trigger.js`
in DevTools on the dashboard tab (10 single-use pre-signed slots, 7-day expiry; regenerate with
`. scripts/env.sh; python3 scripts/presign_console.py 10`).

## Testing the GUI in a browser

Non-visual oracle first (works with no browser): `curl -s localhost:29090/api/state | python3 -m json.tool`.
Then, with Claude in Chrome connected (`tabs_context_mcp` → new tab → `navigate http://localhost:29090`):

| # | Action | Expected on screen | Cross-check |
|---|---|---|---|
| 1 | Load page, idle | pills all green; driver cube solid; **1** solid executor; big number `1 / 4 max` | `kubectl --context sparkflock -n spark get pods` |
| 2 | Console errors | none | `read_console_messages onlyErrors` |
| 3 | Upload big file (above) | ghost cubes appear → solid → count reaches 4; log lines `executor N requested → Running` | `/api/state` pod phases |
| 4 | Wait ~30 s after ingest | extra cubes turn red, sink, vanish; count back to 1; timeline steps 1→4→1 | `kubectl … get pods` |
| 5 | `kubectl … delete pod <executor> --wait=false` | that cube goes red and sinks; Spark replaces it | event log |
| 6 | `docker stop sparkflock-minio` | `minio down` pill red, MinIO cube dims; **restart it after** | `docker ps` |
| 7 | `make cluster-up N=2` | purple standalone platform with master + 2 workers; pill `standalone 2 workers` | `curl localhost:28080/json/` |
| 8 | Kill the SSE (`make topo-down`) | `feed LOST` red within ~4 s | |

**Mutation proof for the screen (not yet done).** Edit `sync()` so a `Running` pod is drawn as `ghost`,
rebuild (`make topo-up`), and confirm step 3 now *fails visually*; revert by reversing that exact edit.
Until that is run, step 1–8 are a checklist, not a proof. Save screenshots with `save_to_disk` as evidence.

## Debrief (2026-10-15)

**Done.** Stack up on Docker/minikube; read-only topology service + 2.5D SPA; `make topo-up/down`; console
trigger with pre-signed uploads; feed proven 1→4→2 on a real 71 MB upload.
**Couldn't be done.** Seeing the dashboard: Chrome extension not connected, so no screenshot, no console-error
check, no mutation proof. Throughput visualisation: no metrics source exists.
**Next.** Reconnect extension → run the table above → mutation-prove → fix whatever the screenshot shows.
