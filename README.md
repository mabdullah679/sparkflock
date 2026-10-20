# SparkFlock — Data Processing and Scaling Demonstration

## Recruiter-friendly overview
SparkFlock is a working demonstration of a data platform that automatically takes JSON files, processes them with Apache Spark, stores the results in PostgreSQL, and scales its processing capacity when workloads grow. It also includes a visual dashboard that shows the system’s workers starting, handling work, and shutting down. The project highlights practical experience with data engineering, cloud-style storage, Kubernetes, testing, reliability, and performance measurement.

## Project description
A containerized, test-driven Spark data pipeline that ingests JSON data, prevents duplicate records, routes invalid data for review, and dynamically scales Kubernetes workers. It includes a read-only topology dashboard and local infrastructure that mirrors cloud data-platform patterns.

---

1| # sparkflock — agent guide
2| 
3| JSON files land in S3-compatible storage → Spark (Structured Streaming) → Postgres.
4| Local stack, all docker: MinIO (S3) + Postgres (RDS stand-in) + Spark 3.5.6 standalone (master, workers, driver as containers) + minikube.
5| Only `submit.sh local` (single JVM, used for staging) still runs Spark on the Mac.
6| Same `spark/job.py` targets real S3 + RDS by env vars only.
7| 
8| Human-facing guide: `README.txt` (plain-English twin: **update both in the same step**).
9| Walkthrough with file:line refs: `presentation/index.html`.
10| Live 2.5D scaling view: `make topo-up` → http://localhost:29090 (see [Topology dashboard](#topology-dashboard)).
11| 
12| ## Status in one breath (2026-10-15)
13| 
14| Done and proven: ingest pipeline, replay safety, bad-data routing, K8s executor scaling 1→4→1.
15| Done, data path proven, **screen never seen**: the topology dashboard.
16| Not done: anything on real AWS; K8s pods → real S3.
17| Next: reconnect the Chrome extension and run the [GUI test](#testing-the-gui-in-a-browser).
18| 
19| ## Verified state (2026-10-12, this Mac, M1 Max / Docker Desktop 23 GiB)
20| 
21| | Claim | Evidence | Command |
22| |---|---|---|
23| | File upload → row, no manual step (9–10 s with 5 s trigger) | `tests/e2e.sh` stage 1 | `make test` |
24| | Replay of same `event_id` never duplicates | stage 2; mutation-proven (remove `ON CONFLICT` → red) | |
25| | Malformed lines → `events_rejects`, job survives | stage 3; mutation-proven (`is_bad := False` → red) | |
26| | Standalone master does **not** add workers; executors == workers started | stages 4–5 | |
27| | K8s dynamic allocation: pods 1→2→4 up, →1 idle; ceiling configurable | `tests/k8s_scale.sh 4` and `2`; mutation-proven (hard-coded ceiling → red) | `make k8s-test MAX=4` |
28| | Empty-prefix startup (S3 has no folders) | stage 1; mutation-proven (remove `ensure_input_prefix` → red) | |
29| | Pretty-printed object / multi-line array ingested, no rejects (also via K8s pods, Python 3.8) | stage 3b + `k8s_scale.sh`; mutation-proven (recovery off → red; validity check off → red) | |
30| | Dashboard feed reports real pod phases (Pending→Running→Terminating→gone) and the event log matches | polled `/api/state` during a 71 MB upload: 1→4→2 executors (2026-10-15) | `curl -s[...]
31| | Pre-signed browser upload lands in the bucket from the dashboard origin | `curl -X PUT` with `Origin` header → 200, object listed (2026-10-15) | `topology/console-trigger.js` |
32| 
33| **NOT verified (do not claim otherwise):**
34| - **The dashboard's canvas has never been seen rendered.** Chrome extension was not connected; only the JSON feed and `node --check` on the JS were exercised. Layout, animation, label overlap, and
35| - `console-trigger.js` was never executed in a real browser (600k-row generation may freeze the tab).
36| - Dashboard behaviour when minikube is stopped / token expired is untested beyond the code path (`k8s.ok=false` → red pill + banner).
37| - `terraform/envs/staging` passes `terraform validate` only. No AWS credentials existed on this machine: never planned, never applied, job never run against real S3/RDS.
38| - K8s pods → real AWS S3 is not built (needs IRSA or a creds Secret; `k8s/submit.sh` hard-wires MinIO via `host.minikube.internal`).
39| - Spark standalone dynamic allocation not tested (not needed for the answer: master never creates workers).
40| - RDS-specific behaviour (forced SSL, t4g.micro throughput, param groups) unobserved. `DB_SSLMODE=require` is wired but unexercised.
41| 
42| ## Measured, not assumed
43| 
44| 80 MB / 600k rows, one batch: ~7.4 s total = 1.4 s stage write + 4.1 s `pg_merge` (single server-side `INSERT … SELECT … ON CONFLICT`) + parse/count. `JDBC_PARTITIONS` 1/2/4/8 → 7.3/7.4/7.5/7.5 s, so JDBC fan-out is not the limit. Executor ceiling 4 vs 2: 17–23 s vs 18–19 s, no consistent gain.
45| Extra executors only parallelise the Spark half; the merge is serial in Postgres.
46| 
47| ## Layout
48| 
49| ```
50| spark/job.py            the pipeline. Config.from_env → build_spark → readStream.json → foreachBatch
51| spark/schema.sql        events, events_rejects, *_stage; run by the job on every start
52| scripts/env.sh          sourced by all scripts; ENV_FILE=… switches local/staging
53| scripts/submit.sh       local → native spark-submit with --packages (REQUIRED, see gotchas); cluster → `compose run` driver container
54| scripts/standalone.sh   up N | down | status: compose profile `cluster`, `--scale spark-worker=N`, waits for N ALIVE
55| docker-compose.yml      minio, postgres; profile `cluster`: spark-master, spark-worker (scaled), spark-submit (driver)
56| bruno/                  API probes: ALIVE standalone workers (:28080/json/), Running K8s executors (:29090/api/state)
57| scripts/gen_events.py   deterministic JSONL generator (--rows --prefix --bad)
58| scripts/reset.sh        wipe incoming/, _checkpoints/, tables
59| k8s/Dockerfile          apache/spark:3.5.6-python3 + 3 jars + job.py
60| k8s/rbac.yaml           ns spark, SA spark, Role to manage pods
61| k8s/submit.sh           cluster-mode submit; MIN/MAX_EXECUTORS, IDLE_TIMEOUT
62| terraform/modules/pipeline   bucket (+ RDS when create_rds)
63| terraform/envs/local    aws provider → MinIO
64| terraform/envs/staging  real AWS; allowed_cidr validated != 0.0.0.0/0
65| tests/e2e.sh            STAGES="0 1 2 3" selects stages
66| tests/k8s_scale.sh N    scaling proof with pod timeline
67| topology/server.py      stdlib feed: polls K8s pods+conf, standalone master, TCP probes → SSE /events, /api/state
68| topology/static/index.html  canvas 2.5D SPA (iso projection, no libs)
69| topology/compose.yml    own compose project `sparkflock-topology`, joins minikube network `sparkflock`
70| k8s/topology-rbac.yaml  SA `topology`: get/list/watch pods+configmaps in ns spark, nothing else
71| scripts/presign_console.py  writes topology/console-trigger.js (stdlib SigV4 pre-signed PUTs)
72| ```
73| 
74| ## Commands
75| 
76| `make help`. Order: `make init up cluster-up stream`, then `make gen-one upload FILE=data/one.json count`.
77| K8s: `make k8s-start`, `make k8s-test MAX=4`. Proof: `make test`.
78| Dashboard: `make topo-up` / `make topo-down` (needs `k8s-start` first).
79| Live scale demo without a terminal: paste `topology/console-trigger.js` into DevTools on the dashboard tab.
80| **After a cold `minikube start`, wait for CoreDNS Ready before `k8s/submit.sh`** (`kubectl -n kube-system wait --for=condition=Ready pod -l k8s-app=kube-dns`); otherwise the driver dies with `Unkn[...]
81| 
82| ## Invariants — do not break
83| 
84| 1. **Idempotency is `ON CONFLICT (event_id) DO NOTHING`** in `MERGE_EVENTS_SQL` (`spark/job.py:47-51`) on a `PRIMARY KEY`. Spark's JDBC writer cannot do upserts; the stage-table + merge pattern ex[...]
85| 2. **`is_bad` routing** (`spark/job.py:224`) must stay ahead of the events write; a null `event_id` hits the PK and kills the batch.
86| 3. **Checkpoint on S3**, not pod disk (`Config.checkpoint_path`), so a restarted driver resumes.
87| 4. **`JDBC_PARTITIONS` caps DB connections.** Raising executors must not raise connections; RDS free tier has few.
88| 5. **Secrets stay out of git.** `.env.example` holds `CHANGE_ME` placeholders only; real values live in `.env` / `.env.staging` (git-ignored). Terraform local gets the MinIO secret via `TF_VAR_s3_[...]
89| 6. **The dashboard is read-only.** `topology-rbac.yaml` grants get/list/watch only; do not add verbs, and do not mount the Docker socket. A "control" button that no RBAC rule permits would be a co[...]
90| 7. **The dashboard may only claim what it measures.** It shows pod phase, standalone worker state and TCP reachability. It has no throughput/CPU signal (no metrics-server); link particles are deco[...]
91| 
92| ## Gotchas that already cost time
93| 
94| - `spark.jars.packages` in the script is ignored under `spark-submit` (JVM already up). Pass `--packages` (`scripts/submit.sh:15`). The script's own setting only serves `python spark/job.py`.
95| - `DriverManager` cannot see `--packages` jars. `run_sql` instantiates the driver via the context classloader (`spark/job.py:131`).
96| - `apache/spark:3.5.6` has no Python; use `-python3`. The pod's Python is 3.8: no `X | Y` type syntax, no 3.9+ stdlib.
97| - Mac `PYSPARK_PYTHON` leaks into the driver pod → `error=2`. `k8s/submit.sh:19` unsets it and pins `spark.pyspark.python=python3`.
98| - Java 26 is the system default; Spark 3.5 needs 21 (`env.sh` sets `JAVA_HOME`).
99| - Empty S3 prefix = nonexistent path = `PATH_NOT_FOUND`. `ensure_input_prefix` creates the marker.
100| - `minikube start` switches the kubectl **default context**. Always pass `--context sparkflock`; `make k8s-start` restores the previous one.
101| - Changing `spark/job.py` requires `docker build` + `minikube image load` (job is baked into the image).
102| - Standalone in docker: the driver **must** be a container on the compose network (`spark-submit` service). Executors in worker containers connect back to the driver; a Mac-side driver on 127.0.0[...]
103| - Stop a running stream driver by container name: `docker rm -f sparkflock-spark-driver` (what `tests/e2e.sh` stage 1 does). Killing only the `docker compose run` client was not tested as a way t[...]
104| - Hadoop 3.3.4 + AWS SDK v1 1.12.262 is the pairing for Spark 3.5.x. Do not bump one alone. Spark 4 / Hadoop 3.4 uses SDK v2 and needs different MinIO settings (untested).
105| 
106| ## Shared machine — contention
107| 
108| Other projects' containers run here (floci on 4566, many `nfl-*`, `claimsdemo-*`, Postgres on 5432/5435/5440/15432…).
109| This project uses only `sparkflock*` containers (`sparkflock` itself is the minikube node) and ports 29000, 29001, 25432, 27077, 28080, 29090, minikube profile `sparkflock`.
110| Never `docker rm`, `docker compose down` outside this folder, or `minikube delete` without `-p sparkflock`.
111| 
112| ## Staging (unverified path)
113| 
114| ```
115| cd terraform/envs/staging && cp terraform.tfvars.example terraform.tfvars   # edit
116| terraform init && terraform plan        # READ IT. RDS bills if left running.
117| terraform apply
118| cp .env.staging.example .env.staging    # fill from `terraform output`
119| ENV_FILE=.env.staging scripts/submit.sh local --mode once
120| terraform destroy
121| ```
122| Credentials: env vars or profile via the default AWS chain (`S3_ENDPOINT` empty). `aws login` SSO
123| tokens are not understood by SDK v1.12.262; export temporary keys instead (`aws configure export-credentials --format env`) — unverified.
124| `publicly_accessible = true` on RDS is a staging convenience bounded by a `/32` security group.
125| 
126| ## Extending
127| 
128| - New fields: `EVENT_SCHEMA` + `EVENT_COLS` + both `schema.sql` tables + both `MERGE_*` SQL strings.
129| - Parallelism per file: `MAX_PARTITION_MB` (default 128 in code, 8 in `.env.example` so an 80 MB file splits).
130| - Compressed JSON (`.gz`) is **not splittable**: one file = one task regardless of size.
131| - Pretty-printed JSON (object or array) is recovered by `recover_multiline` (`spark/job.py:176`): only files with corrupt lines are re-read whole, accepted only if `json.loads` accepts the entire[...]
132| - `spark.read.text(files, wholetext=True)` must use the kwarg; `.option("wholetext", …)` is reset by `.text()`.
133| 
134| ## Topology dashboard
135| 
136| **What it is.** One container (`sparkflock-topology`, :29090) serving a canvas SPA fed by SSE once a second.
137| Platforms: object store (MinIO) → driver → **executors** (K8s pods) → database (Postgres); a second
138| platform shows the native standalone master + workers when `make cluster-up` is running.
139| 
140| | Visual | Meaning | Source |
141| |---|---|---|
142| | dashed ghost cube, pulsing | executor pod `Pending`/`Starting` | pod phase + Ready condition |
143| | solid cube rising | `Running` and Ready | pod list |
144| | red cube sinking, then gone | `Terminating` / removed | `deletionTimestamp` / absent from list |
145| | header pills | k8s API, MinIO, Postgres TCP, standalone worker count | server probes |
146| | `/ N max`, `min · max · idle` | the driver's real dynamic-allocation conf | driver `spark.properties` configmap |
147| | timeline | executor count, last 5 min | server ring buffer (1 sample/s) |
148| 
149| **Credentials.** Token + CA are written to `.spark/topology/` (git-ignored) by `make topo-up` and mounted read-only.
150| The container reaches the API at `https://$(minikube ip):8443` over the `sparkflock` docker network.
151| 
152| **Trigger a scale-up.** `make gen-big upload FILE=data/big.json`, or paste `topology/console-trigger.js`
153| in DevTools on the dashboard tab (10 single-use pre-signed slots, 7-day expiry; regenerate with
154| `. scripts/env.sh; python3 scripts/presign_console.py 10`).
155| 
156| ## Testing the GUI in a browser
157| 
158| Non-visual oracle first (works with no browser): `curl -s localhost:29090/api/state | python3 -m json.tool`.
159| Then, with Claude in Chrome connected (`tabs_context_mcp` → new tab → `navigate http://localhost:29090`):
160| 
161| | # | Action | Expected on screen | Cross-check |
162| |---|---|---|---|
163| | 1 | Load page, idle | pills all green; driver cube solid; **1** solid executor; big number `1 / 4 max` | `kubectl --context sparkflock -n spark get pods` |
164| | 2 | Console errors | none | `read_console_messages onlyErrors` |
165| | 3 | Upload big file (above) | ghost cubes appear → solid → count reaches 4; log lines `executor N requested → Running` | `/api/state` pod phases |
166| | 4 | Wait ~30 s after ingest | extra cubes turn red, sink, vanish; count back to 1; timeline steps 1→4→1 | `kubectl … get pods` |
167| | 5 | `kubectl … delete pod <executor> --wait=false` | that cube goes red and sinks; Spark replaces it | event log |
168| | 6 | `docker stop sparkflock-minio` | `minio down` pill red, MinIO cube dims; **restart it after** | `docker ps` |
169| | 7 | `make cluster-up N=2` | purple standalone platform with master + 2 workers; pill `standalone 2 workers` | `curl localhost:28080/json/` |
170| | 8 | Kill the SSE (`make topo-down`) | `feed LOST` red within ~4 s | |
171| 
172| **Mutation proof for the screen (not yet done).** Edit `sync()` so a `Running` pod is drawn as `ghost`, rebuild (`make topo-up`), and confirm step 3 now *fails visually*; revert by reversing that exact edit.
173| Until that is run, step 1–8 is a checklist, not a proof. Save screenshots with `save_to_disk` as evidence.
174| 
175| ## Debrief (2026-10-15)
176| 
177| **Done.** Stack up on Docker/minikube; read-only topology service + 2.5D SPA; `make topo-up/down`; console trigger with pre-signed uploads; feed proven 1→4→2 on a real 71 MB upload.
178| **Couldn't be done.** Seeing the dashboard: Chrome extension not connected, so no screenshot, no console-error check, no mutation proof. Throughput visualisation: no metrics source exists.
179| **Next.** Reconnect extension → run the table above → mutation-prove → fix whatever the screenshot shows.
180|