# sparkflock - agent guide

JSON files land in S3-compatible storage (MinIO locally) -> Spark Structured Streaming -> Postgres.
Same `spark/job.py` targets real S3 + RDS by env vars only (that path is untested, see README).
Master rules: `~/.claude/CLAUDE.md` (not copied here). Human docs: `README.md` + plain twin `README.txt`
(update both in the same step). Walkthrough with file:line refs: `presentation/index.html`.

## Commands (checked 2026-11-18 unless marked)

| Command | What | Run this session? |
|---|---|---|
| `make help` | list targets | yes |
| `make init up cluster-up stream` | venv, MinIO+Postgres+bucket (needs `terraform`), Spark standalone, always-on job | init/compose only |
| `make test` | `tests/e2e.sh`, 16 checks, ~6 min | NO (see "Shared Docker state") |
| `make k8s-start`, `make k8s-test MAX=4` | minikube scaling proof | NO |
| `make tf-validate-staging` | offline Terraform validate | NO (terraform not installed on this Mac) |
| `python3 presentation/build.py` | regenerate walkthrough; asserts every cited line range still holds its anchor text | yes, and in CI |
| `shellcheck -S error -e SC2148 scripts/*.sh tests/*.sh k8s/submit.sh` | shell lint | yes, and in CI |

CI (`.github/workflows/ci.yml`) runs only the static rows above plus gitleaks and compose-config parsing.
It does NOT run e2e: that needs Docker Spark + ~6 min + a 280 MB jar download. Do not describe CI as proving ingest behaviour.

## Layout

```
spark/job.py        the pipeline: Config.from_env -> build_spark -> readStream.json -> foreachBatch
spark/schema.sql    events, events_rejects, *_stage; applied by the job on every start
scripts/            env.sh (sourced by all), submit.sh, standalone.sh, reset.sh, gen_events.py, presign_console.py
k8s/                Dockerfile (jars baked in), submit.sh (scaling knobs), rbac.yaml, topology-rbac.yaml
topology/           read-only dashboard: server.py (stdlib) + static/index.html (canvas, no libs)
terraform/          modules/pipeline, envs/local (provider -> MinIO), envs/staging (real AWS, never applied)
tests/              e2e.sh (STAGES="0 1 2 3" selects), k8s_scale.sh N
bruno/              API probes (need S3_ACCESS_KEY/S3_SECRET_KEY in bruno/.env, git-ignored)
presentation/       build.py + template.html -> index.html (generated: edit the template, not index.html)
```

## Invariants - do not break

1. Idempotency is `ON CONFLICT (event_id) DO NOTHING` in `MERGE_EVENTS_SQL` (`spark/job.py`) on a PRIMARY KEY.
   Spark's JDBC writer cannot upsert, hence stage table + server-side merge.
2. `is_bad` routing in `foreachBatch` must run before the events write; a null `event_id` would hit the PK and kill the batch.
3. Checkpoint lives on S3 (`Config.checkpoint_path`), not pod disk, so a restarted driver resumes.
4. `JDBC_PARTITIONS` caps DB connections; more executors must not mean more connections.
5. Secrets stay out of git: `.env.example` has `CHANGE_ME` placeholders; real values in `.env` / `.env.staging` (git-ignored by `.env.*`). Gitleaks scanned working tree + all history on 2026-11-18: clean.
6. The dashboard is read-only: `k8s/topology-rbac.yaml` grants get/list/watch on pods+configmaps in ns `spark`. Do not add verbs or mount the Docker socket.
7. The dashboard may only claim what it measures (pod phase, standalone worker state, TCP reachability). No throughput/CPU signal exists.

## Gotchas that already cost time

- `spark.jars.packages` set in code is ignored under `spark-submit`; pass `--packages` (`scripts/submit.sh`, local path). The in-code setting only serves `python spark/job.py`.
- `DriverManager` cannot see `--packages` jars; `run_sql` loads the driver via the context classloader.
- `apache/spark:3.5.6` image's Python is 3.8: no `X | Y` type syntax, no 3.9+ stdlib in `spark/job.py`.
- `k8s/submit.sh` unsets `PYSPARK_PYTHON` (Mac venv path leaks into pods) and pins `spark.pyspark.python=python3`.
- Java: `scripts/env.sh` sets `JAVA_HOME=/opt/homebrew/opt/openjdk@21` (override with `JAVA_HOME_SPARK`); Spark 3.5 does not run on the system Java 26/27.
- Empty S3 prefix = PATH_NOT_FOUND; `ensure_input_prefix` creates the marker.
- `minikube start` switches the kubectl default context; always pass `--context sparkflock`.
- After a cold `minikube start`, wait for CoreDNS Ready before `k8s/submit.sh`.
- Changing `spark/job.py` needs `docker build` + `minikube image load` for K8s (baked in); the docker standalone driver mounts `./spark` live.
- Standalone in docker: the driver must be a container on the compose network (`spark-submit` service); executors cannot reach the Mac's 127.0.0.1.
- Stop a stream driver with `docker rm -f sparkflock-spark-driver`.
- Hadoop 3.3.4 + AWS SDK v1 1.12.262 is the pairing for Spark 3.5.x. Do not bump one alone.

## Shared Docker state - read before running anything destructive

Container names (`sparkflock-*`), compose project `sparkflock`, ports (29000, 29001, 25432, 27077, 28080, 29090) and
minikube profile `sparkflock` are hard-coded. Docker volumes `sparkflock_minio-data` / `sparkflock_pg-data` persist
on the owner's Mac and held ~4.8M dev rows on 2026-11-18. `make reset`, `make test` and `tests/e2e.sh` WIPE them.
Never run them in a sanitize/CI-style clone without the owner's yes. Never `docker rm`/`compose down` outside this folder or `minikube delete` without `-p sparkflock`.

## Open items

- Dashboard canvas was never seen rendered in a browser (recorded 2026-10-15). Needs a Playwright run of the page at :29090 (pills, cubes, `1 / 4 max`, console errors) plus a mutation proof (draw a Running pod as ghost -> test goes red).
- Nothing has run on real AWS; `terraform/envs/staging` was never planned/applied. K8s pods -> real S3 is not built (`k8s/submit.sh` hard-wires MinIO via `host.minikube.internal`).
- No Playwright/UI suite and no CI e2e exist yet.
