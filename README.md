# SparkFlock

A working data-pipeline demo: drop JSON files into S3-style storage, Spark loads them into PostgreSQL without duplicates, bad lines are set aside, and on Kubernetes the Spark workers scale up and down on their own. A read-only 2.5D dashboard shows the worker pods come and go.

Walkthrough with file and line references: [presentation/index.html](presentation/index.html). Plain-English version: [README.txt](README.txt). Agent/maintainer notes: [CLAUDE.md](CLAUDE.md).

```
file.json --> MinIO bucket (S3 stand-in) --> Spark 3.5.6 Structured Streaming --> Postgres 17 (RDS stand-in)
                  incoming/                  polls every 5 s; stage + merge       events / events_rejects
                                                    |
                                  minikube: driver asks Kubernetes for executor pods (1 -> 4 -> 1)
                                                    |
                                  topology dashboard :29090 (read-only SSE view of the pods)
```

Not local: real AWS S3 and RDS. The same `spark/job.py` can target them by env vars, but that path has never been run (see [Not verified](#not-verified)). First builds also download Docker images and about 280 MB of Spark jars from Maven Central.

## Prerequisites

- macOS on Apple silicon with Docker Desktop (the Spark cluster plus minikube want roughly 8 GB free for Docker), minikube, kubectl, terraform, uv, the aws CLI, and Java 21 (`brew install openjdk@21`; `scripts/env.sh` expects `/opt/homebrew/opt/openjdk@21`, override with `JAVA_HOME_SPARK`). Spark 3.5 does not run on Java 26/27.
- Free ports: 29000, 29001 (MinIO), 25432 (Postgres), 27077, 28080 (Spark master), 29090 (dashboard).
- Not needed on the host: Spark, Postgres, MinIO, Python packages beyond the venv `make init` builds.
- No cloud account is needed for anything in this README.

## Run it

```bash
make init          # 1. venv with pyspark 3.5.6, copies .env.example to .env
$EDITOR .env       # 2. replace the CHANGE_ME values (S3_SECRET_KEY needs 8+ chars)
make up            # 3. MinIO + Postgres, bucket created by Terraform
make cluster-up    # 4. Spark master + 1 worker as containers (N=3 for 3)
make stream        # 5. always-on job; leave it running, open a second terminal
make gen-one && make upload FILE=data/one.json   # 6. one JSON message
make count         # 7. ~10 s later: events 1
make cluster-down && make down                   # stop
```

Kubernetes scaling and the dashboard: `make k8s-start`, then `make k8s-test MAX=4` (uploads a big file, prints the pod timeline), `make topo-up` for http://localhost:29090.

If something is in the way:

- Port taken: change the host side in `docker-compose.yml`, and the matching values in `.env` (`S3_ENDPOINT`, `DB_PORT`) and `scripts/env.sh` (`SA_MASTER_*`).
- `make up` fails on Terraform: install `terraform` (it creates the bucket).
- Driver dies with an `Unknown host` error right after `minikube start`: wait for CoreDNS (`kubectl --context sparkflock -n kube-system wait --for=condition=Ready pod -l k8s-app=kube-dns`) and retry.

## What it does

| Action | Expected result |
|---|---|
| Upload a JSON-lines file to `incoming/` while `make stream` runs | rows in `events` about 10 s later (9-10 s measured with the 5 s trigger) |
| Upload the same `event_id`s again | no new rows (`ON CONFLICT (event_id) DO NOTHING`) |
| Line that is not JSON, or has no `event_id` | row in `events_rejects` with the reason; job keeps running |
| Pretty-printed object or multi-line array (up to 32 MB) | ingested whole, no rejects |
| Big file on standalone with 1 worker | 1 executor; the master never adds workers |
| Big file on minikube (`make k8s-test`) | executor pods 1 -> 2 -> 4, back to 1 after the idle timeout (20 s) |
| `make topo-up` | live pod cubes at :29090 |

## Interfaces

| Endpoint (topology, :29090) | Method | Auth |
|---|---|---|
| `/`, `/index.html` | GET | none |
| `/api/state` | GET | none |
| `/events` (SSE, 1 Hz) | GET | none |
| `/healthz` | GET | none |

The service holds a Kubernetes token with get/list/watch on pods and configmaps in namespace `spark` only ([k8s/topology-rbac.yaml](k8s/topology-rbac.yaml)); it has no login, and `topology/compose.yml` publishes it as `29090:8090` (all host interfaces, not just localhost). Keep it on a trusted network.

Config is env-only (read in [spark/job.py](spark/job.py)): `S3_*`, `DB_*`, `MAX_PARTITION_MB`, `JDBC_PARTITIONS`, `MAX_FILES_PER_TRIGGER`, `TRIGGER_SECONDS`; Kubernetes scaling uses `MIN_EXECUTORS`, `MAX_EXECUTORS`, `IDLE_TIMEOUT` ([k8s/submit.sh](k8s/submit.sh)). `make help` lists every command.

## Tests

- `make test` runs [tests/e2e.sh](tests/e2e.sh): 16 checks over the real docker stack (upload to row, replay, bad data, pretty JSON, 1 and 2 workers). About 6 minutes. It **wipes** the local pipeline tables and bucket paths.
- `make k8s-test MAX=4` runs [tests/k8s_scale.sh](tests/k8s_scale.sh) against minikube.
- `make tf-validate-staging` validates the AWS Terraform offline.
- CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs only static checks: shellcheck, Python compile, a check that the walkthrough's cited line numbers still match the code (`python3 presentation/build.py`), compose-file parsing, and a gitleaks scan. It does not run the e2e suite.
- All suites need Docker; none need cloud services or credentials.

## Layout

| Path | Role |
|---|---|
| [spark/job.py](spark/job.py) | the pipeline: read stream, route bad rows, stage, merge |
| [spark/schema.sql](spark/schema.sql) | tables, applied on every job start |
| [docker-compose.yml](docker-compose.yml) | MinIO, Postgres, Spark master/workers/driver |
| [scripts/](scripts/) | env, submit, standalone cluster, reset, data generator, pre-signed URL helper |
| [k8s/](k8s/) | Spark image (jars baked in), scaling submit script, RBAC |
| [topology/](topology/) | dashboard server and canvas page |
| [terraform/](terraform/) | bucket module, local (MinIO) and staging (AWS) envs |
| [tests/](tests/) | e2e and K8s scaling proofs |
| [bruno/](bruno/) | API probes for the stack ([bruno/README.txt](bruno/README.txt)) |
| [presentation/](presentation/) | generated walkthrough with file:line references |
| [sparkflock-guide.html](sparkflock-guide.html) | plain-English HTML guide |
| [CLAUDE.md](CLAUDE.md) | invariants, gotchas, and what not to touch |

## Design decisions

- **Stage table + server-side merge, not Spark's JDBC append.** The JDBC writer can only append or overwrite, so a replayed file would duplicate rows or fail on the primary key. `INSERT ... SELECT ... ON CONFLICT DO NOTHING` makes replay safe.
- **Polling, not an S3 event.** Spark has no S3 notification hook; it lists the prefix every trigger. If the job is down, files wait and it catches up on start.
- **Jars baked into the image.** Otherwise each executor pod downloads about 280 MB from Maven when it starts.
- **Driver is a container on the compose network.** Executors in worker containers connect back to it and cannot reach the Mac's 127.0.0.1.
- **More executors do not make this load faster.** For an 80 MB / 600k-row file, about 7.4 s total: 1.4 s staging write, 4.1 s Postgres merge (one connection, not shareable). Executor ceiling 4 vs 2 measured 17-23 s vs 18-19 s. Measured on the owner's Mac, 2026-10; not re-measured since.
- **Dashboard is read-only on purpose.** A control button the RBAC rule does not permit would be a lie, so none exists.

## Not verified

Recorded by earlier sessions (2026-10-12 to 2026-10-15) and not re-run in the 2026-11-18 cleanup, which only ran static checks.

- The dashboard canvas has never been viewed in a browser, and no UI test exists. Only its JSON feed was checked.
- Nothing ran on real AWS. `terraform/envs/staging` was never planned or applied; RDS behaviour (forced SSL, small-instance speed) is unobserved.
- Kubernetes pods cannot reach real S3: `k8s/submit.sh` hard-wires MinIO.
- Spark standalone auto-scaling is not built (Spark's master does not do it); use the Kubernetes mode.
- Pretty-printed files above 32 MB (`MAX_RECOVER_BYTES`) stay rejected.
