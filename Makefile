# Everyday commands. `make help` lists them.
SHELL := /bin/bash
.DEFAULT_GOAL := help

help: ## show this list
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

init: ## one-time: python venv with pyspark + .env
	uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -r requirements.txt
	@test -f .env || cp .env.example .env

up: ## start MinIO + Postgres, create the bucket with Terraform
	set -a; . ./.env; set +a; docker compose up -d --wait
	set -a; . ./.env; set +a; export TF_VAR_s3_access_key=$$S3_ACCESS_KEY TF_VAR_s3_secret_key=$$S3_SECRET_KEY; \
	  cd terraform/envs/local && terraform init -input=false >/dev/null && terraform apply -auto-approve -input=false

down: ## stop MinIO + Postgres (data kept)
	set -a; . ./.env; set +a; docker compose down

stream: ## run the always-on job on the standalone cluster (picks up new files by itself)
	scripts/submit.sh cluster --mode stream

once: ## process everything pending, then exit
	scripts/submit.sh cluster --mode once

cluster-up: ## start Spark master + N worker containers:  make cluster-up N=2
	scripts/standalone.sh up $(or $(N),1)

cluster-down: ## stop the standalone cluster
	scripts/standalone.sh down

gen-one: ## write data/one.json (a single JSON message)
	.venv/bin/python scripts/gen_events.py --rows 1 --out data/one.json

gen-big: ## write data/big.json (600k rows, ~80 MB)
	.venv/bin/python scripts/gen_events.py --rows 600000 --out data/big.json

upload: ## upload a file to the landing bucket:  make upload FILE=data/one.json
	. scripts/env.sh && aws --endpoint-url $$S3_ENDPOINT s3 cp $(FILE) s3://$$S3_BUCKET/incoming/$$(basename $(FILE))

count: ## row counts in Postgres
	. scripts/env.sh && psql_q "select 'events', count(*) from events union all select 'rejects', count(*) from events_rejects"

reset: ## wipe files, checkpoint and tables (dev only)
	scripts/reset.sh

test: ## end-to-end proof on the docker Spark cluster (~6 min)
	tests/e2e.sh

k8s-start: ## start minikube, build + load the Spark image, apply RBAC + secret
	prev=$$(kubectl config current-context 2>/dev/null); \
	minikube start -p sparkflock --cpus=4 --memory=7g --driver=docker; \
	[ -n "$$prev" ] && kubectl config use-context "$$prev" >/dev/null
	docker build -f k8s/Dockerfile -t sparkflock-spark:3.5.6 .
	-minikube -p sparkflock image rm docker.io/library/sparkflock-spark:3.5.6
	minikube -p sparkflock image load sparkflock-spark:3.5.6
	kubectl --context sparkflock apply -f k8s/rbac.yaml
	. scripts/env.sh && kubectl --context sparkflock -n spark create secret generic sparkflock-creds \
	  --from-literal=S3_ACCESS_KEY="$$S3_ACCESS_KEY" --from-literal=S3_SECRET_KEY="$$S3_SECRET_KEY" \
	  --from-literal=DB_PASSWORD="$$DB_PASSWORD" --dry-run=client -o yaml | kubectl --context sparkflock apply -f -

k8s-test: ## prove executor pods scale up/down:  make k8s-test MAX=4
	tests/k8s_scale.sh $(or $(MAX),4)

k8s-stop: ## delete the minikube cluster
	minikube delete -p sparkflock

topo-up: ## live 2.5D topology dashboard on http://localhost:29090 (needs k8s-start)
	kubectl --context sparkflock apply -f k8s/topology-rbac.yaml >/dev/null
	mkdir -p .spark/topology
	@for i in $$(seq 1 20); do t=$$(kubectl --context sparkflock -n spark get secret topology-token -o jsonpath='{.data.token}' | base64 -d); [ -n "$$t" ] && break; sleep 1; done; \
	  printf '%s' "$$t" > .spark/topology/token
	kubectl --context sparkflock -n spark get secret topology-token -o jsonpath='{.data.ca\.crt}' | base64 -d > .spark/topology/ca.crt
	MINIKUBE_IP=$$(minikube -p sparkflock ip) docker compose -f topology/compose.yml up -d --build

topo-down: ## stop the topology dashboard
	MINIKUBE_IP=x docker compose -f topology/compose.yml down

tf-validate-staging: ## offline check of the AWS staging Terraform (no credentials needed)
	cd terraform/envs/staging && terraform init -input=false -backend=false >/dev/null && terraform validate
