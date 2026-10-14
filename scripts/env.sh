# Sourced by the other scripts. Loads .env and points at the repo's Spark.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
set -a; . "${ENV_FILE:-$ROOT/.env}"; set +a   # ENV_FILE=.env.staging to point at AWS
export JAVA_HOME="${JAVA_HOME_SPARK:-/opt/homebrew/opt/openjdk@21}"   # Spark 3.5 needs Java 8/11/17/21, not 26
export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export SPARK_HOME="$("$ROOT/.venv/bin/python" -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"
export PATH="$ROOT/.venv/bin:$PATH"
[ -n "${S3_ACCESS_KEY:-}" ] && export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"   # MinIO keys; staging uses your real AWS creds
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4"
SA_MASTER_HOST=127.0.0.1; SA_MASTER_PORT=27077; SA_MASTER_UI=28080
[ -n "${S3_ENDPOINT:-}" ] || unset S3_ENDPOINT   # empty endpoint => real AWS
psql_q() { docker exec sparkflock-postgres psql -U "$DB_USER" -d "$DB_NAME" -At -c "$1"; }
