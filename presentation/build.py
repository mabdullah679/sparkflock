#!/usr/bin/env python3
"""Builds presentation/index.html. Every code excerpt is read from the real
file by line range, and an anchor string must appear in that range, so the
line references in the presentation cannot silently drift from the code.
    python3 presentation/build.py
"""
import html
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def code(path: str, a: int, b: int, anchor: str, note: str = "") -> str:
    lines = (ROOT / path).read_text().splitlines()
    chunk = lines[a - 1:b]
    assert any(anchor in l for l in chunk), f"{path}:{a}-{b} does not contain {anchor!r}"
    rows = "".join(
        f'<span class="ln">{a + i}</span>{html.escape(l)}\n' for i, l in enumerate(chunk))
    ref = f"{path}:{a}-{b}" if b > a else f"{path}:{a}"
    n = f'<p class="note">{note}</p>' if note else ""
    return (f'<figure class="code"><figcaption><span class="ref">{ref}</span></figcaption>'
            f'<pre><code>{rows}</code></pre>{n}</figure>')


def ref(path: str, a: int, anchor: str) -> str:
    line = (ROOT / path).read_text().splitlines()[a - 1]
    assert anchor in line, f"{path}:{a} does not contain {anchor!r}: {line!r}"
    return f'<span class="ref">{path}:{a}</span>'


T = {}
T["C_SCHEMA"] = code("spark/job.py", 29, 37, "EVENT_SCHEMA", "A streaming file source needs an explicit schema. <code>_corrupt_record</code> is what lets a bad line become data instead of a crash.")
T["C_STREAM"] = code("spark/job.py", 269, 274, "readStream", "Spark has no S3 event hook. It lists this prefix on every trigger and remembers what it has seen in the checkpoint.")
T["C_TRIGGER"] = code("spark/job.py", 278, 281, "foreachBatch", "<code>stream</code> mode polls every <code>TRIGGER_SECONDS</code>; <code>once</code> drains what is pending and exits.")
T["C_PREFIX"] = code("spark/job.py", 157, 164, "mkdirs", "Found by running it: S3 has no folders, an empty prefix does not exist, and the stream refused to start.")
T["C_BADROUTE"] = code("spark/job.py", 224, 232, "is_bad", "Anything unparseable, or without an <code>event_id</code>, goes to <code>events_rejects</code> with a reason.")
T["C_RECOVER"] = code("spark/job.py", 187, 199, "wholetext", "A pretty-printed message arrives as fragments. Only the failing files are re-read whole, and accepted only if the entire text is one valid JSON object or array.")
T["C_MERGE"] = code("spark/job.py", 47, 51, "ON CONFLICT", "Spark's JDBC writer can only append or overwrite. It writes a stage table, then this runs on one connection.")
T["C_CAP"] = code("spark/job.py", 147, 149, "coalesce", "Scaling executors must not scale database connections. This caps them.")
T["C_PART"] = code("spark/job.py", 107, 109, "maxPartitionBytes", "A big file is cut into pieces of this size. Pieces are the unit of parallelism.")
T["C_DYN"] = code("k8s/submit.sh", 30, 38, "maxExecutors", "These are the scaling controls. Min, max and idle timeout are the three you will actually change.")
T["C_RBAC"] = code("k8s/rbac.yaml", 11, 17, "pods", "Scaling on Kubernetes is physically the driver creating and deleting pods. Without this Role it cannot.")
T["C_WORKERS"] = code("scripts/standalone.sh", 20, 23, "Worker", "Workers exist because this loop started them. Nothing in Spark's standalone master creates one.")
T["C_PKG"] = code("scripts/submit.sh", 15, 16, "--packages")
T["C_TF_LOCAL"] = code("terraform/envs/local/main.tf", 16, 25, "endpoints", "Same AWS provider, pointed at MinIO. The module that creates the bucket is identical in staging.")
T["C_TF_RDS"] = code("terraform/modules/pipeline/main.tf", 36, 49, "aws_db_instance", "Staging only (<code>create_rds = true</code>). Validated, never applied: no AWS credentials were available.")
T["C_TF_GUARD"] = code("terraform/modules/pipeline/variables.tf", 28, 37, "0.0.0.0/0")
for k, v in {
    "R_MERGE": ref("spark/job.py", 50, "ON CONFLICT"),
    "R_BAD": ref("spark/job.py", 224, "is_bad"),
    "R_CKPT": ref("spark/job.py", 279, "checkpointLocation"),
    "R_STREAM": ref("spark/job.py", 269, "readStream"),
    "R_PART": ref("spark/job.py", 109, "maxPartitionBytes"),
    "R_MAXEX": ref("k8s/submit.sh", 35, "maxExecutors"),
    "R_MINEX": ref("k8s/submit.sh", 33, "minExecutors"),
    "R_IDLE": ref("k8s/submit.sh", 36, "executorIdleTimeout"),
    "R_CLS": ref("spark/job.py", 131, "getContextClassLoader"),
    "R_PY": ref("k8s/submit.sh", 19, "unset PYSPARK_PYTHON"),
    "R_MERGETIME": ref("spark/job.py", 252, "pg_merge"),
}.items():
    T[k] = v

tpl = (Path(__file__).parent / "template.html").read_text()
for k, v in T.items():
    tpl = tpl.replace(f"%%{k}%%", v)
assert "%%" not in tpl, "unreplaced placeholder"
(Path(__file__).parent / "index.html").write_text(tpl)
print("wrote presentation/index.html", len(tpl) // 1024, "KB")
