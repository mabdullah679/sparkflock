"""Read-only topology feed: polls K8s (pods + driver conf), the standalone master
and TCP probes once a second, diffs them into an event log, and streams the whole
state to the browser over SSE. Stdlib only. It never writes to anything."""
import json, os, socket, ssl, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API = os.environ.get("K8S_API", "")
NS = os.environ.get("K8S_NAMESPACE", "spark")
MASTER_UI = os.environ.get("STANDALONE_MASTER_UI", "")
PROBES = dict(p.split("=", 1) for p in os.environ.get("PROBES", "").split(",") if "=" in p)
TOKEN = lambda: open("/creds/token").read().strip()
CTX = ssl.create_default_context(cafile="/creds/ca.crt")

cond = threading.Condition()
state = {"seq": 0, "t": 0, "k8s": {"ok": False, "err": "starting"}, "standalone": {"ok": False},
         "services": {}, "conf": {}, "events": [], "history": []}
prev_pods = {}

def get_json(url, headers=None, ctx=None, timeout=2):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return json.load(r)

def k8s(path):
    return get_json(f"{API}{path}", {"Authorization": f"Bearer {TOKEN()}"}, CTX)

def pod_view(p):
    st, md = p.get("status", {}), p["metadata"]
    labels = md.get("labels", {})
    phase = st.get("phase", "Unknown")
    ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions", []))
    if md.get("deletionTimestamp"): phase = "Terminating"
    elif phase == "Running" and not ready: phase = "Starting"
    return {"name": md["name"], "role": labels.get("spark-role", "other"), "phase": phase,
            "created": md.get("creationTimestamp"), "app": labels.get("spark-app-name", "")}

def parse_conf(cm_items):
    out = {}
    for cm in cm_items:
        props = cm.get("data", {}).get("spark.properties", "")
        for line in props.splitlines():
            for key, name in (("spark.dynamicAllocation.maxExecutors", "max"),
                              ("spark.dynamicAllocation.minExecutors", "min"),
                              ("spark.dynamicAllocation.executorIdleTimeout", "idle")):
                if line.startswith(key + "="): out[name] = line.split("=", 1)[1]
    return out

def log(ev, msg, name=""):
    state["events"] = ([{"t": time.time(), "ev": ev, "msg": msg, "name": name}] + state["events"])[:60]

def poll_k8s():
    global prev_pods
    try:
        pods = [pod_view(p) for p in k8s(f"/api/v1/namespaces/{NS}/pods")["items"]]
        conf = parse_conf(k8s(f"/api/v1/namespaces/{NS}/configmaps")["items"])
        cur = {p["name"]: p for p in pods}
        for n, p in cur.items():
            old = prev_pods.get(n)
            if p["role"] != "executor" and p["role"] != "driver": continue
            if old is None: log("spawn", f'{p["role"]} {n.split("-")[-1] if p["role"]=="executor" else n} requested', n)
            elif old["phase"] != p["phase"]: log(p["phase"].lower(), f'{p["role"]} {n.split("-")[-1] if p["role"]=="executor" else n} → {p["phase"]}', n)
        for n, p in prev_pods.items():
            if n not in cur and p["role"] in ("executor", "driver"):
                log("gone", f'{p["role"]} {n.split("-")[-1] if p["role"]=="executor" else n} removed', n)
        prev_pods = cur
        state["k8s"] = {"ok": True, "pods": pods}
        if conf: state["conf"] = conf
    except Exception as e:
        state["k8s"] = {"ok": False, "err": f"{type(e).__name__}: {e}"[:200]}

def poll_standalone():
    if not MASTER_UI: return
    try:
        d = get_json(MASTER_UI + "/json/")
        state["standalone"] = {"ok": True, "url": d.get("url"), "status": d.get("status"),
            "cores": d.get("cores"), "coresused": d.get("coresused"),
            "workers": [{"id": w["id"], "state": w["state"], "cores": w["cores"], "coresused": w["coresused"],
                         "memory": w["memory"]} for w in d.get("workers", [])],
            "apps": [{"name": a["name"], "cores": a["cores"]} for a in d.get("activeapps", [])]}
    except Exception:
        state["standalone"] = {"ok": False}

def poll_services():
    for name, hp in PROBES.items():
        host, port = hp.rsplit(":", 1)
        try:
            socket.create_connection((host, int(port)), timeout=1).close(); state["services"][name] = True
        except Exception: state["services"][name] = False

def loop():
    while True:
        t0 = time.time()
        poll_k8s(); poll_standalone(); poll_services()
        execs = [p for p in state["k8s"].get("pods", []) if p["role"] == "executor" and p["phase"] in ("Running", "Starting", "Pending")]
        state["history"] = (state["history"] + [[int(t0), len(execs)]])[-300:]
        state["t"] = t0; state["seq"] += 1
        with cond: cond.notify_all()
        time.sleep(max(0, 1 - (time.time() - t0)))

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ctype):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store"); self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.send(200, open("static/index.html", "rb").read(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self.send(200, json.dumps(state).encode(), "application/json")
        elif self.path == "/healthz":
            self.send(200, b"ok", "text/plain")
        elif self.path == "/events":
            self.send_response(200); self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store"); self.end_headers()
            seq = -1
            try:
                while True:
                    with cond: cond.wait(timeout=5)
                    chunk = f"data: {json.dumps(state)}\n\n" if state["seq"] != seq else ": ka\n\n"
                    seq = state["seq"]; self.wfile.write(chunk.encode()); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError): pass
        else:
            self.send(404, b"not found", "text/plain")

if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8090), H).serve_forever()
