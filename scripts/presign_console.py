"""Writes topology/console-trigger.js: a paste-into-DevTools snippet that uploads
a big JSON-lines file to MinIO via pre-signed PUT URLs (stdlib SigV4, no secrets
in the output). Each URL is valid for one fixed key for 7 days.
  . scripts/env.sh; python3 scripts/presign_console.py [N_URLS]"""
import datetime, hashlib, hmac, os, sys, urllib.parse
ep, bucket = os.environ["S3_ENDPOINT"].rstrip("/"), os.environ["S3_BUCKET"]
ak, sk, region = os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"], os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
host = urllib.parse.urlparse(ep).netloc
def h(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
def presign(key, expires=604800):
    now = datetime.datetime.now(datetime.timezone.utc); amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    scope = f"{day}/{region}/s3/aws4_request"
    q = {"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-Credential": f"{ak}/{scope}", "X-Amz-Date": amz,
         "X-Amz-Expires": str(expires), "X-Amz-SignedHeaders": "host"}
    cq = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(q.items()))
    path = f"/{bucket}/{urllib.parse.quote(key)}"
    creq = f"PUT\n{path}\n{cq}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD"
    sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(creq.encode()).hexdigest()}"
    k = h(h(h(h(("AWS4" + sk).encode(), day), region), "s3"), "aws4_request")
    return f"{ep}{path}?{cq}&X-Amz-Signature={hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()}"
n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
stamp = datetime.datetime.now().strftime("%m%d%H%M")
urls = [presign(f"incoming/console_{stamp}_{i:02d}.json") for i in range(n)]
js = """// Paste into DevTools console (any tab). Uploads ~600k rows (~70 MB) to MinIO
// -> Spark scales executors up. Watch http://localhost:29090 . Run again for another burst.
(async () => {
  const URLS = %s;
  const used = JSON.parse(localStorage.getItem('sf_used') || '[]');
  const i = URLS.findIndex((_, k) => !used.includes(k));
  if (i < 0) return console.error('all %d pre-signed slots used; re-run scripts/presign_console.py');
  const ROWS = 600000, run = Date.now().toString(36), types = ['click','view','purchase','signup'];
  const lines = new Array(ROWS);
  for (let r = 0; r < ROWS; r++)
    lines[r] = JSON.stringify({event_id: `con-${run}-${r}`, user_id: (Math.random()*1e5)|0,
      event_type: types[r & 3], amount: +(Math.random()*100).toFixed(2), ts: new Date().toISOString().slice(0,19)+'Z'});
  const body = new Blob([lines.join('\\n') + '\\n'], {type: 'application/json'});
  console.log(`uploading ${(body.size/1e6).toFixed(1)} MB as slot ${i}...`);
  const res = await fetch(URLS[i], {method: 'PUT', body});
  if (!res.ok) return console.error('upload failed', res.status, await res.text());
  used.push(i); localStorage.setItem('sf_used', JSON.stringify(used));
  console.log('uploaded. Spark picks it up within ~5s; open http://localhost:29090');
})();
""" % (__import__("json").dumps(urls, indent=1), n)
open(os.path.join(os.path.dirname(__file__), "..", "topology", "console-trigger.js"), "w").write(js)
print(f"wrote topology/console-trigger.js with {n} URLs")
