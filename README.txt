SPARKFLOCK  -  plain-English guide
==================================

WHAT IT DOES
You drop a file full of JSON messages into a storage bucket. A few seconds
later those messages are rows in a database. Nobody presses a button.

    your file  ->  bucket (MinIO)  ->  Spark reads it  ->  database (Postgres)

On your Mac, MinIO pretends to be Amazon S3 and Postgres pretends to be
Amazon RDS. Later the same code points at the real S3 and RDS by changing
settings only, not code.

THE FOUR PARTS
  1. MinIO      the bucket. Files land in the folder "incoming/".
  2. Spark      the worker. It watches the folder and moves new data.
  3. Postgres   the database. Table "events" holds good messages,
                table "events_rejects" holds broken ones.
  4. Terraform  creates the bucket (and, on AWS, the database) from code.
  Minikube      is a small Kubernetes on your Mac. It lets Spark add and
                remove workers on its own when a big file arrives.

WHAT YOU NEED INSTALLED (Mac)
  Docker Desktop, minikube, kubectl, terraform, uv, aws CLI, Java 21
  (brew install openjdk@21). Java 26 does NOT work with this Spark.

HOW TO RUN IT ON YOUR MACHINE
Open a terminal in this folder.

  Step 1   make init          one time: installs Spark into a private folder
  Step 2   make up            starts MinIO + Postgres, creates the bucket
  Step 3   make cluster-up    starts a Spark master and 1 worker
  Step 4   make stream        starts the always-on job (leave it running)

Open a second terminal:

  Step 5   make gen-one       makes a file holding one JSON message
  Step 6   make upload FILE=data/one.json
  Step 7   wait about 10 seconds, then:  make count

You should see 1 event. You touched nothing between step 6 and step 7.
That is the "automatic" part.

Try a big file:   make gen-big     then   make upload FILE=data/big.json
(600,000 messages, 80 MB.)

To stop everything:  make cluster-down   then   make down

TO PROVE IT WORKS (about 6 minutes)
  make test           checks 16 things end to end on your machine.
It should end with the words ALL PASS.

KUBERNETES (minikube) VERSION
  make k8s-start      one time: starts minikube, builds the Spark image
  make k8s-test MAX=4 uploads a big file and watches worker pods appear
                      (1 -> 2 -> 4), then disappear when work is done.
  make k8s-test MAX=2 same file, but the limit is 2 workers.

YOUR QUESTIONS, ANSWERED

Q: Does Spark read from MinIO to the database automatically?
A: Yes, while the job from step 4 is running. It checks the folder every
   5 seconds. Spark cannot be "woken" by S3; it looks. Measured delay from
   upload to row in the database: 9 to 10 seconds.
   If the job is NOT running, nothing happens until you start it. When you
   start it, it catches up on everything it missed.

Q: If the file is bigger, does the Spark master add more workers by itself?
A: No. Measured: with 1 worker running, an 80 MB file was processed by
   exactly 1 worker, and the master did not start a second one. In Spark's
   own "standalone" mode, the number of workers is whatever YOU started.
   A bigger file makes Spark cut the file into more pieces and spread them
   over the workers that exist.

Q: So how does scaling really work?
A: On Kubernetes (minikube) it does scale by itself. The Spark job asks
   Kubernetes for more worker pods when pieces of work are waiting, and
   hands them back after 20 idle seconds. Measured with the same 80 MB
   file: ceiling 4 gave 1 -> 2 -> 4 pods within 10 seconds, then back to 1
   20 to 30 seconds after the work finished. Ceiling 2 gave 1 -> 2 -> 1.

Q: Can we configure it?
A: Yes. In k8s/submit.sh (lines 33-38) three settings control it:
   MIN_EXECUTORS, MAX_EXECUTORS, IDLE_TIMEOUT. Example:
   MAX_EXECUTORS=6 k8s/submit.sh stream
   Piece size (how finely a file is cut) is MAX_PARTITION_MB in .env.

Q: Will more workers make it faster?
A: Not for this file, and we measured why. Of 7.4 seconds for 80 MB:
   about 1.4 s is Spark writing, about 4.1 s is Postgres merging rows
   safely (so replays never make duplicates), and that merge is one
   database job that extra workers cannot share. Across repeated runs
   ceiling 4 took 17 to 23 s and ceiling 2 took 18 to 19 s: no consistent
   gain. More workers help when files are much bigger or
   the work per row is heavier. They do not make the database faster.

WHAT HAPPENS WITH BAD DATA
  A line that is not valid JSON, or has no event_id, does not crash the job.
  It is put in the "events_rejects" table with the reason.
  A message saved as indented, multi-line JSON (one object, or a list of
  objects) is accepted as a whole. Indented files bigger than 32 MB are not.
  Uploading the same file twice does not create duplicate rows.

WHAT IS PROVEN, AND WHAT IS NOT
  Proven on this Mac (MinIO + Postgres + Spark + minikube):
    automatic pickup, replay safety, bad-data handling, worker counts on
    the Spark cluster, pod scaling up and down on minikube.
  NOT done:
    - The staging setup on real AWS (terraform/envs/staging) passes
      "terraform validate" but was never planned or applied: this machine
      has no AWS credentials. The Spark job was never run against real S3
      or real RDS. Real RDS may behave differently from local Postgres
      (SSL is forced, small instances are slower).
    - Kubernetes pods talking to real AWS S3 is not built (needs an AWS
      identity inside the pods).
    - Autoscaling the Spark standalone cluster is not built, and not
      possible by Spark itself; use the Kubernetes mode for that.

STAGING (REAL AWS, FREE CREDITS) - NOT YET TRIED
  1. Log in to AWS in your terminal.
  2. cd terraform/envs/staging
     cp terraform.tfvars.example terraform.tfvars   and fill it in
  3. terraform init && terraform plan    READ the plan. It makes one S3
     bucket and one small RDS Postgres. Then terraform apply.
  4. Copy .env.staging.example to .env.staging, fill from
     "terraform output", then run:
     ENV_FILE=.env.staging scripts/submit.sh local --mode once
  5. When done:  terraform destroy   (RDS costs money if left running)

WHERE THINGS ARE
  spark/job.py            the whole pipeline (one file, ~250 lines)
  spark/schema.sql        the tables
  scripts/                gen_events.py, submit.sh, standalone.sh, ...
  k8s/                    Dockerfile, rbac.yaml, submit.sh (scaling knobs)
  terraform/              modules/pipeline + envs/local + envs/staging
  tests/                  e2e.sh and k8s_scale.sh (the proofs)
  presentation/           index.html - the walkthrough, with file and line refs
  README.md               the same project, written for an AI agent

PORTS USED (chosen not to clash with other projects)
  MinIO 29000 (web console 29001), Postgres 25432, Spark master 27077.

LIVE TOPOLOGY DASHBOARD (added 2026-10-15)

WHAT IT IS
  A web page that draws your stack as 3D-ish blocks and updates every second:
  bucket -> Spark driver -> worker pods -> database. It is read-only: it can
  look at the pods but cannot create or delete anything.

HOW TO OPEN IT
  make topo-up        start it (needs make k8s-start first)
  open                http://localhost:29090
  make topo-down      stop it

WHAT YOU ARE LOOKING AT
  dashed pulsing block   a worker was requested but is not ready yet
  solid block rising     worker is running
  red block sinking      worker is being removed
  top-left pills         green = reachable, red = not
  "/ 4 max"              the real ceiling Spark was told to use
  graph along the bottom number of workers over the last 5 minutes
  The dotted lines show how things are wired. They do NOT show how fast
  data moves. Nothing measures that on this machine.

HOW TO MAKE WORKERS SCALE UP
  Option A (terminal)   make gen-big   then   make upload FILE=data/big.json
  Option B (browser)    open the dashboard, open DevTools console, paste the
                        contents of topology/console-trigger.js, press Enter.
                        It gives you 10 uploads. After that run:
                        . scripts/env.sh; python3 scripts/presign_console.py 10
  Then watch: 1 worker -> up to 4 -> back to 1 about 20 to 30 seconds after
  the work finishes.

IF THE SPARK JOB FAILS RIGHT AFTER minikube START
  The cluster's name lookup (CoreDNS) is still starting. Wait for it:
    kubectl -n kube-system wait --for=condition=Ready pod -l k8s-app=kube-dns
  then submit again.

HOW TO TEST THE SCREEN IN A BROWSER (checklist)
  Quick check without a browser:  curl localhost:29090/api/state
  1  Open the page. All pills green, one solid worker, "1 / 4 max".
  2  Console has no red errors.
  3  Upload the big file. Dashed blocks appear, turn solid, count reaches 4.
  4  Wait 30 seconds. Extra blocks go red, sink, vanish. Count back to 1.
  5  Delete one worker pod by hand. Its block goes red; Spark replaces it.
  6  docker stop sparkflock-minio. MinIO pill goes red. Start it again after.
  7  make cluster-up N=2. A purple second platform shows a master and 2 workers.
  8  make topo-down. Within about 4 seconds the page says "feed LOST".

WHAT WAS DONE (2026-10-15)
  - Stack started on Docker + minikube. One worker pod running.
  - Dashboard service and page built, with make topo-up / topo-down.
  - Browser-console upload built using pre-signed links.
  - Proven with a real 71 MB upload: workers went 1 -> 4 -> 2 (heading to 1)
    and the dashboard's event log recorded each one.

WHAT COULD NOT BE DONE
  - Nobody has SEEN the dashboard yet. The Chrome extension was not
    connected, so there is no screenshot, no console-error check, and the
    drawing code has not been mutation-tested. Only its data feed and the
    JavaScript syntax were checked.
  - The console upload script was never run in a real browser.
  - No speed or CPU display: there is nothing on this machine that measures it.

WHAT IS NEXT
  Reconnect the Chrome extension, run the 8-step checklist above, break the
  drawing code on purpose to prove the check catches it, then fix whatever the
  screenshot shows.

NEW FILES
  topology/                  the dashboard (server, page, compose, console script)
  k8s/topology-rbac.yaml     the read-only permission it uses
  scripts/presign_console.py makes the browser upload links
  Port 29090 added to the ports this project uses.
