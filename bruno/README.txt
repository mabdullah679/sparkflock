SPARKFLOCK PROBES - what this folder is
=======================================

THE PROBLEM THIS PROJECT SOLVES
Data arrives as files (lines of JSON messages). Someone has to get it into a
database, reliably, without a person pressing a button and without bad lines
breaking everything. Sparkflock does that: drop a file in a bucket, and about
10 seconds later its messages are rows in a database.

HOW IT DOES IT
  your file -> bucket (MinIO, a fake Amazon S3)
            -> Spark checks the "incoming/" folder every 5 seconds
            -> database (Postgres, a fake Amazon RDS)

  - Good messages go in table "events".
  - Broken ones (not JSON, or no event_id) go in "events_rejects" with the reason.
  - The same file twice never makes duplicate rows.
  - On Kubernetes, Spark asks for more workers when there is a lot to do and
    gives them back when idle. On plain Spark, you choose the worker count.

WHAT THE PROBES DO
Bruno is a tool that sends requests and checks the answers. Each probe asks
one question about the running stack. Pick the "local" environment first.

  1  Standalone workers alive   Spark master: how many workers are up?
  2  K8s executors running      Kubernetes mode: how many worker pods run?
  3  Upload one load burst      Drops 600,000 messages (about 70 MB) in the bucket.
  4  Spam load                  Runs the burst several times and watches the
                                worker count climb and fall (needs the dashboard).
  5  MinIO live                 Is the bucket server up?
  6  Upload sample (100 rows)   The smallest real test. Then run: make count
                                (events should rise by exactly 100).
  7  Files in incoming/         What is in the bucket's incoming/ folder.

SUGGESTED FIRST RUN (cheapest first)
  5 MinIO live -> 1 Standalone workers alive -> 6 Upload sample -> wait 10 s
  -> make count.  Skip 3 and 4 unless you want to watch scaling: they add
  about 600,000 rows per burst to your database.

WHAT THESE PROBES CANNOT CHECK
  Postgres is not a web service, so Bruno cannot ask it anything. Use
  "make count" for the row totals.

SETUP
  Uploads need S3_ACCESS_KEY and S3_SECRET_KEY in bruno/.env (never committed).
  Addresses and ports are in environments/local.bru.
