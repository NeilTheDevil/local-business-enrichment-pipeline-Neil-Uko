"""
Stage 3: LocalPipe /enrich -- owner name + verified email per business.

Async by design: submit -> job_id -> poll to terminal. Per LocalPipe's docs this
whole flow stays inside THIS script (never an agent loop) so a shell timeout
cannot lose results the user has already been charged for.

Run:  python enrich_localpipe.py --limit 5      # sample
      python enrich_localpipe.py                # all rows
Out:  localpipe_jobs.json      place_id -> job_id (resume point)
      localpipe_results.csv    parsed results
      localpipe.log            progress
"""
import csv, json, os, sys, threading, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
API = "https://uunrlffonnvmbsstkpze.supabase.co/functions/v1"
ANON = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6"
        "InV1bnJsZmZvbm52bWJzc3RrcHplIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA0OTIwMzMs"
        "ImV4cCI6MjA4NjA2ODAzM30.8Ic_U17YrZtsDVnYnIP0XJzrhZJOLDSlJ0AZfTS31FQ")

# Region selection mirrors scrape.py. US filenames stay unsuffixed so existing
# US data keeps working; other regions get a suffix and never collide with it.
PREFIXES = {"us": "general-contractors", "ca": "general-contractors-canada"}
REGION = "us"
if "--region" in sys.argv:
    REGION = sys.argv[sys.argv.index("--region") + 1].lower()
if REGION not in PREFIXES:
    sys.exit(f"Unknown --region '{REGION}'. Available: {', '.join(PREFIXES)}")
PREFIX = PREFIXES[REGION]
SUF = "" if REGION == "us" else f"-{REGION}"

IN_CSV   = os.path.join(HERE, f"{PREFIX}.csv")
JOBS     = os.path.join(HERE, f"localpipe_jobs{SUF}.json")
JOBS_JSONL = os.path.join(HERE, f"localpipe_jobs{SUF}.jsonl")  # append-on-submit
OUT_CSV  = os.path.join(HERE, f"localpipe_results{SUF}.csv")
LOG      = os.path.join(HERE, f"localpipe{SUF}.log")

CONCURRENCY = 8         # submissions. 20 triggered RemoteDisconnected; 8 is proven
MIN_INTERVAL = 0.14     # ~430/min, under the 500/min ceiling
POLL_EVERY = 15         # seconds between sweeps
POLL_CONCURRENCY = 40   # polling is excluded from rate limits, so sweep wide

_lock = threading.Lock()
_last = [0.0]


# Business names contain emoji (a Canadian contractor had U+1F477). On Windows
# the console is cp1252 and print() raises UnicodeEncodeError, which previously
# killed a run mid-poll. Force UTF-8 where supported, and never let logging
# raise regardless.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", "replace").decode("ascii"), flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_env():
    env = {}
    path = os.path.join(HERE, ".env")
    if not os.path.exists(path):
        sys.exit("No .env file found.\n"
                 "  1. cp .env.example .env\n"
                 "  2. paste your API keys into .env\n"
                 "See the README for where to get each key.")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def post(url, payload, headers, timeout=90):
    """Returns (status_code, parsed_json_or_text)."""
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw
    except Exception as e:
        # Network-level failure (RemoteDisconnected, timeout, DNS...). Previously
        # this propagated and killed the whole run mid-submission, orphaning every
        # job already accepted. Code 0 signals "transient -- retry".
        return 0, {"_net_error": f"{type(e).__name__}: {e}"}


def get(url, headers, timeout=90):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw


def throttle():
    with _lock:
        wait = MIN_INTERVAL - (time.time() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()


def submit(row, key, attempt=0):
    """POST /enrich. 202 is SUCCESS. Retries every 429 (a 429 = not accepted)."""
    throttle()
    payload = {
        "business_name": row["business_name"],
        "business_website": row["business_website"],
        "business_location": row["business_location"],
        "want_email": True,
        "want_general_email": True,
        "general_email_fallback_only": True,   # only if owner email missing
        "want_phone": False,                    # 5 credits; off by default
        # LocalPipe's own field name for a caller-supplied correlation id --
        # nothing here depends on Clay. It comes back on the result verbatim.
        "clay_row_id": row["place_id"],
    }
    code, body = post(f"{API}/enrich", payload,
                      {"Content-Type": "application/json", "x-api-key": key})

    if code == 429:
        if attempt >= 8:
            log(f"  429 giving up after 8 retries: {row['business_name'][:40]}")
            return None
        wait = (body.get("retry_after", 1) if isinstance(body, dict) else 1)
        time.sleep(wait + attempt)
        return submit(row, key, attempt + 1)

    if code == 0:                      # transient network error -> back off, retry
        if attempt >= 6:
            log(f"  network giving up after 6 retries: {row['business_name'][:40]}")
            return None
        time.sleep(2 * (attempt + 1))
        return submit(row, key, attempt + 1)

    if code in (200, 202) and isinstance(body, dict) and body.get("job_id"):
        jid = body["job_id"]
        # Persist IMMEDIATELY. A crash after this point must never orphan a job
        # the user has already been charged for.
        with _lock:
            with open(JOBS_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps({"place_id": row["place_id"], "job_id": jid,
                                    "name": row["business_name"]}) + "\n")
        return jid

    log(f"  submit failed HTTP {code}: {str(body)[:160]}")
    return None


def clean(v):
    """LocalPipe returns the literal string 'not found' for misses."""
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "not found" else s


def poll_once(job_id):
    """One status check. Returns (terminal_status|None, records).

    Jobs are polled in wide sweeps rather than one blocking thread each: the
    docs exclude polling endpoints from all rate limits, so a thread-per-job
    design would needlessly serialize hundreds of jobs into small batches.
    """
    url = f"{API}/get-job-results?job_id={job_id}"
    try:
        code, body = get(url, {"apikey": ANON})
    except Exception:
        return None, []                      # transient: treat as still pending
    if isinstance(body, dict):
        st = str(body.get("status", "")).lower()
        if st in ("completed", "complete", "success", "done"):
            # docs show 'records' in the webhook payload; polling may use 'results'
            recs = body.get("records") or body.get("results") or []
            if isinstance(recs, dict):
                recs = recs.get("records", [])
            return "completed", recs
        if st in ("failed", "error", "cancelled"):
            return st, []
    return None, []


def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])

    open(LOG, "w").close()
    key = load_env()["LOCALPIPE_KEY"]

    with open(IN_CSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if limit:
        rows = rows[:limit]

    # ---------- resume: never re-pay for a row already enriched ----------
    prior = {}
    if os.path.exists(OUT_CSV):
        with open(OUT_CSV, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                if r.get("lp_status") == "completed":
                    prior[r["place_id"]] = r
    todo = [r for r in rows if r["place_id"] not in prior]

    # Franchises and multi-branch firms share one website, and LocalPipe keyed on
    # that website mostly returns the same person for every branch -- measured:
    # 109 locations on shared domains produced only 43 distinct results, so 66
    # enrichments were paid for twice. Enrich one branch per domain and fan the
    # answer out to its siblings. --no-dedupe-domains restores per-branch
    # enrichment when you specifically want each local franchisee.
    siblings = {}
    if "--no-dedupe-domains" not in sys.argv:
        seen_dom, kept = {}, []
        for r in todo:
            dom = (r.get("domain") or "").strip().lower()
            if not dom:
                kept.append(r)
                continue
            if dom in seen_dom:
                siblings.setdefault(seen_dom[dom], []).append(r["place_id"])
                continue
            seen_dom[dom] = r["place_id"]
            kept.append(r)
        skipped = len(todo) - len(kept)
        if skipped:
            log(f"Domain dedupe: {skipped} extra branch(es) sharing a domain will "
                f"reuse their sibling's result instead of being enriched again")
        todo = kept
    log(f"Enriching {len(rows)} businesses via LocalPipe (concurrency {CONCURRENCY})")
    if prior:
        log(f"Resume: {len(prior)} already completed, submitting {len(todo)} new")

    # ---------- recover any job_ids submitted by an earlier crashed run ----------
    known = {}
    if os.path.exists(JOBS_JSONL):
        with open(JOBS_JSONL, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        d = json.loads(line)
                        known[d["place_id"]] = d
                    except Exception:
                        continue
    recovered = [r for r in todo if r["place_id"] in known]
    to_submit = [r for r in todo if r["place_id"] not in known]
    if recovered:
        log(f"Recovered {len(recovered)} job_ids from a previous run -- "
            f"polling those instead of resubmitting")

    # ---------- submit ----------
    jobs = {r["place_id"]: {"job_id": known[r["place_id"]]["job_id"],
                            "name": r["business_name"]} for r in recovered}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = {ex.submit(submit, r, key): r for r in to_submit}
        for fut, r in futs.items():
            try:
                jid = fut.result()
            except Exception as e:
                log(f"  submit raised {type(e).__name__} for "
                    f"{r['business_name'][:36]}: {e}")
                jid = None
            if jid:
                jobs[r["place_id"]] = {"job_id": jid, "name": r["business_name"]}

    with open(JOBS, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=1)

    # Hard assertion from the docs: submissions must equal input rows.
    log(f"Have {len(jobs)} job_ids for {len(todo)} outstanding rows")
    if len(jobs) != len(todo):  # docs: submissions MUST equal input rows
        log(f"  WARNING: {len(todo) - len(jobs)} rows failed to submit -- "
            f"these are lost unless resubmitted. Re-run to retry them.")

    # ---------- poll in wide sweeps until every job is terminal ----------
    log(f"Polling {len(jobs)} jobs to terminal status "
        f"(no timeout -- jobs can take 30+ min)...")
    out = {}
    pending = dict(jobs)
    cycle = 0
    while pending:
        cycle += 1
        finished = {}
        with ThreadPoolExecutor(max_workers=POLL_CONCURRENCY) as ex:
            futs = {ex.submit(poll_once, m["job_id"]): pid for pid, m in pending.items()}
            for fut, pid in futs.items():
                st, recs = fut.result()
                if st:
                    finished[pid] = (st, recs[0] if recs else {})

        for pid, (st, rec) in finished.items():
            out[pid] = {"status": st, "rec": rec}
            pending.pop(pid, None)
            log(f"  {jobs[pid]['name'][:32]:<32} {st} "
                f"owner={clean(rec.get('owner_name')) or '-'} "
                f"email={clean(rec.get('owner_email')) or '-'} "
                f"biz={clean(rec.get('business_email')) or '-'}")

        log(f"-- cycle {cycle}: {len(out)}/{len(jobs)} done, {len(pending)} pending")
        # checkpoint so a crash never loses paid-for results
        with open(os.path.join(HERE, "localpipe_partial.json"), "w",
                  encoding="utf-8") as f:
            json.dump(out, f)
        if pending:
            time.sleep(POLL_EVERY)

    # ---------- write ----------
    cols = ["place_id", "business_name", "business_website", "business_location",
            "owner_name", "owner_first_name", "owner_last_name", "owner_email",
            "business_email", "email_provider", "owner_role", "lp_status"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        # Map each skipped sibling back to the branch that was actually enriched.
        fan = {sib: src for src, sibs in siblings.items() for sib in sibs}
        for r in rows:
            pid = r["place_id"]
            if pid in prior and pid not in out:
                w.writerow({c: prior[pid].get(c, "") for c in cols})
                continue
            if pid in fan and pid not in out:
                src = out.get(fan[pid], {})
                rec = src.get("rec", {})
                if rec:
                    w.writerow({
                        "place_id": pid,
                        "business_name": r["business_name"],
                        "business_website": r["business_website"],
                        "business_location": r["business_location"],
                        "owner_name": clean(rec.get("owner_name")),
                        "owner_first_name": clean(rec.get("owner_first_name")),
                        "owner_last_name": clean(rec.get("owner_last_name")),
                        "owner_email": clean(rec.get("owner_email")),
                        "business_email": clean(rec.get("business_email")),
                        "email_provider": clean(rec.get("email_provider")),
                        "owner_role": clean(rec.get("owner_role")),
                        "lp_status": "completed_fanned",
                    })
                    continue
            e = out.get(pid, {})
            rec = e.get("rec", {})
            w.writerow({
                "place_id": pid,
                "business_name": r["business_name"],
                "business_website": r["business_website"],
                "business_location": r["business_location"],
                "owner_name": clean(rec.get("owner_name")),
                "owner_first_name": clean(rec.get("owner_first_name")),
                "owner_last_name": clean(rec.get("owner_last_name")),
                "owner_email": clean(rec.get("owner_email")),
                "business_email": clean(rec.get("business_email")),
                "email_provider": clean(rec.get("email_provider")),
                "owner_role": clean(rec.get("owner_role")),
                "lp_status": e.get("status", "not_submitted"),
            })

    n_owner = sum(1 for v in out.values() if clean(v["rec"].get("owner_email")))
    n_name = sum(1 for v in out.values() if clean(v["rec"].get("owner_name")))
    n_biz = sum(1 for v in out.values() if clean(v["rec"].get("business_email")))
    log(f"RESULT  names {n_name}/{len(rows)}  owner_emails {n_owner}/{len(rows)}  "
        f"business_emails {n_biz}/{len(rows)}")
    log(f"Wrote {OUT_CSV}")
    log("DONE")


if __name__ == "__main__":
    main()
