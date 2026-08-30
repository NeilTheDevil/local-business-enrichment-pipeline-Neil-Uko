"""
Stage 4: waterfall for every business LocalPipe left without an email.

  AI Ark  -> people search by domain, SENIORITY-FILTERED first (an unfiltered
             search on a large firm returns superintendents and recruiters and
             never the CEO), unfiltered only as a fallback for small firms whose
             owner is not seniority-tagged. Rank by title, collapse duplicate
             profiles of one person, then /v2/people/export/single for the email
             (1 credit only when found).
  Prospeo -> if still no email: enrich-person using the best known name
             (LocalPipe's owner_name preferred, else AI Ark's) + company website,
             or a LinkedIn URL when that's all we have. Free on no-match.
  else    -> left empty for the user to handle.

Every AI Ark request carries a browser User-Agent: the default Python UA is
rejected by Cloudflare with "error code: 1010" (HTTP 403), which looks exactly
like an auth failure but is not.

Run:  python enrich_waterfall.py --limit 10     # sample
      python enrich_waterfall.py                # all remaining
Out:  waterfall_progress.jsonl   appended per row, resume-safe
      waterfall_results.csv      parsed results
      waterfall.log
"""
import csv, json, os, re, sys, threading, time, urllib.request, urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
AIARK = "https://api.ai-ark.com/api/developer-portal"
PROSPEO = "https://api.prospeo.io"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# Region selection mirrors scrape.py; US filenames stay unsuffixed.
PREFIXES = {"us": "general-contractors", "ca": "general-contractors-canada"}
REGION = "us"
if "--region" in sys.argv:
    REGION = sys.argv[sys.argv.index("--region") + 1].lower()
if REGION not in PREFIXES:
    sys.exit(f"Unknown --region '{REGION}'. Available: {', '.join(PREFIXES)}")
PREFIX = PREFIXES[REGION]
SUF = "" if REGION == "us" else f"-{REGION}"

IN_CSV = os.path.join(HERE, f"{PREFIX}.csv")
LP_CSV = os.path.join(HERE, f"localpipe_results{SUF}.csv")
PROG = os.path.join(HERE, f"waterfall_progress{SUF}.jsonl")
OUT_CSV = os.path.join(HERE, f"waterfall_results{SUF}.csv")
LOG = os.path.join(HERE, f"waterfall{SUF}.log")

CONCURRENCY = 4        # AI Ark allows 5 req/s, 300/min -- stay under
MIN_INTERVAL = 0.25

# Best decision-maker first. AI Ark returns whoever it has for small firms, so
# this ranks rather than filters.
SENIORITY_RANK = ["owner", "founder", "c_suite", "partner", "vp", "head",
                  "director", "manager", "senior", "entry", "unpaid", "training"]

_lock = threading.Lock()
_last = [0.0]


# Business names contain emoji; on Windows the console is cp1252 and print()
# raises UnicodeEncodeError. Force UTF-8 where supported and never let logging
# raise -- a crash here would abandon jobs already paid for.
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


def throttle():
    with _lock:
        wait = MIN_INTERVAL - (time.time() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()


def req(url, headers, payload=None, method="POST", timeout=90, attempt=0):
    """Returns (code, body). code 0 == transient network error."""
    throttle()
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(url, data=data, headers=headers,
                               method=method if data else "GET")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as x:
            return x.status, json.loads(x.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        if e.code == 429 and attempt < 6:
            time.sleep(2 * (attempt + 1))
            return req(url, headers, payload, method, timeout, attempt + 1)
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw
    except Exception as e:
        if attempt < 4:
            time.sleep(2 * (attempt + 1))
            return req(url, headers, payload, method, timeout, attempt + 1)
        return 0, {"_net_error": f"{type(e).__name__}: {e}"}


# Hosts where many unrelated businesses share one registrable domain. A lookup
# keyed on these returns employees of the HOST, so it can only ever be wrong --
# skip before spending a credit.
SHARED_HOSTS = {
    "facebook.com", "m.facebook.com", "sites.google.com", "google.com",
    "business.site", "wixsite.com", "wix.com", "godaddysites.com", "weebly.com",
    "squarespace.com", "blogspot.com", "wordpress.com", "myshopify.com",
    "linktr.ee", "yelp.com", "instagram.com", "nextdoor.com", "angi.com",
    "houzz.com", "thumbtack.com",
}

MAX_CONTACTS = 3

# Decision-maker titles, in priority order. COO is wanted; CFO/CTO are not.
TITLE_RANK = [
    (re.compile(r"\b(chief\s+executive\s+officer|c\.?e\.?o\.?)\b", re.I), "CEO"),
    (re.compile(r"\b(co[-\s]?founder|founder|founding\s+partner)\b", re.I), "Founder"),
    (re.compile(r"\b(co[-\s]?owner|owner|proprietor)\b", re.I), "Owner"),
    (re.compile(r"\bpresident\b", re.I), "President"),
    (re.compile(r"\b(chief\s+operating\s+officer|c\.?o\.?o\.?)\b", re.I), "COO"),
    (re.compile(r"\bmanaging\s+(director|partner)\b", re.I), "Managing Director"),
    (re.compile(r"\bexecutive\s+director\b", re.I), "Executive Director"),
    (re.compile(r"\bprincipal\b", re.I), "Principal"),
    (re.compile(r"\bpartner\b", re.I), "Partner"),
    (re.compile(r"\b(chairman|chairwoman|chairperson|chair\s+of\s+the\s+board)\b", re.I), "Chairman"),
    (re.compile(r"\bboard\s+(member|director)\b|\bdirector\s*,?\s*board\b", re.I), "Board Member"),
    (re.compile(r"\b(investor|shareholder|stakeholder|backer)\b", re.I), "Investor"),
]

# Checked FIRST, because several non-matches look like matches until it runs:
# "Partner Intelligence Manager" (a manager), "Chief Financial Officer,
# Executive Vice President" (CFO), "Board Member" (not an operator).
TITLE_EXCLUDE = re.compile(
    r"\b(cfo|cto|cmo|cio|cro|chro|ciso)\b"
    r"|chief\s+(financial|technolog\w*|marketing|information|revenue|people|"
    r"human|legal|security|data|product|compliance|strategy|innovation|"
    r"administrative|accounting|nursing|medical)\b"
    r"|\b(vice\s+president|vp|svp|evp|avp)\b"
    r"|\b(manager|coordinator|specialist|estimator|foreman|superintendent|"
    r"supervisor|analyst|assistant|associate|intern|apprentice|engineer|"
    r"architect|accountant|administrator|recruiter|labou?rer|carpenter|"
    r"technician|representative|consultant|advis[eo]r|clerk|receptionist|"
    r"bookkeeper|scheduler|planner|buyer|drafter|designer)\b"
    r"|\bdirector\s+of\b|\bdeputy\b|\binterim\b",
    re.I)

# Seniority buckets to ask AI Ark for. On a 4,000-employee firm an UNFILTERED
# search returns superintendents and recruiters, never the CEO -- so filtering
# is required, not optional. The unfiltered pass is only a fallback for small
# firms whose owner is not seniority-tagged.
AIARK_SENIORITY = ["c_suite", "founder", "owner", "partner", "president",
                   "executive", "chief_executive_officer"]


def classify_title(title):
    """(rank, label) if this title can make a buying decision, else None."""
    if not title:
        return None
    t = title.strip()
    if TITLE_EXCLUDE.search(t):
        return None
    for i, (rx, label) in enumerate(TITLE_RANK):
        if rx.search(t):
            return i, label
    return None


def person_title(p):
    """AI Ark hides the job title in position_groups[].profile_positions[].title
    -- `headline` comes back null."""
    for g in (p.get("position_groups") or []):
        for pos in (g.get("profile_positions") or []):
            if pos.get("title"):
                return pos["title"]
    return ""


def aiark_email_for(person_id, H):
    """Fetch one person's email. 1 credit, charged only when found."""
    code, e = req(f"{AIARK}/v2/people/export/single", H, {"id": person_id})
    if code != 200 or not isinstance(e, dict):
        return ""
    data = e.get("data") or e
    em = data.get("email") or {}
    addr = em.get("value") or em.get("address") or ""
    if not addr:
        outp = em.get("output") or []
        if outp and isinstance(outp, list):
            addr = (outp[0] or {}).get("address", "")
    return addr or ""


def aiark_contacts(domain, H, want=MAX_CONTACTS):
    """Up to `want` decision-makers at this domain, most senior first.

    Two passes. The seniority-filtered pass is the important one: on a
    4,000-employee firm an unfiltered search returns superintendents and
    recruiters and never the CEO. The unfiltered pass only exists to catch small
    firms whose owner is present but not seniority-tagged.
    """
    people, total = [], 0
    for body in (
        {"account": {"domain": {"any": {"include": [domain]}}},
         "contact": {"seniority": {"any": {"include": AIARK_SENIORITY}}},
         "page": 0, "size": 25},
        {"account": {"domain": {"any": {"include": [domain]}}},
         "page": 0, "size": 25},
    ):
        code, d = req(f"{AIARK}/v1/people", H, body)
        if code == 200 and isinstance(d, dict):
            people = d.get("content") or []
            total = d.get("totalElements", len(people))
        if people:
            break
    if not people:
        return [], 0

    # Keep only decision-makers, best title first.
    ranked = []
    for p in people:
        title = person_title(p)
        hit = classify_title(title)
        if not hit:
            continue
        prof = p.get("profile") or {}
        ranked.append((hit[0], {
            "name": prof.get("full_name") or "",
            "first": prof.get("first_name") or "",
            "last": prof.get("last_name") or "",
            "title": title,
            "title_label": hit[1],
            "linkedin": (p.get("link") or {}).get("linkedin") or "",
            "seniority": (p.get("department") or {}).get("seniority") or "",
            "_id": p.get("id"),
        }))
    ranked.sort(key=lambda x: x[0])

    # AI Ark holds duplicate profile records for the same human -- observed as
    # the same person listed twice on one company under two different titles
    # ("CEO" and "Owner"), and twice under the SAME title. 8 companies affected
    # in one run. Collapse by name BEFORE fetching emails, so we do
    # not pay twice for one person and never ship them twice. Sorted best-title
    # first, so the survivor keeps the most senior title.
    seen_names, unique = set(), []
    for rank, c in ranked:
        key = re.sub(r"[^a-z]", "", (c.get("name") or "").lower())
        if key and key in seen_names:
            continue
        if key:
            seen_names.add(key)
        unique.append((rank, c))
    ranked = unique

    out = []
    for _, c in ranked[:want]:
        if c["_id"]:
            c["email"] = aiark_email_for(c["_id"], H)
        out.append(c)
    return out, total


def prospeo_lookup(first, last, domain, linkedin, key):
    """1 credit only when an email is found; no charge on NO_MATCH."""
    H = {"Content-Type": "application/json", "X-KEY": key, "User-Agent": UA}
    extra = {}
    if first and last and domain:
        data = {"first_name": first, "last_name": last, "company_website": domain}
    elif linkedin:
        data = {"linkedin_url": linkedin}
    elif domain:
        # No name anywhere. Search Prospeo by company website for a person_id,
        # then enrich that. Costs 1 credit only if the search returns someone;
        # NO_RESULTS is free.
        code, b = req(f"{PROSPEO}/search-person", H,
                      {"page": 1, "filters": {"company": {"websites": {"include": [domain]}}}})
        if code != 200 or not isinstance(b, dict) or b.get("error"):
            ec = b.get("error_code") if isinstance(b, dict) else code
            return {"prospeo_error": f"search {ec}"}
        results = b.get("results") or []
        if not results:
            return {"prospeo_error": "search NO_RESULTS"}
        person = results[0].get("person") or {}
        ppid = person.get("person_id") or person.get("id")
        if not ppid:
            return {"prospeo_error": "search returned no person_id"}
        extra = {"prospeo_searched_name": person.get("full_name", "")}
        data = {"person_id": ppid}
    else:
        return {"prospeo_skipped": "no name, no linkedin, no domain"}

    code, b = req(f"{PROSPEO}/enrich-person", H, {"data": data})
    if code != 200 or not isinstance(b, dict):
        err = b.get("error_code") if isinstance(b, dict) else str(b)[:60]
        return {**extra, "prospeo_error": f"HTTP {code} {err}"}
    if b.get("error"):
        return {**extra, "prospeo_error": b.get("error_code", "unknown")}
    person = b.get("person") or {}
    em = person.get("email") or {}
    addr = em.get("email") or ""
    # Prospeo masks unrevealed addresses with '*' -- those are not usable.
    if addr and "*" in addr:
        return {**extra, "prospeo_masked": addr, "prospeo_status": em.get("status", "")}
    return {**extra, "prospeo_email": addr, "prospeo_status": em.get("status", ""),
            "prospeo_name": person.get("full_name", ""),
            "prospeo_title": person.get("current_job_title", "") or ""}


def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])

    open(LOG, "a").close()
    env = load_env()
    H_ark = {"Content-Type": "application/json", "X-TOKEN": env["AIARK_KEY"],
             "User-Agent": UA, "Accept": "*/*"}
    pkey = env["PROSPEO_KEY"]

    with open(IN_CSV, encoding="utf-8-sig") as f:
        biz = list(csv.DictReader(f))
    lp = {}
    with open(LP_CSV, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            lp[r["place_id"]] = r

    done = set()
    if os.path.exists(PROG):
        with open(PROG, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        done.add(json.loads(line)["place_id"])
                    except Exception:
                        continue

    targets, skipped_host, skipped_dupe = [], 0, 0
    seen_domains = {}
    for b in biz:
        e = lp.get(b["place_id"], {})
        if (e.get("owner_email") or "").strip() or (e.get("business_email") or "").strip():
            continue                      # LocalPipe already delivered
        if b["place_id"] in done:
            continue                      # already processed by a previous run
        dom = (b.get("domain") or "").strip().lower()
        # A shared host resolves to employees of the HOST, so the lookup is
        # guaranteed useless -- and it was being paid for, then discarded at
        # export. Skip before spending.
        if not dom or dom in SHARED_HOSTS:
            skipped_host += 1
            continue
        # AI Ark and Prospeo key on DOMAIN alone, so every extra location of a
        # franchise returns the same people. 16 Alair Homes storefronts cost 16
        # lookups for one answer. Enrich one per domain and fan the result out.
        if dom in seen_domains:
            seen_domains[dom].append(b["place_id"])
            skipped_dupe += 1
            continue
        seen_domains[dom] = []
        targets.append(b)
    if limit:
        targets = targets[:limit]

    log(f"Waterfall targets: {len(targets)} "
        f"({len(done)} already processed, concurrency {CONCURRENCY})")
    if skipped_dupe or skipped_host:
        log(f"  skipped before spending: {skipped_dupe} extra locations sharing a "
            f"domain, {skipped_host} on a shared host / no domain")

    counter = [0]

    def work(b):
        pid = b["place_id"]
        e = lp.get(pid, {})
        contacts, total = aiark_contacts(b["domain"], H_ark)

        # AI Ark found the person but no address -> Prospeo fills it in.
        for c in contacts:
            if not c.get("email"):
                r = prospeo_lookup(c.get("first", ""), c.get("last", ""),
                                   b["domain"], c.get("linkedin", ""), pkey)
                if r.get("prospeo_email"):
                    c["email"], c["source"] = r["prospeo_email"], "prospeo"
            elif not c.get("source"):
                c["source"] = "aiark"

        # AI Ark had nobody usable -> try Prospeo against the company directly.
        if not any(c.get("email") for c in contacts):
            lp_name = (e.get("owner_name") or "").strip()
            first = (e.get("owner_first_name") or "").strip()
            last = (e.get("owner_last_name") or "").strip()
            if not (first and last) and len(lp_name.split()) >= 2:
                first, last = lp_name.split()[0], lp_name.split()[-1]
            r = prospeo_lookup(first, last, b["domain"], "", pkey)
            if r.get("prospeo_email"):
                ptitle = r.get("prospeo_title", "")
                # A name from LocalPipe is already an owner/decision-maker, so
                # trust it. A blind domain search is not, so it must pass the
                # title filter or we would ship receptionists as "C-suite".
                trusted = bool(first and last and lp_name)
                if trusted or classify_title(ptitle):
                    contacts.append({
                        "name": r.get("prospeo_name") or lp_name,
                        "first": first, "last": last,
                        "title": ptitle,
                        "title_label": (classify_title(ptitle) or (None, "Owner"))[1],
                        "linkedin": "", "seniority": "",
                        "email": r["prospeo_email"], "source": "prospeo",
                    })

        kept = [c for c in contacts if c.get("email")][:MAX_CONTACTS]
        rec = {"place_id": pid, "business_name": b["business_name"],
               "domain": b["domain"], "aiark_people": total,
               "lp_owner_name": e.get("owner_name", ""),
               "contacts": kept,
               "final_email": kept[0]["email"] if kept else "",
               "final_source": kept[0].get("source", "") if kept else ""}
        with _lock:
            with open(PROG, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")
            counter[0] += 1
            n = counter[0]
        if n % 10 == 0 or kept:
            who = "; ".join(f"{c['name']} ({c['title_label']})" for c in kept) or "-"
            log(f"  [{n}/{len(targets)}] {b['business_name'][:26]:<26} "
                f"{len(kept)} contact(s)  {who[:60]}")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        list(ex.map(work, targets))

    # ---------- write results ----------
    recs = []
    with open(PROG, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    recs.append(json.loads(line))
                except Exception:
                    continue
    # ONE ROW PER CONTACT: a company with 3 decision-makers produces 3 rows
    # sharing a place_id, so every row is a single send target.
    cols = ["place_id", "business_name", "domain", "contact_index",
            "contact_name", "contact_title", "contact_title_label",
            "contact_linkedin", "contact_email", "contact_source",
            "aiark_people", "lp_owner_name"]
    # Fan the result of each domain lookup back out to the sibling locations we
    # deliberately skipped, so a 16-branch franchise still gets contacts on all
    # 16 rows while only one lookup was paid for.
    by_pid = {b["place_id"]: b for b in biz}
    fanned = 0
    for r in list(recs):
        dom = (r.get("domain") or "").strip().lower()
        for sib in seen_domains.get(dom, []):
            b = by_pid.get(sib)
            if not b:
                continue
            recs.append({**r, "place_id": sib,
                         "business_name": b.get("business_name", ""),
                         "fanned_from": r.get("place_id")})
            fanned += 1
    if fanned:
        log(f"  fanned {fanned} sibling location(s) from an already-paid lookup")

    flat = []
    for r in recs:
        cs = r.get("contacts") or []
        if not cs:
            flat.append({"place_id": r.get("place_id"),
                         "business_name": r.get("business_name"),
                         "domain": r.get("domain"), "contact_index": 0,
                         "aiark_people": r.get("aiark_people", 0),
                         "lp_owner_name": r.get("lp_owner_name", "")})
            continue
        for i, c in enumerate(cs, 1):
            flat.append({
                "place_id": r.get("place_id"),
                "business_name": r.get("business_name"),
                "domain": r.get("domain"),
                "contact_index": i,
                "contact_name": c.get("name", ""),
                "contact_title": c.get("title", ""),
                "contact_title_label": c.get("title_label", ""),
                "contact_linkedin": c.get("linkedin", ""),
                "contact_email": c.get("email", ""),
                "contact_source": c.get("source", ""),
                "aiark_people": r.get("aiark_people", 0),
                "lp_owner_name": r.get("lp_owner_name", ""),
            })
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in flat:
            w.writerow({c: r.get(c, "") for c in cols})

    withc = [r for r in recs if r.get("contacts")]
    n_contacts = sum(len(r["contacts"]) for r in withc)
    n_ark = sum(1 for r in withc for c in r["contacts"] if c.get("source") == "aiark")
    n_pro = sum(1 for r in withc for c in r["contacts"] if c.get("source") == "prospeo")
    titles = Counter(c.get("title_label", "?") for r in withc for c in r["contacts"])
    log(f"RESULT  companies {len(recs)}  with contacts {len(withc)}  "
        f"contacts {n_contacts} (aiark {n_ark}, prospeo {n_pro})  "
        f"still empty {len(recs) - len(withc)}")
    log(f"  avg contacts per company found: "
        f"{n_contacts / len(withc):.2f}" if withc else "  none")
    log(f"  titles: {dict(titles.most_common())}")
    log(f"Wrote {OUT_CSV}")
    log("DONE")


if __name__ == "__main__":
    main()
