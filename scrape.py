"""
Stage 1 + 2: Scrape local businesses across the configured cities, dedup,
filter, derive chain/multi-location flags, emit <=TARGET_TOTAL rows.

Two interchangeable scrape providers -- ScraperTech is OPTIONAL:

  python scrape.py                        # ScraperTech if its key is set,
                                          # otherwise LocalPipe automatically
  python scrape.py --provider localpipe   # force LocalPipe (one key for
                                          # the whole pipeline)
  python scrape.py --provider scrapertech # force ScraperTech
  python scrape.py --reuse-raw            # rebuild from cache, ZERO API calls

Both providers feed identical columns downstream, so the export schema is the
same either way. See the README for which to pick.

Out:  raw/<city>.json          one file per city (audit trail)
      general-contractors.csv  final rows, enrichment-ready
      scrape.log               progress + failures
"""
import csv, json, os, re, sys, time, urllib.request, urllib.error
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))

KEYWORD      = "general contractors"
TARGET_TOTAL = 500     # hard cap on final rows
PER_CITY     = 25      # request ceiling per city; ~800 raw before filtering
WORKERS      = 4       # conservative: ScraperTech rate limits are undocumented
TIMEOUT      = 300

# Longitudes below are WEST -> stored NEGATIVE. Getting this wrong silently
# scrapes the wrong hemisphere and returns plausible-looking garbage.
CITIES_US = [
    ("New York City, NY",     40.7143, -74.0060), ("Los Angeles, CA",   34.0522, -118.2437),
    ("Brooklyn, NY",          40.6501, -73.9496), ("Chicago, IL",       41.8500, -87.6500),
    ("Queens, NY",            40.6815, -73.8365), ("Houston, TX",       29.7633, -95.3633),
    ("Phoenix, AZ",           33.4484, -112.0740),("Philadelphia, PA",  39.9524, -75.1636),
    ("San Antonio, TX",       29.4241, -98.4936), ("Manhattan, NY",     40.7834, -73.9663),
    ("San Diego, CA",         32.7157, -117.1647),("The Bronx, NY",     40.8499, -73.8664),
    ("Dallas, TX",            32.7831, -96.8067), ("Jacksonville, FL",  30.3322, -81.6556),
    ("Fort Worth, TX",        32.7254, -97.3208), ("San Jose, CA",      37.3394, -121.8950),
    ("Austin, TX",            30.2672, -97.7431), ("Columbus, OH",      39.9612, -82.9988),
    ("Charlotte, NC",         35.2271, -80.8431), ("Indianapolis, IN",  39.7684, -86.1580),
    ("San Francisco, CA",     37.7749, -122.4194),("Seattle, WA",       47.6062, -122.3321),
    ("Denver, CO",            39.7392, -104.9847),("Washington, DC",    38.8951, -77.0364),
    ("Nashville, TN",         36.1659, -86.7844), ("Oklahoma City, OK", 35.4676, -97.5164),
    ("El Paso, TX",           31.7587, -106.4869),("Boston, MA",        42.3584, -71.0598),
    ("Portland, OR",          45.5234, -122.6762),("Detroit, MI",       42.3314, -83.0457),
    ("Las Vegas, NV",         36.1750, -115.1372),("New South Memphis, TN", 35.0868, -90.0568),
    # --- secondary markets and suburbs: where the small operators actually are.
    ("Louisville, KY",        38.2527, -85.7585), ("Baltimore, MD",      39.2904, -76.6122),
    ("Milwaukee, WI",         43.0389, -87.9065), ("Albuquerque, NM",    35.0844, -106.6504),
    ("Tucson, AZ",            32.2226, -110.9747),("Fresno, CA",         36.7378, -119.7871),
    ("Sacramento, CA",        38.5816, -121.4944),("Kansas City, MO",    39.0997, -94.5786),
    ("Mesa, AZ",              33.4152, -111.8315),("Atlanta, GA",        33.7490, -84.3880),
    ("Omaha, NE",             41.2565, -95.9345), ("Colorado Springs, CO",38.8339,-104.8214),
    ("Raleigh, NC",           35.7796, -78.6382), ("Virginia Beach, VA", 36.8529, -75.9780),
    ("Long Beach, CA",        33.7701, -118.1937),("Oakland, CA",        37.8044, -122.2712),
    ("Minneapolis, MN",       44.9778, -93.2650), ("Tulsa, OK",          36.1540, -95.9928),
    ("Bakersfield, CA",       35.3733, -119.0187),("Wichita, KS",        37.6872, -97.3301),
    ("Aurora, CO",            39.7294, -104.8319),("Tampa, FL",          27.9506, -82.4572),
    ("New Orleans, LA",       29.9511, -90.0715), ("Cleveland, OH",      41.4993, -81.6944),
    ("Anaheim, CA",           33.8366, -117.9143),("Lexington, KY",      38.0406, -84.5037),
    ("Riverside, CA",         33.9806, -117.3755),("Newark, NJ",         40.7357, -74.1724),
    ("Saint Paul, MN",        44.9537, -93.0900), ("Cincinnati, OH",     39.1031, -84.5120),
    ("Orlando, FL",           28.5383, -81.3792), ("Pittsburgh, PA",     40.4406, -79.9959),
    ("St. Louis, MO",         38.6270, -90.1994), ("Greensboro, NC",     36.0726, -79.7920),
    ("Buffalo, NY",           42.8864, -78.8784), ("Boise, ID",          43.6150, -116.2023),
    ("Salt Lake City, UT",    40.7608, -111.8910),("Richmond, VA",       37.5407, -77.4360),
    ("Des Moines, IA",        41.5868, -93.6250), ("Spokane, WA",        47.6588, -117.4260),
    ("Baton Rouge, LA",       30.4515, -91.1871), ("Birmingham, AL",     33.5186, -86.8104),
]

# All of Canada is west of Greenwich, so every longitude here is negative too.
CITIES_CA = [
    ("Toronto, ON",           43.6532, -79.3832), ("Montreal, QC",       45.5017, -73.5673),
    ("Vancouver, BC",         49.2827, -123.1207),("Calgary, AB",        51.0447, -114.0719),
    ("Edmonton, AB",          53.5461, -113.4938),("Ottawa, ON",         45.4215, -75.6972),
    ("Mississauga, ON",       43.5890, -79.6441), ("Winnipeg, MB",       49.8951, -97.1384),
    ("Quebec City, QC",       46.8139, -71.2080), ("Hamilton, ON",       43.2557, -79.8711),
    ("Brampton, ON",          43.7315, -79.7624), ("Surrey, BC",         49.1913, -122.8490),
    ("Kitchener, ON",         43.4516, -80.4925), ("Halifax, NS",        44.6488, -63.5752),
    ("Laval, QC",             45.6066, -73.7124), ("London, ON",         42.9849, -81.2453),
    ("Markham, ON",           43.8561, -79.3370), ("Vaughan, ON",        43.8361, -79.4983),
    ("Gatineau, QC",          45.4765, -75.7013), ("Saskatoon, SK",      52.1332, -106.6700),
    ("Longueuil, QC",         45.5312, -73.5185), ("Burnaby, BC",        49.2488, -122.9805),
    ("Regina, SK",            50.4452, -104.6189),("Richmond, BC",       49.1666, -123.1336),
    ("Richmond Hill, ON",     43.8828, -79.4403), ("Oakville, ON",       43.4675, -79.6877),
    ("Burlington, ON",        43.3255, -79.7990), ("Oshawa, ON",         43.8971, -78.8658),
    ("Windsor, ON",           42.3149, -83.0364), ("St. Catharines, ON", 43.1594, -79.2469),
    ("Victoria, BC",          48.4284, -123.3656),("Barrie, ON",         44.3894, -79.6903),
    # --- secondary markets and suburbs: where the small operators actually are.
    # The top-25 results in a big metro are national firms; small contractors
    # live further down the list and in cities like these.
    ("Sherbrooke, QC",        45.4042, -71.8929), ("Trois-Rivieres, QC", 46.3432, -72.5432),
    ("Saguenay, QC",          48.4280, -71.0685), ("Levis, QC",          46.8033, -71.1779),
    ("Terrebonne, QC",        45.7000, -73.6333), ("Kelowna, BC",        49.8880, -119.4960),
    ("Abbotsford, BC",        49.0504, -122.3045),("Kamloops, BC",       50.6745, -120.3273),
    ("Nanaimo, BC",           49.1659, -123.9401),("Chilliwack, BC",     49.1579, -121.9514),
    ("Coquitlam, BC",         49.2838, -122.7932),("Langley, BC",        49.1044, -122.6604),
    ("Saanich, BC",           48.4840, -123.3810),("Kingston, ON",       44.2312, -76.4860),
    ("Guelph, ON",            43.5448, -80.2482), ("Greater Sudbury, ON",46.4917, -80.9930),
    ("Thunder Bay, ON",       48.3809, -89.2477), ("Waterloo, ON",       43.4643, -80.5204),
    ("Cambridge, ON",         43.3616, -80.3144), ("Milton, ON",         43.5183, -79.8774),
    ("Ajax, ON",              43.8509, -79.0204), ("Whitby, ON",         43.8975, -78.9429),
    ("Pickering, ON",         43.8384, -79.0868), ("Niagara Falls, ON",  43.0896, -79.0849),
    ("Brantford, ON",         43.1394, -80.2644), ("Peterborough, ON",   44.3091, -78.3197),
    ("Sarnia, ON",            42.9745, -82.4066), ("Kanata, ON",         45.3088, -75.8987),
    ("Moncton, NB",           46.0878, -64.7782), ("Saint John, NB",     45.2733, -66.0633),
    ("Fredericton, NB",       45.9636, -66.6431), ("St. John's, NL",     47.5615, -52.7126),
    ("Charlottetown, PE",     46.2382, -63.1311), ("Red Deer, AB",       52.2681, -113.8112),
    ("Lethbridge, AB",        49.6956, -112.8451),("St. Albert, AB",     53.6305, -113.6256),
    ("Airdrie, AB",           51.2917, -114.0144),
]

# Each region gets its own output filename and raw/ cache. Sharing either would
# let one region silently overwrite the other's export.
REGIONS = {
    "us": {"cities": CITIES_US, "country": "us", "prefix": "general-contractors"},
    "ca": {"cities": CITIES_CA, "country": "ca", "prefix": "general-contractors-canada"},
}
# Populated after the state tables are defined, below.

# Rebound by main() from --region. Defaults keep the original US behaviour.
REGION = "us"
CITIES = CITIES_US
COUNTRY = "us"
OUT_PREFIX = "general-contractors"
RAW = os.path.join(HERE, "raw", "us")
LOG = os.path.join(HERE, "scrape-us.log")


# Business names contain emoji; on Windows the console is cp1252 and print()
# raises UnicodeEncodeError. Force UTF-8 where supported and never let logging
# raise.
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
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def mcp_call(url, tool, arguments, timeout=TIMEOUT):
    """One MCP tools/call over streamable HTTP. Returns parsed inner payload."""
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": tool, "arguments": arguments},
    }).encode()
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        outer = json.loads(resp.read().decode())
    if "error" in outer:
        raise RuntimeError(f"MCP error: {outer['error']}")
    return json.loads(outer["result"]["content"][0]["text"])


# ---------- normalization helpers (used for chain grouping) ----------

# Hosts where many UNRELATED businesses share one registrable domain. Grouping
# on these would falsely merge distinct companies into a fake "chain", so they
# are never grouped -- each row on one of these stays a singleton.
SHARED_HOSTS = {
    "facebook.com", "m.facebook.com", "sites.google.com", "business.site",
    "wixsite.com", "wix.com", "godaddysites.com", "weebly.com", "squarespace.com",
    "blogspot.com", "wordpress.com", "myshopify.com", "linktr.ee", "yelp.com",
    "instagram.com", "nextdoor.com", "angi.com", "houzz.com", "thumbtack.com",
}

SUFFIXES = {
    "llc", "l.l.c", "inc", "inc.", "incorporated", "corp", "corp.", "corporation",
    "co", "co.", "company", "ltd", "ltd.", "limited", "lp", "llp", "pllc", "pc",
    "the", "and", "&",
}


def norm_name(name):
    """Lowercase, drop branch qualifiers, punctuation and legal suffixes."""
    if not name:
        return ""
    n = name.lower()
    n = re.split(r"\s+[-–—#]\s*", n)[0]        # "Acme - Downtown" / "Acme #3" -> "acme"
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    tokens = [t for t in n.split() if t and t not in SUFFIXES]
    return " ".join(tokens)


def root_domain(website):
    """Registrable-ish domain. US-only list, so a simple netloc strip is adequate."""
    if not website:
        return ""
    w = website.strip().lower()
    w = re.sub(r"^https?://", "", w)
    w = w.split("/")[0].split("?")[0].split("#")[0]
    if w.startswith("www."):
        w = w[4:]
    return w if "." in w else ""


# ---------- stage 1: scrape ----------

def scrape_city(url, city, lat, lng):
    try:
        data = mcp_call(url, "search_maps", {
            "query": KEYWORD,
            "lat": str(lat), "lng": str(lng),
            # country matters: without it "London, ON" can resolve to London, UK.
            "limit": str(PER_CITY), "country": COUNTRY, "lang": "en",
        })
    except Exception as e:                      # network / MCP / parse failure
        log(f"  FAIL  {city}: {type(e).__name__}: {e}")
        return city, []

    if data.get("status") != "ok":
        log(f"  FAIL  {city}: status={data.get('status')}")
        return city, []

    rows = data.get("data") or []
    os.makedirs(RAW, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9]+", "_", city)
    with open(os.path.join(RAW, f"{safe}.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=1)
    log(f"  ok    {city}: {len(rows)} results")
    return city, rows


# ---------- stage 1 (alternative provider): LocalPipe /maps-search ----------
# Lets you run the whole pipeline on ONE key. See README for the tradeoff:
# LocalPipe bills per LEAD RETURNED here, ScraperTech bills per REQUEST.

LP_API = "https://uunrlffonnvmbsstkpze.supabase.co/functions/v1"
# Public anon key published in LocalPipe's own docs -- used only for the
# polling endpoints, which are excluded from rate limits. Not a secret.
LP_ANON = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6"
           "InV1bnJsZmZvbm52bWJzc3RrcHplIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA0OTIwMzMs"
           "ImV4cCI6MjA4NjA2ODAzM30.8Ic_U17YrZtsDVnYnIP0XJzrhZJOLDSlJ0AZfTS31FQ")

# Matches the state/province code at the end of an address, tolerating either a
# US ZIP ("TX 78753", "TX 78753-1234") or a Canadian postal code
# ("ON M5V 2A1", "ON M5V2A1"), plus an optional trailing country name.
STATE_RE = re.compile(
    r",\s*([A-Z]{2})\b"
    r"(?:\s+(?:\d{5}(?:-\d{4})?|[A-Z]\d[A-Z]\s?\d[A-Z]\d))?"
    r"\s*(?:,\s*(?:Canada|USA|United States|US))?\s*$"
)


def derive_state(full_address):
    """LocalPipe's maps-search has no `state` field; ScraperTech does. Parse it
    from the address tail so the export schema stays identical across providers.
    Handles US states and Canadian provinces."""
    if not full_address:
        return ""
    m = STATE_RE.search(full_address.strip())
    return m.group(1) if m else ""


CITY_STATE_RE = re.compile(r",\s*([A-Z]{2})\s*$")

# Quebec listings spell the province out ("Montreal, Quebec"), which no
# two-letter regex will catch -- that alone left 49 rows with a blank province.
FULL_NAMES = {
    # Canada
    "ontario": "ON", "quebec": "QC", "québec": "QC", "british columbia": "BC",
    "alberta": "AB", "manitoba": "MB", "saskatchewan": "SK", "nova scotia": "NS",
    "new brunswick": "NB", "newfoundland and labrador": "NL", "newfoundland": "NL",
    "prince edward island": "PE", "yukon": "YT", "northwest territories": "NT",
    "nunavut": "NU",
    # US
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC", "washington dc": "DC",
}

CA_STATES = {"ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE",
             "YT", "NT", "NU"}
US_STATES = set(FULL_NAMES.values()) - CA_STATES

# Border cities leak: a Windsor, ON search returned 6 Detroit, MI businesses
# because the lat/lng radius crosses the river. Rows resolving to a province or
# state outside the region are dropped. Blanks are KEPT -- we cannot prove those
# are foreign, and discarding them would silently lose real businesses.
REGIONS["us"]["valid_states"] = US_STATES
REGIONS["ca"]["valid_states"] = CA_STATES


def _tail_full_name(text):
    """Map a spelled-out province/state at the end of a string to its code."""
    if not text:
        return ""
    tail = text.strip().rstrip(".").split(",")[-1].strip().lower()
    return FULL_NAMES.get(tail, "")


def resolve_state(row):
    """Fill the `state` column whatever the provider gives us.

    ScraperTech populates `state` for US rows but returns null for Canadian ones,
    where the province instead rides along in `city` ("Toronto, ON"). Without
    this fallback the whole column came back empty for Canada, and `state` is
    part of the locked export schema.
    """
    s = (row.get("state") or "").strip()
    if s:
        return FULL_NAMES.get(s.lower(), s)
    addr, city = row.get("full_address") or "", row.get("city") or ""
    s = derive_state(addr)
    if s:
        return s
    m = CITY_STATE_RE.search(city.strip())
    if m:
        return m.group(1)
    # Spelled-out province, e.g. "Montreal, Quebec"
    return _tail_full_name(city) or _tail_full_name(addr)


def _json_req(url, payload=None, headers=None, timeout=TIMEOUT):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body


def normalize_localpipe(r):
    """Map a LocalPipe maps-search row onto the ScraperTech field names the rest
    of the pipeline already expects."""
    addr = r.get("full_address") or ""
    return {
        "name": r.get("name"),
        "phone_number": r.get("phone"),          # LocalPipe: `phone`
        "website": r.get("website"),
        "full_address": addr,
        "city": r.get("city"),
        "state": derive_state(addr),             # not supplied -> derived
        "latitude": r.get("latitude"),
        "longitude": r.get("longitude"),
        "place_id": r.get("place_id"),
        "business_id": r.get("business_id"),
        "types": r.get("types") or [],
        "price_level": r.get("price_level"),
        "rating": r.get("rating"),
        "review_count": r.get("review_count"),
        "is_claimed": r.get("is_claimed"),
        "verified": None,                        # LocalPipe does not report this
        # LocalPipe omits closure flags; absent means "not known closed", which
        # is the safe default -- it only ever costs us a row we could have dropped.
        "is_permanently_closed": False,
        "is_temporarily_closed": False,
        "working_hours": r.get("working_hours"),
    }


def scrape_localpipe(key):
    """Batch cities <=25 per call (API limit), poll to done, paginate fully.

    Returns [(city_label, [raw_rows]), ...]. LocalPipe answers a batch in ONE
    response with no per-row attribution back to the input label, so rows are
    bucketed by the `city` each result reports.
    """
    headers = {"Content-Type": "application/json", "x-api-key": key}
    batches = [CITIES[i:i + 25] for i in range(0, len(CITIES), 25)]
    log(f"LocalPipe maps-search: {len(CITIES)} cities in {len(batches)} batch(es) "
        f"(API caps 25 locations x 5 keywords per call)")

    task_ids = []
    for n, batch in enumerate(batches, 1):
        payload = {
            "query": [KEYWORD],
            "location": [c for c, _, _ in batch],
            "limit": PER_CITY,
            # Server-side filter: skips websiteless businesses before they are
            # billed, which is exactly what we would discard locally anyway.
            "filters": {"has_website": True},
        }
        code, body = _json_req(f"{LP_API}/maps-search", payload, headers)
        if code in (200, 202) and isinstance(body, dict) and body.get("task_id"):
            task_ids.append(body["task_id"])
            log(f"  batch {n}/{len(batches)} accepted: task_id={body['task_id']}")
        else:
            log(f"  batch {n}/{len(batches)} FAILED HTTP {code}: {str(body)[:200]}")

    if not task_ids:
        sys.exit("LocalPipe maps-search accepted no batches -- nothing to scrape.")

    # Poll to terminal, then paginate every page. Never time out: the docs are
    # explicit that jobs can queue for many minutes and never expire.
    all_rows = []
    for task_id in task_ids:
        while True:
            code, body = _json_req(
                f"{LP_API}/get-scrape-results?task_id={task_id}&limit=5000&offset=0",
                None, {"apikey": LP_ANON})
            status = (body or {}).get("status") if isinstance(body, dict) else None
            if status in ("success", "done", "completed"):
                break
            if status in ("failed", "error", "cancelled"):
                log(f"  task {task_id}: {status}")
                body = None
                break
            prog = (body or {}).get("progress") if isinstance(body, dict) else None
            log(f"  task {task_id}: {status or 'pending'}"
                + (f" leads={prog.get('leads_found')}" if prog else ""))
            time.sleep(15)
        if not body:
            continue

        offset, page = 0, body
        while True:
            rows = page.get("data") or []
            all_rows.extend(rows)
            pg = page.get("pagination") or {}
            if not pg.get("has_more"):
                break
            offset = pg.get("next_offset", offset + 5000)
            _, page = _json_req(
                f"{LP_API}/get-scrape-results?task_id={task_id}&limit=5000&offset={offset}",
                None, {"apikey": LP_ANON})
            if not isinstance(page, dict):
                break
        log(f"  task {task_id}: {len(all_rows)} rows accumulated")

    # Bucket by the city each row reports, matched back to our input labels.
    labels = {c.split(",")[0].strip().lower(): c for c, _, _ in CITIES}
    buckets = OrderedDict((c, []) for c, _, _ in CITIES)
    unmatched = []
    for r in all_rows:
        row = normalize_localpipe(r)
        key_city = (row.get("city") or "").split(",")[0].strip().lower()
        label = labels.get(key_city)
        if label:
            buckets[label].append(row)
        else:
            unmatched.append(row)
    if unmatched:
        # Keep them: a returned city name that doesn't match our label is still
        # a real business. Park under the first label so nothing is dropped.
        log(f"  {len(unmatched)} rows whose city didn't match an input label (kept)")
        buckets[CITIES[0][0]].extend(unmatched)

    os.makedirs(RAW, exist_ok=True)
    for city, rows in buckets.items():
        safe = re.sub(r"[^A-Za-z0-9]+", "_", city)
        with open(os.path.join(RAW, f"{safe}.json"), "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=1)
    return list(buckets.items())


def load_existing_rows(path):
    """Read a previous export back into the internal row shape.

    Used by --add: rows already exported (and already paid to enrich) are pinned
    into the new output rather than re-selected, so their place_ids keep linking
    to existing enrichment results and are never re-charged.
    """
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            def num(v):
                try:
                    return float(v) if v not in (None, "") else None
                except ValueError:
                    return None
            out.append({
                "name": r.get("business_name"),
                "_city": r.get("business_location"),
                "website": r.get("business_website"),
                "phone_number": r.get("phone"),
                "full_address": r.get("full_address"),
                "city": r.get("city"),
                "state": r.get("state"),
                "rating": num(r.get("rating")),
                "review_count": num(r.get("review_count")),
                "types": [t for t in (r.get("types") or "").split("; ") if t],
                "is_claimed": r.get("is_claimed"),
                "verified": r.get("verified"),
                "place_id": r.get("place_id"),
                "business_id": r.get("business_id"),
                "latitude": num(r.get("latitude")),
                "longitude": num(r.get("longitude")),
                "is_permanently_closed": False,
                "is_temporarily_closed": False,
            })
    return out


def main():
    global REGION, CITIES, COUNTRY, OUT_PREFIX, RAW, LOG, TARGET_TOTAL, PER_CITY
    if "--region" in sys.argv:
        REGION = sys.argv[sys.argv.index("--region") + 1].lower()
    if REGION not in REGIONS:
        sys.exit(f"Unknown --region '{REGION}'. Available: {', '.join(REGIONS)}")
    cfg = REGIONS[REGION]
    CITIES, COUNTRY, OUT_PREFIX = cfg["cities"], cfg["country"], cfg["prefix"]
    RAW = os.path.join(HERE, "raw", REGION)
    LOG = os.path.join(HERE, f"scrape-{REGION}.log")

    def argval(flag, cast=int):
        return cast(sys.argv[sys.argv.index(flag) + 1]) if flag in sys.argv else None

    TARGET_TOTAL = argval("--target") or TARGET_TOTAL
    PER_CITY = argval("--per-city") or PER_CITY
    # --add N: keep everything already exported and add N NEW businesses on top.
    add_new = argval("--add")
    # --exclude PATH: drop any business already present in that CSV, WITHOUT
    # carrying its rows into the output. Use when starting a list afresh but the
    # previous batch is already loaded elsewhere and must not be duplicated.
    exclude_path = (sys.argv[sys.argv.index("--exclude") + 1]
                    if "--exclude" in sys.argv else None)

    open(LOG, "w").close()
    log(f"Region: {REGION.upper()}  ({len(CITIES)} cities, country={COUNTRY})")
    log(f"Depth: limit {PER_CITY}/city  |  "
        + (f"adding {add_new} NEW rows to the existing export"
           if add_new else f"target {TARGET_TOTAL} rows"))
    env = load_env()

    # Provider selection. ScraperTech is OPTIONAL: with no key, or with
    # --provider localpipe, the scrape runs on the LocalPipe key alone.
    provider = "scrapertech"
    if "--provider" in sys.argv:
        provider = sys.argv[sys.argv.index("--provider") + 1].lower()
    elif not env.get("SCRAPERTECH_KEY", "").strip() or \
            env.get("SCRAPERTECH_KEY", "").startswith("your_"):
        provider = "localpipe"
    if provider not in ("scrapertech", "localpipe"):
        sys.exit(f"Unknown --provider '{provider}'. Use scrapertech or localpipe.")

    url = None
    if provider == "scrapertech":
        key = env.get("SCRAPERTECH_KEY", "").strip()
        if not key or key.startswith("your_"):
            sys.exit("SCRAPERTECH_KEY missing from .env.\n"
                     "Either add it, or run with:  python scrape.py --provider localpipe")
        url = f"https://mcp.scraper.tech/{key}"
    else:
        lp = env.get("LOCALPIPE_KEY", "").strip()
        if not lp or lp.startswith("lp_your_"):
            sys.exit("LOCALPIPE_KEY missing from .env -- required for --provider localpipe.")
    log(f"Scrape provider: {provider}")

    log(f"Scraping '{KEYWORD}' across {len(CITIES)} cities "
        f"(limit {PER_CITY}/city, {WORKERS} workers)")

    results = []
    if "--reuse-raw" in sys.argv:
        # Rebuild from cached raw/*.json -- costs ZERO API requests. Use this
        # when only the flagging/filtering logic changed, not the scrape.
        log("--reuse-raw: loading cached raw/ files, no API calls")
        for city, _, _ in CITIES:
            safe = re.sub(r"[^A-Za-z0-9]+", "_", city)
            path = os.path.join(RAW, f"{safe}.json")
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    results.append((city, json.load(f)))
            else:
                log(f"  MISS  {city}: no cached file")
                results.append((city, []))
    elif provider == "localpipe":
        results = scrape_localpipe(env["LOCALPIPE_KEY"].strip())
    else:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = [ex.submit(scrape_city, url, c, la, ln) for c, la, ln in CITIES]
            for fut in futures:
                results.append(fut.result())

    raw_total = sum(len(r) for _, r in results)
    failed = [c for c, r in results if not r]
    log(f"Raw results: {raw_total}   cities returning data: "
        f"{len(CITIES) - len(failed)}/{len(CITIES)}")
    if failed:
        log(f"Cities with no data: {', '.join(failed)}")

    # ---------- dedup by place_id, keep first city that saw it ----------
    by_city = OrderedDict()
    seen = set()
    dupes = 0
    for city, rows in results:
        keep = []
        for r in rows:
            pid = r.get("place_id")
            if not pid or pid in seen:
                dupes += 1
                continue
            seen.add(pid)
            r["_city"] = city
            keep.append(r)
        if keep:
            by_city[city] = keep
    deduped = sum(len(v) for v in by_city.values())
    log(f"After place_id dedup: {deduped}  (removed {dupes} dupes/blank ids)")

    # ---------- filter: closed, websiteless, and out-of-region rows ----------
    valid_states = REGIONS[REGION]["valid_states"]
    dropped_closed = dropped_nosite = dropped_foreign = 0
    foreign_examples = []
    for city, rows in by_city.items():
        keep = []
        for r in rows:
            if r.get("is_permanently_closed") or r.get("is_temporarily_closed"):
                dropped_closed += 1
                continue
            if not root_domain(r.get("website")):
                dropped_nosite += 1
                continue
            st = resolve_state(r)
            if st and st not in valid_states:
                dropped_foreign += 1
                if len(foreign_examples) < 4:
                    foreign_examples.append(f"{(r.get('name') or '')[:28]} [{st}]")
                continue
            keep.append(r)
        by_city[city] = keep
    filtered = sum(len(v) for v in by_city.values())
    log(f"After filter: {filtered}  (dropped {dropped_closed} closed, "
        f"{dropped_nosite} without a usable website, "
        f"{dropped_foreign} outside {REGION.upper()})")
    if foreign_examples:
        log(f"  out-of-region examples: {', '.join(foreign_examples)}")

    # ---------- stage 2: chain flags, computed on the FULL filtered set ----------
    # Done BEFORE truncation so location counts use every location we saw,
    # not just the 500 that survive the cap.
    # Key = root domain ALONE. Evidence for dropping the name from the key:
    # turnerconstruction.com carried both "turner" and "turner construction",
    # splitting one 7-location brand into a 5 and a 2. Same for hoffmancorp.com,
    # topnotchbuilders1.com, grandeurhillsgroup.com.
    # Guards: no domain -> singleton; shared site-builder host -> singleton.
    # --add: pin previously exported rows so they are never dropped, and remove
    # them from the new-candidate pools so the N we add really are new.
    pinned = []
    if add_new:
        pinned = load_existing_rows(os.path.join(HERE, f"{OUT_PREFIX}.csv"))
        pinned_ids = {r["place_id"] for r in pinned}
        before = sum(len(v) for v in by_city.values())
        for city in by_city:
            by_city[city] = [r for r in by_city[city]
                             if r.get("place_id") not in pinned_ids]
        after = sum(len(v) for v in by_city.values())
        log(f"--add: pinned {len(pinned)} existing rows; "
            f"removed {before - after} of them from the new pool; "
            f"{after} new candidates remain")

    # Chain flags are computed over pinned + new together, so location counts
    # reflect the whole known set rather than just this batch.
    all_rows = [r for rows in by_city.values() for r in rows] + pinned
    groups = defaultdict(list)
    for r in all_rows:
        dom = root_domain(r.get("website"))
        r["_key"] = dom if dom and dom not in SHARED_HOSTS else None
        if r["_key"]:
            groups[r["_key"]].append(r)

    for r in all_rows:
        k = r.get("_key")
        n = len({x.get("place_id") for x in groups[k]}) if k else 1
        r["_location_count"] = n
        r["_is_multi"] = "yes" if n >= 2 else "no"
        r["_chain_cities"] = "; ".join(sorted({x["_city"] for x in groups[k]})) if k else ""

    multi = sum(1 for v in by_city.values() for r in v if r["_is_multi"] == "yes")
    log(f"Multi-location flagged: {multi} rows across "
        f"{len([k for k, v in groups.items() if len({x['place_id'] for x in v}) >= 2])} brands")

    # ---------- round-robin truncate to TARGET_TOTAL (even city spread) ----------
    # --exclude: remove businesses already delivered in a previous batch, by
    # place_id AND by domain (the same firm can surface under a new place_id
    # after a re-scrape, which would still ship as a duplicate downstream).
    if exclude_path:
        ex_pids, ex_doms = set(), set()
        with open(exclude_path, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                if r.get("place_id"):
                    ex_pids.add(r["place_id"])
                if r.get("domain"):
                    ex_doms.add(r["domain"].strip().lower())
        removed_pid = removed_dom = 0
        for city in by_city:
            keep = []
            for r in by_city[city]:
                if r.get("place_id") in ex_pids:
                    removed_pid += 1
                    continue
                if root_domain(r.get("website")) in ex_doms:
                    removed_dom += 1
                    continue
                keep.append(r)
            by_city[city] = keep
        log(f"--exclude {os.path.basename(exclude_path)}: removed {removed_pid} by "
            f"place_id and {removed_dom} by domain "
            f"({len(ex_pids)} prior businesses, {len(ex_doms)} prior domains)")

    # Franchises are excluded by default. Every branch shares one website, and
    # all enrichment providers key on the domain, so branches resolve to the same
    # one or two people: measured, 6 branches of one franchise brand returned 1
    # distinct email. They are also the wrong prospect -- head office is not the
    # buyer.
    # --keep-chains restores them.
    if "--keep-chains" not in sys.argv:
        dropped_chain = 0
        for city in by_city:
            keep = []
            for r in by_city[city]:
                if r.get("_is_multi") == "yes":
                    dropped_chain += 1
                    continue
                keep.append(r)
            by_city[city] = keep
        if dropped_chain:
            log(f"Franchise filter: dropped {dropped_chain} multi-location rows "
                f"(--keep-chains to keep them)")

    want = add_new if add_new else TARGET_TOTAL
    picked, i = [], 0
    pools = list(by_city.values())
    while len(picked) < want and any(len(p) > i for p in pools):
        for p in pools:
            if i < len(p):
                picked.append(p[i])
                if len(picked) >= want:
                    break
        i += 1
    if add_new and len(picked) < want:
        log(f"  WARNING: only {len(picked)} new businesses available, wanted {want}. "
            f"Raise --per-city or add more cities.")
    final = pinned + picked
    log(f"Selected: {len(picked)} new"
        + (f" + {len(pinned)} pinned = {len(final)} total" if pinned else ""))

    # ---------- write CSV ----------
    out = os.path.join(HERE, f"{OUT_PREFIX}.csv")
    cols = ["business_name", "business_location", "business_website", "domain",
            "phone", "full_address", "city", "state", "rating", "review_count",
            "types", "is_claimed", "verified", "location_count", "is_multi_location",
            "chain_cities", "place_id", "business_id", "latitude", "longitude"]
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in final:
            w.writerow({
                "business_name": r.get("name") or "",
                "business_location": r.get("_city") or "",
                "business_website": r.get("website") or "",
                "domain": root_domain(r.get("website")),
                "phone": r.get("phone_number") or "",
                "full_address": r.get("full_address") or "",
                "city": r.get("city") or "",
                "state": resolve_state(r),
                "rating": r.get("rating") if r.get("rating") is not None else "",
                "review_count": r.get("review_count") if r.get("review_count") is not None else "",
                "types": "; ".join(r.get("types") or []),
                "is_claimed": r.get("is_claimed"),
                "verified": r.get("verified"),
                "location_count": r["_location_count"],
                "is_multi_location": r["_is_multi"],
                "chain_cities": r["_chain_cities"],
                "place_id": r.get("place_id") or "",
                "business_id": r.get("business_id") or "",
                "latitude": r.get("latitude") if r.get("latitude") is not None else "",
                "longitude": r.get("longitude") if r.get("longitude") is not None else "",
            })

    # ---------- assertions (success criteria) ----------
    pids = [r.get("place_id") for r in final]
    problems = []
    if len(set(pids)) != len(pids):
        problems.append(f"duplicate place_ids: {len(pids) - len(set(pids))}")
    cap = (len(pinned) + add_new) if add_new else TARGET_TOTAL
    if len(final) > cap:
        problems.append(f"row count {len(final)} exceeds cap {cap}")
    if add_new and pinned:
        lost = {r["place_id"] for r in pinned} - set(pids)
        if lost:
            problems.append(f"{len(lost)} previously exported rows went missing")
    if any(not root_domain(r.get("website")) for r in final):
        problems.append("rows without a website survived the filter")
    log(f"Cities represented in final CSV: "
        f"{len({r['_city'] for r in final})}/{len(CITIES)}")
    log("ASSERTIONS: " + ("PASS" if not problems else "FAIL -> " + "; ".join(problems)))
    log(f"Wrote {out}")
    log("DONE")


if __name__ == "__main__":
    main()
