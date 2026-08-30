"""
Build ONE deliverable CSV in Downloads: every business, enriched and not,
merging LocalPipe (stage 3) with the AI Ark -> Prospeo waterfall (stage 4).

  Downloads/general-contractors-enriched.csv

result_tier, in file order:
  1_EMAIL_FOUND          an email was found by any source
  2_NAME_ONLY_NO_EMAIL   a decision-maker name is known but no email
  3_EMPTY                nothing found -- fill these yourself
  4_NOT_ENRICHED_YET     not processed (present so no row is ever dropped)

email_source: localpipe_owner | localpipe_business | aiark | prospeo
"""
import csv, os, sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS = os.path.join(os.path.expanduser("~"), "Downloads")
# Region selection mirrors scrape.py; US filenames stay unsuffixed.
PREFIXES = {"us": "general-contractors", "ca": "general-contractors-canada"}
REGION = "us"
if "--region" in sys.argv:
    REGION = sys.argv[sys.argv.index("--region") + 1].lower()
if REGION not in PREFIXES:
    sys.exit(f"Unknown --region '{REGION}'. Available: {', '.join(PREFIXES)}")
PREFIX = PREFIXES[REGION]
SUF = "" if REGION == "us" else f"-{REGION}"

OUT = os.path.join(DOWNLOADS, f"{PREFIX}-enriched.csv")

SRC_LIST = os.path.join(HERE, f"{PREFIX}.csv")
SRC_LP = os.path.join(HERE, f"localpipe_results{SUF}.csv")
SRC_WF = os.path.join(HERE, f"waterfall_results{SUF}.csv")

# A business whose "website" is a shared host (facebook.com, sites.google.com,
# instagram.com...) must NEVER be used as a company-domain lookup key: AI Ark and
# Prospeo then return employees of the HOST. That is how a renovation contractor
# whose "website" was a sites.google.com page came back with an email address at
# the host's own domain. Purge those.
SHARED_HOSTS = {
    "facebook.com", "m.facebook.com", "sites.google.com", "google.com",
    "business.site", "wixsite.com", "wix.com", "godaddysites.com", "weebly.com",
    "squarespace.com", "blogspot.com", "wordpress.com", "myshopify.com",
    "linktr.ee", "yelp.com", "instagram.com", "nextdoor.com", "angi.com",
    "houzz.com", "thumbtack.com",
}

COLS = ["result_tier", "business_name", "business_location", "business_website",
        "domain", "phone", "full_address", "city", "state", "rating", "review_count",
        "location_count", "is_multi_location", "chain_cities",
        "contact_name", "contact_linkedin",
        "best_email", "email_source", "email_domain_match", "email_provider",
        "owner_name", "owner_email", "business_email",
        "aiark_name", "aiark_email", "prospeo_email", "place_id"]


def read(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def main():
    os.makedirs(DOWNLOADS, exist_ok=True)
    businesses = read(SRC_LIST)
    lp = {r["place_id"]: r for r in read(SRC_LP)}
    # The waterfall emits ONE ROW PER CONTACT, so group them by place_id --
    # a company can have up to 3 decision-makers.
    wf = {}
    for r in read(SRC_WF):
        if (r.get("contact_email") or "").strip():
            wf.setdefault(r["place_id"], []).append(r)
    n_wf_contacts = sum(len(v) for v in wf.values())
    print(f"Source: {len(businesses)} businesses | LocalPipe {len(lp)} | "
          f"waterfall {n_wf_contacts} contacts across {len(wf)} companies")

    out = []
    for b in businesses:
        e = lp.get(b["place_id"], {})
        wcs = wf.get(b["place_id"], [])

        owner_email = (e.get("owner_email") or "").strip()
        biz_email = (e.get("business_email") or "").strip()

        # Discard stage-4 contacts sourced from a shared-host domain (see above):
        # a business whose "website" is sites.google.com resolves to Google staff.
        if (b.get("domain") or "").lower() in SHARED_HOSTS:
            wcs = []

        owner_name = (e.get("owner_name") or "").strip()

        # Build the contact rows for this business. LocalPipe's owner (verified)
        # comes first, then each waterfall decision-maker as its own row.
        variants = []
        if owner_email:
            variants.append({"name": owner_name, "linkedin": "", "title": "",
                             "email": owner_email, "src": "localpipe_owner",
                             "ark": "", "pro": ""})
        for c in wcs:
            s = (c.get("contact_source") or "aiark").strip()
            variants.append({
                "name": (c.get("contact_name") or "").strip(),
                "linkedin": (c.get("contact_linkedin") or "").strip(),
                "title": (c.get("contact_title") or "").strip(),
                "email": c["contact_email"].strip(),
                "src": s,
                "ark": c["contact_email"].strip() if s == "aiark" else "",
                "pro": c["contact_email"].strip() if s == "prospeo" else "",
            })
        # Generic business mailbox only if nothing better exists for this company.
        if not variants and biz_email:
            variants.append({"name": owner_name, "linkedin": "", "title": "",
                             "email": biz_email, "src": "localpipe_business",
                             "ark": "", "pro": ""})
        if not variants:
            variants.append({"name": owner_name, "linkedin": "", "title": "",
                             "email": "", "src": "", "ark": "", "pro": ""})

        # One address per company, even when two contacts resolve to it. Seen in
        # the wild: AI Ark returning the same person twice, two family members
        # sharing one mailbox, and AI Ark + Prospeo naming different people for
        # the same address. Variants are already ordered best-first, so keeping
        # the first occurrence keeps the most senior/most trusted attribution.
        seen_emails, deduped = set(), []
        for v in variants:
            k = v["email"].strip().lower()
            if k and k in seen_emails:
                continue
            if k:
                seen_emails.add(k)
            deduped.append(v)
        variants = deduped

        for v in variants:
            best, src = v["email"], v["src"]
            contact = v["name"] or owner_name
            if not e and not wcs:
                tier = "4_NOT_ENRICHED_YET"
            elif best:
                tier = "1_EMAIL_FOUND"
            elif contact:
                tier = "2_NAME_ONLY_NO_EMAIL"
            else:
                tier = "3_EMPTY"
            row = {c: b.get(c, "") for c in COLS if c in b}
            row.update({
                "result_tier": tier,
                "contact_name": contact,
                "contact_linkedin": v["linkedin"],
                "best_email": best,
                "email_source": src,
                # QA flag: "no" means the address sits on a different domain than
                # the business site. Often a legitimate alternate corporate domain
                # (tcco.com for Turner), sometimes a wrong match -- eyeball first.
                "email_domain_match": ("" if not best else
                                       ("yes" if best.split("@")[-1].lower()
                                        == (b.get("domain") or "").lower() else "no")),
                "email_provider": (e.get("email_provider") or "").strip(),
                "owner_name": owner_name,
                "owner_email": owner_email,
                "business_email": biz_email,
                "aiark_name": v["name"] if v["src"] == "aiark" else "",
                "aiark_email": v["ark"],
                "prospeo_email": v["pro"],
                "_title": v["title"],          # supplementary file only
            })
            out.append(row)

    out.sort(key=lambda r: (r["result_tier"], r["business_location"],
                            r["business_name"], r["contact_name"]))

    with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
        w_ = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w_.writeheader()
        for r in out:
            w_.writerow({c: r.get(c, "") for c in COLS})

    tiers = Counter(r["result_tier"] for r in out)
    srcs = Counter(r["email_source"] for r in out if r["email_source"])
    print(f"\nWrote {OUT}\n")
    for t in sorted(tiers):
        print(f"  {t:<24} {tiers[t]:>4}")
    print("\n  emails by source:")
    for s, n in srcs.most_common():
        print(f"    {s:<22} {n:>4}")

    # Supplementary file: same contacts plus their job titles, which have no home
    # in the locked 27-column schema.
    contact_rows = [r for r in out if r["best_email"] and r.get("_title")]
    if contact_rows:
        sup = os.path.join(DOWNLOADS, f"{PREFIX}-CONTACT-TITLES.csv")
        scols = ["business_name", "business_location", "domain", "contact_name",
                 "contact_title", "best_email", "email_source", "place_id"]
        with open(sup, "w", newline="", encoding="utf-8-sig") as f:
            w_ = csv.DictWriter(f, fieldnames=scols, extrasaction="ignore")
            w_.writeheader()
            for r in contact_rows:
                w_.writerow({**{c: r.get(c, "") for c in scols},
                             "contact_title": r["_title"]})
        print(f"\n  supplementary: {os.path.basename(sup)}  {len(contact_rows)} rows with titles")

    problems = []
    # place_id is INTENTIONALLY non-unique now: one row per contact means a
    # company with 3 decision-makers appears 3 times. Assert instead that every
    # business is represented and that no duplicate (place_id, email) exists.
    if len({r["place_id"] for r in out}) != len(businesses):
        problems.append(f"{len({r['place_id'] for r in out})} distinct businesses "
                        f"in output vs {len(businesses)} in")
    seen = [(r["place_id"], r["best_email"]) for r in out if r["best_email"]]
    if len(set(seen)) != len(seen):
        problems.append(f"{len(seen) - len(set(seen))} duplicate (place_id, email) rows")
    for r in out:
        if r["result_tier"] == "1_EMAIL_FOUND" and not r["best_email"]:
            problems.append("tier 1 without email"); break
        if r["result_tier"] == "3_EMPTY" and (r["best_email"] or r["contact_name"]):
            problems.append("tier 3 with data"); break
        if r["best_email"] and "*" in r["best_email"]:
            problems.append("masked email leaked into best_email"); break
    print("\nASSERTIONS: " + ("PASS" if not problems else "FAIL -> " + "; ".join(problems)))


if __name__ == "__main__":
    main()
