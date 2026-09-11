"""
Build ONE CSV that matches the ORIGINAL LocalPipe export exactly, plus a single
extra column.

  columns 1-25 : the LocalPipe export, same names, same order
  column 26    : Contact Title -- the job title of the person on this row, and
                 the only way to tell an owner from a hired decision maker.

Found emails are written into the NATIVE fields, not parallel ones:
  * every found address goes into `Primary Email` (+ `Primary Email Type`)
  * if the person is a PRINCIPAL (owner/founder/CEO/president/partner/COO/
    principal/MD) their details also populate `Owner Name`, `Owner First Name`,
    `Owner Last Name`, `Owner Email` and `Owner Phone` -- because that IS the
    owner's email. A General Manager fills Primary Email only.

ONE ROW PER CONTACT: a business with 3 decision makers is 3 rows sharing a
domain and Google Place ID. Every business keeps at least one row.
"""
import csv,json,os,re,sys,collections
csv.field_size_limit(10**9)
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
from waterfall import PRINCIPAL, classify

# Who makes the list. PRINCIPAL own the business and write back to Owner Email;
# General Manager runs it and fills Primary Email only -- that split is what the
# Contact Title column exists to make visible.
ALLOWED = PRINCIPAL | {"General Manager"}

HERE=os.path.dirname(os.path.abspath(__file__))
SRC=os.environ.get("LP_EXPORT","")   # the provider export this run must match
OUT=os.environ.get("LP_OUT") or (os.path.splitext(SRC)[0]+" - WATERFALL.csv")
if not SRC:
    raise SystemExit("set LP_EXPORT=/path/to/provider-export.csv")

SOURCE_COLS=["Business Name","Business Website","Business Location","Address","City","State",
 "Zip Code","Phone","Rating","Review Count","Business Type","Price Level","Google Place ID",
 "Google Business ID","Latitude","Longitude","Claimed","Owner Name","Owner First Name",
 "Owner Last Name","Owner Email","Owner Phone","Business Email","Primary Email","Primary Email Type"]
HDR=SOURCE_COLS+["Contact Title"]

def val(x,k):
    v=(x.get(k) or "").strip()
    return "" if v.lower() in ("not found","n/a","null") else v
def bare(u):
    u=re.sub(r"^https?://","",str(u or "").lower())
    return re.sub(r"^www\.","",u).split("/")[0].strip()

rows=list(csv.DictReader(open(SRC,encoding="utf-8-sig")))
wf={}
pp=os.path.join(HERE,"waterfall_progress.jsonl")
if os.path.exists(pp):
    for line in open(pp,encoding="utf-8"):
        try:
            r=json.loads(line); wf[r["domain"]]=r
        except Exception: pass

out=[]; tally=collections.Counter(); filled=0; owner_filled=0
seen_email=set(); deduped=0
dropped=collections.Counter()
for x in rows:
    d=bare(val(x,"Business Website"))
    base={c:(x.get(c) or "") for c in SOURCE_COLS}     # verbatim, incl. "not found"
    res=wf.get(d)
    contacts=(res or {}).get("contacts") or []
    # Re-apply the CURRENT title rules to cached results: the run was executed
    # under looser rules, and re-filtering here is free where re-running is not.
    kept=[]
    for c in contacts:
        lab,rank=classify(c.get("title") or c.get("role") or "")
        if lab is None:
            dropped[c.get("title") or c.get("role") or "(blank)"]+=1
            continue
        if lab not in ALLOWED:          # owners + General Manager
            dropped[f"(not an owner) {lab}"]+=1
            continue
        c=dict(c); c["role"]=lab; kept.append((rank,c))
    contacts=[c for _,c in sorted(kept,key=lambda t:t[0])]
    contacts=contacts[:3]                      # hard cap: 3 per business
    if val(x,"Primary Email") or not contacts:         # untouched business row
        r=dict(base); r["Contact Title"]=""
        out.append(r); tally["unchanged" if val(x,"Primary Email") else "no new contact"]+=1
        continue
    emitted=0
    for c in contacts[:3]:                             # ONE ROW PER CONTACT
        r=dict(base)
        nm=(c.get("name") or "").strip()
        parts=nm.split()
        first=parts[0] if parts else ""
        last=" ".join(parts[1:]) if len(parts)>1 else ""
        email=c.get("email") or ""
        role=c.get("role") or ""
        if email and email.lower() in seen_email:
            deduped+=1          # same owner, second Google listing -- skip
            continue
        if email: seen_email.add(email.lower())
        r["Primary Email"]=email
        r["Primary Email Type"]="Person direct email"
        r["Contact Title"]=c.get("title") or role
        is_principal = role in PRINCIPAL or role.startswith("Owner")
        if is_principal and email:
            r["Owner Name"]=nm; r["Owner First Name"]=first; r["Owner Last Name"]=last
            r["Owner Email"]=email
            if c.get("phone"): r["Owner Phone"]=c["phone"]
            owner_filled+=1
        filled+=1; emitted+=1
        out.append(r)
    if emitted==0:                 # every contact deduped away -- keep the business
        r=dict(base); r["Contact Title"]=""
        out.append(r); tally["dedup only"]+=1
        continue
    tally["enriched"]+=1

biz_in={(x.get("Google Place ID") or "") for x in rows if x.get("Google Place ID")}
biz_out={r["Google Place ID"] for r in out if r["Google Place ID"]}
assert biz_in==biz_out, f"business lost: {len(biz_in-biz_out)}"
key=[(r["Google Place ID"],r["Primary Email"].lower()) for r in out if r["Primary Email"]]
assert len(key)==len(set(key)), "duplicate (place_id, Primary Email) -- would email twice"
assert len(HDR)==26, HDR
with open(OUT,"w",newline="",encoding="utf-8-sig") as f:
    w=csv.DictWriter(f,fieldnames=HDR); w.writeheader(); w.writerows(out)

pe=sum(1 for r in out if r["Primary Email"].strip() and r["Primary Email"].strip().lower()!="not found")
dupe=len([e for e,c in collections.Counter(
    r["Primary Email"].lower() for r in out if r["Primary Email"] and r["Contact Title"]).items() if c>1])
print(f"rows {len(out)} (from {len(rows)} businesses) | columns {len(HDR)} = 25 original + Contact Title")
print(f"assertions: no business lost PASS | (place_id, Primary Email) unique PASS")
print(f"new emails written into Primary Email : {filled}")
print(f"  ...of which written to Owner Email  : {owner_filled}  (principals only)")
print(f"  ...General Manager rows (not owners) : {filled-owner_filled}")
print(f"total rows now carrying a Primary Email: {pe}")
print(f"duplicate-email rows removed (same owner, 2nd listing): {deduped}")
print(f"same new email on >1 business AFTER dedupe: {dupe}   <- must be 0")
for k,v in tally.most_common(): print(f"   {k:20} {v}")
print(f"\ndropped by the tightened title rules: {sum(dropped.values())}")
for k,v in dropped.most_common(10): print(f"   {k[:40]:40} {v}")
print("->",OUT)
