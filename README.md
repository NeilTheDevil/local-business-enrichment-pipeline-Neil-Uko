# Local Business Enrichment Pipeline

Scrape local businesses from Google Maps, then find the **owner's name and email** through a four-source waterfall — falling through to the next provider only when the previous one comes up empty.

Built for cold outbound lead lists. Pure Python standard library: **no `pip install` required.**

```
ScraperTech  ->  chain flags  ->  LocalPipe  ->  AI Ark  ->  Prospeo  ->  Clay  ->  blank
  (scrape)       (free, local)     (primary)    (fallback)  (fallback)   (site +
                                                                        Facebook
                                                                         scrape)
```

The first five stages are this repo. The sixth is downstream — the export is built to drop
into a Clay table that finishes the rows the paid databases cannot fill. See
[Finishing the list in Clay](#finishing-the-list-in-clay-44--84).

**📊 [Visual walkthrough: Six Stages to 84%](https://claude.ai/code/artifact/c9873572-fc93-4263-a87e-466535642bfb)** — the whole flow stage by stage, with the tool used at each one and what it costs. Start here if you want the shape before the detail.

## Measured results

Four real runs on general contractors, all aggregate — no contact data here.

| Run | Scope | Businesses | Emails | Rate |
|---|---|---|---|---|
| 1 | 32 US metros | 500 | **284** | 57% |
| 2 | 32 Canadian metros | 500 | **306** | 61% |
| 3 | 69 Canadian cities, depth 150 | 2,500 | **1,275** | 44% |
| 4 | 74 US cities / 37 states, depth 150 | 2,000 | **1,161** | 41% |

**Run 1 by source:** LocalPipe owner 114 (verified) · LocalPipe business 88 (generic, unverified) · AI Ark 73 · Prospeo 9. The waterfall recovered **82 emails from the 298 LocalPipe missed**, cutting dead rows from 171 to 93. LocalPipe carries the bulk; the fallbacks are worth running but are not the main engine — that holds across all four runs.

**These are the pipeline's own numbers, not the finished list.** Rates are businesses with at least one email, over businesses. The rows this pipeline cannot fill get finished downstream — see [Finishing the list in Clay](#finishing-the-list-in-clay-44--84), which took Run 3 from 44% to **84%** on the same base.

**The rate drops as you go deeper, and that's the correct trade.** Run 3 went from 32 metros to 69 cities at depth 150 and the email rate fell 61% → 44% — while absolute emails rose about 4×. Deeper scrapes reach smaller operators, and smaller operators are less well covered by every provider. If volume is the goal, expect the percentage to fall and count the absolute number instead.

**Two things that did *not* hold up:**

- *"Canadian coverage will be thinner than US."* Predicted 45–55%; Canada came in at **61%** against the US's 57%, and LocalPipe alone cleared 53% of Canada versus 40% of the US.
- *"A 5-row sample tells you the hit rate."* Both the US and the first Canadian sample returned **0/5 emails**, and the full runs then landed at 57% and 61%. Samples validate *plumbing and output shape*, not yield. Do not re-scope a run on a 5-row result.

## Setup

```bash
git clone <this-repo>
cd local-business-enrichment-pipeline
cp .env.example .env      # then paste your own keys into .env
```

Requires **Python 3.8+**. Nothing else — no dependencies.

**You only strictly need one key: LocalPipe.** Everything else is optional.

| Key | Required? | Without it |
|---|---|---|
| `LOCALPIPE_KEY` | **yes** | nothing runs |
| `SCRAPERTECH_KEY` | no | scraping falls back to LocalPipe automatically |
| `AIARK_KEY` | no | stage 4a is skipped |
| `PROSPEO_KEY` | no | stage 4b is skipped |

## Do I need ScraperTech?

**No.** LocalPipe has its own Google Maps scraper, so a single LocalPipe key runs the entire pipeline:

```bash
python scrape.py --provider localpipe
```

Leave `SCRAPERTECH_KEY` blank and the pipeline picks LocalPipe on its own — no flag needed. Both providers feed **identical columns** downstream, so your export schema is the same either way.

**So why does this repo use ScraperTech by default?** One reason: **cost at scale.**

| | ScraperTech | LocalPipe `/maps-search` |
|---|---|---|
| **Billing** | per **request** | per **lead returned** |
| Real example | 32 requests → 800 businesses | 800 businesses → 800 leads billed |
| Speed | synchronous, seconds | async: submit → poll, minutes |
| Batching | 1 city per request | up to 5 keywords × 25 cities per request |
| Server-side filters | none (filter locally) | `has_website`, `min_rating`, etc. |
| Extra fields | `state`, `verified`, closed flags | — (`state` is derived from the address) |
| Keys to manage | a second one | none, you already have it |

**Use ScraperTech when** you scrape large volumes, re-run often, or want the scrape to not consume the same credit pool your enrichment needs — its per-request billing makes big scrapes dramatically cheaper, and it returns closed-business flags that save you enriching dead listings.

**Skip ScraperTech when** your scope is small or one-off, you'd rather manage a single key and a single bill, or you want LocalPipe's server-side `has_website` filter to avoid paying for businesses you'd discard anyway.

For a few hundred leads the difference is minor — **just use LocalPipe.** The second key starts paying for itself in the thousands.

⚠️ **LocalPipe's `limit` is approximate — budget for overshoot.** In testing, `limit: 5` on one city returned **50** businesses, all of them billed. Treat `PER_CITY` as a hint, not a ceiling, and start small when you're calibrating spend. ScraperTech's `limit` behaves as a true ceiling by comparison.

## Usage

```bash
# Stage 1+2 -- scrape and flag chains
python scrape.py                      # auto-picks provider from your keys
python scrape.py --region ca          # Canada instead of the US
python scrape.py --provider localpipe # force LocalPipe (one key, no ScraperTech)
python scrape.py --reuse-raw          # rebuild from cache, ZERO API calls
python scrape.py --target 2000        # override TARGET_TOTAL for this run
python scrape.py --per-city 150       # override PER_CITY depth for this run
python scrape.py --add 2000           # keep what you already exported, add 2000 NEW
python scrape.py --exclude prior.csv  # drop anything already delivered in prior.csv
python scrape.py --keep-chains        # keep franchises (dropped by default)

# Stage 3 -- owner name + email
python enrich_localpipe.py --limit 5  # sample first, always
python enrich_localpipe.py            # full run; skips rows already done
python enrich_localpipe.py --no-dedupe-domains   # one lookup per branch, not per brand

# Stage 4 -- waterfall over whatever stage 3 missed
python enrich_waterfall.py --limit 10
python enrich_waterfall.py

# Export -> your Downloads folder
python split_results.py               # merged file, all sources
```

**Always run the `--limit` sample first.** Stages 3 and 4 spend credits — but see above: a small sample validates that the plumbing works and the output looks right. It does **not** forecast your hit rate, and a 0/5 sample has twice preceded a ~60% full run.

**Two defaults worth knowing before your first run**, because both change what you get:

- **Franchises are dropped**, not just flagged. See [chain detection](#how-chain-detection-works-free-no-api). `--keep-chains` restores them.
- **One paid lookup per root domain**, with the answer fanned out to the other branches. `--no-dedupe-domains` restores per-branch enrichment.

**`--add` vs `--exclude`.** `--add N` keeps everything already in your export and appends N genuinely new businesses, pinning the prior `place_id`s so they are never dropped and never re-enriched. `--exclude prior.csv` is for when the earlier batch has already been handed off and you want the new list to be entirely fresh — it removes matches **by `place_id` and by domain**. Use `--exclude` when a previous batch left the building; use `--add` when you are growing one list over time.

## Regions

`--region us` (default, **74 cities across 37 states**) and `--region ca` (**69 cities across all 10 provinces**) are built in. **Pass the same `--region` to every stage** — it selects the city list, the `country` sent to the API, the output filename, and the `raw/` cache:

```bash
python scrape.py            --region ca
python enrich_localpipe.py  --region ca
python enrich_waterfall.py  --region ca
python split_results.py     --region ca      # -> general-contractors-canada-enriched.csv
```

US filenames are unsuffixed, every other region gets a suffix, so regions never overwrite each other and each keeps its own cache.

Two things that bite when scraping outside the US:

- **`country` is not optional.** Without it, "London, ON" happily returns London, **UK**.
- **Border cities leak across.** A Windsor, ON search returned 6 **Detroit, MI** businesses — the lat/lng radius crosses the river. Rows resolving to a province/state outside the region are dropped automatically and the count is logged. Rows with no resolvable location are *kept*, since being unprovable isn't the same as being foreign.
- **Provinces aren't always 2-letter codes.** Quebec listings say `"Montreal, Quebec"`, and ScraperTech returns `state: null` for Canada entirely, putting the province in `city`. `resolve_state()` handles all three shapes; without it the whole column came back empty.

Adding a region: append a `CITIES_XX` list and a `REGIONS` entry (with its `valid_states` set) in `scrape.py`, and add the prefix to `PREFIXES` in the other three scripts.

## Configuring your search

Edit the constants at the top of `scrape.py`:

```python
KEYWORD      = "general contractors"   # what to search
TARGET_TOTAL = 500                     # hard cap on final rows
PER_CITY     = 25                      # request ceiling per city
CITIES = [("Austin, TX", 30.2672, -97.7431), ...]   # name, lat, lng
```

⚠️ **Western longitudes must be negative.** `97.7431° W` is `-97.7431`. Getting this wrong silently scrapes the wrong hemisphere and returns plausible-looking garbage.

## Depth is how you reach the small operators

`PER_CITY` (or `--per-city`) is not just a volume dial — it decides *what kind of business* you get.

**Results 1–25 in a big metro are the national firms.** Results 25–150 are the one-truck outfits. If your offer is for the owner-operator, a shallow scrape returns exactly the wrong list, no matter how many cities you add.

With ScraperTech this costs nothing: `limit` is a true ceiling billed **per request**, so one request at `limit: 1000` on a query that exhausts at 209 returns 209 and still counts as one request. **Always set it high.** With LocalPipe the opposite is true — it bills per lead returned, so depth is exactly what you pay for.

The tradeoff is the one in [Measured results](#measured-results): deeper lists have a lower email rate, because the providers cover small businesses less well. You are trading percentage for absolute volume, and for most outbound that is the right trade.

## Contacts: decision-makers only

The waterfall returns **up to 3 contacts per company** (`MAX_CONTACTS` in `enrich_waterfall.py`), and only people who can make a buying call.

**Included**, most senior first: CEO · Founder / Co-Founder · Owner / Co-Owner / Proprietor · President · COO · Managing Director / Managing Partner · Executive Director · Principal · Partner · Chairman · Board Member · Investor / Shareholder.

**Excluded:** CFO, CTO, CMO, CIO and every other functional chief · all VPs (VP, SVP, EVP, AVP) · manager, coordinator, specialist, estimator, foreman, superintendent, supervisor, engineer, architect, accountant, recruiter · anything matching "Director of X".

Two rules make this actually work:

1. **Exclusions are checked BEFORE inclusions.** Otherwise "Partner Intelligence Manager" reads as a partner, "Chief Financial Officer, Executive Vice President" reads as a chief, and "Assistant to the CEO" reads as a CEO.
2. **Trust the source that already knows.** When LocalPipe supplied the name, that person is the owner by definition — no title check. When Prospeo searches by domain with no name, the result could be anyone, so it MUST pass the title filter or receptionists ship as "C-suite".

Across 344 contacts in Run 4 the filter admitted CEO 98 · Owner 94 · President 69 · COO 33 · Founder 23 · Partner 12 · Principal 5 · Managing Director 5 · Chairman 3 · Board Member 1 · Investor 1 — and zero CFOs, CTOs, VPs or project managers.

## Spending less: dedupe before you pay, not after

Deduping the output only hides waste you already paid for. Two filters run on the target list **before a single paid call**:

1. **One lookup per root domain.** AI Ark and Prospeo key on the domain alone, so every extra branch of one brand returns the identical people. Enrich one, then fan the result out to its siblings — coverage is unchanged, cost is not. Measured on one run: **109 locations sharing a domain produced only 43 distinct results**, so 66 enrichments were paid for twice.
2. **Drop shared hosts up front.** A lookup keyed on `facebook.com` or `sites.google.com` returns employees of the *host*, so it can only ever be wrong. In an early run 18 of 43 wasted waterfall lookups were exactly this.

There is a third dedupe at export: **the same address can arrive twice for one company** — one person returned under two profile records, two family members sharing a mailbox, or AI Ark and Prospeo naming different people at the same address. All three are real, all three are collapsed.

## Output

One CSV lands in your Downloads folder, sorted by tier, then location, then name, and encoded UTF-8-with-BOM so Excel opens it cleanly.

**`general-contractors-enriched.csv`** — 27 columns, every source merged. **This schema is frozen, and that is deliberate:** the export is uploaded into a Clay table that is mapped against these exact columns and re-run over time. Adding, renaming or reordering a column breaks those mappings and makes runs incomparable across time. If you genuinely need to capture something new, put it in a separate supplementary file — never a 28th column. Key fields:

| Column | Meaning |
|---|---|
| `result_tier` | `1_EMAIL_FOUND` / `2_NAME_ONLY_NO_EMAIL` / `3_EMPTY` / `4_NOT_ENRICHED_YET` |
| `best_email` | the address to use — owner email preferred, else generic business email |
| `email_source` | `localpipe_owner` / `localpipe_business` / `aiark` / `prospeo` |
| `email_domain_match` | `no` = address is on a different domain than the website — **QA these** |
| `location_count`, `is_multi_location`, `chain_cities` | chain / franchise detection |

**One row per contact, so `place_id` repeats.** A company with 3 decision-makers is 3 rows sharing a `place_id`, which keeps every row a single send target ready to upload. This means `place_id` is **not** a unique key in the export — assert on `(place_id, email)` instead, which is what `split_results.py` does. That assertion is not decorative: it caught 4 duplicate rows on one run that would otherwise have emailed the same person twice.

Because `email_source` records which provider supplied each address, this single file already shows what any one stage contributed — filter on it rather than exporting a per-stage file.

### The 27 columns, in order

Every one of these must be present, spelled exactly this way, in exactly this order. The Clay template maps against these names — rename or reorder one and that mapping silently stops matching.

| # | Column | What it holds | Where it comes from |
|---|---|---|---|
| 1 | `result_tier` | `1_EMAIL_FOUND` / `2_NAME_ONLY_NO_EMAIL` / `3_EMPTY` / `4_NOT_ENRICHED_YET` | computed at export |
| 2 | `business_name` | the business as Google lists it | scrape |
| 3 | `business_location` | **the city you searched** ("Austin, TX") — not necessarily where the business sits | scrape |
| 4 | `business_website` | full URL as listed | scrape |
| 5 | `domain` | root domain, `www.` and path stripped | derived from #4 |
| 6 | `phone` | listed phone number | scrape |
| 7 | `full_address` | complete street address | scrape |
| 8 | `city` | **the business's own city** — compare with #3 for border leaks | scrape |
| 9 | `state` | state / province code; resolved for CA, where the provider returns `null` | derived |
| 10 | `rating` | Google star rating | scrape |
| 11 | `review_count` | number of Google reviews | scrape |
| 12 | `location_count` | distinct `place_id`s sharing this domain | computed, free |
| 13 | `is_multi_location` | `yes` / `no` — chain flag | computed, free |
| 14 | `chain_cities` | which scraped cities this brand appears in | computed, free |
| 15 | `contact_name` | **the person this row is addressed to** | best available source |
| 16 | `contact_linkedin` | their LinkedIn URL, when known | AI Ark |
| 17 | `best_email` | **the address to actually send to** | best available source |
| 18 | `email_source` | `localpipe_owner` / `localpipe_business` / `aiark` / `prospeo` | provenance |
| 19 | `email_domain_match` | `yes` / `no` — does #17's domain equal #5? **QA the `no`s** | computed |
| 20 | `email_provider` | mail host behind the address (Google Workspace, Outlook…) | LocalPipe |
| 21 | `owner_name` | owner as LocalPipe named them | LocalPipe |
| 22 | `owner_email` | LocalPipe's **verified** owner address | LocalPipe |
| 23 | `business_email` | generic mailbox — **unverified**, `info@`-style | LocalPipe |
| 24 | `aiark_name` | person AI Ark named, when AI Ark supplied this row | AI Ark |
| 25 | `aiark_email` | address AI Ark supplied | AI Ark |
| 26 | `prospeo_email` | address Prospeo supplied | Prospeo |
| 27 | `place_id` | Google's location ID — **the row match key** | scrape |

**Three that are easy to misread:**

- **#3 `business_location` vs #8 `city`.** #3 is the city you *searched*; #8 is where the business actually *is*. When they disagree you are usually looking at a border leak — a Windsor, ON search returning a Detroit, MI business.
- **#17 `best_email` vs #22/#23/#25/#26.** `best_email` is the one to send to. The others are the raw per-source values kept for provenance, and the same address deliberately appears in two places — `owner_email` and `best_email` on one row hold the same value by design. **Counting duplicates across the whole sheet double-counts every row**: 33 real duplicates once read as 1,341.
- **#27 `place_id` is not unique.** One row per contact means a business with three decision-makers is three rows sharing a `place_id`. The uniqueness key is `(place_id, best_email)`.

**Which ones the Clay workflow actually depends on:** `place_id` to match rows back, `business_website` and `domain` to scrape, `business_name` plus `city` / `full_address` to identify the right business, and `result_tier` / `best_email` to decide which rows still need work. The rest ride along as context. **All 27 should still be present** — the template maps the whole schema, and a missing column is a broken mapping, not a smaller one.

**If your Clay table was edited, check it before you import.** Open the table's column list and compare it against the 27 above, name for name and position for position. A renamed column does not error — it just silently stops receiving data, and you find out when a whole column comes back empty.

**One supplementary file:** `*-CONTACT-TITLES.csv`, also in Downloads. Job titles have no column in the fixed 27, so they live here alongside the contact and `place_id`. If you need to capture something new, this is the pattern — a separate file, rather than a 28th column that breaks every downstream mapping.

## Finishing the list in Clay (44% → 84%)

This pipeline stops where the paid databases stop. It does **not** stop where the list stops being useful.

Every run leaves two kinds of unfinished row: `2_NAME_ONLY_NO_EMAIL` (you know the decision-maker, you have no address) and `3_EMPTY` (nothing at all). Those are not failures — they are rows the LinkedIn-shaped databases have no record for, which is exactly what you should expect when your ICP is the owner-operator.

The export drops straight into a Clay table — that is what the frozen 27-column schema is *for* — and two Claygent passes finish the job:

1. **Scrape the company website.** Homepage first, then the About / Contact pages, looking for a published `info@`, `support@`, `contact@` or similar. Small local businesses very often publish an address they never gave to any B2B database.
2. **If that finds nothing, scrape the Facebook page.** Local trades frequently maintain a Facebook business page more actively than their own site, and list a contact address there.

**Measured, on one 2,500-business Canadian run:** the pipeline alone reached an email for **~1,100 of 2,500 businesses (44%)**. After the two Claygent passes the finished list reached **~2,100 of 2,500 — 84%.**

Both figures are on the same base: *businesses with at least one email*, not export rows. Counting rows flatters the number, because a business with three decision-makers contributes three rows.

### Use the template

You do not have to rebuild any of this. The Clay table is shared as a template:

**→ [Clay table template](https://app.clay.com/shared-table/share_0tkdynlqKCi6wUMYRQS)**

1. Open the link and **create a table from the template.** The columns and the two Claygent passes come with it, already wired.
2. **Import your export CSV** — `general-contractors-enriched.csv` straight out of `split_results.py`, no edits.
3. **Match the incoming CSV columns to the table's columns.** They line up by name, which is the entire reason the 27-column schema is frozen: change a column here and the mapping stops matching.
4. Run it. The rest of the workflow is already mapped and needs no further setup.

Two things worth knowing before you send what comes out:

- **These are generic mailboxes, not the decision-maker.** They are unverified by definition — run them through a verifier, and expect them to convert differently from the owner addresses in `email_source = localpipe_owner`. Keep `email_source` populated so you can always tell the two apart.
- **Run the cheap sources first, in this order.** Website scraping is close to free compared with per-credit enrichment, so it is tempting to lead with it — but it returns generic mailboxes, while LocalPipe and the waterfall return *named owners*. Spend the credits on the rows where a person is reachable, then let the scrape mop up whatever is left. Reversing the order fills your list with `info@` and stops you ever finding the owner.

## How chain detection works (free, no API)

Group businesses by **root domain**, count distinct `place_id`s. Two guards matter:

1. **Group by domain, never by name.** "Turner" and "Turner Construction Company" share `turnerconstruction.com`; keying on the name splits one 7-location brand into a 5 and a 2.
2. **Blocklist shared hosts.** A business whose website is `sites.google.com` or `facebook.com` must never be grouped or looked up by that domain — you'll match employees of the *host*. This is not hypothetical: an early run returned an address at the *host's* domain — a Google employee — for a Los Angeles renovation contractor whose site was a `sites.google.com` page.

Correctly identified Turner, DPR, Gilbane, Suffolk, JE Dunn, and Mortenson in the test run.

**Franchises are dropped by default, not merely flagged.** Two reasons. Cost: every branch shares one website and all the enrichment providers key on the domain, so six branches of one brand returned **1 distinct email** in testing. Fit: a national contractor's head office is not the buyer for a local-operator offer — the independent owner is. Run `--keep-chains` when you genuinely want the chains.

**The honest limit of this flag.** `is_multi_location` is derived from the cities in *your* scrape, so it detects multi-branch presence **within your scraped footprint** — not nationwide franchise status. A national brand that appears in only one of your cities is not flagged and will slip through. Catching the rest would need a known-brand blocklist, which this repo does not have.

## Provider gotchas (learned the hard way)

**ScraperTech** — `limit` is a ceiling, not a page size: one request returns everything Google has for that query. `offset` paging is unreliable (13% overlap with page 1), so always dedup by `place_id`. Billed per request, not per result. **There is no usage or quota endpoint** — `/account`, `/usage`, `/quota` and `/balance` all 404, so remaining searches are visible only in the web panel; the MCP `tools/list` call is free and is the way to confirm a key is live without spending a search. The MCP endpoint is `https://mcp.scraper.tech/<key>` — key in the URL path — and it speaks streamable HTTP, not only SSE. It returns `state` for US rows but **`null` for Canadian ones**, where the province rides inside `city` instead ("Toronto, ON").

**LocalPipe** — asynchronous: submit, then poll. The generic mailbox field is `business_email`, **never** `general_email`. Misses come back as the literal string `"not found"`, never null. Business emails are returned **unverified** on credit plans — run them through a verifier before sending. Re-submitting an already-enriched business hits your own cache (`cached_from_user: 1`) instead of re-charging. **Submission concurrency 8 is proven; 20 caused `RemoteDisconnected` mid-run.** Polling is excluded from rate limits, so poll in wide sweeps rather than one thread per job. Its own `/maps-search` bills **per lead returned** and its `limit` overshoots — a request for 5 returned 50, all billed.

**AI Ark** — Cloudflare blocks Python's default User-Agent with a `403 / error code: 1010` that looks exactly like a bad API key. Send a browser User-Agent on every call; the auth header is `X-TOKEN` with the raw key, no prefix. **The job title is not in `headline`,** which comes back `null` — it lives at `position_groups[].profile_positions[].title`, and `department.seniority` is a coarse bucket (`senior`, `director`, `mid_level`) that is not a substitute for reading the real title. **`/v1/people/export` requires a webhook** and 400s without one; with no public webhook URL, use the synchronous `POST /v2/people/export/single` instead. **Search seniority-filtered first** — see [Corrections](#corrections) — and note that AI Ark holds **duplicate profile records for the same human**, so collapse candidates by name before paying for their email.

**Prospeo** — needs a name, so it cannot start from a bare domain; search for a `person_id` first, then enrich. Masked addresses contain `*` and are unusable — treat them as a miss, not a hit. `free_enrichment: true` means no credit was charged. Prospeo fires at **two** points in the waterfall: once per AI Ark contact who has a name but no email, and again at company level if no contact produced an email at all, using the best name known (LocalPipe's preferred, else AI Ark's).

**Both AI Ark and Prospeo have thin coverage of small local businesses** — roughly 30% of small US contractors had any person on file. They are LinkedIn-shaped B2B databases; sole proprietors are largely absent. The waterfall's hit rate tracks that directly: **27% on a metro-heavy US list, 16% on a deliberately small-operator Canadian list.** LocalPipe carries the bulk regardless.

## Corrections

Notes rot. A wrong note is worse than no note, so conclusions that turned out wrong are corrected here in public rather than quietly deleted.

**"AI Ark's seniority filter destroys recall" — WRONG.** An earlier version of this README said to search unfiltered. That was based on small-business misses, and it was the wrong read. On a 4,145-employee firm the **filtered** search returned 13 people including the President & CEO, while the **unfiltered** search returned superintendents, recruiters and field coordinators — the CEO is unreachable without the filter. The misses that produced the original claim were companies AI Ark has **no records for at all** (0 filtered *and* 0 unfiltered), not a filtering artefact. The code does filtered-first with unfiltered as a fallback for small firms whose owner is not seniority-tagged.

**"Canadian coverage will be thinner than US" — WRONG.** Predicted 45–55%, came in at 61% against the US's 57%.

**"A 5-row sample tells you the hit rate" — MISLEADING.** Two separate 0/5 samples preceded full runs at 57% and 61%. Samples validate plumbing, not yield.

**"Excluding by `place_id` is enough" — INCOMPLETE.** On Run 4, **133 businesses matched a prior domain under a new `place_id`** after a re-scrape. Excluding on `place_id` alone would have shipped all 133 as duplicates of companies already delivered. `--exclude` now matches on both.

## Design notes

- **Resumable.** Every script skips work already completed, so a crash or timeout costs nothing. `scrape.py --reuse-raw` rebuilds exports from cached JSON with zero API calls.
- **Job IDs persist immediately.** Each accepted job is appended to disk the instant the API returns it. An early version wrote them only after all submissions finished — a mid-run disconnect orphaned ~495 already-accepted (and already-charged) jobs.
- **Bounded concurrency + 429 retry.** A 429 means the call was *not* accepted; without a retry that record is silently lost. Concurrency 8 is proven stable.
- **Assertions on every stage.** Row counts, duplicate keys, and tier consistency are checked before anything is written, and failures print loudly. They earn their keep: the export assertion caught 4 duplicate `(place_id, email)` rows on one run that would otherwise have emailed the same address twice.
- **Logging never raises.** A non-ASCII business name — an emoji in a company name, an accented Québec name — kills a run on Windows, where the console is cp1252 and `print()` raises `UnicodeEncodeError`. That happened mid-poll, *after* jobs were already charged. stdout is forced to UTF-8 and every log call is guarded.
- **A check that errors is not a check that passes.** A secret scan once printed "clean" because the command itself failed and fell through to the else branch. Verify the check ran, not just that it printed something reassuring.

## Costs

| Provider | Model |
|---|---|
| ScraperTech | per request — 32 requests scraped 800 businesses; 74 requests scraped 10,864 |
| LocalPipe | ~2 credits per email, charged only when found |
| AI Ark | per credit, charged only when found |
| Prospeo | 1 credit per success, free on no-match |

**Scraping is cheap; enrichment is not.** A 74-city run at depth 150 cost 74 of 60,000 available ScraperTech requests. Your volume ceiling is set by enrichment credits, not by scrape quota — so scrape wide, bank the surplus, and enrich deliberately. `--reuse-raw` and `--add` let you widen a list later without re-scraping anything.

## Before you send

- **Verify the generic `info@`-style addresses.** LocalPipe returns them unverified on credit plans, and they can be **30–47% of a list**. Run them through a verifier (MillionVerifier, ZeroBounce) before sending. Owner addresses come back verified and do not need a second pass.
- Review rows where `email_domain_match` is `no`. Most are free-mail (gmail/yahoo) which is normal for small contractors; the rest are usually legitimate alternate corporate domains, but some are wrong matches.
- **Dedupe by email** — multi-location chains repeat the same contact across rows.
- **Do not ship a run that reports FAIL.** Assertions run automatically at every stage and fail loudly; a failing assertion means the export is wrong, not that the assertion is fussy.
- **When you report a duplicate count, say which column you counted.** `owner_email` and `best_email` hold the same value on the same row by design, so a whole-sheet duplicate check double-counts every row — 33 real duplicates once read as 1,341.
