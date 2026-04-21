"""
Harris County Lead Scraper — Playwright Edition v3
Loads pages directly without form submission where possible.
"""

import re
import logging
import asyncio
import hashlib
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# SCORING & RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

RESOLUTION_LABELS = {
    "deed_transfer":    ("Deed transferred to new buyer",        "SOLD",       "chip-sold"),
    "loan_reinstated":  ("Loan reinstated / modification filed", "REINSTATED", "chip-reinstated"),
    "notice_rescinded": ("Foreclosure notice rescinded",         "WITHDRAWN",  "chip-withdrawn"),
    "sale_cancelled":   ("Sale cancelled by lender",             "CANCELLED",  "chip-closed"),
    "bankruptcy_stay":  ("Bankruptcy stay filed",                "STAYED",     "chip-closed"),
    "taxes_paid":       ("Tax balance paid — account current",   "PAID",       "chip-paid"),
    "payment_plan":     ("Payment agreement entered",            "AGREEMENT",  "chip-reinstated"),
    "suit_dismissed":   ("Tax suit dismissed",                   "DISMISSED",  "chip-closed"),
    "estate_sold":      ("Property sold by estate or heirs",     "SOLD",       "chip-sold"),
    "case_closed":      ("Probate case closed / settled",        "CLOSED",     "chip-closed"),
}

PROBATE_PTS = {"estate":25,"outstate":22,"multiheir":18,"taxcombo":30,"fccombo":30,"recent":15}
FC_PTS      = {"equity":28,"vacant":25,"fc-tax":30,"divorce":22,"fc-probate":25}
TAX_PTS     = {"tx-fc":32,"tx-vacant":26,"tx-oos":24,"tx-dist":22,"tx-hival":28,
               "tx-probate":30,"tx-long":25,"tx-rental":18,"tx-small":15}

def check_resolved(lead):
    s = lead.get("resolution_status","active")
    if not s or s == "active": return {"resolved": False}
    info = RESOLUTION_LABELS.get(s)
    if info:
        label, stamp, chip = info
        return {"resolved": True, "reason": label, "stamp": stamp, "chip": chip}
    return {"resolved": True, "reason": f"Settled ({s})", "stamp": "SETTLED", "chip": "chip-closed"}

def score_lead(lead):
    t = lead.get("type","")
    pts = lead.get("base_score", 40)
    sigs = lead.get("signals",[])
    pts += sum((FC_PTS if t=="foreclosure" else TAX_PTS if t=="tax" else PROBATE_PTS).get(s,0) for s in sigs)
    score = min(100, pts)
    tier = 1 if score>=90 else 2 if score>=75 else 3 if score>=60 else 4 if score>=45 else 5
    return score, tier

def lead_id(lead):
    key = f"{lead.get('address','')}{lead.get('owner','')}".lower().strip()
    return hashlib.md5(key.encode()).hexdigest()[:16]

# ─────────────────────────────────────────────────────────────────────────────
# BROWSER
# ─────────────────────────────────────────────────────────────────────────────

async def get_browser():
    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    logger.info("Launching Chromium...")
    browser = await pw.chromium.launch(
        headless=True,
        args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--disable-gpu"]
    )
    logger.info("Chromium launched")
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        viewport={"width":1280,"height":900}
    )
    return pw, browser, ctx

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def get_table_rows(page):
    """Extract all rows from the largest table on the page."""
    tables = await page.locator("table").all()
    if not tables:
        return []
    # Pick the table with the most rows
    best, best_count = None, 0
    for t in tables:
        rows = await t.locator("tr").all()
        if len(rows) > best_count:
            best, best_count = t, len(rows)
    if not best:
        return []
    rows = await best.locator("tr").all()
    result = []
    for row in rows[1:]:  # skip header
        cells = await row.locator("td").all()
        texts = [re.sub(r'\s+',' ',(await c.inner_text()).strip()) for c in cells]
        if any(texts):
            result.append(texts)
    return result

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — PRE-FORECLOSURE via HCAD public search
# We search pdata.hcad.org for recent deed-of-trust filings instead of
# the clerk site which requires form interaction
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_foreclosures_async(ctx, days_back=45):
    leads = []
    page = await ctx.new_page()
    try:
        # Harris County Clerk foreclosure notices — direct URL with date range
        start = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end   = date.today().strftime("%m/%d/%Y")

        # Use direct URL with date params — avoids needing to interact with ASP.NET form
        url = "https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx"
        logger.info(f"FRCL: loading {url}")
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        title = await page.title()
        logger.info(f"FRCL: title='{title}'")

        # Log ALL inputs to find right field names
        inputs = await page.locator("input").all()
        logger.info(f"FRCL: found {len(inputs)} inputs total")
        for i, inp in enumerate(inputs[:20]):
            try:
                iid   = await inp.get_attribute("id") or ""
                iname = await inp.get_attribute("name") or ""
                itype = await inp.get_attribute("type") or ""
                logger.info(f"FRCL input[{i}]: id='{iid}' name='{iname}' type='{itype}'")
            except Exception:
                pass

        # Fill ANY visible text inputs in order
        text_inputs = await page.locator("input[type='text']:visible").all()
        logger.info(f"FRCL: {len(text_inputs)} visible text inputs")
        if len(text_inputs) >= 2:
            await text_inputs[0].fill(start, force=True)
            await text_inputs[1].fill(end, force=True)
            await page.wait_for_timeout(500)

        # Click any visible submit/button
        btns = await page.locator("input[type='submit']:visible, input[type='button']:visible, button:visible").all()
        for btn in btns:
            try:
                val = (await btn.get_attribute("value") or await btn.inner_text() or "").strip()
                logger.info(f"FRCL btn: '{val}'")
                if re.search(r"search|find|submit|go|view", val, re.I):
                    await btn.click(force=True)
                    await page.wait_for_timeout(6000)
                    break
            except Exception:
                continue

        rows = await get_table_rows(page)
        logger.info(f"FRCL: {len(rows)} data rows")
        if rows: logger.info(f"FRCL sample: {rows[0]}")

        for texts in rows:
            if len(texts) < 3: continue
            try:
                filing_date = texts[0]
                instrument  = texts[1] if len(texts)>1 else ""
                grantor     = texts[2] if len(texts)>2 else ""
                address     = texts[3] if len(texts)>3 else ""
                mortgagee   = texts[4] if len(texts)>4 else ""
                amount_raw  = texts[5] if len(texts)>5 else ""

                if not address or not grantor or len(address) < 5:
                    continue

                amt = re.sub(r"[^\d.]","",amount_raw)
                amount = f"${float(amt):,.0f}" if amt else amount_raw

                try:
                    fd = datetime.strptime(filing_date,"%m/%d/%Y")
                    sale_date = (fd + timedelta(days=45)).strftime("%m/%d/%Y")
                except Exception:
                    sale_date = ""

                lead = {
                    "id": lead_id({"address":address,"owner":grantor}),
                    "type": "foreclosure",
                    "owner": grantor, "address": address, "amount": amount,
                    "filing_date": filing_date, "sale_date": sale_date,
                    "source": instrument, "lender": mortgagee,
                    "base_score": 38, "signals": [],
                    "equity": None, "occupancy": None, "court_case": None,
                    "resolution_status": "active",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                lead["score"], lead["tier"] = score_lead(lead)
                leads.append(lead)
            except Exception as e:
                logger.warning(f"FRCL row error: {e}")

    except Exception as e:
        logger.error(f"FRCL failed: {e}")
    finally:
        await page.close()

    logger.info(f"FRCL: {len(leads)} leads")
    return leads

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — TAX DELINQUENT (static listing page — no form needed)
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_tax_async(ctx):
    leads = []
    page = await ctx.new_page()
    try:
        logger.info("TAX: loading hctax.net sale listing...")
        await page.goto("https://www.hctax.net/Property/TaxSales/Index", timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        title = await page.title()
        content = await page.content()
        logger.info(f"TAX: title='{title}' content_len={len(content)}")

        # Get all links to sale list PDFs or detail pages
        links = await page.locator("a[href*='taxsale'], a[href*='listing'], a[href*='sale']").all()
        logger.info(f"TAX: found {len(links)} sale links")

        # Try direct listing URL
        await page.goto("https://www.hctax.net/Property/listings/taxsalelisting", timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        title2 = await page.title()
        logger.info(f"TAX listing: title='{title2}'")

        # Get sale date
        sale_date = ""
        try:
            heading = await page.locator("h1,h2,h3").first.inner_text()
            dm = re.search(r"(\w+ \d+,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4})", heading)
            if dm: sale_date = dm.group(1)
        except Exception:
            pass

        rows = await get_table_rows(page)
        logger.info(f"TAX: {len(rows)} data rows")
        # Log first 5 rows to see exact column structure
        for i, r in enumerate(rows[:5]):
            logger.info(f"TAX row[{i}]: {r}")

        for texts in rows:
            if len(texts) < 2: continue
            try:
                # Log every row to find the right column mapping
                # Harris County tax sale page columns vary — detect by content
                # Find which column has an address (contains TX or street pattern)
                address = ""
                owner   = ""
                amount_raw = ""
                account = ""

                for j, t in enumerate(texts):
                    # Address: contains TX and looks like a street
                    if re.search(r"TX|Houston|Harris", t, re.I) and re.search(r"\d+\s+\w+", t):
                        address = t
                    # Amount: starts with $ or is a number
                    elif re.search(r"^\$[\d,]+|^[\d,]+\.\d{2}$", t.strip()):
                        amount_raw = t
                    # Account: short numeric string
                    elif re.match(r"^\d{6,}$", t.strip()):
                        account = t
                    # Owner: everything else that's not empty and not a date
                    elif t and not re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", t) and not owner:
                        owner = t

                if not address or not owner or len(address) < 5:
                    continue

                amt = re.sub(r"[^\d.]","",amount_raw)
                tax_amt = f"${float(amt):,.0f} owed" if amt else "Amount TBD"

                lead = {
                    "id": lead_id({"address":address,"owner":owner}),
                    "type": "tax",
                    "owner": owner, "address": address, "amount": tax_amt,
                    "filing_date": "", "sale_date": sale_date,
                    "source": f"hctax.net #{account}", "lender": None,
                    "base_score": 30, "signals": [],
                    "years_delinquent": None, "mailing_state": None,
                    "prop_sqft": None, "tax_to_value": None, "occupancy": None,
                    "resolution_status": "active",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                lead["score"], lead["tier"] = score_lead(lead)
                leads.append(lead)
            except Exception as e:
                logger.warning(f"TAX row error: {e}")

    except Exception as e:
        logger.error(f"TAX failed: {e}")
    finally:
        await page.close()

    logger.info(f"TAX: {len(leads)} leads")
    return leads

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — PROBATE via District Clerk public search
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_probate_async(ctx, days_back=30):
    leads = []
    page = await ctx.new_page()
    try:
        start = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end   = date.today().strftime("%m/%d/%Y")

        logger.info("PROBATE: loading district clerk...")
        await page.goto("https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx", timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        title = await page.title()
        content = await page.content()
        logger.info(f"PROBATE: title='{title}' content_len={len(content)}")

        # Log all select elements to find case type dropdown
        selects = await page.locator("select").all()
        logger.info(f"PROBATE: found {len(selects)} select elements")
        for i, sel in enumerate(selects):
            try:
                sel_id = await sel.get_attribute("id") or ""
                opts = await sel.locator("option").all()
                opt_texts = [await o.inner_text() for o in opts[:5]]
                logger.info(f"PROBATE select[{i}] id='{sel_id}' options={opt_texts}")
            except Exception:
                pass

        # Log all text inputs
        inputs = await page.locator("input[type='text']").all()
        logger.info(f"PROBATE: found {len(inputs)} text inputs")
        for i, inp in enumerate(inputs):
            try:
                inp_id = await inp.get_attribute("id") or ""
                logger.info(f"PROBATE input[{i}] id='{inp_id}'")
            except Exception:
                pass

        # Try to select probate and search
        for sel in selects:
            try:
                opts = await sel.locator("option").all()
                for opt in opts:
                    txt = await opt.inner_text()
                    if re.search(r"prob", txt, re.I):
                        await sel.select_option(label=txt)
                        logger.info(f"PROBATE: selected case type '{txt}'")
                        break
            except Exception:
                pass

        # Fill date fields
        filled = 0
        for inp in inputs:
            try:
                inp_id = (await inp.get_attribute("id") or "").lower()
                if "from" in inp_id or "start" in inp_id or (filled == 0 and "date" in inp_id):
                    await inp.fill(start)
                    filled += 1
                elif "to" in inp_id or "end" in inp_id or (filled == 1 and "date" in inp_id):
                    await inp.fill(end)
                    filled += 1
            except Exception:
                pass

        # Click search — try force click on all buttons
        btns = await page.locator("input[type='submit'], input[type='button'], button").all()
        logger.info(f"PROBATE: found {len(btns)} buttons")
        for btn in btns:
            try:
                val = await btn.get_attribute("value") or await btn.inner_text()
                if re.search(r"search|find|submit", val, re.I):
                    await btn.click(force=True)
                    logger.info(f"PROBATE: clicked '{val}'")
                    await page.wait_for_timeout(5000)
                    break
            except Exception:
                pass

        rows = await get_table_rows(page)
        logger.info(f"PROBATE: {len(rows)} data rows")
        if rows: logger.info(f"PROBATE sample: {rows[0]}")

        for texts in rows:
            if len(texts) < 2: continue
            try:
                case_num    = texts[0]
                file_date   = texts[1] if len(texts)>1 else ""
                case_style  = texts[2] if len(texts)>2 else ""
                case_status = texts[3] if len(texts)>3 else ""

                if re.search(r"closed|dismissed|settled|disposed", case_status, re.I):
                    continue
                if not re.search(r"^(Estate of|Heirs of|In re)", case_style, re.I):
                    continue

                signals = []
                if re.search(r"^Estate of", case_style, re.I): signals.append("estate")
                if re.search(r"heirs|et al", case_style, re.I): signals.append("multiheir")
                try:
                    fd = datetime.strptime(file_date,"%m/%d/%Y")
                    if (datetime.today()-fd).days <= 14: signals.append("recent")
                except Exception:
                    pass

                lead = {
                    "id": lead_id({"address":case_num,"owner":case_style}),
                    "type": "probate",
                    "owner": case_style.strip(), "address": "",
                    "amount": "Est. TBD",
                    "filing_date": file_date, "sale_date": None,
                    "source": f"Dist. Clerk #{case_num}", "lender": None,
                    "base_score": 35, "signals": signals,
                    "heirs": "Multiple" if "multiheir" in signals else "Unknown",
                    "mailing": "", "resolution_status": "active",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                lead["score"], lead["tier"] = score_lead(lead)
                leads.append(lead)
            except Exception as e:
                logger.warning(f"PROBATE row error: {e}")

    except Exception as e:
        logger.error(f"PROBATE failed: {e}")
    finally:
        await page.close()

    logger.info(f"PROBATE: {len(leads)} leads")
    return leads

# ─────────────────────────────────────────────────────────────────────────────
# CROSS-SIGNALS + DEDUP + SCRUB
# ─────────────────────────────────────────────────────────────────────────────

def apply_cross_signals(leads):
    tax_a = {l["address"].lower() for l in leads if l["type"]=="tax"}
    fc_a  = {l["address"].lower() for l in leads if l["type"]=="foreclosure"}
    pb_a  = {l["address"].lower() for l in leads if l["type"]=="probate"}
    for l in leads:
        a = l["address"].lower()
        s = l.get("signals",[])
        if l["type"]=="probate":
            if a in tax_a and "taxcombo" not in s: s.append("taxcombo")
            if a in fc_a  and "fccombo"  not in s: s.append("fccombo")
        elif l["type"]=="foreclosure":
            if a in tax_a and "fc-tax"     not in s: s.append("fc-tax")
            if a in pb_a  and "fc-probate" not in s: s.append("fc-probate")
        elif l["type"]=="tax":
            if a in fc_a and "tx-fc"      not in s: s.append("tx-fc")
            if a in pb_a and "tx-probate" not in s: s.append("tx-probate")
        l["signals"] = s
        l["score"], l["tier"] = score_lead(l)
    return leads

def deduplicate(leads):
    seen = {}
    for l in leads:
        if l["id"] not in seen: seen[l["id"]] = l
    return list(seen.values()), len(leads)-len(seen)

def scrub_settled(leads):
    active, settled = [], []
    for l in leads:
        r = check_resolved(l)
        if r["resolved"]: l["_resolution"]=r; settled.append(l)
        else: active.append(l)
    return active, settled

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def run_full_scrape_async():
    logger.info("="*60)
    logger.info("Harris County Scraper starting")
    logger.info("="*60)
    start = datetime.utcnow()
    pw = browser = ctx = None
    fc_leads = tax_leads = probate_leads = []

    try:
        pw, browser, ctx = await get_browser()
        fc_leads      = await scrape_foreclosures_async(ctx, days_back=45)
        tax_leads     = await scrape_tax_async(ctx)
        probate_leads = await scrape_probate_async(ctx, days_back=30)
    except Exception as e:
        logger.error(f"Scrape error: {e}")
    finally:
        if ctx:     await ctx.close()
        if browser: await browser.close()
        if pw:      await pw.stop()

    all_leads = fc_leads + tax_leads + probate_leads
    all_leads = apply_cross_signals(all_leads)
    all_leads, dupes = deduplicate(all_leads)
    active, settled = scrub_settled(all_leads)
    elapsed = (datetime.utcnow()-start).total_seconds()

    logger.info(f"Done: {len(active)} active leads in {elapsed:.0f}s")
    return {
        "scraped_at": start.isoformat(),
        "elapsed_seconds": round(elapsed,1),
        "raw_total": len(fc_leads)+len(tax_leads)+len(probate_leads),
        "duplicates_removed": dupes,
        "settled_removed": len(settled),
        "active_leads": len(active),
        "by_type": {
            "foreclosure": len([l for l in active if l["type"]=="foreclosure"]),
            "tax":         len([l for l in active if l["type"]=="tax"]),
            "probate":     len([l for l in active if l["type"]=="probate"]),
        },
        "settled_log": [{"owner":l["owner"],"address":l["address"],"type":l["type"],
                         "reason":l.get("_resolution",{}).get("reason",""),
                         "stamp":l.get("_resolution",{}).get("stamp","")} for l in settled],
        "leads": active,
    }

def run_full_scrape():
    return asyncio.run(run_full_scrape_async())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_full_scrape()
    print(f"Active: {result['active_leads']} | By type: {result['by_type']}")
