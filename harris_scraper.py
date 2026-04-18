"""
Harris County Lead Scraper — Playwright Edition
Uses headless Chromium to render JavaScript and handle ASP.NET session tokens.
Sources:
  - Pre-Foreclosure : https://www.cclerk.hctx.net (FRCL filings)
  - Tax Delinquent  : https://www.hctax.net/Property/listings/taxsalelisting
  - Probate         : https://www.hcdistrictclerk.com (public case search)
  - Enrichment      : https://hcad.org (owner, value, sq footage)
"""

import re
import time
import logging
import hashlib
import asyncio
from datetime import datetime, date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# RESOLUTION + SCORING (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

RESOLUTION_LABELS = {
    "deed_transfer":    ("Deed transferred to new buyer",        "SOLD",        "chip-sold"),
    "loan_reinstated":  ("Loan reinstated / modification filed", "REINSTATED",  "chip-reinstated"),
    "notice_rescinded": ("Foreclosure notice rescinded",         "WITHDRAWN",   "chip-withdrawn"),
    "sale_cancelled":   ("Sale cancelled by lender",             "CANCELLED",   "chip-closed"),
    "bankruptcy_stay":  ("Bankruptcy stay filed",                "STAYED",      "chip-closed"),
    "taxes_paid":       ("Tax balance paid — account current",   "PAID",        "chip-paid"),
    "payment_plan":     ("Payment agreement entered",            "AGREEMENT",   "chip-reinstated"),
    "suit_dismissed":   ("Tax suit dismissed",                   "DISMISSED",   "chip-closed"),
    "exemption_filed":  ("Exemption removed balance",            "RESOLVED",    "chip-closed"),
    "estate_sold":      ("Property sold by estate or heirs",     "SOLD",        "chip-sold"),
    "case_closed":      ("Probate case closed / settled",        "CLOSED",      "chip-closed"),
    "heirs_resolved":   ("Heirs reached agreement",              "RESOLVED",    "chip-paid"),
    "title_cleared":    ("Title cleared / quiet title granted",  "CLEARED",     "chip-paid"),
}

PROBATE_SIGNAL_POINTS = {
    "estate": 25, "outstate": 22, "multiheir": 18,
    "taxcombo": 30, "fccombo": 30, "recent": 15,
}
FC_SIGNAL_POINTS = {
    "equity": 28, "vacant": 25, "fc-tax": 30,
    "divorce": 22, "fc-probate": 25,
}
TAX_SIGNAL_POINTS = {
    "tx-fc": 32, "tx-vacant": 26, "tx-oos": 24,
    "tx-dist": 22, "tx-hival": 28, "tx-probate": 30,
    "tx-long": 25, "tx-rental": 18, "tx-small": 15,
}


def check_resolved(lead):
    status = lead.get("resolution_status", "active")
    if status == "active" or not status:
        return {"resolved": False}
    info = RESOLUTION_LABELS.get(status)
    if info:
        label, stamp, chip = info
        return {"resolved": True, "reason": label, "stamp": stamp, "chip": chip}
    return {"resolved": True, "reason": f"Settled ({status})", "stamp": "SETTLED", "chip": "chip-closed"}


def score_lead(lead):
    lead_type = lead.get("type", "")
    base = lead.get("base_score", 40)
    signals = lead.get("signals", [])
    if lead_type == "foreclosure":
        pts = base + sum(FC_SIGNAL_POINTS.get(s, 0) for s in signals)
    elif lead_type == "tax":
        pts = base + sum(TAX_SIGNAL_POINTS.get(s, 0) for s in signals)
    else:
        pts = base + sum(PROBATE_SIGNAL_POINTS.get(s, 0) for s in signals)
    score = min(100, pts)
    tier = 1 if score >= 90 else 2 if score >= 75 else 3 if score >= 60 else 4 if score >= 45 else 5
    return score, tier


def lead_id(lead):
    key = f"{lead.get('address','')}{lead.get('owner','')}".lower().strip()
    return hashlib.md5(key.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT BROWSER HELPER
# ─────────────────────────────────────────────────────────────────────────────

async def get_browser():
    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-first-run",
            "--no-zygote",
            "--single-process",
        ]
    )
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )
    return pw, browser, context


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: PRE-FORECLOSURE — Harris County Clerk FRCL Filings
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_foreclosures_async(context, days_back=45):
    leads = []
    logger.info("Scraping pre-foreclosure filings from cclerk.hctx.net...")
    page = await context.new_page()
    try:
        await page.goto("https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)

        start_date = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end_date = date.today().strftime("%m/%d/%Y")

        # Fill date fields
        from_field = page.locator("input[id*='From'], input[id*='from'], input[name*='From']").first
        to_field   = page.locator("input[id*='To'], input[id*='to'], input[name*='To']").first

        await from_field.fill(start_date)
        await to_field.fill(end_date)

        # Click search
        search_btn = page.locator("input[type='submit'], input[value*='Search'], button:has-text('Search')").first
        await search_btn.click()
        await page.wait_for_load_state("networkidle", timeout=20000)

        # Extract table rows
        # Diagnostic logging
        page_title = await page.title()
        page_url = page.url
        tables_count = await page.locator("table").count()
        all_text = await page.locator("body").inner_text()
        logger.info(f"FRCL page: title='{page_title}' url='{page_url}' tables={tables_count}")
        logger.info(f"FRCL body preview: {all_text[:300]}")

        rows = await page.locator("table tr").all()
        logger.info(f"FRCL: Found {len(rows)} rows")

        for row in rows[1:]:  # skip header
            cells = await row.locator("td").all()
            if len(cells) < 4:
                continue
            try:
                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]

                filing_date = texts[0] if len(texts) > 0 else ""
                instrument  = texts[1] if len(texts) > 1 else ""
                grantor     = texts[2] if len(texts) > 2 else ""
                address     = texts[3] if len(texts) > 3 else ""
                mortgagee   = texts[4] if len(texts) > 4 else ""
                amount_raw  = texts[5] if len(texts) > 5 else ""

                if not address or not grantor:
                    continue

                # Parse amount
                amt_clean = re.sub(r"[^\d.]", "", amount_raw)
                loan_amount = f"${float(amt_clean):,.0f}" if amt_clean else amount_raw

                # Estimate sale date (~45 days after filing)
                try:
                    fd = datetime.strptime(filing_date, "%m/%d/%Y")
                    sale_dt = fd + timedelta(days=45)
                    # Round to first Tuesday of next month
                    sale_date = sale_dt.strftime("%m/%d/%Y")
                except Exception:
                    sale_date = ""

                lead = {
                    "id": lead_id({"address": address, "owner": grantor}),
                    "type": "foreclosure",
                    "owner": grantor,
                    "address": address,
                    "amount": loan_amount,
                    "filing_date": filing_date,
                    "sale_date": sale_date,
                    "source": instrument,
                    "lender": mortgagee,
                    "base_score": 38,
                    "signals": [],
                    "equity": None,
                    "occupancy": None,
                    "court_case": None,
                    "resolution_status": "active",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                score, tier = score_lead(lead)
                lead["score"] = score
                lead["tier"] = tier
                leads.append(lead)

            except Exception as e:
                logger.warning(f"FRCL row error: {e}")

    except Exception as e:
        logger.error(f"FRCL scrape failed: {e}")
    finally:
        await page.close()

    logger.info(f"FRCL: {len(leads)} foreclosure leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: TAX DELINQUENT — hctax.net
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_tax_async(context):
    leads = []
    logger.info("Scraping tax delinquent list from hctax.net...")
    page = await context.new_page()
    try:
        await page.goto("https://www.hctax.net/Property/listings/taxsalelisting", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)

        # Get sale date from page
        sale_date = ""
        try:
            header = await page.locator("h1, h2, h3, .sale-date, [class*='date']").first.inner_text()
            date_match = re.search(r"(\w+ \d+,?\s*\d{4})", header)
            if date_match:
                sale_date = date_match.group(1)
        except Exception:
            pass

        # Diagnostic logging
        page_title = await page.title()
        tables_count = await page.locator("table").count()
        all_text = await page.locator("body").inner_text()
        logger.info(f"TAX page: title='{page_title}' tables={tables_count}")
        logger.info(f"TAX body preview: {all_text[:300]}")

        rows = await page.locator("table tr").all()
        logger.info(f"Tax sale: Found {len(rows)} rows")

        for row in rows[1:]:
            cells = await row.locator("td").all()
            if len(cells) < 3:
                continue
            try:
                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]

                account    = texts[0] if len(texts) > 0 else ""
                owner      = texts[1] if len(texts) > 1 else ""
                address    = texts[2] if len(texts) > 2 else ""
                amount_raw = texts[3] if len(texts) > 3 else ""

                if not address or not owner:
                    continue

                amt_clean = re.sub(r"[^\d.]", "", amount_raw)
                tax_amount = f"${float(amt_clean):,.0f} owed" if amt_clean else amount_raw

                lead = {
                    "id": lead_id({"address": address, "owner": owner}),
                    "type": "tax",
                    "owner": owner,
                    "address": address,
                    "amount": tax_amount,
                    "filing_date": "",
                    "sale_date": sale_date,
                    "source": f"hctax.net acct #{account}",
                    "lender": None,
                    "base_score": 30,
                    "signals": [],
                    "years_delinquent": None,
                    "mailing_state": None,
                    "prop_sqft": None,
                    "tax_to_value": None,
                    "occupancy": None,
                    "resolution_status": "active",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                score, tier = score_lead(lead)
                lead["score"] = score
                lead["tier"] = tier
                leads.append(lead)

            except Exception as e:
                logger.warning(f"Tax row error: {e}")

    except Exception as e:
        logger.error(f"Tax scrape failed: {e}")
    finally:
        await page.close()

    logger.info(f"Tax: {len(leads)} tax delinquent leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: PROBATE — Harris County District Clerk
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_probate_async(context, days_back=30):
    leads = []
    logger.info("Scraping probate filings from hcdistrictclerk.com...")
    page = await context.new_page()
    try:
        await page.goto("https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)

        start_date = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end_date = date.today().strftime("%m/%d/%Y")

        # Select case type PROB
        try:
            case_type = page.locator("select[id*='CaseType'], select[id*='caseType'], select[name*='CaseType']").first
            await case_type.select_option(label="Probate")
        except Exception:
            try:
                await case_type.select_option(value="PROB")
            except Exception:
                logger.warning("Could not select probate case type")

        # Fill dates
        try:
            from_field = page.locator("input[id*='FileDate'][id*='From'], input[id*='filedate'][id*='from']").first
            to_field   = page.locator("input[id*='FileDate'][id*='To'], input[id*='filedate'][id*='to']").first
            await from_field.fill(start_date)
            await to_field.fill(end_date)
        except Exception:
            logger.warning("Could not fill date fields for probate")

        # Search
        search_btn = page.locator("input[type='submit'], input[value*='Search'], button:has-text('Search')").first
        await search_btn.click()
        await page.wait_for_load_state("networkidle", timeout=20000)

        # Diagnostic logging
        page_title = await page.title()
        tables_count = await page.locator("table").count()
        all_text = await page.locator("body").inner_text()
        logger.info(f"PROBATE page: title='{page_title}' tables={tables_count}")
        logger.info(f"PROBATE body preview: {all_text[:300]}")

        rows = await page.locator("table tr").all()
        logger.info(f"Probate: Found {len(rows)} rows")

        for row in rows[1:]:
            cells = await row.locator("td").all()
            if len(cells) < 3:
                continue
            try:
                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]

                case_number = texts[0] if len(texts) > 0 else ""
                file_date   = texts[1] if len(texts) > 1 else ""
                case_style  = texts[2] if len(texts) > 2 else ""
                case_status = texts[3] if len(texts) > 3 else ""

                # Skip closed cases
                if re.search(r"\b(closed|dismissed|settled|disposed)\b", case_status, re.I):
                    continue

                if not case_style:
                    continue

                is_estate = bool(re.search(r"^(Estate of|In re)", case_style, re.I))
                is_heirs  = bool(re.search(r"Heirs of", case_style, re.I))
                if not (is_estate or is_heirs):
                    continue

                signals = []
                if is_estate:
                    signals.append("estate")
                if re.search(r"heirs|et al|multiple", case_style, re.I):
                    signals.append("multiheir")
                try:
                    fd = datetime.strptime(file_date, "%m/%d/%Y")
                    if (datetime.today() - fd).days <= 14:
                        signals.append("recent")
                except Exception:
                    pass

                lead = {
                    "id": lead_id({"address": case_number, "owner": case_style}),
                    "type": "probate",
                    "owner": case_style.strip(),
                    "address": "",
                    "amount": "Est. TBD",
                    "filing_date": file_date,
                    "sale_date": None,
                    "source": f"Dist. Clerk #{case_number}",
                    "lender": None,
                    "base_score": 35,
                    "signals": signals,
                    "heirs": "Multiple heirs" if "multiheir" in signals else "Unknown",
                    "mailing": "",
                    "resolution_status": "active",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                score, tier = score_lead(lead)
                lead["score"] = score
                lead["tier"] = tier
                leads.append(lead)

            except Exception as e:
                logger.warning(f"Probate row error: {e}")

    except Exception as e:
        logger.error(f"Probate scrape failed: {e}")
    finally:
        await page.close()

    logger.info(f"Probate: {len(leads)} probate leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-SIGNAL DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def apply_cross_signals(all_leads):
    tax_addrs     = {l["address"].lower() for l in all_leads if l["type"] == "tax"}
    fc_addrs      = {l["address"].lower() for l in all_leads if l["type"] == "foreclosure"}
    probate_addrs = {l["address"].lower() for l in all_leads if l["type"] == "probate"}

    for lead in all_leads:
        addr = lead["address"].lower()
        sigs = lead.get("signals", [])

        if lead["type"] == "probate":
            if addr in tax_addrs and "taxcombo" not in sigs:
                sigs.append("taxcombo")
            if addr in fc_addrs and "fccombo" not in sigs:
                sigs.append("fccombo")
        elif lead["type"] == "foreclosure":
            if addr in tax_addrs and "fc-tax" not in sigs:
                sigs.append("fc-tax")
            if addr in probate_addrs and "fc-probate" not in sigs:
                sigs.append("fc-probate")
        elif lead["type"] == "tax":
            if addr in fc_addrs and "tx-fc" not in sigs:
                sigs.append("tx-fc")
            if addr in probate_addrs and "tx-probate" not in sigs:
                sigs.append("tx-probate")

        lead["signals"] = sigs
        lead["score"], lead["tier"] = score_lead(lead)

    return all_leads


def deduplicate(leads):
    seen = {}
    for lead in leads:
        lid = lead["id"]
        if lid not in seen:
            seen[lid] = lead
    deduped = list(seen.values())
    return deduped, len(leads) - len(deduped)


def scrub_settled(leads):
    active, settled = [], []
    for lead in leads:
        result = check_resolved(lead)
        if result["resolved"]:
            lead["_resolution"] = result
            settled.append(lead)
        else:
            active.append(lead)
    return active, settled


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def run_full_scrape_async():
    logger.info("=" * 60)
    logger.info("Harris County Lead Scraper (Playwright) — starting")
    logger.info("=" * 60)

    start = datetime.utcnow()
    pw = browser = context = None

    try:
        pw, browser, context = await get_browser()

        fc_leads      = await scrape_foreclosures_async(context, days_back=45)
        tax_leads     = await scrape_tax_async(context)
        probate_leads = await scrape_probate_async(context, days_back=30)

        all_leads = fc_leads + tax_leads + probate_leads
        logger.info(f"Raw: FC={len(fc_leads)} Tax={len(tax_leads)} Probate={len(probate_leads)}")

        all_leads = apply_cross_signals(all_leads)
        all_leads, dupe_count = deduplicate(all_leads)
        active_leads, settled_leads = scrub_settled(all_leads)

    finally:
        if context: await context.close()
        if browser: await browser.close()
        if pw:      await pw.stop()

    elapsed = (datetime.utcnow() - start).total_seconds()

    return {
        "scraped_at":          start.isoformat(),
        "elapsed_seconds":     round(elapsed, 1),
        "raw_total":           len(fc_leads) + len(tax_leads) + len(probate_leads),
        "duplicates_removed":  dupe_count,
        "settled_removed":     len(settled_leads),
        "active_leads":        len(active_leads),
        "by_type": {
            "foreclosure": len([l for l in active_leads if l["type"] == "foreclosure"]),
            "tax":         len([l for l in active_leads if l["type"] == "tax"]),
            "probate":     len([l for l in active_leads if l["type"] == "probate"]),
        },
        "settled_log": [
            {"owner": l["owner"], "address": l["address"], "type": l["type"],
             "reason": l.get("_resolution", {}).get("reason", "Unknown"),
             "stamp":  l.get("_resolution", {}).get("stamp", "SETTLED")}
            for l in settled_leads
        ],
        "leads": active_leads,
    }


def run_full_scrape():
    """Synchronous wrapper for use from Flask."""
    return asyncio.run(run_full_scrape_async())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_full_scrape()
    print(f"\nActive leads: {result['active_leads']}")
    print(f"By type: {result['by_type']}")
