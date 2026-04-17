"""
Harris County Lead Scraper
Pulls pre-foreclosure, tax delinquent, and probate leads from public county sources.
Sources:
  - Pre-Foreclosure : https://www.cclerk.hctx.net (FRCL filings)
  - Tax Delinquent  : https://www.hctax.net/Property/listings/taxsalelisting
  - Probate         : https://www.hcdistrictclerk.com (public case search)
  - Property data   : https://pdata.hcad.org (owner, value, exemptions)
"""

import re
import time
import logging
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
# RESOLUTION STATUS DETECTION
# Strict allowlist: only 'active' stays. Anything else = settled + removed.
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


def check_resolved(lead: dict) -> dict:
    status = lead.get("resolution_status", "active")
    if status == "active" or not status:
        return {"resolved": False}
    info = RESOLUTION_LABELS.get(status)
    if info:
        label, stamp, chip = info
        return {"resolved": True, "reason": label, "stamp": stamp, "chip": chip}
    return {"resolved": True, "reason": f"Settled ({status})", "stamp": "SETTLED", "chip": "chip-closed"}


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL SCORING
# ─────────────────────────────────────────────────────────────────────────────

PROBATE_SIGNAL_POINTS = {
    "estate":    25, "outstate": 22, "multiheir": 18,
    "taxcombo":  30, "fccombo":  30, "recent":    15,
}

FC_SIGNAL_POINTS = {
    "equity":     28, "vacant":    25, "fc-tax":  30,
    "divorce":    22, "fc-probate": 25,
}

TAX_SIGNAL_POINTS = {
    "tx-fc":      32, "tx-vacant": 26, "tx-oos":   24,
    "tx-dist":    22, "tx-hival":  28, "tx-probate": 30,
    "tx-long":    25, "tx-rental": 18, "tx-small":  15,
}


def score_lead(lead: dict) -> tuple[int, int]:
    """Returns (score, tier)."""
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


def lead_id(lead: dict) -> str:
    """Stable unique ID based on address + owner."""
    key = f"{lead.get('address','')}{lead.get('owner','')}".lower().strip()
    return hashlib.md5(key.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# HCAD PROPERTY ENRICHMENT
# Fetches assessed value, owner mailing address, exemptions, sq footage
# ─────────────────────────────────────────────────────────────────────────────

HCAD_SEARCH = "https://public.hcad.org/records/details.asp"


def enrich_from_hcad(address: str) -> dict:
    """
    Queries HCAD public search for a property address.
    Returns enrichment dict with: assessed_value, mailing_state,
    sq_footage, homestead_exempt, owner_name
    """
    enrichment = {}
    try:
        params = {"crypt": "", "acct": "", "taxyear": str(date.today().year), "addr": address}
        resp = SESSION.get(HCAD_SEARCH, params=params, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Assessed value
        val_cell = soup.find(string=re.compile(r"Appraised Value", re.I))
        if val_cell:
            val_row = val_cell.find_parent("tr")
            if val_row:
                cells = val_row.find_all("td")
                if len(cells) >= 2:
                    raw = cells[-1].get_text(strip=True).replace("$", "").replace(",", "")
                    try:
                        enrichment["assessed_value"] = int(raw)
                    except ValueError:
                        pass

        # Mailing state (owner out-of-state detection)
        mail_cell = soup.find(string=re.compile(r"Mailing Address", re.I))
        if mail_cell:
            mail_row = mail_cell.find_parent("tr")
            if mail_row:
                text = mail_row.get_text(" ", strip=True)
                state_match = re.search(r"\b([A-Z]{2})\s+\d{5}", text)
                if state_match:
                    enrichment["mailing_state"] = state_match.group(1)

        # Building sq footage
        sqft_cell = soup.find(string=re.compile(r"Building Area|Sq Ft|Living Area", re.I))
        if sqft_cell:
            sqft_row = sqft_cell.find_parent("tr")
            if sqft_row:
                cells = sqft_row.find_all("td")
                if len(cells) >= 2:
                    raw = cells[-1].get_text(strip=True).replace(",", "")
                    try:
                        enrichment["prop_sqft"] = int(raw)
                    except ValueError:
                        pass

        # Homestead exemption (owner-occupied signal)
        hs = soup.find(string=re.compile(r"Homestead", re.I))
        enrichment["homestead_exempt"] = hs is not None

    except Exception as e:
        logger.warning(f"HCAD enrichment failed for '{address}': {e}")

    return enrichment


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: PRE-FORECLOSURE — Harris County Clerk FRCL Filings
# ─────────────────────────────────────────────────────────────────────────────

FRCL_BASE = "https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx"


def scrape_foreclosures(days_back: int = 45) -> list[dict]:
    """
    Scrapes FRCL (Foreclosure) filings from Harris County Clerk.
    Looks back `days_back` days (default 45 — notices typically filed 30-45 days before sale).
    """
    leads = []
    logger.info("Scraping pre-foreclosure filings from cclerk.hctx.net...")

    try:
        # Get the search form to capture ViewState / session tokens
        resp = SESSION.get(FRCL_BASE, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        viewstate     = soup.find("input", {"id": "__VIEWSTATE"})
        viewstategenerator = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
        eventvalidation    = soup.find("input", {"id": "__EVENTVALIDATION"})

        start_date = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end_date   = date.today().strftime("%m/%d/%Y")

        payload = {
            "__VIEWSTATE":          viewstate["value"] if viewstate else "",
            "__VIEWSTATEGENERATOR": viewstategenerator["value"] if viewstategenerator else "",
            "__EVENTVALIDATION":    eventvalidation["value"] if eventvalidation else "",
            "__EVENTTARGET":        "",
            "__EVENTARGUMENT":      "",
            "ctl00$ContentPlaceHolder1$txtFromDate": start_date,
            "ctl00$ContentPlaceHolder1$txtToDate":   end_date,
            "ctl00$ContentPlaceHolder1$btnSearch":   "Search",
        }

        resp2 = SESSION.post(FRCL_BASE, data=payload, timeout=30)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        # Results table
        table = soup2.find("table", {"id": re.compile(r"GridView|gv|results", re.I)})
        if not table:
            table = soup2.find("table", class_=re.compile(r"grid|result|data", re.I))

        if not table:
            logger.warning("FRCL: Could not find results table — page structure may have changed")
            return leads

        rows = table.find_all("tr")[1:]  # skip header
        logger.info(f"FRCL: Found {len(rows)} raw rows")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            try:
                texts = [c.get_text(strip=True) for c in cells]

                # Typical FRCL columns: File Date | Instrument # | Grantor | Address | Mortgagee | Original Amount
                filing_date  = texts[0] if len(texts) > 0 else ""
                instrument   = texts[1] if len(texts) > 1 else ""
                grantor      = texts[2] if len(texts) > 2 else ""
                address      = texts[3] if len(texts) > 3 else ""
                mortgagee    = texts[4] if len(texts) > 4 else ""
                amount_raw   = texts[5] if len(texts) > 5 else ""

                # Parse sale date from notice text if available (typically ~45 days out)
                sale_date = None
                notice_text = row.get_text(" ")
                sale_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", notice_text)
                if sale_match and sale_match.group(1) != filing_date:
                    sale_date = sale_match.group(1)

                if not address or not grantor:
                    continue

                # Skip if no valid Houston-area address
                if not re.search(r"\b(Houston|Katy|Spring|Cypress|Humble|Pasadena|Pearland|Kingwood|Conroe|Baytown|Channelview|Crosby|Huffman|Tomball)\b", address, re.I):
                    continue

                # Parse loan amount
                loan_amount = ""
                amt_clean = re.sub(r"[^\d.]", "", amount_raw)
                if amt_clean:
                    try:
                        loan_amount = f"${float(amt_clean):,.0f}"
                    except ValueError:
                        loan_amount = amount_raw

                lead = {
                    "id":               lead_id({"address": address, "owner": grantor}),
                    "type":             "foreclosure",
                    "owner":            grantor,
                    "address":          address,
                    "amount":           loan_amount,
                    "filing_date":      filing_date,
                    "sale_date":        sale_date or "",
                    "source":           instrument,
                    "lender":          mortgagee,
                    "base_score":       38,
                    "signals":          [],
                    "equity":           None,
                    "occupancy":        None,
                    "court_case":       None,
                    "resolution_status":"active",
                    "scraped_at":       datetime.utcnow().isoformat(),
                }

                # Enrich from HCAD
                time.sleep(0.5)
                enrichment = enrich_from_hcad(address)
                lead.update(enrichment)

                # Detect signals
                if lead.get("mailing_state") and lead["mailing_state"] != "TX":
                    pass  # FC doesn't use out-of-state as signal, but noted

                if enrichment.get("prop_sqft", 9999) < 1200:
                    lead["signals"].append("vacant")  # small + potentially vacant

                # Score
                score, tier = score_lead(lead)
                lead["score"] = score
                lead["tier"]  = tier

                leads.append(lead)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"FRCL row parse error: {e}")
                continue

    except Exception as e:
        logger.error(f"FRCL scrape failed: {e}")

    logger.info(f"FRCL: {len(leads)} foreclosure leads extracted")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: TAX DELINQUENT — hctax.net monthly sale listing
# ─────────────────────────────────────────────────────────────────────────────

TAX_SALE_URL = "https://www.hctax.net/Property/listings/taxsalelisting"


def scrape_tax_delinquent() -> list[dict]:
    """
    Scrapes Harris County Tax Office delinquent property sale listing.
    Published monthly before each first-Tuesday sale.
    """
    leads = []
    logger.info("Scraping tax delinquent list from hctax.net...")

    try:
        resp = SESSION.get(TAX_SALE_URL, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find sale date from page header
        sale_date = ""
        sale_header = soup.find(string=re.compile(r"Sale Date", re.I))
        if sale_header:
            sd_match = re.search(r"(\w+ \d+,?\s*\d{4})", sale_header.find_parent().get_text())
            if sd_match:
                sale_date = sd_match.group(1)

        # Results table
        table = soup.find("table")
        if not table:
            logger.warning("Tax sale: No table found on page")
            return leads

        rows = table.find_all("tr")[1:]
        logger.info(f"Tax sale: Found {len(rows)} raw rows")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            try:
                texts = [c.get_text(" ", strip=True) for c in cells]

                # Typical columns: Account # | Owner Name | Property Address | Amount | Legal Description
                account   = texts[0] if len(texts) > 0 else ""
                owner     = texts[1] if len(texts) > 1 else ""
                address   = texts[2] if len(texts) > 2 else ""
                amount_raw = texts[3] if len(texts) > 3 else ""

                if not address or not owner:
                    continue

                # Parse tax owed amount
                tax_amount = ""
                amt_clean = re.sub(r"[^\d.]", "", amount_raw)
                if amt_clean:
                    try:
                        tax_amount = f"${float(amt_clean):,.0f} owed"
                    except ValueError:
                        tax_amount = amount_raw

                lead = {
                    "id":               lead_id({"address": address, "owner": owner}),
                    "type":             "tax",
                    "owner":            owner,
                    "address":          address,
                    "amount":           tax_amount,
                    "filing_date":      "",   # will be filled by HCAD delinquency history
                    "sale_date":        sale_date,
                    "source":           f"hctax.net acct #{account}",
                    "base_score":       30,
                    "signals":          [],
                    "years_delinquent": None,
                    "mailing_state":    None,
                    "prop_sqft":        None,
                    "tax_to_value":     None,
                    "occupancy":        None,
                    "resolution_status":"active",
                    "scraped_at":       datetime.utcnow().isoformat(),
                }

                # Enrich from HCAD
                time.sleep(0.5)
                enrichment = enrich_from_hcad(address)
                lead.update(enrichment)

                # Compute tax-to-value ratio
                if lead.get("assessed_value") and amt_clean:
                    try:
                        ratio = (float(amt_clean) / lead["assessed_value"]) * 100
                        lead["tax_to_value"] = f"{ratio:.0f}%"
                        if ratio >= 20:
                            lead["signals"].append("tx-hival")
                    except Exception:
                        pass

                # Out-of-state owner
                if lead.get("mailing_state") and lead["mailing_state"] != "TX":
                    lead["signals"].append("tx-oos")

                # Non-owner-occupied (no homestead exemption)
                if not lead.get("homestead_exempt", True):
                    lead["signals"].append("tx-rental")

                # Small property
                if lead.get("prop_sqft") and lead["prop_sqft"] < 1100:
                    lead["signals"].append("tx-small")

                score, tier = score_lead(lead)
                lead["score"] = score
                lead["tier"]  = tier

                leads.append(lead)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"Tax sale row error: {e}")
                continue

    except Exception as e:
        logger.error(f"Tax sale scrape failed: {e}")

    logger.info(f"Tax sale: {len(leads)} tax delinquent leads extracted")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: PROBATE — Harris County District Clerk
# ─────────────────────────────────────────────────────────────────────────────

DISTRICT_CLERK_SEARCH = "https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx"


def scrape_probate(days_back: int = 30) -> list[dict]:
    """
    Scrapes probate case filings from Harris County District Clerk.
    Searches for case type PROB filed in last `days_back` days.
    """
    leads = []
    logger.info("Scraping probate filings from hcdistrictclerk.com...")

    try:
        resp = SESSION.get(DISTRICT_CLERK_SEARCH, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Capture ASP.NET form tokens
        viewstate  = soup.find("input", {"id": "__VIEWSTATE"})
        eventval   = soup.find("input", {"id": "__EVENTVALIDATION"})
        vstateggen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

        start_date = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end_date   = date.today().strftime("%m/%d/%Y")

        payload = {
            "__VIEWSTATE":          viewstate["value"] if viewstate else "",
            "__EVENTVALIDATION":    eventval["value"] if eventval else "",
            "__VIEWSTATEGENERATOR": vstateggen["value"] if vstateggen else "",
            "__EVENTTARGET":        "",
            "__EVENTARGUMENT":      "",
            # Case type field — "PROB" targets probate cases
            "ctl00$ContentPlaceHolder1$ddlCaseType": "PROB",
            "ctl00$ContentPlaceHolder1$txtFileDateFrom": start_date,
            "ctl00$ContentPlaceHolder1$txtFileDateTo":   end_date,
            "ctl00$ContentPlaceHolder1$btnSearch":       "Search",
        }

        resp2 = SESSION.post(DISTRICT_CLERK_SEARCH, data=payload, timeout=30)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        table = soup2.find("table", {"id": re.compile(r"Grid|grid|result", re.I)})
        if not table:
            table = soup2.find("table")

        if not table:
            logger.warning("Probate: No results table found")
            return leads

        rows = table.find_all("tr")[1:]
        logger.info(f"Probate: Found {len(rows)} raw rows")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            try:
                texts = [c.get_text(" ", strip=True) for c in cells]

                case_number  = texts[0] if len(texts) > 0 else ""
                file_date    = texts[1] if len(texts) > 1 else ""
                case_style   = texts[2] if len(texts) > 2 else ""  # "Estate of John Doe"
                case_status  = texts[3] if len(texts) > 3 else ""
                attorney     = texts[4] if len(texts) > 4 else ""

                # Skip closed/settled cases immediately
                if re.search(r"\b(closed|dismissed|settled|disposed)\b", case_status, re.I):
                    continue

                if not case_style:
                    continue

                # Detect Estate of / Heirs of
                is_estate   = bool(re.search(r"^(Estate of|In re|In the Matter of)", case_style, re.I))
                is_heirs    = bool(re.search(r"Heirs of", case_style, re.I))
                if not (is_estate or is_heirs):
                    continue

                # Parse decedent name for display
                owner_display = case_style.strip()

                # Detect multiple heirs
                multi_heir = bool(re.search(r"heirs|et al|multiple", case_style, re.I))

                signals = []
                if is_estate:
                    signals.append("estate")
                if multi_heir:
                    signals.append("multiheir")

                # Filing recency
                try:
                    fd = datetime.strptime(file_date, "%m/%d/%Y")
                    days_old = (datetime.today() - fd).days
                    if days_old <= 14:
                        signals.append("recent")
                except Exception:
                    pass

                lead = {
                    "id":               lead_id({"address": case_number, "owner": owner_display}),
                    "type":             "probate",
                    "owner":            owner_display,
                    "address":          "",   # enriched below via HCAD search by owner
                    "amount":           "Est. TBD",
                    "filing_date":      file_date,
                    "sale_date":        None,
                    "source":           f"Dist. Clerk #{case_number}",
                    "base_score":       35,
                    "signals":          signals,
                    "heirs":            "Multiple heirs" if multi_heir else "Unknown",
                    "mailing":          "",
                    "resolution_status":"active",
                    "case_status":      case_status,
                    "scraped_at":       datetime.utcnow().isoformat(),
                }

                score, tier = score_lead(lead)
                lead["score"] = score
                lead["tier"]  = tier

                leads.append(lead)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"Probate row error: {e}")
                continue

    except Exception as e:
        logger.error(f"Probate scrape failed: {e}")

    logger.info(f"Probate: {len(leads)} probate leads extracted")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-SIGNAL DETECTION
# Boosts leads that appear in multiple source lists
# ─────────────────────────────────────────────────────────────────────────────

def apply_cross_signals(all_leads: list[dict]) -> list[dict]:
    """
    After all sources are scraped, detect leads that appear in multiple lists
    and add cross-source signals (which carry the highest point values).
    Then rescore all leads.
    """
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


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPLICATION + SETTLED SCRUB
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(leads: list[dict]) -> tuple[list[dict], int]:
    seen = {}
    for lead in leads:
        lid = lead["id"]
        if lid not in seen:
            seen[lid] = lead
    deduped = list(seen.values())
    removed = len(leads) - len(deduped)
    return deduped, removed


def scrub_settled(leads: list[dict]) -> tuple[list[dict], list[dict]]:
    """Remove any lead with a non-active resolution_status. Strict allowlist."""
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

def run_full_scrape() -> dict:
    """
    Runs the complete scrape pipeline:
    1. Scrape all three sources
    2. Apply cross-signals
    3. Deduplicate
    4. Scrub settled cases (strict allowlist — only 'active' survives)
    5. Return results summary
    """
    logger.info("=" * 60)
    logger.info("Harris County Lead Scraper — full scrape starting")
    logger.info("=" * 60)

    start = datetime.utcnow()
    all_leads = []

    fc_leads      = scrape_foreclosures(days_back=45)
    tax_leads     = scrape_tax_delinquent()
    probate_leads = scrape_probate(days_back=30)

    all_leads = fc_leads + tax_leads + probate_leads
    logger.info(f"Raw totals — FC: {len(fc_leads)} | Tax: {len(tax_leads)} | Probate: {len(probate_leads)}")

    all_leads = apply_cross_signals(all_leads)
    all_leads, dupe_count = deduplicate(all_leads)
    active_leads, settled_leads = scrub_settled(all_leads)

    elapsed = (datetime.utcnow() - start).total_seconds()

    summary = {
        "scraped_at":     start.isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "raw_total":      len(fc_leads) + len(tax_leads) + len(probate_leads),
        "duplicates_removed": dupe_count,
        "settled_removed":  len(settled_leads),
        "active_leads":     len(active_leads),
        "by_type": {
            "foreclosure": len([l for l in active_leads if l["type"] == "foreclosure"]),
            "tax":         len([l for l in active_leads if l["type"] == "tax"]),
            "probate":     len([l for l in active_leads if l["type"] == "probate"]),
        },
        "settled_log": [
            {
                "owner":   l["owner"],
                "address": l["address"],
                "type":    l["type"],
                "reason":  l.get("_resolution", {}).get("reason", "Unknown"),
                "stamp":   l.get("_resolution", {}).get("stamp", "SETTLED"),
            }
            for l in settled_leads
        ],
        "leads": active_leads,
    }

    logger.info(f"Scrape complete: {len(active_leads)} active leads | {len(settled_leads)} settled removed | {elapsed:.1f}s")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_full_scrape()
    print(f"\nActive leads: {result['active_leads']}")
    print(f"Settled removed: {result['settled_removed']}")
    print(f"By type: {result['by_type']}")
