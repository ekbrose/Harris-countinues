"""
Harris County Lead Scraper v5
Fixes based on exact field names and page structure from diagnostic logs.
"""

import re
import logging
import hashlib
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─────────────────────────────────────────────────────────────────────────────
# SCORING & RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

RESOLUTION_LABELS = {
    "deed_transfer":    ("Deed transferred to new buyer",        "SOLD",       "chip-sold"),
    "loan_reinstated":  ("Loan reinstated / modification filed", "REINSTATED", "chip-reinstated"),
    "notice_rescinded": ("Foreclosure notice rescinded",         "WITHDRAWN",  "chip-withdrawn"),
    "taxes_paid":       ("Tax balance paid — account current",   "PAID",       "chip-paid"),
    "payment_plan":     ("Payment agreement entered",            "AGREEMENT",  "chip-reinstated"),
    "estate_sold":      ("Property sold by estate or heirs",     "SOLD",       "chip-sold"),
    "case_closed":      ("Probate case closed / settled",        "CLOSED",     "chip-closed"),
}

PROBATE_PTS = {"estate":25,"outstate":22,"multiheir":18,"taxcombo":30,"fccombo":30,"recent":15}
FC_PTS      = {"equity":28,"vacant":25,"fc-tax":30,"divorce":22,"fc-probate":25}
TAX_PTS     = {"tx-fc":32,"tx-vacant":26,"tx-oos":24,"tx-dist":22,"tx-hival":28,
               "tx-probate":30,"tx-long":25,"tx-rental":18,"tx-small":15}

def check_resolved(lead):
    s = lead.get("resolution_status", "active")
    if not s or s == "active":
        return {"resolved": False}
    info = RESOLUTION_LABELS.get(s)
    if info:
        label, stamp, chip = info
        return {"resolved": True, "reason": label, "stamp": stamp, "chip": chip}
    return {"resolved": True, "reason": f"Settled ({s})", "stamp": "SETTLED", "chip": "chip-closed"}

def score_lead(lead):
    t = lead.get("type", "")
    pts = lead.get("base_score", 40)
    sigs = lead.get("signals", [])
    pts += sum((FC_PTS if t=="foreclosure" else TAX_PTS if t=="tax" else PROBATE_PTS).get(s, 0) for s in sigs)
    score = min(100, pts)
    tier = 1 if score>=90 else 2 if score>=75 else 3 if score>=60 else 4 if score>=45 else 5
    return score, tier

def lead_id(address, owner):
    key = f"{address}{owner}".lower().strip()
    return hashlib.md5(key.encode()).hexdigest()[:16]

def make_lead(type_, owner, address, amount, filing_date, sale_date, source, base_score=38, signals=None, **kw):
    lead = {
        "id": lead_id(address, owner),
        "type": type_, "owner": owner, "address": address,
        "amount": amount, "filing_date": filing_date, "sale_date": sale_date,
        "source": source, "lender": kw.get("lender"),
        "base_score": base_score, "signals": signals or [],
        "equity": kw.get("equity"), "occupancy": kw.get("occupancy"),
        "court_case": kw.get("court_case"),
        "years_delinquent": kw.get("years_delinquent"),
        "mailing_state": kw.get("mailing_state"),
        "prop_sqft": kw.get("prop_sqft"),
        "tax_to_value": kw.get("tax_to_value"),
        "heirs": kw.get("heirs"), "mailing": kw.get("mailing"),
        "assessed_value": kw.get("assessed_value"),
        "homestead_exempt": kw.get("homestead_exempt"),
        "resolution_status": "active",
        "scraped_at": datetime.utcnow().isoformat(),
    }
    lead["score"], lead["tier"] = score_lead(lead)
    return lead

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — FORECLOSURE via Harris County Clerk PDF listing
# The web form requires JS. Instead we fetch the public FRCL filing index
# directly from the county's bulk data endpoint.
# ─────────────────────────────────────────────────────────────────────────────

def scrape_foreclosures(days_back=45):
    leads = []
    logger.info("FRCL: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # The county clerk posts monthly foreclosure listings as a public page
        # We can search by filing type FR (Foreclosure) via the public records search
        start = (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        end   = date.today().strftime("%Y-%m-%d")

        # Try the county clerk's newer REST-style search
        search_url = "https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx"
        session.headers.update({"Referer": search_url})

        # First GET to capture ASP.NET tokens
        r0 = session.get(search_url, timeout=20)
        soup0 = BeautifulSoup(r0.text, "html.parser")

        def getval(name):
            el = soup0.find("input", {"name": name})
            return el["value"] if el and el.get("value") else ""

        # The real date fields are rendered by JavaScript — not in the raw HTML.
        # Fall back to HCAD's public foreclosure data which IS accessible via requests.
        # HCAD pdata has a real property search we can use.
        logger.info("FRCL: county clerk form requires JS. Trying HCAD public records search...")

        # Search HCAD for recent deed-of-trust / foreclosure related filings
        hcad_url = "https://public.hcad.org/records/details.asp"

        # Instead, use the Harris County Appraisal District's public data download
        # which lists all properties with recent status changes
        # pdata.hcad.org has bulk files we can fetch directly
        pdata_url = "https://pdata.hcad.org/download/2026.html"
        r1 = session.get(pdata_url, timeout=20)
        logger.info(f"FRCL/HCAD pdata: status={r1.status_code} len={len(r1.text)}")

        # Log what we got
        soup1 = BeautifulSoup(r1.text, "html.parser")
        links = soup1.find_all("a", href=True)
        logger.info(f"FRCL/HCAD: found {len(links)} links")
        for lnk in links[:10]:
            logger.info(f"  link: {lnk['href']} | {lnk.get_text(strip=True)}")

    except Exception as e:
        logger.error(f"FRCL failed: {e}")

    logger.info(f"FRCL: {len(leads)} leads (form requires JS — see TAX source for live data)")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — TAX DELINQUENT
# The listing page shows vertical property cards, not a table.
# Each property is a block of label:value pairs.
# Format confirmed from logs:
#   Sale#: 1 | Type: SALE | Cause#: 201835374 | Judgment: 07/02/2025
#   Tax Years in Judgement: 2015-2024 | etc.
# ─────────────────────────────────────────────────────────────────────────────

def scrape_tax():
    leads = []
    logger.info("TAX: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        url = "https://www.hctax.net/Property/listings/taxsalelisting"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        logger.info(f"TAX: status={resp.status_code} len={len(resp.text)}")

        # Get sale date from page header
        sale_date = ""
        for tag in soup.find_all(["h1","h2","h3","strong","b"]):
            text = tag.get_text(" ", strip=True)
            if re.search(r"sale date|auction", text, re.I):
                dm = re.search(r"(\w+ \d+,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4})", text)
                if dm:
                    sale_date = dm.group(1)
                    logger.info(f"TAX: sale date='{sale_date}'")
                    break

        # The page uses vertical label:value format per property block
        # Find all table rows and parse label:value pairs into property dicts
        all_tables = soup.find_all("table")
        logger.info(f"TAX: {len(all_tables)} tables")

        # Collect all text in label:value pairs across the page
        # Each property is a group of rows with labels in col 0, values in col 1
        current_prop = {}
        properties = []

        # First: log every unique label seen on the page
        all_labels_seen = set()
        for table in all_tables:
            for row in table.find_all("tr"):
                cells = [td.get_text(" ", strip=True) for td in row.find_all(["td","th"])]
                if len(cells) >= 2:
                    label = cells[0].strip().rstrip(":").strip()
                    if label and label not in all_labels_seen:
                        all_labels_seen.add(label)
                        logger.info(f"TAX label seen: '{label}' = '{cells[1].strip()[:60]}'")

        # Reset and parse properly
        for table in all_tables:
            for row in table.find_all("tr"):
                cells = [td.get_text(" ", strip=True) for td in row.find_all(["td","th"])]
                if len(cells) < 2:
                    continue

                label = cells[0].strip().rstrip(":").strip()
                value = cells[1].strip() if len(cells) > 1 else ""

                if label == "Sale#":
                    # New property block starting
                    if current_prop and len(current_prop) > 2:
                        properties.append(current_prop)
                    current_prop = {"Sale#": value}
                elif current_prop is not None and label:
                    current_prop[label] = value

        # Don't forget the last property
        if current_prop and current_prop.get("Address"):
            properties.append(current_prop)

        logger.info(f"TAX: parsed {len(properties)} property blocks")
        if properties:
            # Log ALL fields in first property to find owner/address labels
            for k, v in properties[0].items():
                logger.info(f"TAX prop field: '{k}' = '{v}'")

        for prop in properties:
            try:
                owner   = prop.get("Owner Name", prop.get("Owner", "")).strip()
                address = prop.get("Property Address", prop.get("Address", "")).strip()
                cause   = prop.get("Cause#", prop.get("Cause Number", "")).strip()
                years   = prop.get("Tax Years in Judgement", prop.get("Tax Years", "")).strip()
                judg_dt = prop.get("Judgment", prop.get("Judgment Date", "")).strip()
                amt     = prop.get("Amount", prop.get("Judgment Amount", prop.get("Total Due", ""))).strip()

                if not owner or not address or len(address) < 5:
                    continue

                # Estimate years delinquent from tax year range
                years_delinquent = None
                yr_match = re.search(r"(\d{4})\s*[-–]\s*(\d{4})", years)
                if yr_match:
                    years_delinquent = int(yr_match.group(2)) - int(yr_match.group(1)) + 1

                signals = []
                if years_delinquent and years_delinquent >= 3:
                    signals.append("tx-long")

                amt_clean = re.sub(r"[^\d.]", "", amt)
                tax_amt = f"${float(amt_clean):,.0f} owed" if amt_clean else "Amount TBD"

                leads.append(make_lead(
                    "tax", owner, address, tax_amt,
                    judg_dt, sale_date,
                    f"hctax.net Cause#{cause}",
                    base_score=30, signals=signals,
                    years_delinquent=years_delinquent
                ))
            except Exception as e:
                logger.warning(f"TAX prop error: {e}")

        # If vertical parsing got nothing, try flat table approach
        if not leads:
            logger.info("TAX: vertical parse got 0 — trying flat table approach")
            for table in all_tables:
                rows = table.find_all("tr")
                if len(rows) < 3:
                    continue
                header = [th.get_text(strip=True) for th in rows[0].find_all(["th","td"])]
                logger.info(f"TAX flat header: {header}")
                for row in rows[1:6]:
                    cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
                    logger.info(f"TAX flat row: {cells}")

    except Exception as e:
        logger.error(f"TAX failed: {e}")

    logger.info(f"TAX: {len(leads)} leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — PROBATE via Harris County District Clerk
# From diagnostic logs, the form uses:
#   - Docket type: codeDescDropdownlist (select[9])
#   - Date from: monthFromDropdownlist, dayFromDropdownlist, yearFromDropdownlist
#   - Date to:   monthToDropdownlist, dayToDropdownlist, yearToDropdownlist
#   - CAPTCHA: txtDocCaptchaText — must solve or skip this search type
# Strategy: Search by party name "Estate of" instead — no captcha needed
# ─────────────────────────────────────────────────────────────────────────────

def scrape_probate(days_back=30):
    leads = []
    logger.info("PROBATE: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        url = "https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        logger.info(f"PROBATE: page loaded, status={resp.status_code}")

        def getval(name):
            el = soup.find("input", {"name": name}) or soup.find("input", {"id": name})
            return el["value"] if el and el.get("value") else ""

        # Build POST payload using party name search for "Estate of"
        # This searches the Plaintiff/Party name field — no captcha required
        payload = {
            "__EVENTTARGET":    "",
            "__EVENTARGUMENT":  "",
            "__VIEWSTATE":      getval("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": getval("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": getval("__EVENTVALIDATION"),
            # Party name search
            "txtPartyName":     "Estate of",
            # Party search type: starts with
            "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder2$ContentPlaceHolder2$ddlPartyNameSearchType": "Starts With",
            # Court type: Probate courts
            "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder2$ContentPlaceHolder2$ddlCourtType": "",
            # Active cases only
            "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder2$ContentPlaceHolder2$ddlPartyCaseStatusID": "ACTIVE  - CIVIL",
        }

        # Date dropdowns — from 30 days ago to today
        start_dt = date.today() - timedelta(days=days_back)
        end_dt   = date.today()

        prefix = "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder2$ContentPlaceHolder2$"
        payload[f"{prefix}monthFromDropdownlist"] = start_dt.strftime("%B")   # "March"
        payload[f"{prefix}dayFromDropdownlist"]   = str(start_dt.day)          # "22"
        payload[f"{prefix}yearFromDropdownlist"]  = str(start_dt.year)         # "2026"
        payload[f"{prefix}monthToDropdownlist"]   = end_dt.strftime("%B")
        payload[f"{prefix}dayToDropdownlist"]     = str(end_dt.day)
        payload[f"{prefix}yearToDropdownlist"]    = str(end_dt.year)

        # Find and add the search button
        btn = soup.find("input", {"type": "submit"})
        if btn:
            payload[btn.get("name","")] = btn.get("value","Search")
            logger.info(f"PROBATE: submit button name='{btn.get('name')}' value='{btn.get('value')}'")

        logger.info(f"PROBATE: POSTing party name search for 'Estate of'...")
        resp2 = session.post(url, data=payload, timeout=30, allow_redirects=True)
        logger.info(f"PROBATE: response status={resp2.status_code} len={len(resp2.text)} url={resp2.url}")
        logger.info(f"PROBATE: response preview: {resp2.text[:300]}")
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        # Find results
        tables = soup2.find_all("table")
        logger.info(f"PROBATE: {len(tables)} tables in response")

        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None) if tables else None
        if not best:
            logger.warning("PROBATE: no results table")
            return leads

        rows = best.find_all("tr")
        logger.info(f"PROBATE: {len(rows)} rows")
        for i, row in enumerate(rows[:4]):
            cells = [td.get_text(" ", strip=True) for td in row.find_all(["td","th"])]
            logger.info(f"PROBATE row[{i}]: {cells}")

        for row in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            try:
                case_num    = cells[0].strip()
                file_date   = cells[1].strip() if len(cells) > 1 else ""
                case_style  = cells[2].strip() if len(cells) > 2 else ""
                case_status = cells[3].strip() if len(cells) > 3 else ""

                if re.search(r"closed|dismissed|settled|disposed", case_status, re.I):
                    continue
                if not re.search(r"Estate of|Heirs of|In re", case_style, re.I):
                    continue

                signals = ["estate"]
                if re.search(r"heirs|et al", case_style, re.I):
                    signals.append("multiheir")
                try:
                    fd = datetime.strptime(file_date, "%m/%d/%Y")
                    if (datetime.today() - fd).days <= 14:
                        signals.append("recent")
                except Exception:
                    pass

                leads.append(make_lead(
                    "probate", case_style.strip(), "",
                    "Est. TBD", file_date, None,
                    f"Dist. Clerk #{case_num}",
                    base_score=35, signals=signals,
                    heirs="Multiple" if "multiheir" in signals else "Unknown"
                ))
            except Exception as e:
                logger.warning(f"PROBATE row error: {e}")

    except Exception as e:
        logger.error(f"PROBATE failed: {e}")

    logger.info(f"PROBATE: {len(leads)} leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-SIGNALS + DEDUP + SCRUB
# ─────────────────────────────────────────────────────────────────────────────

def apply_cross_signals(leads):
    tax_a = {l["address"].lower() for l in leads if l["type"] == "tax"}
    fc_a  = {l["address"].lower() for l in leads if l["type"] == "foreclosure"}
    pb_a  = {l["address"].lower() for l in leads if l["type"] == "probate"}
    for l in leads:
        a = l["address"].lower()
        s = l.get("signals", [])
        if l["type"] == "probate":
            if a in tax_a and "taxcombo" not in s: s.append("taxcombo")
            if a in fc_a  and "fccombo"  not in s: s.append("fccombo")
        elif l["type"] == "foreclosure":
            if a in tax_a and "fc-tax"     not in s: s.append("fc-tax")
            if a in pb_a  and "fc-probate" not in s: s.append("fc-probate")
        elif l["type"] == "tax":
            if a in fc_a and "tx-fc"      not in s: s.append("tx-fc")
            if a in pb_a and "tx-probate" not in s: s.append("tx-probate")
        l["signals"] = s
        l["score"], l["tier"] = score_lead(l)
    return leads

def deduplicate(leads):
    seen = {}
    for l in leads:
        if l["id"] not in seen:
            seen[l["id"]] = l
    return list(seen.values()), len(leads) - len(seen)

def scrub_settled(leads):
    active, settled = [], []
    for l in leads:
        r = check_resolved(l)
        if r["resolved"]:
            l["_resolution"] = r
            settled.append(l)
        else:
            active.append(l)
    return active, settled


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_full_scrape():
    logger.info("=" * 60)
    logger.info("Harris County Scraper v5 starting")
    logger.info("=" * 60)
    start = datetime.utcnow()

    fc_leads      = scrape_foreclosures(days_back=45)
    tax_leads     = scrape_tax()
    probate_leads = scrape_probate(days_back=30)

    all_leads = fc_leads + tax_leads + probate_leads
    logger.info(f"Raw: FC={len(fc_leads)} Tax={len(tax_leads)} Probate={len(probate_leads)}")

    all_leads = apply_cross_signals(all_leads)
    all_leads, dupes = deduplicate(all_leads)
    active, settled  = scrub_settled(all_leads)
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"Done: {len(active)} active leads in {elapsed:.0f}s")

    return {
        "scraped_at": start.isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "raw_total": len(fc_leads) + len(tax_leads) + len(probate_leads),
        "duplicates_removed": dupes,
        "settled_removed": len(settled),
        "active_leads": len(active),
        "by_type": {
            "foreclosure": len([l for l in active if l["type"] == "foreclosure"]),
            "tax":         len([l for l in active if l["type"] == "tax"]),
            "probate":     len([l for l in active if l["type"] == "probate"]),
        },
        "settled_log": [
            {"owner": l["owner"], "address": l["address"], "type": l["type"],
             "reason": l.get("_resolution", {}).get("reason", ""),
             "stamp":  l.get("_resolution", {}).get("stamp", "")}
            for l in settled
        ],
        "leads": active,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_full_scrape()
    print(f"\nActive: {result['active_leads']} | By type: {result['by_type']}")
