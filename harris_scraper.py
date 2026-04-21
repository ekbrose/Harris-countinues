"""
Harris County Lead Scraper — v4
- FRCL: Uses hctax.net tax sale listing (already has FC notices embedded)
- TAX:  Parses hctax.net delinquent property listing directly  
- PROBATE: Uses Harris County District Clerk public search
"""

import re
import logging
import asyncio
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

def make_lead(type_, owner, address, amount, filing_date, sale_date, source, base_score=38, signals=None, **kwargs):
    lead = {
        "id": lead_id(address, owner),
        "type": type_,
        "owner": owner,
        "address": address,
        "amount": amount,
        "filing_date": filing_date,
        "sale_date": sale_date,
        "source": source,
        "lender": kwargs.get("lender"),
        "base_score": base_score,
        "signals": signals or [],
        "equity": kwargs.get("equity"),
        "occupancy": kwargs.get("occupancy"),
        "court_case": kwargs.get("court_case"),
        "years_delinquent": kwargs.get("years_delinquent"),
        "mailing_state": kwargs.get("mailing_state"),
        "prop_sqft": kwargs.get("prop_sqft"),
        "tax_to_value": kwargs.get("tax_to_value"),
        "heirs": kwargs.get("heirs"),
        "mailing": kwargs.get("mailing"),
        "assessed_value": kwargs.get("assessed_value"),
        "homestead_exempt": kwargs.get("homestead_exempt"),
        "resolution_status": "active",
        "scraped_at": datetime.utcnow().isoformat(),
    }
    lead["score"], lead["tier"] = score_lead(lead)
    return lead

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — PRE-FORECLOSURE
# Scrapes Harris County Clerk foreclosure notice search using requests + BS4
# The page uses ASP.NET webforms — we POST directly with the form tokens
# ─────────────────────────────────────────────────────────────────────────────

def scrape_foreclosures(days_back=45):
    leads = []
    logger.info("FRCL: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        url = "https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract ASP.NET form tokens
        def val(id_):
            el = soup.find("input", {"id": id_}) or soup.find("input", {"name": id_})
            return el["value"] if el and el.get("value") else ""

        start = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end   = date.today().strftime("%m/%d/%Y")

        # Find the actual field names for from/to dates
        all_inputs = soup.find_all("input")
        logger.info(f"FRCL: found {len(all_inputs)} inputs on page")
        for inp in all_inputs:
            logger.info(f"FRCL input: id='{inp.get('id','')}' name='{inp.get('name','')}' type='{inp.get('type','')}'")

        # Find date inputs — look for text inputs that aren't hidden
        text_inputs = [i for i in all_inputs if i.get("type","text") == "text"]
        logger.info(f"FRCL: {len(text_inputs)} text inputs")

        # Build POST payload
        payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": val("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
            "__VIEWSTATEENCRYPTED": val("__VIEWSTATEENCRYPTED"),
            "__EVENTVALIDATION": val("__EVENTVALIDATION"),
        }

        # Add date fields using whatever names the form has
        if len(text_inputs) >= 2:
            payload[text_inputs[0].get("name", "fromDate")] = start
            payload[text_inputs[1].get("name", "toDate")]   = end

        # Find submit button name/value
        submit = soup.find("input", {"type": "submit"}) or soup.find("input", {"type": "button"})
        if submit:
            payload[submit.get("name", "btnSearch")] = submit.get("value", "Search")
            logger.info(f"FRCL submit: name='{submit.get('name')}' value='{submit.get('value')}'")

        logger.info(f"FRCL: POSTing with dates {start} to {end}")
        resp2 = session.post(url, data=payload, timeout=30)
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        # Find results table — largest table on page
        tables = soup2.find_all("table")
        logger.info(f"FRCL: {len(tables)} tables in response")

        best_table = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
        if not best_table:
            logger.warning("FRCL: no tables found in response")
            return leads

        rows = best_table.find_all("tr")[1:]  # skip header
        logger.info(f"FRCL: {len(rows)} data rows")
        if rows:
            sample = [td.get_text(strip=True) for td in rows[0].find_all("td")]
            logger.info(f"FRCL sample row: {sample}")

        for row in rows:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 3:
                continue
            try:
                filing_date = cells[0]
                instrument  = cells[1] if len(cells) > 1 else ""
                grantor     = cells[2] if len(cells) > 2 else ""
                address     = cells[3] if len(cells) > 3 else ""
                mortgagee   = cells[4] if len(cells) > 4 else ""
                amount_raw  = cells[5] if len(cells) > 5 else ""

                if not address or not grantor or len(address) < 5:
                    continue
                # Skip header-like rows
                if re.search(r"filing date|grantor|instrument", grantor, re.I):
                    continue

                amt = re.sub(r"[^\d.]", "", amount_raw)
                amount = f"${float(amt):,.0f}" if amt else "TBD"

                try:
                    fd = datetime.strptime(filing_date, "%m/%d/%Y")
                    sale_date = (fd + timedelta(days=45)).strftime("%m/%d/%Y")
                except Exception:
                    sale_date = ""

                leads.append(make_lead(
                    "foreclosure", grantor, address, amount,
                    filing_date, sale_date, instrument,
                    base_score=38, lender=mortgagee
                ))
            except Exception as e:
                logger.warning(f"FRCL row error: {e}")

    except Exception as e:
        logger.error(f"FRCL failed: {e}")

    logger.info(f"FRCL: {len(leads)} leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — TAX DELINQUENT
# Scrapes hctax.net tax sale listing with requests + BS4
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

        logger.info(f"TAX: page title='{soup.title.string if soup.title else 'none'}'")

        # Get sale date
        sale_date = ""
        for tag in soup.find_all(["h1","h2","h3","p","span"]):
            text = tag.get_text(strip=True)
            dm = re.search(r"Sale Date[:\s]+([A-Za-z]+ \d+,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4})", text, re.I)
            if dm:
                sale_date = dm.group(1)
                logger.info(f"TAX: sale date = '{sale_date}'")
                break

        # Find all tables
        tables = soup.find_all("table")
        logger.info(f"TAX: {len(tables)} tables found")

        # Find the largest table (the property listing)
        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
        if not best:
            logger.warning("TAX: no table found")
            return leads

        rows = best.find_all("tr")
        logger.info(f"TAX: {len(rows)} total rows in best table")

        # Log header to understand columns
        if rows:
            header = [th.get_text(strip=True) for th in rows[0].find_all(["th","td"])]
            logger.info(f"TAX header: {header}")

        # Log first 3 data rows
        for i, row in enumerate(rows[1:4]):
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            logger.info(f"TAX row[{i}]: {cells}")

        for row in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            try:
                # Use header to map columns if available
                # Common columns: Account#, Owner Name, Property Address, Amount Due
                # Try to detect by content
                account = owner = address = amount_raw = ""

                for j, cell in enumerate(cells):
                    cell = cell.strip()
                    if not cell:
                        continue
                    # Account number: all digits
                    if re.match(r"^\d{6,}$", cell):
                        account = cell
                    # Address: contains a number followed by street name + TX
                    elif re.search(r"^\d+\s+\w+.*(TX|Houston|Katy|Spring|Cypress|Humble|Pasadena)", cell, re.I):
                        address = cell
                    # Amount: dollar amount
                    elif re.match(r"^\$?[\d,]+\.?\d*$", cell) and float(re.sub(r"[^\d.]","",cell) or 0) > 100:
                        amount_raw = cell
                    # Owner: text with letters, not a date, not already assigned
                    elif re.search(r"[A-Za-z]{2,}", cell) and not re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", cell) and not owner:
                        owner = cell

                if not owner or not address:
                    continue

                amt = re.sub(r"[^\d.]", "", amount_raw)
                tax_amt = f"${float(amt):,.0f} owed" if amt else "Amount TBD"

                signals = []
                # Out of state: mailing address check would go here in production
                # High value ratio signal added when we have assessed value

                leads.append(make_lead(
                    "tax", owner, address, tax_amt,
                    "", sale_date,
                    f"hctax.net #{account}",
                    base_score=30, signals=signals
                ))
            except Exception as e:
                logger.warning(f"TAX row error: {e}")

    except Exception as e:
        logger.error(f"TAX failed: {e}")

    logger.info(f"TAX: {len(leads)} leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — PROBATE
# Uses requests + BS4 to POST the district clerk search form
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

        logger.info(f"PROBATE: page title='{soup.title.string if soup.title else 'none'}'")

        # Log all inputs and selects
        all_inputs = soup.find_all("input")
        all_selects = soup.find_all("select")
        logger.info(f"PROBATE: {len(all_inputs)} inputs, {len(all_selects)} selects")

        for sel in all_selects:
            opts = [o.get_text(strip=True) for o in sel.find_all("option")[:8]]
            logger.info(f"PROBATE select id='{sel.get('id','')}' name='{sel.get('name','')}' opts={opts}")

        for inp in all_inputs[:15]:
            logger.info(f"PROBATE input: id='{inp.get('id','')}' name='{inp.get('name','')}' type='{inp.get('type','')}'")

        # Build POST payload
        def val(id_):
            el = soup.find("input", {"id": id_}) or soup.find("input", {"name": id_})
            return el["value"] if el and el.get("value") else ""

        payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": val("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": val("__EVENTVALIDATION"),
        }

        start = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end   = date.today().strftime("%m/%d/%Y")

        # Find case type select and set to probate
        for sel in all_selects:
            opts = sel.find_all("option")
            for opt in opts:
                if re.search(r"prob", opt.get_text(), re.I):
                    payload[sel.get("name", "")] = opt.get("value", opt.get_text())
                    logger.info(f"PROBATE: selecting case type '{opt.get_text()}'")
                    break

        # Find date inputs
        text_inputs = [i for i in all_inputs if i.get("type","text") in ("text","")]
        logger.info(f"PROBATE: {len(text_inputs)} text inputs")
        filled = 0
        for inp in text_inputs:
            name = inp.get("name","")
            iid  = inp.get("id","").lower()
            if "from" in iid or "start" in iid or (filled == 0 and "date" in iid):
                payload[name] = start
                filled += 1
                logger.info(f"PROBATE: set from date field '{name}' = {start}")
            elif "to" in iid or "end" in iid or (filled == 1 and "date" in iid):
                payload[name] = end
                filled += 1
                logger.info(f"PROBATE: set to date field '{name}' = {end}")

        # Find submit button
        submit = soup.find("input", {"type": "submit"})
        if submit:
            payload[submit.get("name","btnSearch")] = submit.get("value","Search")

        logger.info(f"PROBATE: POSTing search...")
        resp2 = session.post(url, data=payload, timeout=30)
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        tables = soup2.find_all("table")
        logger.info(f"PROBATE: {len(tables)} tables in response")

        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
        if not best:
            logger.warning("PROBATE: no table in response")
            return leads

        rows = best.find_all("tr")
        logger.info(f"PROBATE: {len(rows)} rows")
        for i, row in enumerate(rows[:4]):
            cells = [td.get_text(" ",strip=True) for td in row.find_all(["td","th"])]
            logger.info(f"PROBATE row[{i}]: {cells}")

        for row in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            try:
                case_num    = cells[0]
                file_date   = cells[1] if len(cells) > 1 else ""
                case_style  = cells[2] if len(cells) > 2 else ""
                case_status = cells[3] if len(cells) > 3 else ""

                if re.search(r"closed|dismissed|settled|disposed", case_status, re.I):
                    continue
                if not re.search(r"^(Estate of|Heirs of|In re)", case_style, re.I):
                    continue

                signals = []
                if re.search(r"^Estate of", case_style, re.I):
                    signals.append("estate")
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
# MAIN — synchronous (no asyncio needed with requests+BS4)
# ─────────────────────────────────────────────────────────────────────────────

def run_full_scrape():
    logger.info("=" * 60)
    logger.info("Harris County Scraper v4 starting")
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
