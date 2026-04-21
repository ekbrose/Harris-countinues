"""
Harris County Lead Scraper v6
Realistic approach based on actual page structures discovered:

TAX:  hctax.net listing has Cause# but no owner/address (JS-loaded).
      We enrich each Cause# via the county's case lookup API.
      
PROBATE: District Clerk blocks direct POST. Use their public case search
         by searching for "Estate" as party name via GET parameters.

FRCL: County clerk form requires JS session. Use Linebarger Goggan's
      public filing search which lists FRCL notices with full details.
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
    "Connection": "keep-alive",
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
# SOURCE 1 — FORECLOSURE
# Harris County Appraisal District posts public data files at pdata.hcad.org
# The "real_acct" file has owner + address for all properties.
# We cross-reference with the county clerk filing index.
# For now we use the Linebarger attorney's public filing list.
# ─────────────────────────────────────────────────────────────────────────────

def scrape_foreclosures(days_back=45):
    leads = []
    logger.info("FRCL: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Try fetching the Harris County Clerk's foreclosure filing records
        # via their public records portal using a direct search URL
        start_str = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
        end_str   = date.today().strftime("%m/%d/%Y")

        # The county clerk has an alternate search endpoint that works via GET
        # for instrument type FR (Foreclosure)
        url = "https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx"
        session.headers.update({"Referer": "https://www.cclerk.hctx.net/"})

        r = session.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        # Extract ALL hidden fields for ASP.NET postback
        hidden = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name","")
            val  = inp.get("value","")
            if name: hidden[name] = val

        logger.info(f"FRCL: captured {len(hidden)} hidden fields")

        # The date fields are injected by JS. Try __doPostBack approach instead:
        # Trigger the search by posting with a specific __EVENTTARGET
        payload = dict(hidden)
        payload.update({
            "__EVENTTARGET":   "ctl00$ContentPlaceHolder1$btnSearch",
            "__EVENTARGUMENT": "",
        })

        # Try finding any visible inputs we might have missed
        all_inputs = soup.find_all("input")
        for inp in all_inputs:
            name = inp.get("name","")
            val  = inp.get("value","")
            itype = inp.get("type","text")
            if name and itype != "hidden":
                logger.info(f"FRCL non-hidden: name='{name}' type='{itype}' value='{val[:30]}'")
                payload[name] = val

        # Try to set date fields by common ASP.NET naming patterns
        for date_field_name in [
            "ctl00$ContentPlaceHolder1$txtFromDate",
            "ctl00$ContentPlaceHolder1$txtToDate",
            "ctl00$ContentPlaceHolder1$txtBeginDate",
            "ctl00$ContentPlaceHolder1$txtEndDate",
            "txtFromDate", "txtToDate", "txtBeginDate", "txtEndDate",
        ]:
            if "From" in date_field_name or "Begin" in date_field_name:
                payload[date_field_name] = start_str
            elif "To" in date_field_name or "End" in date_field_name:
                payload[date_field_name] = end_str

        r2 = session.post(url, data=payload, timeout=30)
        soup2 = BeautifulSoup(r2.text, "html.parser")
        logger.info(f"FRCL POST: status={r2.status_code} len={len(r2.text)}")

        # Check if we got any tables with data
        tables = soup2.find_all("table")
        logger.info(f"FRCL: {len(tables)} tables in response")
        for t in tables:
            rows = t.find_all("tr")
            if len(rows) > 2:
                logger.info(f"FRCL table with {len(rows)} rows")
                for row in rows[:3]:
                    cells = [td.get_text(" ",strip=True) for td in row.find_all(["td","th"])]
                    logger.info(f"FRCL row: {cells}")

        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None) if tables else None
        if best:
            rows = best.find_all("tr")[1:]
            for row in rows:
                cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
                if len(cells) < 3: continue
                try:
                    filing_date = cells[0]
                    instrument  = cells[1] if len(cells)>1 else ""
                    grantor     = cells[2] if len(cells)>2 else ""
                    address     = cells[3] if len(cells)>3 else ""
                    mortgagee   = cells[4] if len(cells)>4 else ""
                    amount_raw  = cells[5] if len(cells)>5 else ""

                    if not address or not grantor or len(address)<5: continue
                    if re.search(r"filing date|grantor|instrument", grantor, re.I): continue

                    amt = re.sub(r"[^\d.]","",amount_raw)
                    amount = f"${float(amt):,.0f}" if amt else "TBD"
                    try:
                        fd = datetime.strptime(filing_date,"%m/%d/%Y")
                        sale_date = (fd+timedelta(days=45)).strftime("%m/%d/%Y")
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
# Page has Cause# but no owner/address (JS-loaded per property).
# Strategy: Use the cause numbers to look up each case in the district clerk
# OR use HCAD's property search with the cause number.
# Simpler: use the county's delinquent tax search which does have owner/address.
# ─────────────────────────────────────────────────────────────────────────────

def scrape_tax():
    leads = []
    logger.info("TAX: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # First get the sale listing to collect Cause numbers
        url = "https://www.hctax.net/Property/listings/taxsalelisting"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Collect all cause numbers and tax year ranges
        cause_data = []
        current = {}
        all_tables = soup.find_all("table")

        for table in all_tables:
            for row in table.find_all("tr"):
                cells = [td.get_text(" ",strip=True) for td in row.find_all(["td","th"])]
                if len(cells) < 2: continue
                label = cells[0].strip().rstrip(":").strip()
                value = cells[1].strip() if len(cells)>1 else ""

                if label == "Sale#":
                    if current.get("Cause#"):
                        cause_data.append(current)
                    current = {"Sale#": value}
                elif label == "Cause#":
                    current["Cause#"] = value
                elif label == "Tax Years in Judgement":
                    current["Tax Years"] = value
                elif label == "Judgment":
                    current["Judgment"] = value
                elif label == "Type":
                    current["Type"] = value
                elif label == "Precinct":
                    current["Precinct"] = value

        if current.get("Cause#"):
            cause_data.append(current)

        logger.info(f"TAX: collected {len(cause_data)} cause numbers")

        # Get sale date
        sale_date = ""
        page_text = soup.get_text()
        dm = re.search(r"Sale Date[:\s]+([A-Za-z]+ \d+,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4})", page_text, re.I)
        if dm: sale_date = dm.group(1)

        # Now look up each cause number via the county's delinquent tax search
        # which returns owner and address
        delinquent_search = "https://caopay.harriscountytx.gov/"

        for item in cause_data[:50]:  # limit to 50 per scrape run
            cause_num = item.get("Cause#","")
            if not cause_num: continue

            try:
                # Search the Harris County Attorney's delinquent account lookup
                search_url = f"https://caopay.harriscountytx.gov/Account/Search?causeNumber={cause_num}"
                r = session.get(search_url, timeout=15)
                logger.info(f"TAX lookup {cause_num}: status={r.status_code} len={len(r.text)}")

                if r.status_code == 200 and len(r.text) > 200:
                    s2 = BeautifulSoup(r.text, "html.parser")
                    # Log first 200 chars to see structure
                    logger.info(f"TAX lookup preview: {r.text[:200]}")

                    # Look for owner name and address patterns
                    owner   = ""
                    address = ""

                    # Try common HTML patterns
                    for tag in s2.find_all(["td","span","div","p","li"]):
                        text = tag.get_text(" ", strip=True)
                        if re.search(r"^\d+\s+\w+.*(Houston|TX|Katy|Spring)", text, re.I) and len(text) < 100:
                            address = text
                        elif re.search(r"[A-Z][a-z]+,?\s+[A-Z]", text) and 5 < len(text) < 60 and not owner:
                            owner = text

                    if owner and address:
                        years_raw = item.get("Tax Years","")
                        years_delinquent = None
                        yr = re.search(r"(\d{4})\s*[-–]\s*(\d{4})", years_raw)
                        if yr:
                            years_delinquent = int(yr.group(2)) - int(yr.group(1)) + 1

                        signals = []
                        if years_delinquent and years_delinquent >= 3:
                            signals.append("tx-long")

                        leads.append(make_lead(
                            "tax", owner, address, "Amount TBD",
                            item.get("Judgment",""), sale_date,
                            f"hctax.net Cause#{cause_num}",
                            base_score=30, signals=signals,
                            years_delinquent=years_delinquent
                        ))
                        logger.info(f"TAX: added lead {owner} @ {address}")

            except Exception as e:
                logger.warning(f"TAX cause lookup error for {cause_num}: {e}")
                continue

        # If cause lookup didn't work, fall back to the delinquent account search
        if not leads:
            logger.info("TAX: cause lookup got 0 leads — trying delinquent search directly")
            try:
                delinq_url = "https://www.hctax.net/Property/DelinquentTax"
                r3 = session.get(delinq_url, timeout=20)
                soup3 = BeautifulSoup(r3.text, "html.parser")
                logger.info(f"TAX delinquent search: status={r3.status_code} len={len(r3.text)}")
                logger.info(f"TAX delinquent preview: {r3.text[:400]}")
            except Exception as e:
                logger.warning(f"TAX delinquent search failed: {e}")

    except Exception as e:
        logger.error(f"TAX failed: {e}")

    logger.info(f"TAX: {len(leads)} leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — PROBATE
# District Clerk blocks direct POST (120 byte response = redirect/block).
# Use their public GET-based search instead.
# ─────────────────────────────────────────────────────────────────────────────

def scrape_probate(days_back=30):
    leads = []
    logger.info("PROBATE: starting...")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Try the public records search via GET with query params
        # The district clerk has a public search at a different endpoint
        base_url = "https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx"

        # First visit the page to get cookies and session state
        r0 = session.get(base_url, timeout=20)
        soup0 = BeautifulSoup(r0.text, "html.parser")
        logger.info(f"PROBATE: initial GET status={r0.status_code}")

        # Get all hidden fields
        hidden = {}
        for inp in soup0.find_all("input", {"type": "hidden"}):
            name = inp.get("name","")
            val  = inp.get("value","")
            if name: hidden[name] = val
        logger.info(f"PROBATE: {len(hidden)} hidden fields captured")

        # Build search payload using EXACT field IDs from diagnostic logs
        prefix = "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder2$ContentPlaceHolder2$"
        start_dt = date.today() - timedelta(days=days_back)
        end_dt   = date.today()

        payload = dict(hidden)
        payload.update({
            "__EVENTTARGET":   "",
            "__EVENTARGUMENT": "",
            # Search by party name "Estate"
            "txtPartyName":    "Estate",
            # Date dropdowns using exact IDs from logs
            f"{prefix}monthFromDropdownlist": start_dt.strftime("%B"),
            f"{prefix}dayFromDropdownlist":   str(start_dt.day),
            f"{prefix}yearFromDropdownlist":  str(start_dt.year),
            f"{prefix}monthToDropdownlist":   end_dt.strftime("%B"),
            f"{prefix}dayToDropdownlist":     str(end_dt.day),
            f"{prefix}yearToDropdownlist":    str(end_dt.year),
            # Court type: leave blank to search all
            f"{prefix}ddlCourtType":          "",
            # Party connection: All
            f"{prefix}ddlPartyConnection":    "All",
        })

        # Find and include the search button
        for btn in soup0.find_all("input", {"type": "submit"}):
            name = btn.get("name","")
            val  = btn.get("value","Search")
            if name:
                payload[name] = val
                logger.info(f"PROBATE: adding submit btn '{name}' = '{val}'")
                break

        logger.info(f"PROBATE: POSTing with {len(payload)} fields...")
        r2 = session.post(base_url, data=payload, timeout=30, allow_redirects=True)
        logger.info(f"PROBATE: response status={r2.status_code} len={len(r2.text)} url={r2.url}")

        if len(r2.text) < 500:
            logger.warning(f"PROBATE: short response: {r2.text[:200]}")
            # Try alternate approach — search via the case docket search
            docket_url = "https://www.hcdistrictclerk.com/eDocs/Public/CaseDetailsPrinting.aspx"
            logger.info("PROBATE: trying docket search as fallback...")

            # Use the public case search for probate court
            # Probate courts in Harris County are numbered 1-4
            for court in ["Probate Court 1", "Probate Court 2", "Probate Court 3", "Probate Court 4"]:
                try:
                    court_url = f"https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx?Tab=1&court={court}"
                    r_court = session.get(court_url, timeout=15)
                    logger.info(f"PROBATE court search '{court}': status={r_court.status_code} len={len(r_court.text)}")
                    if len(r_court.text) > 1000:
                        logger.info(f"PROBATE court preview: {r_court.text[:300]}")
                        break
                except Exception as ce:
                    logger.warning(f"PROBATE court {court}: {ce}")
            return leads

        soup2 = BeautifulSoup(r2.text, "html.parser")
        tables = soup2.find_all("table")
        logger.info(f"PROBATE: {len(tables)} tables in response")

        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None) if tables else None
        if not best:
            logger.warning("PROBATE: no results table")
            logger.info(f"PROBATE response preview: {r2.text[:500]}")
            return leads

        rows = best.find_all("tr")
        logger.info(f"PROBATE: {len(rows)} rows found")
        for i, row in enumerate(rows[:4]):
            cells = [td.get_text(" ",strip=True) for td in row.find_all(["td","th"])]
            logger.info(f"PROBATE row[{i}]: {cells}")

        for row in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 2: continue
            try:
                case_num    = cells[0].strip()
                file_date   = cells[1].strip() if len(cells)>1 else ""
                case_style  = cells[2].strip() if len(cells)>2 else ""
                case_status = cells[3].strip() if len(cells)>3 else ""

                if re.search(r"closed|dismissed|settled|disposed", case_status, re.I):
                    continue
                if not re.search(r"Estate|Heirs|In re", case_style, re.I):
                    continue

                signals = ["estate"]
                if re.search(r"heirs|et al", case_style, re.I):
                    signals.append("multiheir")
                try:
                    fd = datetime.strptime(file_date, "%m/%d/%Y")
                    if (datetime.today()-fd).days <= 14:
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

def run_full_scrape():
    logger.info("="*60)
    logger.info("Harris County Scraper v6 starting")
    logger.info("="*60)
    start = datetime.utcnow()

    fc_leads      = scrape_foreclosures(days_back=45)
    tax_leads     = scrape_tax()
    probate_leads = scrape_probate(days_back=30)

    all_leads = fc_leads + tax_leads + probate_leads
    logger.info(f"Raw: FC={len(fc_leads)} Tax={len(tax_leads)} Probate={len(probate_leads)}")

    all_leads = apply_cross_signals(all_leads)
    all_leads, dupes = deduplicate(all_leads)
    active, settled  = scrub_settled(all_leads)
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
        "settled_log": [
            {"owner":l["owner"],"address":l["address"],"type":l["type"],
             "reason":l.get("_resolution",{}).get("reason",""),
             "stamp":l.get("_resolution",{}).get("stamp","")}
            for l in settled
        ],
        "leads": active,
    }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_full_scrape()
    print(f"\nActive: {result['active_leads']} | By type: {result['by_type']}")
