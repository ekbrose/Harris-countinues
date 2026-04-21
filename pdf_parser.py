"""
Harris County + Multi-County PDF Parser v2
- Auto-detects county from PDF content
- Looks up HCAD assessed value for Harris County properties
- Computes equity % = (assessed_value - loan_balance) / assessed_value
- Supports: Harris, Fort Bend, Montgomery, Galveston, Brazoria,
            Bexar, Dallas, Collin, Denton, Gillespie, Williamson, Tarrant
"""

import re
import logging
import hashlib
import requests
from datetime import datetime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
}

# ─────────────────────────────────────────────────────────────────────────────
# COUNTY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

# Maps city/zip patterns to counties
COUNTY_CITY_MAP = {
    "harris":      ["houston","pasadena","baytown","humble","katy","spring","cypress","kingwood","channelview","crosby","huffman","tomball","deer park","la marque","friendswood","seabrook","webster","clear lake"],
    "fort bend":   ["sugar land","missouri city","richmond","rosenberg","stafford","pearland","fulshear","needville","fresno","sienna"],
    "montgomery":  ["conroe","the woodlands","spring","magnolia","willis","montgomery","oak ridge","shenandoah","cut and shoot"],
    "galveston":   ["galveston","texas city","league city","friendswood","dickinson","santa fe","hitchcock","la marque","bacliff","san leon"],
    "brazoria":    ["pearland","alvin","angleton","lake jackson","clute","freeport","manvel","brookside village","danbury"],
    "bexar":       ["san antonio","converse","universal city","schertz","leon valley","windcrest","kirby","selma","live oak","helotes","garden ridge"],
    "dallas":      ["dallas","irving","garland","mesquite","carrollton","richardson","grand prairie","duncanville","desoto","cedar hill","lancaster","rowlett","balch springs","farmers branch","addison","coppell","highland park","university park"],
    "collin":      ["plano","mckinney","allen","frisco","wylie","murphy","sachse","anna","celina","fairview","lucas","parker","princeton","prosper","melissa"],
    "denton":      ["denton","lewisville","flower mound","carrollton","highland village","corinth","lake dallas","argyle","sanger","aubrey","little elm","the colony","lantana","northlake"],
    "gillespie":   ["fredericksburg","harper","stonewall","kerrville"],
    "williamson":  ["round rock","cedar park","georgetown","leander","pflugerville","taylor","hutto","liberty hill","jarrell","florence","brushy creek","sun city"],
    "tarrant":     ["fort worth","arlington","mansfield","bedford","euless","hurst","grapevine","colleyville","keller","southlake","north richland hills","richland hills","watauga","saginaw","azle","weatherford","burleson","crowley","kennedale","forest hill","white settlement"],
}

# Maps zip code prefixes to counties
COUNTY_ZIP_MAP = {
    "770": "harris", "771": "harris", "772": "harris", "773": "harris", "774": "harris",
    "774": "fort bend", "775": "fort bend",
    "773": "montgomery", "774": "montgomery",
    "775": "galveston", "776": "galveston",
    "775": "brazoria", "776": "brazoria",
    "782": "bexar", "783": "bexar",
    "750": "dallas", "751": "dallas", "752": "dallas", "753": "dallas", "754": "dallas",
    "750": "collin", "752": "collin", "753": "collin",
    "760": "denton", "762": "denton",
    "786": "gillespie",
    "786": "williamson", "787": "williamson",
    "760": "tarrant", "761": "tarrant", "762": "tarrant",
}

COUNTY_KEYWORDS = {
    "harris":     ["harris county","hcad","harris co"],
    "fort bend":  ["fort bend","fortbend","fbcad"],
    "montgomery": ["montgomery county","montgomery co","mcad"],
    "galveston":  ["galveston county","galveston co","gcad"],
    "brazoria":   ["brazoria county","brazoria co","bcad"],
    "bexar":      ["bexar county","bexar co","bcad","bcad.us"],
    "dallas":     ["dallas county","dallas co","dcad"],
    "collin":     ["collin county","collin co","collincad"],
    "denton":     ["denton county","denton co","dentoncad"],
    "gillespie":  ["gillespie county","gillespie co"],
    "williamson": ["williamson county","williamson co","wcad"],
    "tarrant":    ["tarrant county","tarrant co","tad.org"],
}

def detect_county(text, address=""):
    """Auto-detect county from PDF text and address."""
    combined = f"{text} {address}".lower()

    # 1. Check explicit county keywords in text
    for county, keywords in COUNTY_KEYWORDS.items():
        if any(kw in combined for kw in keywords):
            return county.title() + " County"

    # 2. Check city names in address
    addr_lower = address.lower()
    for county, cities in COUNTY_CITY_MAP.items():
        if any(city in addr_lower for city in cities):
            return county.title() + " County"

    # 3. Check zip code prefix
    zip_match = re.search(r'\b(\d{5})\b', address)
    if zip_match:
        zip_code = zip_match.group(1)
        prefix = zip_code[:3]
        if prefix in COUNTY_ZIP_MAP:
            return COUNTY_ZIP_MAP[prefix].title() + " County"

    return "Harris County"  # default


# ─────────────────────────────────────────────────────────────────────────────
# HCAD EQUITY LOOKUP (Harris County only)
# ─────────────────────────────────────────────────────────────────────────────

def lookup_hcad_value(address):
    """
    Looks up assessed value for a Harris County address via HCAD public records.
    Returns assessed_value (int) or None.
    """
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # HCAD public address search
        search_url = "https://public.hcad.org/records/details.asp"
        params = {"addr": address, "taxyear": str(datetime.now().year)}
        resp = session.get(search_url, params=params, timeout=10)

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for appraised/assessed value in the page
        for pattern in [
            r"Appraised\s+Value[^\d]*\$([\d,]+)",
            r"Total\s+Appraised[^\d]*\$([\d,]+)",
            r"Market\s+Value[^\d]*\$([\d,]+)",
        ]:
            m = re.search(pattern, resp.text, re.IGNORECASE)
            if m:
                val_str = m.group(1).replace(",", "")
                val = int(val_str)
                if val > 10000:
                    logger.info(f"HCAD: {address[:40]} → ${val:,}")
                    return val

        # Try table cell extraction
        cells = soup.find_all("td")
        for i, cell in enumerate(cells):
            if re.search(r"apprais|market|total value", cell.get_text(), re.IGNORECASE):
                if i + 1 < len(cells):
                    val_text = cells[i+1].get_text(strip=True).replace("$","").replace(",","")
                    try:
                        val = int(float(val_text))
                        if val > 10000:
                            return val
                    except ValueError:
                        pass

    except Exception as e:
        logger.debug(f"HCAD lookup failed for '{address}': {e}")

    return None


def compute_equity(assessed_value, loan_amount_str):
    """
    Compute equity % from assessed value and loan amount string.
    Returns (equity_pct_str, equity_signal) or (None, None).
    """
    try:
        # Parse loan amount
        loan_clean = re.sub(r"[^\d.]", "", loan_amount_str or "")
        if not loan_clean:
            return None, None
        loan = float(loan_clean)

        if not assessed_value or assessed_value <= 0 or loan <= 0:
            return None, None

        equity_pct = ((assessed_value - loan) / assessed_value) * 100

        equity_str = f"{equity_pct:.0f}%"

        # Assign signal based on equity tier
        if equity_pct >= 50:
            signal = "equity"   # High equity — strong signal
        elif equity_pct >= 30:
            signal = "equity"
        else:
            signal = None       # Low equity — not a strong lead

        return equity_str, signal

    except Exception:
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def score_lead(lead):
    FC_PTS   = {"equity":28,"vacant":25,"fc-tax":30,"divorce":22,"fc-probate":25}
    TAX_PTS  = {"tx-fc":32,"tx-vacant":26,"tx-oos":24,"tx-dist":22,"tx-hival":28,
                "tx-probate":30,"tx-long":25,"tx-rental":18,"tx-small":15}
    PROB_PTS = {"estate":25,"outstate":22,"multiheir":18,"taxcombo":30,"fccombo":30,"recent":15}

    t = lead.get("type", "")
    pts = lead.get("base_score", 38)
    sigs = lead.get("signals", [])
    pts += sum((FC_PTS if t=="foreclosure" else TAX_PTS if t=="tax" else PROB_PTS).get(s,0) for s in sigs)

    # Equity bonus: rank high-equity leads higher
    equity_str = lead.get("equity") or ""
    try:
        eq_pct = float(equity_str.replace("%",""))
        if eq_pct >= 60:   pts += 15
        elif eq_pct >= 40: pts += 10
        elif eq_pct >= 20: pts += 5
    except Exception:
        pass

    score = min(100, pts)
    tier = 1 if score>=90 else 2 if score>=75 else 3 if score>=60 else 4 if score>=45 else 5
    return score, tier


def lead_id(address, owner):
    key = f"{address}{owner}".lower().strip()
    return hashlib.md5(key.encode()).hexdigest()[:16]


def detect_signals(owner, address, text):
    signals = []
    combined = f"{owner} {address} {text}".lower()
    if re.search(r"estate of|heirs of", combined):  signals.append("fc-probate")
    if re.search(r"divor|dissolution", combined):    signals.append("divorce")
    if re.search(r"vacant|unoccupied|abandoned", combined): signals.append("vacant")
    if re.search(r"delinquent tax|tax lien", combined):     signals.append("fc-tax")
    if re.search(r"^estate of", owner.lower()):      signals.append("estate")
    if re.search(r"heirs|et al|\(\d+\)", owner.lower()): signals.append("multiheir")
    return list(set(signals))


# ─────────────────────────────────────────────────────────────────────────────
# PDF TEXT PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_single_notice(text, filename=""):
    """Extract a single lead from a notice text block."""

    # Owner / Grantor
    owner = ""
    for pat in [
        r"(?:Grantor[s]?|Trustor[s]?|Borrower[s]?)[:\s,]+([A-Z][A-Za-z\s,\.&'-]{3,60}?)(?:\n|,\s*(?:a|an|the)\s|\s{3,})",
        r"(?:Estate of)\s+([A-Za-z\s,\.]+?)(?:\n|,|\.|$)",
        r"(?:Heirs of)\s+([A-Za-z\s,\.]+?)(?:\n|,|\.|$)",
        r"^([A-Z][A-Z\s,\.&'-]{5,50}?),?\s+(?:a\s+(?:Texas|married|single|widow)|Grantor)",
    ]:
        m = re.search(pat, text, re.MULTILINE | re.IGNORECASE)
        if m:
            candidate = m.group(1).strip().strip(",").strip()
            if len(candidate) > 3:
                owner = candidate
                break

    if not owner:
        caps = re.search(r"^([A-Z][A-Z\s]{5,40}(?:,\s+[A-Z][A-Z\s]{2,20})?)\s*$", text, re.MULTILINE)
        if caps:
            owner = caps.group(1).strip()

    # Property Address
    address = ""
    for pat in [
        r"(?:Property Address|Street Address|Located at|Situated at)[:\s]+(\d+[^,\n]{5,80}(?:TX|Texas)\s*\d{5})",
        r"(\d{3,6}\s+[A-Za-z][A-Za-z\s]{3,40}(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Blvd|Court|Ct|Way|Place|Pl|Circle|Cir|Trail|Trl)\.?[^,\n]{0,30}(?:TX|Texas)\s*\d{5})",
        r"(\d{3,6}\s+\w+(?:\s+\w+){1,6}[,\s]+(?:Houston|San Antonio|Dallas|Fort Worth|Austin|Plano|McKinney|Frisco|Arlington|Garland|Irving|Conroe|Sugar Land|Missouri City|Pearland|Round Rock|Cedar Park|Georgetown|Leander|Denton|Lewisville|Flower Mound|Galveston|Texas City|League City|Angleton|Fredericksburg|Mansfield|Bedford|Euless|Hurst|Grapevine|Keller|Southlake)[,\s]+TX\s*\d{5})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            address = re.sub(r'\s+', ' ', m.group(1).strip())
            if len(address) > 10:
                break

    # Loan Amount
    amount = ""
    for pat in [
        r"(?:Original(?:\s+Principal)?(?:\s+Balance)?|Note Amount|Loan Amount|Indebtedness)[:\s]+\$?([\d,]+(?:\.\d{2})?)",
        r"(?:Total Amount Due|Amount Due|Balance Due)[:\s]+\$?([\d,]+(?:\.\d{2})?)",
        r"\$\s*([\d,]{4,}(?:\.\d{2})?)\s*(?:being|representing|as|,)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            amt_str = m.group(1).replace(",","")
            try:
                amt_val = float(amt_str)
                if amt_val > 1000:
                    amount = f"${amt_val:,.0f}"
                    break
            except ValueError:
                pass

    # Filing Date
    filing_date = ""
    for pat in [
        r"(?:Filed|Recording Date|Date of Filing|Instrument Date)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(?:dated?)\s+(\w+\s+\d{1,2},?\s+\d{4})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            filing_date = m.group(1).strip()
            break

    # Sale Date
    sale_date = ""
    for pat in [
        r"(?:Sale Date|Date of Sale|Auction Date|Foreclosure Sale)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(?:will be sold|shall be sold)[^.]*?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(?:first Tuesday)[^.]*?(\w+\s+\d{1,2},?\s+\d{4})",
        r"on\s+(\w+\s+\d{1,2},?\s+\d{4})\s*(?:between|at|from)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            sale_date = m.group(1).strip()
            break

    # Lender
    lender = ""
    for pat in [
        r"(?:Mortgagee|Beneficiary|Lender|Noteholder)[:\s]+([A-Za-z][A-Za-z\s,\.&'-]{3,60}?)(?:\n|,\s+a\s|$)",
        r"(?:payable to|in favor of)\s+([A-Za-z][A-Za-z\s,\.&'-]{3,60}?)(?:\n|,|$)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            lender = m.group(1).strip()
            if len(lender) > 3:
                break

    # Source
    source = filename or "PDF Upload"
    inst = re.search(r"(?:Instrument|Document|Recording)\s*(?:No\.?|Number)[:\s]+([A-Z0-9-]+)", text, re.IGNORECASE)
    if inst:
        source = f"FRCL-{inst.group(1)}"

    # Validate
    if not owner and not address:
        return None
    if not owner:
        owner = "Unknown Owner"
    if not address:
        any_addr = re.search(r"\d{3,6}\s+[A-Za-z][A-Za-z\s]{3,30}(?:St|Ave|Rd|Dr|Ln|Blvd|Ct|Way|Pl)", text, re.IGNORECASE)
        if any_addr:
            address = any_addr.group(0).strip()
        else:
            return None

    # County detection
    county = detect_county(text, address)

    # Signals
    signals = detect_signals(owner, address, text)

    # Lead type
    lead_type = "foreclosure"
    if re.search(r"Estate of|Heirs of|Probate|Intestate", owner, re.IGNORECASE):
        lead_type = "probate"
    elif re.search(r"Tax Lien|Delinquent Tax|Tax Sale", text, re.IGNORECASE):
        lead_type = "tax"

    # HCAD equity lookup (Harris County only, has address)
    assessed_value = None
    equity_str = None
    if county == "Harris County" and address and amount:
        assessed_value = lookup_hcad_value(address)
        if assessed_value:
            equity_str, equity_signal = compute_equity(assessed_value, amount)
            if equity_signal and equity_signal not in signals:
                signals.append(equity_signal)

    lead = {
        "id":               lead_id(address, owner),
        "type":             lead_type,
        "owner":            owner,
        "address":          address,
        "amount":           amount or "TBD",
        "filing_date":      filing_date,
        "sale_date":        sale_date,
        "source":           source,
        "lender":           lender or None,
        "county":           county,
        "base_score":       38,
        "signals":          signals,
        "equity":           equity_str,
        "assessed_value":   assessed_value,
        "occupancy":        None,
        "court_case":       None,
        "years_delinquent": None,
        "mailing_state":    None,
        "prop_sqft":        None,
        "tax_to_value":     None,
        "heirs":            "Multiple" if "multiheir" in signals else None,
        "mailing":          None,
        "homestead_exempt": None,
        "resolution_status":"active",
        "scraped_at":       datetime.utcnow().isoformat(),
    }
    lead["score"], lead["tier"] = score_lead(lead)
    return lead


def parse_foreclosure_pdf(text, filename=""):
    leads = []
    logger.info(f"PDF parser: {len(text)} chars from '{filename}'")

    # Split into blocks by notice headers
    split_pat = r"(?:NOTICE OF (?:TRUSTEE'?S?|SUBSTITUTE TRUSTEE'?S?|FORECLOSURE) SALE|DEED OF TRUST)"
    parts = re.split(f"(?i)({split_pat})", text)

    blocks = []
    if len(parts) > 1:
        i = 0
        while i < len(parts):
            if re.match(f"(?i){split_pat}", parts[i]):
                block = parts[i] + (parts[i+1] if i+1 < len(parts) else "")
                blocks.append(block)
                i += 2
            else:
                if parts[i].strip() and i > 0:
                    blocks.append(parts[i])
                i += 1
    else:
        blocks = [text]

    logger.info(f"PDF parser: {len(blocks)} notice blocks")

    for block in blocks:
        lead = parse_single_notice(block, filename)
        if lead:
            leads.append(lead)
            logger.info(f"PDF: extracted '{lead['owner']}' @ '{lead['address']}' [{lead['county']}] equity={lead.get('equity','N/A')} score={lead['score']}")

    if not leads:
        lead = parse_single_notice(text, filename)
        if lead:
            leads.append(lead)

    logger.info(f"PDF parser: {len(leads)} leads extracted")
    return leads


def parse_pdf_bytes(pdf_bytes, filename=""):
    """Main entry: raw PDF bytes → list of lead dicts."""
    try:
        import pdfplumber
        import io

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            logger.info(f"PDF: {len(pdf.pages)} pages in '{filename}'")
            full_text = "\n\n".join(page.extract_text() or "" for page in pdf.pages)

        logger.info(f"PDF: extracted {len(full_text)} chars")
        if len(full_text) < 50:
            logger.warning("PDF: very little text — may be scanned/image PDF")
            return []

        return parse_foreclosure_pdf(full_text, filename)

    except ImportError:
        logger.error("pdfplumber not installed")
        return []
    except Exception as e:
        logger.error(f"PDF parse error: {e}")
        return []
