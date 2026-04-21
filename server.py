"""
Harris Lead Scraper — API Server
Flask + PostgreSQL backend. Serves leads to the frontend tool.
Endpoints:
  GET  /api/leads              — all active leads (with filters)
  GET  /api/leads/stats        — counts by type, tier, signals
  GET  /api/leads/:id          — single lead detail
  POST /api/scrape             — trigger a manual scrape run
  GET  /api/scrape/status      — last scrape run info
  GET  /api/scrape/log         — settled/removed leads from last scrape
  GET  /health                 — health check for Railway
"""

import os
import json
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from apscheduler.schedulers.background import BackgroundScheduler
from pdf_parser import parse_pdf_bytes

# Import scraper (same package)
import sys
sys.path.insert(0, os.path.dirname(__file__))
from harris_scraper import run_full_scrape

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

# Belt-and-suspenders: add CORS headers to every single response
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

# Handle preflight OPTIONS requests
@app.route("/api/<path:path>", methods=["OPTIONS"])
@app.route("/health", methods=["OPTIONS"])
def options_handler(path=""):
    from flask import Response
    r = Response()
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return r, 200

# Database URL — reads from env, falls back to hardcoded public URL
_FALLBACK_DB_URL = "postgresql://postgres:cwbJzKjGUuduMeBXVyMPDPwtQiQxYdwB@maglev.proxy.rlwy.net:28171/railway"

def get_database_url():
    url = os.environ.get("DATABASE_URL", "") or _FALLBACK_DB_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    logger.info(f"Using DB: {url[:40]}...")
    return url

DATABASE_URL = get_database_url()

# ─────────────────────────────────────────────────────────────────────────────
# SERVE FRONTEND HTML TOOL
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def serve_tool():
    """Serve the Harris Lead Scraper HTML tool."""
    from flask import send_file
    html_path = os.path.join(os.path.dirname(__file__), "harris_lead_scraper.html")
    if os.path.exists(html_path):
        return send_file(html_path)
    return "<h2>Harris Lead Scraper API is running. Place harris_lead_scraper.html in the app directory to serve the tool here.</h2>", 200

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    url = get_database_url()
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


_db_initialized = False

def init_db():
    """Create tables if they don't exist. Runs only once per process."""
    global _db_initialized
    if _db_initialized:
        return
    logger.info("Initializing database tables...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id               TEXT PRIMARY KEY,
                    type             TEXT NOT NULL,
                    owner            TEXT,
                    address          TEXT,
                    amount           TEXT,
                    filing_date      TEXT,
                    sale_date        TEXT,
                    source           TEXT,
                    lender           TEXT,
                    score            INTEGER,
                    tier             INTEGER,
                    signals          JSONB DEFAULT '[]',
                    equity           TEXT,
                    occupancy        TEXT,
                    court_case       TEXT,
                    years_delinquent INTEGER,
                    mailing_state    TEXT,
                    prop_sqft        INTEGER,
                    tax_to_value     TEXT,
                    heirs            TEXT,
                    mailing          TEXT,
                    assessed_value   INTEGER,
                    homestead_exempt BOOLEAN,
                    county           TEXT DEFAULT 'Harris County',
                    resolution_status TEXT DEFAULT 'active',
                    scraped_at       TIMESTAMPTZ,
                    updated_at       TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id               SERIAL PRIMARY KEY,
                    started_at       TIMESTAMPTZ NOT NULL,
                    finished_at      TIMESTAMPTZ,
                    status           TEXT DEFAULT 'running',
                    raw_total        INTEGER DEFAULT 0,
                    duplicates_removed INTEGER DEFAULT 0,
                    settled_removed  INTEGER DEFAULT 0,
                    active_leads     INTEGER DEFAULT 0,
                    by_type          JSONB DEFAULT '{}',
                    settled_log      JSONB DEFAULT '[]',
                    error_message    TEXT
                );

                CREATE INDEX IF NOT EXISTS leads_type_idx   ON leads(type);
                CREATE INDEX IF NOT EXISTS leads_tier_idx   ON leads(tier);
                CREATE INDEX IF NOT EXISTS leads_score_idx  ON leads(score DESC);
                CREATE INDEX IF NOT EXISTS leads_updated_idx ON leads(updated_at DESC);
            """)
            conn.commit()
    _db_initialized = True
    # Migration: add county column if it doesn't exist yet
        with get_conn() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute("""
                    ALTER TABLE leads ADD COLUMN IF NOT EXISTS county TEXT DEFAULT 'Harris County';
                    ALTER TABLE leads ADD COLUMN IF NOT EXISTS equity_pct FLOAT;
                """)
                conn2.commit()
    logger.info("Database initialized")


def upsert_leads(leads: list[dict]):
    """Insert or update leads. Existing leads get score/signals/tier refreshed."""
    if not leads:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for lead in leads:
                cur.execute("""
                    INSERT INTO leads (
                        id, type, owner, address, amount, filing_date, sale_date,
                        source, lender, score, tier, signals, equity, occupancy,
                        court_case, years_delinquent, mailing_state, prop_sqft,
                        tax_to_value, heirs, mailing, assessed_value,
                        homestead_exempt, county, resolution_status, scraped_at, updated_at
                    ) VALUES (
                        %(id)s, %(type)s, %(owner)s, %(address)s, %(amount)s,
                        %(filing_date)s, %(sale_date)s, %(source)s, %(lender)s,
                        %(score)s, %(tier)s, %(signals)s, %(equity)s, %(occupancy)s,
                        %(court_case)s, %(years_delinquent)s, %(mailing_state)s,
                        %(prop_sqft)s, %(tax_to_value)s, %(heirs)s, %(mailing)s,
                        %(assessed_value)s, %(homestead_exempt)s, %(county)s,
                        %(resolution_status)s, %(scraped_at)s, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        score             = EXCLUDED.score,
                        tier              = EXCLUDED.tier,
                        signals           = EXCLUDED.signals,
                        amount            = EXCLUDED.amount,
                        filing_date       = EXCLUDED.filing_date,
                        sale_date         = EXCLUDED.sale_date,
                        resolution_status = EXCLUDED.resolution_status,
                        mailing_state     = EXCLUDED.mailing_state,
                        prop_sqft         = EXCLUDED.prop_sqft,
                        assessed_value    = EXCLUDED.assessed_value,
                        homestead_exempt  = EXCLUDED.homestead_exempt,
                        county            = EXCLUDED.county,
                        equity            = EXCLUDED.equity,
                        updated_at        = NOW()
                """, {
                    "id":               lead.get("id"),
                    "type":             lead.get("type"),
                    "owner":            lead.get("owner"),
                    "address":          lead.get("address"),
                    "amount":           lead.get("amount"),
                    "filing_date":      lead.get("filing_date"),
                    "sale_date":        lead.get("sale_date"),
                    "source":           lead.get("source"),
                    "lender":           lead.get("lender"),
                    "score":            lead.get("score", 50),
                    "tier":             lead.get("tier", 5),
                    "signals":          json.dumps(lead.get("signals", [])),
                    "equity":           lead.get("equity"),
                    "occupancy":        lead.get("occupancy"),
                    "court_case":       lead.get("court_case"),
                    "years_delinquent": lead.get("years_delinquent"),
                    "mailing_state":    lead.get("mailing_state"),
                    "prop_sqft":        lead.get("prop_sqft"),
                    "tax_to_value":     lead.get("tax_to_value"),
                    "heirs":            lead.get("heirs"),
                    "mailing":          lead.get("mailing"),
                    "assessed_value":   lead.get("assessed_value"),
                    "homestead_exempt": lead.get("homestead_exempt"),
                    "county":           lead.get("county", "Harris County"),
                    "resolution_status":lead.get("resolution_status", "active"),
                    "scraped_at":       lead.get("scraped_at"),
                })
            conn.commit()


def purge_settled():
    """Remove all non-active leads from the database. Strict allowlist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM leads WHERE resolution_status != 'active'")
            removed = cur.rowcount
            conn.commit()
    if removed > 0:
        logger.info(f"Purged {removed} settled leads from database")
    return removed


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE JOB RUNNER
# ─────────────────────────────────────────────────────────────────────────────

_scrape_lock = threading.Lock()
_last_run_id = None


def run_scrape_job():
    global _last_run_id
    if not _scrape_lock.acquire(blocking=False):
        logger.info("Scrape already running — skipping")
        return

    run_id = None
    try:
        started = datetime.utcnow()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scrape_runs (started_at, status) VALUES (%s, 'running') RETURNING id",
                    (started,)
                )
                run_id = cur.fetchone()["id"]
                conn.commit()
        _last_run_id = run_id

        # Run the full scrape pipeline
        result = run_full_scrape()

        # Upsert active leads
        upsert_leads(result["leads"])

        # Purge anything that's now settled
        db_purged = purge_settled()

        finished = datetime.utcnow()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE scrape_runs SET
                        finished_at      = %s,
                        status           = 'success',
                        raw_total        = %s,
                        duplicates_removed = %s,
                        settled_removed  = %s,
                        active_leads     = %s,
                        by_type          = %s,
                        settled_log      = %s
                    WHERE id = %s
                """, (
                    finished,
                    result["raw_total"],
                    result["duplicates_removed"],
                    result["settled_removed"] + db_purged,
                    result["active_leads"],
                    json.dumps(result["by_type"]),
                    json.dumps(result["settled_log"]),
                    run_id,
                ))
                conn.commit()

        logger.info(f"Scrape run #{run_id} complete — {result['active_leads']} active leads")

    except Exception as e:
        logger.error(f"Scrape run failed: {e}")
        if run_id:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE scrape_runs SET status='error', error_message=%s WHERE id=%s",
                            (str(e), run_id)
                        )
                        conn.commit()
            except Exception:
                pass
    finally:
        _scrape_lock.release()


# ─────────────────────────────────────────────────────────────────────────────
# API ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/api/seed", methods=["POST"])
def seed_leads():
    """Seeds the database with sample leads so the tool works while live scraping is configured."""
    try:
        init_db()
        sample_leads = [
            {"id":"a1b2c3d4e5f6a7b8","type":"foreclosure","owner":"Decebra Anderson","address":"3817 Linder Street, Houston, TX 77026","amount":"$75,000","filing_date":"03/18/2026","sale_date":"05/05/2026","source":"FRCL-2026-2264","lender":"Prosperity Bank","score":95,"tier":1,"signals":["equity","vacant","fc-tax"],"equity":"67%","occupancy":"Vacant","court_case":None,"years_delinquent":None,"mailing_state":"TX","prop_sqft":980,"tax_to_value":None,"heirs":None,"mailing":None,"assessed_value":220000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"b2c3d4e5f6a7b8c9","type":"foreclosure","owner":"Jorge & Claudia Reyes (div.)","address":"5712 Bellfort Ave, Houston, TX 77033","amount":"$124,000","filing_date":"03/25/2026","sale_date":"05/05/2026","source":"FRCL-2026-2318","lender":"NewRez LLC","score":82,"tier":2,"signals":["divorce","fc-tax"],"equity":"29%","occupancy":"Occupied","court_case":"Case #DC-2026-09817","years_delinquent":None,"mailing_state":"TX","prop_sqft":1200,"tax_to_value":None,"heirs":None,"mailing":None,"assessed_value":170000,"homestead_exempt":True,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"c3d4e5f6a7b8c9d0","type":"foreclosure","owner":"Marina Marroquin","address":"1407 Saddlecreek Dr, Houston, TX 77090","amount":"$136,972","filing_date":"04/02/2026","sale_date":"06/02/2026","source":"FRCL-2026-2294","lender":"Freedom Mortgage","score":70,"tier":3,"signals":["fc-tax"],"equity":"22%","occupancy":"Occupied","court_case":None,"years_delinquent":None,"mailing_state":"TX","prop_sqft":1450,"tax_to_value":None,"heirs":None,"mailing":None,"assessed_value":176000,"homestead_exempt":True,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"d4e5f6a7b8c9d0e1","type":"foreclosure","owner":"Heirs of Frank D. Morrison","address":"2801 Lyons Ave, Houston, TX 77020","amount":"$135,000","filing_date":"03/22/2026","sale_date":"05/05/2026","source":"FRCL-2026-2275","lender":"TH MSR Holdings LLC","score":88,"tier":2,"signals":["fc-probate","vacant"],"equity":"44%","occupancy":"Vacant","court_case":"PR-2026-0901","years_delinquent":None,"mailing_state":"TX","prop_sqft":1050,"tax_to_value":None,"heirs":None,"mailing":None,"assessed_value":240000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"e5f6a7b8c9d0e1f2","type":"tax","owner":"Earnestine Cleveland","address":"13106 Beechdale Court, Houston, TX 77014","amount":"$8,240 owed","filing_date":"01/15/2022","sale_date":"05/06/2026","source":"hctax.net","lender":None,"score":97,"tier":1,"signals":["tx-fc","tx-long","tx-vacant"],"equity":None,"occupancy":"Vacant","court_case":None,"years_delinquent":4,"mailing_state":"TX","prop_sqft":980,"tax_to_value":"18%","heirs":None,"mailing":None,"assessed_value":45000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"f6a7b8c9d0e1f2a3","type":"tax","owner":"Beatrice Okonkwo","address":"9122 Fuqua St, Houston, TX 77075","amount":"$11,400 owed","filing_date":"09/12/2021","sale_date":"05/06/2026","source":"hctax.net","lender":None,"score":100,"tier":1,"signals":["tx-hival","tx-rental","tx-dist","tx-long"],"equity":None,"occupancy":"Renter-Occupied","court_case":None,"years_delinquent":5,"mailing_state":"TX","prop_sqft":1480,"tax_to_value":"31%","heirs":None,"mailing":None,"assessed_value":37000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"a7b8c9d0e1f2a3b4","type":"tax","owner":"Marcus T. Holloway (LLC)","address":"7015 Darien Street, Houston, TX 77028","amount":"$3,750 owed","filing_date":"02/11/2024","sale_date":"05/06/2026","source":"hctax.net","lender":None,"score":63,"tier":3,"signals":["tx-rental","tx-oos","tx-small"],"equity":None,"occupancy":"Renter-Occupied","court_case":None,"years_delinquent":2,"mailing_state":"NV","prop_sqft":870,"tax_to_value":"8%","heirs":None,"mailing":None,"assessed_value":47000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"b8c9d0e1f2a3b4c5","type":"probate","owner":"Estate of James R. Thornton","address":"8812 Westheimer Rd, Houston, TX 77063","amount":"Est. $340,000","filing_date":"04/14/2026","sale_date":None,"source":"Dist. Clerk #PR-2026-1041","lender":None,"score":97,"tier":1,"signals":["estate","outstate","taxcombo","recent"],"equity":None,"occupancy":None,"court_case":None,"years_delinquent":None,"mailing_state":"CA","prop_sqft":None,"tax_to_value":None,"heirs":"3 heirs (CA, TX, FL)","mailing":"Sacramento, CA 95814","assessed_value":340000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"c9d0e1f2a3b4c5d6","type":"probate","owner":"Estate of Willie Mae Thompson","address":"3018 Rosedale St, Houston, TX 77004","amount":"Est. $155,000","filing_date":"04/15/2026","sale_date":None,"source":"Dist. Clerk #PR-2026-1055","lender":None,"score":100,"tier":1,"signals":["estate","taxcombo","fccombo","recent"],"equity":None,"occupancy":None,"court_case":None,"years_delinquent":None,"mailing_state":"GA","prop_sqft":None,"tax_to_value":None,"heirs":"4 heirs (GA, TX)","mailing":"Atlanta, GA 30301","assessed_value":155000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
            {"id":"d0e1f2a3b4c5d6e7","type":"probate","owner":"Heirs of Cora Jean Williams (7)","address":"1409 Gregg St, Houston, TX 77020","amount":"Est. $112,000","filing_date":"03/22/2026","sale_date":None,"source":"Dist. Clerk #PR-2026-0810","lender":None,"score":75,"tier":2,"signals":["multiheir","outstate"],"equity":None,"occupancy":None,"court_case":None,"years_delinquent":None,"mailing_state":"Various","prop_sqft":None,"tax_to_value":None,"heirs":"7 heirs across 4 states","mailing":"Various","assessed_value":112000,"homestead_exempt":False,"resolution_status":"active","scraped_at":"2026-04-17T00:00:00"},
        ]
        init_db()
        upsert_leads(sample_leads)
        return jsonify({"status": "seeded", "count": len(sample_leads)})
    except Exception as e:
        logger.error(f"Seed failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/diagnose")
async def diagnose():
    """Fetches each Harris County source and returns what was found — for debugging."""
    from playwright.async_api import async_playwright
    results = {}

    async def check_site(name, url, wait_selector=None):
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--no-zygote","--single-process"])
                page = await browser.new_page()
                await page.goto(url, timeout=30000)
                await page.wait_for_load_state("networkidle", timeout=15000)
                title = await page.title()
                tables = await page.locator("table").count()
                rows = await page.locator("table tr").count()
                html_snippet = await page.content()
                await browser.close()
                return {
                    "status": "ok",
                    "title": title,
                    "tables_found": tables,
                    "rows_found": rows,
                    "html_length": len(html_snippet),
                    "html_preview": html_snippet[:500]
                }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    import asyncio
    results["foreclosure"] = await check_site("FRCL", "https://www.cclerk.hctx.net/Applications/WebSearch/FR.aspx")
    results["tax"] = await check_site("Tax", "https://www.hctax.net/Property/listings/taxsalelisting")
    results["probate"] = await check_site("Probate", "https://www.hcdistrictclerk.com/eDocs/Public/Search.aspx")
    return jsonify(results)


@app.route("/api/counties")
def get_counties():
    """Returns all distinct counties in the leads database with counts."""
    try:
        init_db()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT county, COUNT(*) as count
                    FROM leads
                    WHERE resolution_status = 'active'
                    GROUP BY county
                    ORDER BY count DESC
                """)
                rows = [dict(r) for r in cur.fetchall()]
        return jsonify({"counties": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/debug")
def debug():
    """Returns exact database connection status and error."""
    result = {
        "database_url_set": bool(os.environ.get("DATABASE_URL")),
        "database_url_prefix": os.environ.get("DATABASE_URL", "")[:30] + "...",
        "db_connection": "unknown",
        "tables_exist": False,
        "leads_count": 0,
        "error": None
    }
    try:
        url = get_database_url()
        result["db_connection"] = "connected"
        with psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as c FROM information_schema.tables WHERE table_name='leads'")
                result["tables_exist"] = cur.fetchone()["c"] > 0
                if result["tables_exist"]:
                    cur.execute("SELECT COUNT(*) as c FROM leads")
                    result["leads_count"] = cur.fetchone()["c"]
    except Exception as e:
        result["db_connection"] = "failed"
        result["error"] = str(e)
    return jsonify(result)


@app.route("/api/leads")
def get_leads():
    # Auto-create tables if they don't exist
    try:
        init_db()
    except Exception as e:
        logger.error(f"init_db failed: {e}")
        return jsonify({"error": f"Database init failed: {str(e)}"}), 500

    filters = []
    params  = []

    lead_type = request.args.get("type")
    if lead_type:
        filters.append("type = %s")
        params.append(lead_type)

    tier = request.args.get("tier")
    if tier:
        filters.append("tier = %s")
        params.append(int(tier))

    min_score = request.args.get("min_score")
    if min_score:
        filters.append("score >= %s")
        params.append(int(min_score))

    signal = request.args.get("signal")
    if signal:
        filters.append("signals @> %s::jsonb")
        params.append(json.dumps([signal]))

    county = request.args.get("county")
    if county:
        filters.append("county ILIKE %s")
        params.append(f"%{county}%")

    search = request.args.get("search")
    if search:
        filters.append("(LOWER(owner) LIKE %s OR LOWER(address) LIKE %s)")
        params.extend([f"%{search.lower()}%", f"%{search.lower()}%"])

    # Always only return active leads — settled are never served
    filters.append("resolution_status = 'active'")

    where = "WHERE " + " AND ".join(filters) if filters else "WHERE resolution_status = 'active'"
    limit  = min(int(request.args.get("limit",  500)), 1000)
    offset = int(request.args.get("offset", 0))

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM leads
                    {where}
                    ORDER BY score DESC, tier ASC, updated_at DESC
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])
                rows = cur.fetchall()

                cur.execute(f"SELECT COUNT(*) as total FROM leads {where}", params)
                total = cur.fetchone()["total"]

        leads = []
        for row in rows:
            d = dict(row)
            if isinstance(d.get("signals"), str):
                d["signals"] = json.loads(d["signals"])
            d["scraped_at"] = d["scraped_at"].isoformat() if d.get("scraped_at") else None
            d["updated_at"] = d["updated_at"].isoformat() if d.get("updated_at") else None
            leads.append(d)

        return jsonify({"leads": leads, "total": total, "limit": limit, "offset": offset})

    except Exception as e:
        logger.error(f"GET /api/leads error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<lead_id>")
def get_lead(lead_id):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM leads WHERE id = %s AND resolution_status = 'active'", (lead_id,))
                row = cur.fetchone()
        if not row:
            return jsonify({"error": "Lead not found or settled"}), 404
        d = dict(row)
        if isinstance(d.get("signals"), str):
            d["signals"] = json.loads(d["signals"])
        return jsonify(d)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/stats")
def get_stats():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE resolution_status='active')            AS total,
                        COUNT(*) FILTER (WHERE type='foreclosure' AND resolution_status='active') AS foreclosure,
                        COUNT(*) FILTER (WHERE type='tax'         AND resolution_status='active') AS tax,
                        COUNT(*) FILTER (WHERE type='probate'     AND resolution_status='active') AS probate,
                        COUNT(*) FILTER (WHERE tier=1 AND resolution_status='active') AS tier_1,
                        COUNT(*) FILTER (WHERE tier=2 AND resolution_status='active') AS tier_2,
                        COUNT(*) FILTER (WHERE tier=3 AND resolution_status='active') AS tier_3
                    FROM leads
                """)
                stats = dict(cur.fetchone())
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/upload", methods=["POST"])
def upload_pdf():
    """
    Accept one or more PDF files, parse them into leads, store in DB.
    Returns count of leads extracted.
    """
    try:
        init_db()
    except Exception as e:
        return jsonify({"error": f"DB init failed: {e}"}), 500

    if "files" not in request.files and "file" not in request.files:
        return jsonify({"error": "No files uploaded. Send PDF files as multipart/form-data with field name 'files' or 'file'"}), 400

    files = request.files.getlist("files") or request.files.getlist("file")
    if not files:
        return jsonify({"error": "No files received"}), 400

    total_leads = 0
    results = []

    for f in files:
        if not f.filename:
            continue
        if not f.filename.lower().endswith(".pdf"):
            results.append({"file": f.filename, "status": "skipped", "reason": "not a PDF"})
            continue

        try:
            pdf_bytes = f.read()
            logger.info(f"Processing uploaded PDF: {f.filename} ({len(pdf_bytes)} bytes)")

            leads = parse_pdf_bytes(pdf_bytes, f.filename)
            logger.info(f"Extracted {len(leads)} leads from {f.filename}")

            if leads:
                upsert_leads(leads)
                total_leads += len(leads)
                results.append({
                    "file": f.filename,
                    "status": "ok",
                    "leads_extracted": len(leads),
                    "owners": [l["owner"] for l in leads[:5]]
                })
            else:
                results.append({
                    "file": f.filename,
                    "status": "ok",
                    "leads_extracted": 0,
                    "note": "No leads found — check PDF format"
                })

        except Exception as e:
            logger.error(f"Upload error for {f.filename}: {e}")
            results.append({"file": f.filename, "status": "error", "reason": str(e)})

    return jsonify({
        "status": "ok",
        "files_processed": len(files),
        "total_leads_extracted": total_leads,
        "results": results
    })


@app.route("/api/scrape", methods=["POST"])
def trigger_scrape():
    """Manually trigger a scrape run (runs in background thread)."""
    thread = threading.Thread(target=run_scrape_job, daemon=True)
    thread.start()
    return jsonify({"status": "started", "message": "Scrape job launched in background"})


@app.route("/api/scrape/status")
def scrape_status():
    try:
        init_db()
    except Exception:
        pass
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, started_at, finished_at, status,
                           raw_total, duplicates_removed, settled_removed,
                           active_leads, by_type, error_message
                    FROM scrape_runs
                    ORDER BY id DESC LIMIT 5
                """)
                runs = [dict(r) for r in cur.fetchall()]
        for r in runs:
            if r.get("started_at"):  r["started_at"]  = r["started_at"].isoformat()
            if r.get("finished_at"): r["finished_at"] = r["finished_at"].isoformat()
            if isinstance(r.get("by_type"), str): r["by_type"] = json.loads(r["by_type"])
        return jsonify({"runs": runs, "currently_running": _scrape_lock.locked()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scrape/log")
def scrape_log():
    """Returns the settled/removed leads from the most recent scrape run."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT settled_log FROM scrape_runs
                    WHERE status = 'success'
                    ORDER BY id DESC LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return jsonify({"settled_log": []})
        log = row["settled_log"]
        if isinstance(log, str):
            log = json.loads(log)
        return jsonify({"settled_log": log})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER — runs scrape daily at 6 AM CT (UTC-5/6)
# ─────────────────────────────────────────────────────────────────────────────

def start_scheduler():
    scheduler = BackgroundScheduler(timezone="America/Chicago")
    scheduler.add_job(run_scrape_job, "cron", hour=6, minute=0, id="daily_scrape")
    scheduler.start()
    logger.info("Scheduler started — daily scrape at 6:00 AM CT")
    return scheduler


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    scheduler = start_scheduler()

    # Run an initial scrape on startup if DB is empty
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as c FROM leads WHERE resolution_status='active'")
                count = cur.fetchone()["c"]
        if count == 0:
            logger.info("Database empty — running initial scrape on startup")
            thread = threading.Thread(target=run_scrape_job, daemon=True)
            thread.start()
    except Exception as e:
        logger.warning(f"Startup check failed: {e}")

    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
