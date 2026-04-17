# Harris County Lead Scraper — Backend

Full-stack deployment: Python scraper + Flask API + PostgreSQL on Railway.

---

## What This Does

| Component | Purpose |
|-----------|---------|
| `harris_scraper.py` | Scrapes pre-foreclosure (cclerk.hctx.net), tax delinquent (hctax.net), and probate (hcdistrictclerk.com) filings daily. Enriches each lead from HCAD. Removes settled cases. |
| `server.py` | Flask REST API serving leads to your HTML tool. Runs scrapes on a schedule (6 AM CT daily). |
| PostgreSQL | Stores all active leads. Settled cases are purged on every scrape run. |
| HTML tool | Your frontend — connect it to the Railway URL and it pulls live data. |

---

## Deploy to Railway (Step by Step)

### 1. Create a Railway account
Go to **https://railway.app** and sign up (free).

### 2. Install the Railway CLI (optional but useful)
```bash
npm install -g @railway/cli
railway login
```

### 3. Create a new project
- Go to https://railway.app/new
- Click **"Deploy from GitHub repo"**
- Push this folder to a GitHub repo first:

```bash
cd harris-scraper
git init
git add .
git commit -m "Initial commit"
# Create a repo on github.com, then:
git remote add origin https://github.com/YOUR_USERNAME/harris-scraper.git
git push -u origin main
```

- Select your repo in Railway

### 4. Add a PostgreSQL database
- In your Railway project, click **"+ New"**
- Select **"Database" → "Add PostgreSQL"**
- Railway automatically sets the `DATABASE_URL` environment variable

### 5. Set environment variables
In your Railway service settings → Variables, add:
```
DATABASE_URL   = (auto-set by Railway PostgreSQL — do not change)
PORT           = 8080
```

### 6. Deploy
Railway will auto-detect the Dockerfile and build. First deploy takes ~2 minutes.

### 7. Get your URL
- Go to your service in Railway
- Click **"Settings" → "Networking" → "Generate Domain"**
- You'll get a URL like: `https://harris-scraper-api.up.railway.app`

### 8. Connect the HTML tool
- Open `harris_lead_scraper.html` in your browser
- Click **⚙ SETUP** → **Connect to Railway API**
- Paste your Railway URL
- Click **SAVE & CONNECT**
- Click **▶ RUN SCRAPE**

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/leads` | All active leads (query params: type, tier, signal, search, min_score) |
| GET | `/api/leads/:id` | Single lead detail |
| GET | `/api/leads/stats` | Counts by type, tier |
| POST | `/api/scrape` | Trigger a manual scrape |
| GET | `/api/scrape/status` | Last 5 scrape run results |
| GET | `/api/scrape/log` | Settled/removed leads from last run |

### Example queries
```
GET /api/leads?type=foreclosure&tier=1
GET /api/leads?signal=tx-fc&min_score=75
GET /api/leads?search=houston+77004
GET /api/leads?type=probate&signal=estate
```

---

## Scrape Schedule

The server runs a full scrape automatically at **6:00 AM Central Time** every day.

You can also trigger a manual scrape any time by clicking **RUN SCRAPE** in the HTML tool, or by hitting `POST /api/scrape` directly.

---

## What Gets Scraped

### Pre-Foreclosure (cclerk.hctx.net)
- Pulls FRCL filings from the last 45 days
- Captures: grantor (owner), property address, loan amount, filing date, sale date, lender/mortgagee
- Enriches each lead with HCAD: assessed value, mailing address, sq footage, homestead exemption

### Tax Delinquent (hctax.net)
- Pulls the monthly pre-sale delinquent property list
- Captures: account number, owner, address, tax amount owed, scheduled sale date
- Enriches with HCAD: computes tax-to-value ratio, detects out-of-state owner, non-owner-occupied

### Probate (hcdistrictclerk.com)
- Searches for PROB case type filed in last 30 days
- Captures: case number, filing date, decedent/estate name, case status
- Skips any case already marked closed/dismissed/settled at the source

---

## Settled Case Scrubbing

Only leads with `resolution_status = 'active'` are ever stored or served.  
Anything non-active is **removed immediately** — no toggle, no override.

Removed statuses include:
- `deed_transfer` — sold to new buyer
- `loan_reinstated` — foreclosure cured
- `notice_rescinded` — notice withdrawn by trustee
- `taxes_paid` — tax account now current
- `payment_plan` — payment agreement entered
- `estate_sold` — probate property sold
- `case_closed` — probate case settled/closed
- Any unknown status — treated as settled and removed

---

## Cost on Railway

| Resource | Free tier | Paid |
|----------|-----------|------|
| Compute | $5/month credit (usually covers it) | ~$5-10/month |
| PostgreSQL | Included | Included |
| Bandwidth | Generous | Generous |

Most users will stay within the free $5/month credit.

---

## Local Development

```bash
# Install deps
pip install -r requirements.txt

# Set env var
export DATABASE_URL="postgresql://user:pass@localhost:5432/harris"

# Run scraper standalone
python harris_scraper.py

# Run API server
python server.py
```
