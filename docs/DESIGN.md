# Rajawali Intelligence Portfolio Conversion — Design Document

**Created:** 2026-03-27
**Status:** Approved by user

---

## Summary

Convert the personal-to-production Rajawali Intelligence AI chatbot into a portfolio-ready project with synthetic data, API key input via web UI, and deployment to a private GitHub repository with demo deployment.

---

## Decisions Summary

| Aspect | Decision |
|--------|----------|
| **Monolithic structure** | Keep as-is (single `app_demo.py`) |
| **Portfolio location** | `portfolio/rajawali-intelligence-demo/` (isolated) |
| **Modes included** | Document + Market only |
| **Modes removed** | Data Insight (FE + BE) |
| **Database** | SQLite with synthetic data |
| **Embeddings** | LanceDB with synthetic documents |
| **API key** | Web UI input (primary) + demo key fallback |
| **Authentication** | Mock auth (demo accounts) |
| **Repository** | GitHub Private + Demo Deployment |

---

## Architecture

```
portfolio/rajawali-intelligence-demo/
├── app_demo.py                      # Main app (adapted from app_debug.py)
├── summarizer_pipeline.py           # Document processing (copied)
├── chart_generator.py               # Chart generation (copied)
├── conversation_memory_async.py     # Memory management (adapted for SQLite)
├── query_config.py                  # ISP provider data (copied, public)
├── routing_config.yaml              # Mode routing (copied)
│
├── data/
│   ├── __init__.py
│   ├── demo.db                      # SQLite (generated)
│   ├── lancedb/                     # LanceDB embeddings (generated)
│   └── synthetic/
│       ├── generate_market_data.py  # ISP product data
│       ├── generate_documents.py    # Synthetic policy PDFs
│       └── seed_lancedb.py          # Embed documents to LanceDB
│
├── templates/                       # Frontend (copied + adapted)
│   ├── index.html                   # + API key modal
│   ├── index_test.html
│   ├── source_peek.html
│   ├── multi_page_viewer.html
│   ├── pdf_viewer.html
│   └── channel_peek.html            # May remove if channel features unused
│
├── static/
│   └── logo/logo.png
│
├── .env.example
├── .env.demo                        # gitignored, shared privately
├── requirements.txt
└── README.md
```

---

## Synthetic Data Design

### Market Mode Data (SQLite)

**Table: `dashboard_product_detail`**
- ISP product listings with pricing, speed, location
- ~500 synthetic records
- Realistic distribution: Jakarta ~25%, Surabaya ~15%, etc.

**Table: `dashboard_provider_matpro`**
- Provider promotional materials
- ~500 synthetic records
- Correlated with product_detail

**Table: `model_usage_daily`**
- Rate limiting table (kept from original)
- For demo accounts

**Distribution patterns:**
- Providers: IndiHome (40%), First Media (20%), MyRepublic (15%), CBN (10%), Biznet (10%), Iconnet (5%)
- Speeds: 30Mbps (30%), 50Mbps (35%), 100Mbps (25%), 200Mbps (10%)
- Prices: Correlated with speed (Rp200k - Rp1.5M range)
- Locations: Jakarta, Surabaya, Bandung, Medan, Semarang, Bali with realistic population weights

### Document Mode Data (LanceDB)

**15 synthetic policy documents:**

| # | Document | Content |
|---|----------|---------|
| 1 | `faq_indihome.pdf` | FAQ about IndiHome services |
| 2 | `faq_broadband.pdf` | FAQ about broadband technology |
| 3 | `sop_instalasi.pdf` | Installation SOP procedures |
| 4 | `sop_maintenance.pdf` | Maintenance and repair SOP |
| 5 | `policy_billing.pdf` | Billing and payment policy |
| 6 | `policy_refund.pdf` | Refund and cancellation policy |
| 7 | `policy_sla.pdf` | Service Level Agreement terms |
| 8 | `policy_privacy.pdf` | Privacy and data protection |
| 9 | `guide_troubleshooting.pdf` | Common troubleshooting steps |
| 10 | `guide_router_setup.pdf` | Router configuration guide |
| 11 | `guide_speed_test.pdf` | How to test and report speed issues |
| 12 | `terms_service.pdf` | Terms of service |
| 13 | `warranty_equipment.pdf` | Equipment warranty terms |
| 14 | `upgrade_procedure.pdf` | Package upgrade/downgrade procedures |
| 15 | `network_coverage.pdf` | Coverage area information |

**Embedding process:**
1. Generate synthetic PDF content (text-based, no complex formatting)
2. Run `summarizer_pipeline.py` to process and embed
3. Store in LanceDB `document_summaries` and `document_pages` tables

---

## API Key Handling

### Web UI Flow

```
User visits app
    ↓
Check localStorage for saved API key
    ↓
┌─ Key exists → Auto-populate, proceed
│
└─ No key → Show modal:
           "Enter your Google Gemini API Key"
           [_____________________]
           [ ] Remember for this session
           [Submit]
           ↓
           Key stored in localStorage (optional)
           ↓
           Key sent with each request via header
```

### Backend Flow

```python
# app_demo.py

# Per-request API key handling
def get_api_key(request: Request, api_key: str = None):
    # Priority: request body > header > demo fallback
    key = api_key or request.headers.get("X-API-Key") or os.environ.get("GEMINI_API_KEY_DEMO")
    if not key:
        raise HTTPException(401, "API key required")
    return key

# Usage in endpoints
@app.post("/chat")
async def chat(request: Request, query: ChatRequest, api_key: str = None):
    key = get_api_key(request, api_key)
    genai.configure(api_key=key)
    # ... rest of logic
```

### Demo Key Sharing

- `.env.demo` file created with limited-quota API key
- File is `gitignore`'d
- Shared privately via encrypted message/email to selected reviewers
- For deployed demo: environment variable `GEMINI_API_KEY_DEMO`

---

## Code Changes from Production

### Files to Remove (Data Insight)

| File/Section | Action |
|--------------|--------|
| `data_insight/` directory | Remove entirely |
| Data Insight routes in `app_demo.py` | Remove endpoints |
| Data Insight mode selector in `index.html` | Remove button/tab |
| `routing_config.yaml` Data Insight entries | Remove |

### Files to Modify

| File | Changes |
|------|---------|
| `app_demo.py` | 1. Replace MySQL with SQLite connection<br>2. Add API key handling per-request<br>3. Remove LDAP auth, add mock auth<br>4. Remove Data Insight routes<br>5. Update LanceDB path |
| `conversation_memory_async.py` | Replace MySQL with SQLite |
| `index.html` | Add API key input modal |
| `summarizer_pipeline.py` | Update LanceDB path, make portable |

### Files to Copy As-Is

| File | Notes |
|------|-------|
| `chart_generator.py` | No changes needed |
| `query_config.py` | Public ISP data, no changes |
| `routing_config.yaml` | Remove Data Insight entries |
| All templates | Copy all, modify only `index.html` |
| Static assets | Copy as-is |

---

## Database Schema (SQLite)

```sql
-- Market mode tables
CREATE TABLE dashboard_product_detail (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    product_name TEXT NOT NULL,
    speed_mbps INTEGER,
    price INTEGER,
    price_unit TEXT DEFAULT 'IDR',
    locations TEXT,
    source TEXT,
    EventDate DATE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dashboard_provider_matpro (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    package_name TEXT,
    speed INTEGER,
    price INTEGER,
    found TEXT,
    gimmicks TEXT,
    source TEXT,
    timestamp DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Rate limiting
CREATE TABLE model_usage_daily (
    user_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    usage_date DATE NOT NULL,
    request_count INTEGER DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, model_name, usage_date)
);

-- Conversation memory (simplified)
CREATE TABLE conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE conversation_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    summary TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Demo users
CREATE TABLE demo_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO demo_users (username) VALUES
    ('demo'), ('reviewer'), ('test');
```

---

## Deployment Strategy

### GitHub Repository

- Private repository
- Invite selected reviewers as collaborators
- `.env.demo` shared privately (not in repo)

### Demo Deployment (Optional)

- Platform: Railway, Render, or Fly.io
- Environment: `GEMINI_API_KEY_DEMO` set via platform secrets
- URL shared privately with reviewers
- Demo accounts: `demo` / `reviewer` / `test` (no password or simple password)

---

## Verification Checklist

Before considering portfolio complete:

- [ ] `app_demo.py` runs without MySQL/LDAP dependencies
- [ ] SQLite database generated with synthetic data
- [ ] LanceDB seeded with 15 synthetic documents
- [ ] API key modal works in browser
- [ ] Document mode queries return relevant results
- [ ] Market mode queries return synthetic data
- [ ] Data Insight completely removed (no routes, no UI)
- [ ] Demo key fallback works
- [ ] README includes setup instructions for reviewers
- [ ] No corporate credentials in code

---

## Next Steps

Proceed to implementation planning via `superpowers:writing-plans` skill.