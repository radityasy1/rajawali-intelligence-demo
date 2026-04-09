# Rajawali Intelligence - Portfolio Demo

AI-powered chatbot demo for portfolio showcase. Features Document Q&A and Market Analysis modes using RAG (Retrieval-Augmented Generation) with Google Gemini.

## Features

- **Document Mode**: Q&A based on synthetic policy documents
- **Market Mode**: ISP product comparison and analysis
- **API Key Input**: Enter your Gemini API key directly in the web UI
- **Mock Authentication**: Demo accounts for testing

## Demo Accounts

| Username | Password |
|----------|----------|
| demo | demo123 |
| reviewer | review123 |
| test | test123 |

## Prerequisites

- Python 3.10+
- Google Gemini API Key ([Get free key](https://makersuite.google.com/app/apikey))

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd rajawali-intelligence-demo
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Initialize Database

```bash
cd data/synthetic
python seed_database.py
```

### 3. (Optional) Embed Documents

If you want document search to work, embed the synthetic documents:

```bash
python seed_database.py --embed --api-key YOUR_GEMINI_API_KEY

cd ../.. # get back to root/main project directory
```

### 4. Run the Application

```bash
# Set your API key (or enter in web UI)
export GEMINI_API_KEY=your-api-key

# Run server
uvicorn async_app:app --reload --port 8000
```

### 5. Access the Application

Open http://localhost:8000 in your browser.

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | Yes* | Your Gemini API key |
| `GEMINI_API_KEY_DEMO` | No | Shared demo key for reviewers |
| `DB_PATH` | No | SQLite database path (default: `./data/demo.db`) |
| `LANCEDB_PATH` | No | LanceDB path (default: `./data/lancedb`) |

*API key can also be entered via the web UI.

## Project Structure

```
rajawali-intelligence-demo/
├── app_demo.py              # Main FastAPI application
├── summarizer_pipeline.py   # Document processing
├── chart_generator.py       # Chart generation
├── conversation_memory_async.py
├── query_config.py          # ISP configuration
├── routing_config.yaml      # Mode routing
│
├── data/
│   ├── demo.db              # SQLite database
│   ├── lancedb/             # Document embeddings
│   └── synthetic/           # Data generators
│       ├── generate_market_data.py
│       ├── generate_documents.py
│       └── seed_database.py
│
├── templates/               # HTML templates
│   └── index.html           # Main UI (with API key modal)
│
├── static/                  # Static assets
│
├── requirements.txt
├── .env.example
└── README.md
```

## Modes

### Document Mode

Ask questions about synthetic policy documents:
- FAQ (IndiHome, Broadband)
- SOPs (Installation, Maintenance)
- Policies (Billing, Refund, SLA, Privacy)
- Guides (Troubleshooting, Router Setup)
- Legal (Terms of Service, Warranty)

### Market Mode

Compare ISP products across Indonesian providers:
- IndiHome, First Media, MyRepublic, CBN, Biznet, Iconnet
- Speed tiers: 10-200 Mbps
- Price ranges: Rp150k - Rp1.5M
- Location coverage

## Development

### Regenerate Synthetic Data

```bash
cd data/synthetic
python generate_market_data.py > market_data.sql
python generate_documents.py
```

### Reset Database

```bash
rm data/demo.db
cd data/synthetic
python seed_database.py
```

## License

This is a portfolio demo project. Not for production use.

## Author

Created as a portfolio demonstration project.
