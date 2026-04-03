"""
Seed SQLite database and LanceDB with synthetic data.
Run this script to initialize the portfolio demo database.
"""

import os
import sys
import sqlite3
import asyncio

# Add parent paths for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data.synthetic.generate_market_data import generate_all_product_details, generate_all_provider_matpro
from data.synthetic.generate_documents import generate_all_documents


# ============================================================================
# SQLite Schema and Seeding
# ============================================================================

SQLITE_SCHEMA = """
-- Market mode: product listings
CREATE TABLE IF NOT EXISTS dashboard_product_detail (
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

-- Market mode: provider promotional materials
CREATE TABLE IF NOT EXISTS dashboard_provider_matpro (
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

-- Rate limiting (for demo accounts)
CREATE TABLE IF NOT EXISTS model_usage_daily (
    user_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    usage_date DATE NOT NULL,
    request_count INTEGER DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, model_name, usage_date)
);

-- Conversation memory
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversation_summaries (
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    summary_text TEXT NOT NULL,
    last_summarized_turn_id INTEGER,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ,
    PRIMARY KEY (user_id, session_id)
);

-- Demo users (mock auth)
CREATE TABLE IF NOT EXISTS demo_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    display_name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Session tracking
CREATE TABLE IF NOT EXISTS user_sessions (
    session_token TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    user_email TEXT,
    user_name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    last_accessed DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversation_async_jobs (
    job_id TEXT PRIMARY KEY,
    convo_status TEXT NOT NULL,
    input_params TEXT,
    result TEXT,
    thinking_step TEXT,
    progress_percentage INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

DEMO_USERS = [
    ('demo', 'Demo User'),
    ('reviewer', 'Portfolio Reviewer'),
    ('test', 'Test Account'),
]

RESET_TABLES = [
    'dashboard_product_detail',
    'dashboard_provider_matpro',
    'model_usage_daily',
    'conversations',
    'conversation_memory',
    'conversation_summaries',
    'demo_users',
    'sessions',
    'user_sessions',
    'conversation_async_jobs',
]


def create_sqlite_database(db_path):
    """Create SQLite database with schema."""
    reset_in_place = False

    # Remove existing database when possible. On Windows/OneDrive this may fail
    # even when the file is still writable, so fall back to dropping tables.
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except PermissionError:
            reset_in_place = True

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if reset_in_place:
        for table_name in RESET_TABLES:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create schema
    cursor.executescript(SQLITE_SCHEMA)

    # Insert demo users
    cursor.executemany(
        "INSERT INTO demo_users (username, display_name) VALUES (?, ?)",
        DEMO_USERS
    )

    conn.commit()
    conn.close()
    print(f"[OK] Created SQLite database: {db_path}")


def seed_market_data(db_path):
    """Seed market data tables with synthetic data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Generate and insert product details
    products = generate_all_product_details()
    cursor.executemany(
        """INSERT INTO dashboard_product_detail
           (id, provider, product_name, speed_mbps, price, price_unit, locations, source, EventDate)
           VALUES (:id, :provider, :product_name, :speed_mbps, :price, :price_unit, :locations, :source, :EventDate)""",
        products
    )
    print(f"[OK] Inserted {len(products)} records into dashboard_product_detail")

    # Generate and insert provider matpro
    providers = generate_all_provider_matpro()
    cursor.executemany(
        """INSERT INTO dashboard_provider_matpro
           (id, provider, package_name, speed, price, found, gimmicks, source, timestamp)
           VALUES (:id, :provider, :package_name, :speed, :price, :found, :gimmicks, :source, :timestamp)""",
        providers
    )
    print(f"[OK] Inserted {len(providers)} records into dashboard_provider_matpro")

    conn.commit()
    conn.close()


# ============================================================================
# LanceDB Seeding
# ============================================================================

def seed_lancedb(docs_dir, lancedb_path):
    """Initialize LanceDB for document embeddings."""
    try:
        import lancedb
        import pyarrow as pa

        # Create LanceDB connection
        os.makedirs(lancedb_path, exist_ok=True)
        db = lancedb.connect(lancedb_path)

        # Define schema for document_pages
        schema_pages = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 768)),
            pa.field("document_id", pa.string()),
            pa.field("page_number", pa.int32()),
            pa.field("content", pa.string()),
            pa.field("summary", pa.string()),
        ])

        # Define schema for document_summaries
        schema_summaries = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 768)),
            pa.field("document_id", pa.string()),
            pa.field("title", pa.string()),
            pa.field("summary", pa.string()),
            pa.field("total_pages", pa.int32()),
        ])

        # Create tables if they don't exist
        if "document_pages" not in db.table_names():
            db.create_table("document_pages", schema=schema_pages)
        if "document_summaries" not in db.table_names():
            db.create_table("document_summaries", schema=schema_summaries)

        print(f"[OK] Created LanceDB tables at: {lancedb_path}")
        print("[INFO] Note: Document embeddings require Gemini API key.")
        print("       Run 'python seed_database.py --embed' after setting GEMINI_API_KEY")

    except ImportError:
        print("[WARN] LanceDB not installed. Skipping vector database setup.")
        print("       Install with: pip install lancedb pyarrow")


async def embed_documents(docs_dir, lancedb_path, api_key):
    """Embed documents using Gemini API. Requires API key."""
    try:
        import google.generativeai as genai
        import lancedb

        genai.configure(api_key=api_key)

        # Find all document files
        doc_files = [f for f in os.listdir(docs_dir) if f.endswith('.txt')]

        for doc_file in doc_files:
            doc_path = os.path.join(docs_dir, doc_file)
            with open(doc_path, 'r', encoding='utf-8') as f:
                content = f.read()

            doc_id = doc_file.replace('.txt', '')

            # Split content into pages (roughly 500 words per page)
            words = content.split()
            pages = [' '.join(words[i:i+500]) for i in range(0, len(words), 500)]

            for page_num, page_content in enumerate(pages, 1):
                try:
                    # Generate embedding
                    result = genai.embed_content(
                        model="models/gemini-embedding-001",
                        content=page_content,
                        task_type="retrieval_document"
                    )

                    # Store in LanceDB
                    db = lancedb.connect(lancedb_path)
                    table = db.open_table("document_pages")

                    # Insert record
                    table.add([{
                        "vector": result['embedding'],
                        "document_id": doc_id,
                        "page_number": page_num,
                        "content": page_content,
                        "summary": page_content[:200] + "..."
                    }])

                    print(f"[OK] Embedded: {doc_id} page {page_num}")

                except Exception as e:
                    print(f"[ERR] Error embedding {doc_id} page {page_num}: {e}")

        print(f"[OK] Embedded {len(doc_files)} documents")

    except ImportError:
        print("[ERR] google-generativeai not installed. Install with: pip install google-generativeai")


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Initialize database for portfolio demo."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, 'demo.db')
    lancedb_path = os.path.join(base_dir, 'lancedb')
    docs_dir = os.path.join(os.path.dirname(__file__), 'documents')

    print("=" * 60)
    print("Rajawali Intelligence - Portfolio Demo Database Setup")
    print("=" * 60)

    # Create SQLite database
    print("\n[1/4] Creating SQLite database...")
    create_sqlite_database(db_path)

    # Seed market data
    print("\n[2/4] Seeding market data...")
    seed_market_data(db_path)

    # Generate documents
    print("\n[3/4] Generating synthetic documents...")
    generate_all_documents(docs_dir)

    # Initialize LanceDB
    print("\n[4/4] Initializing LanceDB...")
    seed_lancedb(docs_dir, lancedb_path)

    print("\n" + "=" * 60)
    print("[DONE] Database setup complete!")
    print(f"   SQLite: {db_path}")
    print(f"   LanceDB: {lancedb_path}")
    print(f"   Documents: {docs_dir}")
    print("=" * 60)
    print("\nTo embed documents, run:")
    print("  python seed_database.py --embed --api-key YOUR_GEMINI_API_KEY")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Seed portfolio demo database')
    parser.add_argument('--embed', action='store_true', help='Embed documents (requires API key)')
    parser.add_argument('--api-key', type=str, help='Gemini API key for embedding')

    args = parser.parse_args()

    if args.embed:
        if not args.api_key:
            print("Error: --api-key required when using --embed")
            sys.exit(1)
        docs_dir = os.path.join(os.path.dirname(__file__), 'documents')
        lancedb_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'lancedb')
        asyncio.run(embed_documents(docs_dir, lancedb_path, args.api_key))
    else:
        main()
