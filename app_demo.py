# -*- coding: utf-8 -*-
"""
Rajawali Intelligence - Portfolio Demo Version
==============================================

This is a modified version of app_debug.py for portfolio demonstration purposes.

MODIFICATIONS MADE:
1. Replaced MySQL with SQLite (DB_PATH instead of DB_CONFIG)
2. Replaced LDAP authentication with mock auth (DEMO_USERS)
3. Removed data_insight module imports and routes
4. Updated LanceDB path to relative ./data/lancedb
5. Added API key endpoint for demo key fallback (/api/demo-key)
6. Updated SQL queries to use SQLite syntax (? instead of %s)

NOTE: Some async_db_pool references remain for Market mode queries.
These would need additional conversion for full SQLite support.

For production deployment, see the original app_debug.py.
"""

# ===============================================
# STANDARD LIBRARY IMPORTS
# ===============================================
import os
import ssl
import time
import json
import logging
import hashlib
import secrets
import threading
import uuid
import base64
import shutil
import re
import io
from io import StringIO
import glob
import mimetypes
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta
from functools import wraps
from collections import defaultdict, deque, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from urllib.parse import quote, unquote
import difflib
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Any, Union, AsyncGenerator, Generator
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path as FilePath
import lancedb
import aiofiles
import contextvars

from decimal import Decimal

from PIL import Image

# ===============================================
# THIRD-PARTY IMPORTS - WEB FRAMEWORKS
# ===============================================
import flask
from flask import Flask, request, jsonify, send_file, render_template, Response, stream_with_context
from flask_cors import CORS
from werkzeug.utils import secure_filename

from starlette.middleware.sessions import SessionMiddleware

# FastAPI and async dependencies (for conservative migration)
from fastapi import FastAPI, APIRouter, HTTPException, Query, Request, BackgroundTasks, status, Path, Form, Depends, Header, UploadFile, File
from fastapi.responses import JSONResponse, Response, StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from chart_generator import process_charts_in_response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import pydantic
from pydantic import BaseModel, Field
import aiosqlite
import sqlite3

# PORTFOLIO DEMO: Mock auth (no LDAP)
# LDAP import removed - using mock authentication instead


# ===============================================
# THIRD-PARTY IMPORTS - DATA & AI
# ===============================================
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import google.generativeai as genai
import google.generativeai.types as genai_types 
from google.generativeai.types import GenerationConfig
from google.api_core import exceptions as google_exceptions
from google import genai as genai_new
from google.genai import types as genai_new_types

# ===============================================
# THIRD-PARTY IMPORTS - DATABASE & CACHING
# ===============================================
# PORTFOLIO DEMO: SQLite instead of MySQL
import sqlite3
import aiosqlite
from cachetools import TTLCache

# ===============================================
# THIRD-PARTY IMPORTS - GEOSPATIAL & ML
# ===============================================
import geopandas as gpd
from shapely.geometry import Point, Polygon, box
from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import gc

# ===============================================
# THIRD-PARTY IMPORTS - FILE PROCESSING
# ===============================================
import fitz

# ===============================================
# LOCAL IMPORTS
# ===============================================
from conversation_memory_async import ConversationMemory
from summarizer_pipeline import (
    process_document_and_generate_data, 
    insert_document_data_to_db, 
    insert_data_to_lancedb,
    get_db_connection as get_pipeline_db_conn
)

# ===============================================
# ENVIRONMENT & LOGGING SETUP
# ===============================================
load_dotenv()

# PORTFOLIO DEMO: data_insight module removed - only Document and Market modes

# Main application logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Frontend event logging
frontend_logger = logging.getLogger('frontend_events')
frontend_log_handler = logging.FileHandler('frontend_events.log', encoding='utf-8')
frontend_log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
frontend_log_handler.setFormatter(frontend_log_formatter)
frontend_logger.addHandler(frontend_log_handler)
frontend_logger.setLevel(logging.DEBUG)

# ===============================================
# PROXY & SSL CONFIGURATION
# ===============================================
# PORTFOLIO DEMO: strip inherited corporate proxy variables.
def sanitize_demo_network_env() -> None:
    removed = []
    for env_name in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        if os.environ.pop(env_name, None):
            removed.append(env_name)
    if removed:
        logging.info(f"Portfolio demo startup removed proxy env vars: {', '.join(removed)}")

# ===============================================
# FILE SYSTEM PATHS CONFIGURATION
# ===============================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SYNTHETIC_DOCS_DIR = os.path.join(DATA_DIR, "synthetic", "documents")
MARKET_SOURCES_BASE_DIR_DEFAULT = os.path.join(DATA_DIR, "market_sources")

def _resolve_demo_dir(env_value: Optional[str], fallback_dir: str) -> str:
    if env_value:
        normalized = env_value.strip()
        if os.name == "nt" and normalized.startswith("/"):
            return fallback_dir
        return normalized
    return fallback_dir


POLICY_DIR = _resolve_demo_dir(os.environ.get("POLICY_DIR"), SYNTHETIC_DOCS_DIR)
BROADBAND_DIR = _resolve_demo_dir(os.environ.get("BROADBAND_DIR"), SYNTHETIC_DOCS_DIR)
CERT_PATH = os.environ.get("CERT_PATH")
KEY_PATH = os.environ.get("KEY_PATH")
CA_BUNDLE_PATH = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
MARKET_SOURCES_BASE_DIR = _resolve_demo_dir(os.environ.get("MARKET_SOURCES_BASE_DIR"), MARKET_SOURCES_BASE_DIR_DEFAULT)
RAG_TRACE_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rag_trace.jsonl")

THUMBNAIL_SIZE = (200, 200)
MAX_IMAGE_SIZE = (1200, 1200)
CACHE_TTL_SECONDS = 3600
CLEANUP_INTERVAL_SECONDS = 600

# Validate critical paths (Only RAG related)
os.makedirs(POLICY_DIR, exist_ok=True)
os.makedirs(BROADBAND_DIR, exist_ok=True)
os.makedirs(MARKET_SOURCES_BASE_DIR, exist_ok=True)

# ===============================================
# DATABASE CONFIGURATION
# ===============================================
# --- LanceDB Setup (Portfolio Demo: relative path) ---
LANCEDB_PATH = os.environ.get("LANCEDB_PATH", os.path.join(os.path.dirname(__file__), "data", "lancedb"))
try:
    db = lancedb.connect(LANCEDB_PATH)
    # Ensure tables exist or open them safely
    tbl_doc_summaries = db.open_table("document_summaries") if "document_summaries" in db.table_names() else None
    tbl_doc_pages = db.open_table("document_pages") if "document_pages" in db.table_names() else None
    tbl_channel_messages = db.open_table("channel_messages") if "channel_messages" in db.table_names() else None
except Exception as e:
    logging.error(f"Failed to initialize LanceDB: {e}")
    tbl_doc_summaries = None
    tbl_doc_pages = None
    tbl_channel_messages = None

# --- SQLite Configuration (Portfolio Demo: replaces MySQL) ---
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "data", "demo.db"))
CURRENT_REQUEST_GEMINI_API_KEY = contextvars.ContextVar("current_request_gemini_api_key", default=None)
sanitize_demo_network_env()

# Demo users for mock authentication
DEMO_USERS = {
    'demo': {'password': 'demo123', 'display_name': 'Demo User'},
    'reviewer': {'password': 'reviewer123', 'display_name': 'Portfolio Reviewer'},
    'test': {'password': 'test123', 'display_name': 'Test Account'}
}

# Async DB Config (Portfolio Demo: SQLite doesn't need async pool)
# ASYNC_DB_CONFIG removed - using synchronous SQLite

def get_db_connection():
    """Establishes a connection to the SQLite database (Portfolio Demo)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise RuntimeError(f"Database connection failed: {e}") from e


def ensure_sqlite_demo_schema():
    """Ensure runtime-required SQLite tables exist for the portfolio demo."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS dashboard_summary_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            source_path TEXT,
            timestamp_utc TEXT,
            model_used TEXT,
            document_summary TEXT,
            page_number INTEGER,
            page_content TEXT,
            page_summary TEXT,
            page_visual_complexity TEXT,
            page_requires_advanced_processing INTEGER DEFAULT 0,
            page_requires_experimental_processing INTEGER DEFAULT 0,
            doc_has_complex_visuals INTEGER DEFAULT 0,
            doc_has_highly_complex_visuals INTEGER DEFAULT 0,
            doc_pages_req_advanced TEXT,
            doc_pages_req_experimental TEXT,
            prompt_suggestion_1 TEXT,
            prompt_suggestion_2 TEXT,
            prompt_suggestion_3 TEXT
        );

        CREATE TABLE IF NOT EXISTS model_usage_daily (
            user_id TEXT NOT NULL,
            model_name TEXT NOT NULL,
            usage_date DATE NOT NULL,
            request_count INTEGER DEFAULT 0,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, model_name, usage_date)
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

        CREATE TABLE IF NOT EXISTS user_sessions (
            session_token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            user_email TEXT,
            user_name TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME,
            last_accessed DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
    except sqlite3.Error as e:
        logging.warning(f"Could not ensure SQLite demo schema at startup: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


ensure_sqlite_demo_schema()


def build_sqlite_in_clause(values):
    """Return a SQLite IN clause placeholder string and flattened params."""
    normalized_values = list(values or [])
    if not normalized_values:
        return "IN (NULL)", []
    return f"IN ({', '.join(['?'] * len(normalized_values))})", normalized_values


def get_effective_gemini_api_key(api_key_override: Optional[str] = None) -> Optional[str]:
    """Resolve Gemini API key from request override first, then environment fallback, then demo key."""
    if api_key_override:
        return api_key_override.strip()
    current_key = CURRENT_REQUEST_GEMINI_API_KEY.get()
    if current_key:
        return current_key.strip()
    # Fallback to main API key or demo key for portfolio demo
    return os.environ.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY_DEMO")


def create_google_genai_client(api_key: str):
    """Create google.genai client for the portfolio demo."""
    return genai_new.Client(api_key=api_key)

# ===============================================
# TEMPLATE
# ===============================================
templates = Jinja2Templates(directory="templates")


# ===============================================
# LOGIN CONFIGURATION (Portfolio Demo: Mock Auth)
# ===============================================
# LDAP removed - using mock authentication
# Original LDAP config:
# LDAP_SERVER = "ldap://10.250.193.116:389"
# LDAP_DOMAIN = "telkomsel.co.id"
# LDAP_BASE_DN = "dc=Telkomsel,dc=co,dc=id"
SESSION_TIMEOUT_MINUTES=120

# ===============================================
# AI MODEL CONFIGURATION
# ===============================================
MINIMUM_MODEL=os.environ.get("MINIMUM_MODEL", "gemini-flash-lite-latest")
STANDARD_MODEL=os.environ.get("STANDARD_MODEL", "gemini-flash-lite-latest")
ADVANCED_MODEL=os.environ.get("ADVANCED_MODEL", "gemini-flash-lite")
EXPERIMENTAL_MODEL=os.environ.get("EXPERIMENTAL_MODEL", "gemini-flash-lite")
EMBEDDING_MODEL=os.environ.get("EMBEDDING_MODEL", "models/gemini-embedding-001")
MARKET_OUTPUT_TOKEN = 8192 * 8
FILTER_EXTRACTION_TIMEOUT_SECONDS = 8

DEFAULT_LLM_MODEL = "gemini-3-flash-preview"
ALLOWED_MODEL_OVERRIDES = {
    "gemini-3.1-pro-preview",
    "gemini-3-flash-preview",
    "gemini-3.1-flash-lite-preview",
}

PRO_MODEL_DAILY_LIMIT = 10  # max requests per user per day
RATE_LIMITED_MODELS = {
    "gemini-3.1-pro-preview": PRO_MODEL_DAILY_LIMIT,
}

THINKING_CAPABLE_PREFIXES = ("gemini-2.5", "gemini-3")

def model_supports_thinking(model_name: str) -> bool:
    """Check if the model supports extended thinking (thought parts in responses)."""
    return any(model_name.startswith(p) for p in THINKING_CAPABLE_PREFIXES)

OWN_BRAND_PROVIDERS = {'indihome', 'telkomsel', 'tsel one', 'telkomsel one', 'eznet', 'ih lite'}
KNOWN_DEMO_LOCATIONS = [
    "jakarta", "surabaya", "bandung", "medan", "semarang",
    "bali", "makassar", "palembang", "tangerang", "depok"
]

# ===============================================
# APPLICATION CONSTANTS
# ===============================================
MAX_RAW_PROMO_CHARS = 15000
MAX_RAW_PRODUCT_CHARS = 60000
MAX_CONTEXT_CHARS_MARKET = 50000
MAX_DOCUMENTS_TO_USE = 5
MAX_PAGES_PER_DOC_CONTEXT = 30
MAX_IMAGES_PER_DOC = 25
MAX_HTML_VALIDATION_CHARS = 80000
MAX_PRODUCTS_PER_PROVIDER_TO_LLM = 50
MAX_PRODUCTS_PER_SPEED_TIER = 10
MAX_PROMOS_PER_PROVIDER_TO_LLM = 20
MAX_DATA_SECTION_CHARS_MARKET = 200000
MAX_HTML_FIX_RETRIES = 1

SEMANTIC_PAGE_LIMIT = 60
SEMANTIC_SIMILARITY_THRESHOLD = 0.4
RETRY_CONFIG_DEFAULT = {"retry_total": 3, "retry_interval_sec": 1.0}

# ===============================================
# THREADING AND CACHE
# ===============================================
MAX_CONCURRENT_JOBS = 10
thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS)
job_storage = {}
job_storage_lock = threading.Lock()
document_index_lock = threading.Lock()

market_data_cache = TTLCache(maxsize=300, ttl=3600)
memory_cache = {}
cache_timestamps = {}

# Location matching metrics
location_match_metrics = {
    'total_queries': 0,
    'successful_matches': 0,
    'no_location_data': 0,
    'location_not_found': 0
}

# ===============================================
# LOGGING PATHS
# ===============================================
RAW_LLM_REPLY_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "raw_llm_reply_debug.txt")
os.makedirs(os.path.dirname(RAW_LLM_REPLY_LOG_FILE), exist_ok=True)

# ===============================================
# ASYNC SERVICE COMPONENTS
# ===============================================
async_db_pool = None


async def ensure_rate_limit_table():
    """Create model_usage_daily table if it doesn't exist."""
    global async_db_pool
    if not async_db_pool:
        return
    try:
        async with async_db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS model_usage_daily (
                        user_id       VARCHAR(255) NOT NULL,
                        model_name    VARCHAR(100) NOT NULL,
                        usage_date    DATE NOT NULL,
                        request_count INT NOT NULL DEFAULT 0,
                        updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (user_id, model_name, usage_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                await conn.commit()
        logger.info("✅ model_usage_daily table ensured")
    except Exception as e:
        logger.warning(f"⚠️ Could not ensure rate limit table: {e}")


async def check_and_increment_model_quota(user_id: str, model_name: str) -> tuple:
    """
    Atomically increment daily counter. Returns (allowed, current_count, daily_limit).
    Uses INSERT ... ON DUPLICATE KEY UPDATE for race-condition-free upsert.
    """
    if model_name not in RATE_LIMITED_MODELS:
        return (True, 0, 0)

    daily_limit = RATE_LIMITED_MODELS[model_name]
    global async_db_pool
    if not async_db_pool:
        # If DB is unavailable, allow the request (fail open)
        return (True, 0, daily_limit)

    try:
        async with async_db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Atomic upsert
                await cursor.execute("""
                    INSERT INTO model_usage_daily (user_id, model_name, usage_date, request_count)
                    VALUES (%s, %s, CURDATE(), 1)
                    ON DUPLICATE KEY UPDATE request_count = request_count + 1, updated_at = NOW()
                """, (user_id, model_name))
                await conn.commit()

                # Read back current count
                await cursor.execute("""
                    SELECT request_count FROM model_usage_daily
                    WHERE user_id = %s AND model_name = %s AND usage_date = CURDATE()
                """, (user_id, model_name))
                row = await cursor.fetchone()
                count = row[0] if row else 1

                if count > daily_limit:
                    return (False, count, daily_limit)
                return (True, count, daily_limit)
    except Exception as e:
        logger.error(f"Rate limit check failed: {e}")
        return (True, 0, daily_limit)  # Fail open


def background_job_storage_cleanup():
    """
    Periodically removes old entries from job_storage to prevent memory leaks.
    Runs in a background thread.
    """
    global job_storage, job_storage_lock
    
    while True:
        time.sleep(CLEANUP_INTERVAL_SECONDS)
        
        try:
            current_time = time.time()
            keys_to_delete = []
            count_deleted = 0
            
            # Acquire lock to safely iterate
            with job_storage_lock:
                if 'source_stream_cache' in job_storage:
                    cache = job_storage['source_stream_cache']
                    
                    # Identify expired keys
                    for key, entry in cache.items():
                        # specific check: entry must be a dict with 'timestamp'
                        if isinstance(entry, dict) and 'timestamp' in entry:
                            if current_time - entry['timestamp'] > CACHE_TTL_SECONDS:
                                keys_to_delete.append(key)
                        # Fallback: if it's old format (just data), treat as expired to clean up
                        elif not isinstance(entry, dict) or 'timestamp' not in entry:
                             keys_to_delete.append(key)

                    # Remove expired keys
                    for key in keys_to_delete:
                        cache.pop(key, None)  # pop avoids KeyError if key was already removed
                        count_deleted += 1
            
            if count_deleted > 0:
                logger.info(f"🧹 [Memory Cleanup] Removed {count_deleted} expired items from source_stream_cache.")
                
        except Exception as e:
            logger.error(f"Error in background job storage cleanup: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages application-level resources.
    Cleaned for Market/Document RAG mode.
    """
    # conversation_memory to globals
    global document_index, INDONESIAN_ISPS, thread_pool_executor, conversation_memory

    # --- STARTUP LOGIC ---
    logger.info("🚀 FastAPI application worker starting up...")

    # PORTFOLIO DEMO: No async DB pool needed - using SQLite
    # Initialize Conversation Memory (SQLAlchemy Sync Engine with SQLite)
    try:
        conversation_memory = ConversationMemory(db_path=DB_PATH)
        logger.info("✅ Conversation Memory initialized (SQLite)")
    except Exception as e:
        logger.error(f"⚠️ Failed to initialize Conversation Memory: {e}")
    
    # Initialize event loop for blocking operations
    loop = asyncio.get_event_loop()
    
    # Load document index
    if document_index is None:
        try:
            document_index = await loop.run_in_executor(None, load_document_data_from_db)
            logger.info(f"✅ Worker loaded {len(document_index)} documents")
        except Exception as e:
            logger.warning(f"⚠️ Document index loading failed: {e}")
            document_index = {}

    # --- Start Memory Cleanup Thread ---
    cleanup_thread = threading.Thread(target=background_job_storage_cleanup, daemon=True)
    cleanup_thread.start()
    logger.info("✅ Memory cleanup background thread started")
    
    # Load ISP configuration
    logger.info(f"✅ ISP Config loaded from file source: {len(PROVIDERS)} providers available.")
    
    # Initialize thread pool executor
    if thread_pool_executor is None:
        thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS)
        logger.info(f"✅ Worker thread pool created with {MAX_CONCURRENT_JOBS} max workers")
    
    logger.info("✅ FastAPI worker startup complete")
    
    yield
    
    # --- SHUTDOWN LOGIC ---
    logger.info("🔌 FastAPI application worker shutting down...")
    
    if thread_pool_executor:
        thread_pool_executor.shutdown(wait=True)
        logger.info("✅ Thread pool shut down")
    
    if async_db_pool:
        try:
            async_db_pool.close()
            await async_db_pool.wait_closed()
            logger.info("✅ Async DB pool closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing DB pool: {e}")

    # Cleanup Conversation Memory Engine
    if 'conversation_memory' in globals() and conversation_memory:
        try:
            conversation_memory.cleanup()
            logger.info("✅ Conversation Memory engine disposed")
        except Exception as e:
            logger.warning(f"⚠️ Error cleaning up Conversation Memory: {e}")
    
    logger.info("✅ FastAPI worker shutdown complete")


# Conservative FastAPI app setup
async_app = FastAPI(
    title="RAG Intelligence Service",
    description="Market and Document Analysis Service",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan
)

# Create a router that we will mount twice
api_router = APIRouter()

async_app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SESSION_SECRET_KEY", "default-insecure-secret-key-please-change")
)

async_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if not os.path.exists("static"):
    os.makedirs("static")
    
# Legacy static path
async_app.mount("/static", StaticFiles(directory="static"), name="static")

# New prefixed static path
async_app.mount("/rajawaliai/static", StaticFiles(directory="static"), name="static_prefixed")


# --- Provider Website Mapping --- #
PROVIDER_WEBSITES = {
    "firstmedia": "https://www.firstmedia.com/",
    "biznet": "https://biznethome.net/",
    "iconnet": "https://iconnet.id/",
    "indihome": "https://indihome.co.id/",
    "myrepublic": "https://myrepublic.co.id/",
    "xlhome": "https://www.xlhome.co.id/",
    "globalxtreme": "https://globalxtreme.net/",
}


# Import configuration
from query_config import (
    INTENT_PATTERNS, SPEED_PATTERNS, PROVIDER_PATTERN, PROVIDERS, LOCATION_PATTERNS,
    STOP_WORDS, FEATURE_KEYWORDS, KNOWN_CITIES, PROVIDER_LOOKUP, LOCATION_LOOKUP,
    PERFORMANCE_LIMITS, PROVIDER_EXTRACTION_EXCLUSIONS, NATIONAL_PROVIDER_EXCLUSIONS
)

logger = logging.getLogger(__name__)

# Map legacy global variable to the new config list
INDONESIAN_ISPS = sorted(PROVIDERS)

# Define standard mapping (Replacing the dynamic DB loader)
PROVIDER_FIELD_MAPS = {
    provider: {
        "location": ["locations"],
        "speed": ["speed_mbps"],
        "price": ["price"],
        "name": ["product_name"],
        "gimmicks": ["gimmicks"]
    }
    for provider in PROVIDERS
}

# ----------------------------------------------------------------------------------------------------------------------------------- #
#  MAIN: LLM-related class and helper
# ----------------------------------------------------------------------------------------------------------------------------------- #
# Define a Pydantic model for your request body for validation
class ChatRequest(BaseModel):
    query: str
    mode: str = "auto"
    selected_sources: Optional[list] = []
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    model_override: Optional[str] = None

# Model for the successful response from the chat job submission
class JobSubmissionResponse(BaseModel):
    job_id: str
    session_id: str
    message: str

class LoginRequest(BaseModel):
    username: str
    password: str

class GetSourcesRequest(BaseModel):
    user_id: str
    session_id: str
    query: str

class SuggestionRequest(BaseModel):
    selected_paths: Optional[List[str]] = [] 
    limit: int = 3

# ===============================================
# AUTHENTICATION SERVICE COMPONENTS
# ===============================================
# Portfolio demo uses mock auth. Keep Laravel URL optional for compatibility.
LARAVEL_AUTH_URL_CONFIG = os.getenv("LARAVEL_AUTH_URL", "")
if not LARAVEL_AUTH_URL_CONFIG:
    logger.warning("LARAVEL_AUTH_URL is not set. Using portfolio demo mock authentication only.")

# Pydantic models for type-hinting and request body validation (matches your style)
class LoginCredentials(pydantic.BaseModel):
    username: str = Field(..., min_length=1, max_length=50, description="User's LDAP username")
    password: str = Field(..., min_length=1, description="User's LDAP password")

class UserInfo(pydantic.BaseModel):
    username: str
    name: str

class AuthSuccessResponse(pydantic.BaseModel):
    success: bool
    user: UserInfo

class AuthStatusResponse(pydantic.BaseModel):
    authenticated: bool
    user: Optional[UserInfo] = None

class LogoutResponse(pydantic.BaseModel):
    success: bool
    message: str


async def get_current_user(authorization: Optional[str] = Header(None)) -> str:
    """
    Extract username from session token in Authorization header
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header required"
        )
    
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization format. Use 'Bearer <token>'"
        )
    
    session_token = authorization.replace("Bearer ", "")
    
    # Get username from database session
    username = get_user_from_db_session(session_token)
    
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session. Please login again."
        )
    
    return username
        

def load_employer_data_sync():
    """Sync version of employer loader for Flask compatibility"""
    global _flask_employer_cache, _flask_cache_timestamp
    
    csv_path = "/home/rajawalia3/radityayud/python_project/bot_telegram/users/user_data.csv"
    
    try:
        if os.path.exists(csv_path):
            current_mtime = os.path.getmtime(csv_path)
            
            # Return cached data if file hasn't changed
            if _flask_employer_cache is not None and _flask_cache_timestamp == current_mtime:
                return _flask_employer_cache
            
            # Load new data
            df = pd.read_csv(csv_path)
            _flask_employer_cache = df.set_index('user_id')['employer'].to_dict()
            _flask_cache_timestamp = current_mtime
            
            logger.info(f"✅ Loaded {len(_flask_employer_cache)} employer records (Flask sync)")
        else:
            logger.warning(f"❌ Employer CSV not found at {csv_path}")
            _flask_employer_cache = {}
            
    except Exception as e:
        logger.error(f"❌ Error loading employer data (Flask): {e}")
        _flask_employer_cache = {}
    
    return _flask_employer_cache


# --- Channel Access Permission Check ---
# Employers allowed to access internal Telegram channel content
CHANNEL_ALLOWED_EMPLOYERS = {
    'telkomsel', 'telkomsel one', 'tsel one', 'indihome', 'telkom',
    'telkom akses', 'telkom regional', 'rajawali'
}

def user_has_channel_access(user_id: str) -> bool:
    """
    Check if user has permission to access internal channel content.
    Uses employer data from user_data.csv to determine access.
    
    Args:
        user_id: The user identifier (Telegram ID or internal ID)
    
    Returns:
        bool: True if user's employer is in the allowed list, False otherwise
    """
    try:
        employer_data = load_employer_data_sync()
        if not employer_data:
            # If employer data unavailable, deny by default
            logger.warning(f"Channel access check: No employer data available for user {user_id}")
            return False
        
        user_employer = employer_data.get(str(user_id), '')
        if not user_employer:
            logger.debug(f"Channel access check: No employer record for user {user_id}")
            return False
        
        # Normalize employer string for comparison
        employer_lower = str(user_employer).lower().strip()
        
        # Check if employer is in allowed list
        has_access = employer_lower in CHANNEL_ALLOWED_EMPLOYERS or any(
            allowed in employer_lower for allowed in CHANNEL_ALLOWED_EMPLOYERS
        )
        
        if has_access:
            logger.debug(f"Channel access GRANTED for user {user_id} (employer: {user_employer})")
        else:
            logger.debug(f"Channel access DENIED for user {user_id} (employer: {user_employer})")
        
        return has_access
        
    except Exception as e:
        logger.error(f"Error checking channel access for user {user_id}: {e}")
        return False


# ----------------------------------------------------------------------------------------------------------------------------------- #
#  MAIN: FAST QUERY Processor
# ----------------------------------------------------------------------------------------------------------------------------------- #
class FastQueryProcessor:
    """
    Ultra-fast query processor with enhanced fuzzy matching.
    fully driven by query_config.py for singular-source maintenance.
    """
    
    def __init__(self):
        # 1. Load Configurations directly from the module
        self.stop_words = STOP_WORDS
        self.feature_keywords = FEATURE_KEYWORDS
        self.known_cities = KNOWN_CITIES
        self.provider_lookup = PROVIDER_LOOKUP
        self.location_lookup = LOCATION_LOOKUP
        self.provider_exclusions = set(PROVIDER_EXTRACTION_EXCLUSIONS)
        self.national_exclusions = NATIONAL_PROVIDER_EXCLUSIONS
        self.limits = PERFORMANCE_LIMITS

        # 1b. Load routing config from YAML
        self.routing_config = self._load_routing_config()

        # 2. Compile Patterns dynamically from Config
        self.compiled_patterns = self._compile_patterns()

        # 3. Cleanup patterns (kept efficient and generic)
        self.prefix_cleanup_pattern = re.compile(
            r'^\b(?:kota|kabupaten|kab|daerah|kotamadya|kt|wilayah|area)\b\s*',
            re.IGNORECASE
        )
        self.suffix_cleanup_pattern = re.compile(
            r'\s*\b(?:city|kota|area|district|wilayah|regency)\b$',
            re.IGNORECASE
        )
        self.provider_cleanup_pattern = re.compile(r'\b(?:pt|cv|tbk)\b\s*', re.IGNORECASE)

    @staticmethod
    def _load_routing_config() -> Dict:
        """Load routing signals from routing_config.yaml."""
        import yaml
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'routing_config.yaml')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to load routing_config.yaml: {e}")
            return {}

    # ------------------- Canonical Normalization and Helper Methods -------------------

    def normalize_location(self, location_str: str) -> str:
        """
        Canonical function to normalize a location string.
        """
        if not location_str or not isinstance(location_str, str):
            return ""

        # Basic cleaning
        normalized = location_str.lower().strip()
        normalized = normalized.replace('-', ' ').replace('_', ' ')

        # Remove prefixes/suffixes
        normalized = self.prefix_cleanup_pattern.sub('', normalized)
        normalized = self.suffix_cleanup_pattern.sub('', normalized)

        # Collapse spaces
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized

    def check_location_match(self, db_location_str: str, query_location_str: str) -> bool:
        """
        Checks if a database location matches a query string using canonical lookup.
        """
        if not db_location_str or not query_location_str:
            return False
        
        # Resolve both to their canonical forms using the Config Lookup
        db_canonical = self._fuzzy_match_location(db_location_str)
        query_canonical = self._fuzzy_match_location(query_location_str)
        
        return db_canonical is not None and db_canonical == query_canonical

    def find_matching_segment(self, db_locations_str: str, query_location: str) -> Optional[str]:
        """
        Parses a comma-separated DB string and returns the specific segment 
        that matches the query.
        """
        if not db_locations_str or not query_location:
            return None
            
        # Split by comma and check each part using the processor's logic
        segments = [s.strip() for s in db_locations_str.split(',')]
        for segment in segments:
            if self.check_location_match(segment, query_location):
                return segment
        return None

    def format_locations_for_llm(self, db_locations_str: str, query_location_str: Optional[str] = None) -> str:
        """
        Formats DB location strings for LLM consumption.
        """
        if not db_locations_str or not isinstance(db_locations_str, str) or db_locations_str.upper() in ['N/A', 'NULL', '']:
            return "Area cakupan: Tidak disebutkan"

        db_locations_list = [loc.strip() for loc in db_locations_str.split(',') if loc.strip()]
        if not db_locations_list:
            return "Area cakupan: Tidak disebutkan"

        # Try to find the specific queried location to highlight it
        if query_location_str:
            matching_locations = [
                db_loc for db_loc in db_locations_list 
                if self.check_location_match(db_loc, query_location_str)
            ]
            
            if matching_locations:
                primary_match = matching_locations[0].title()
                total_count = len(db_locations_list)
                if total_count > 1:
                    return f"Area cakupan: {primary_match} dan {total_count-1} kota lainnya"
                else:
                    return f"Area cakupan: {primary_match}"

        # Default formatting
        sample_size = 3
        total_count = len(db_locations_list)
        if total_count <= sample_size:
            return f"Area cakupan: {', '.join(loc.title() for loc in db_locations_list)}"
        else:
            sample = [loc.title() for loc in db_locations_list[:sample_size]]
            return f"Area cakupan: {', '.join(sample)} dan {total_count-sample_size} kota lainnya"

    # ------------------- Internal Matching & Analysis Logic -------------------

    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """
        Compiles all regex patterns defined in query_config.py.
        """
        compiled = {}
        
        # Compile Intent Patterns
        for intent, patterns in INTENT_PATTERNS.items():
            # Combine list of strings into one OR regex
            combined_pattern = '|'.join(f'({pattern})' for pattern in patterns)
            compiled[intent] = re.compile(combined_pattern, re.IGNORECASE)
        
        # Compile Speed Patterns
        speed_pattern = '|'.join(f'({pattern})' for pattern in SPEED_PATTERNS)
        compiled['speeds'] = re.compile(speed_pattern, re.IGNORECASE)
        
        # Compile Provider Pattern
        compiled['providers'] = re.compile(PROVIDER_PATTERN, re.IGNORECASE)
        
        # Compile Location Patterns
        location_pattern = '|'.join(f'({pattern})' for pattern in LOCATION_PATTERNS)
        compiled['locations'] = re.compile(location_pattern, re.IGNORECASE)
        
        return compiled

    def _fuzzy_match_provider(self, query_word: str) -> str:
        """
        Matches provider names using the centralized PROVIDER_LOOKUP from config.
        """
        query_clean = query_word.lower().strip()
        
        # 1. Direct Lookup
        if query_clean in self.provider_lookup:
            return self.provider_lookup[query_clean]
        
        # 2. Cleaned Lookup (remove PT/CV)
        query_cleaned_corp = self.provider_cleanup_pattern.sub('', query_clean).strip()
        if query_cleaned_corp in self.provider_lookup:
            return self.provider_lookup[query_cleaned_corp]
        
        # 3. Substring/Reverse Lookup (expensive fallback, restricted length)
        # Note: Most variations should be in PROVIDER_LOOKUP already.
        if len(query_clean) >= 4:
            for provider_var, canonical in self.provider_lookup.items():
                if query_clean in provider_var or provider_var in query_clean:
                    return canonical
                    
        return None

    def _fuzzy_match_location(self, query_word: str) -> str:
        """
        Matches locations using the centralized LOCATION_LOOKUP from config.
        """
        # Normalize inputs
        query_normalized = self.normalize_location(query_word)
        if not query_normalized:
            return None
        
        # 1. Direct Lookup (covers aliases like 'sby', 'jogja' defined in config)
        if query_normalized in self.location_lookup:
            return self.location_lookup[query_normalized]
        
        # 2. Spaceless Lookup (covers 'kulonprogo' vs 'kulon progo')
        if ' ' in query_normalized:
            no_space_version = query_normalized.replace(' ', '')
            if no_space_version in self.location_lookup:
                return self.location_lookup[no_space_version]

        # 3. Partial/Prefix Lookup for valid cities
        if len(query_normalized) >= 4:
            for city_var, canonical in self.location_lookup.items():
                if query_normalized == city_var:
                    return canonical
                # Be careful with partials (e.g., 'bandung' matching 'bandung barat')
                # Only match if it starts with the query and implies a variation
                if city_var.startswith(query_normalized) and len(city_var) < len(query_normalized) + 8:
                   return canonical
        
        return None

    def _score_signals(self, query_lower: str, signals: list, score_per_match: int) -> int:
        """Score a list of signal strings against the query. Supports regex (\\b prefix)."""
        total = 0
        import re as _re
        for trigger in signals:
            if trigger.startswith('\\b'):
                if _re.search(trigger, query_lower):
                    total += score_per_match
            elif trigger in query_lower:
                total += score_per_match
        return total

    def determine_routing(self, query: str, fast_results: Dict) -> tuple:
        """
        Determines RAG mode using scoring based on routing_config.yaml.
        Supports 3 modes: document, market, data_insight.
        Returns: (mode, total_score, strong_score)
        """
        query_lower = query.lower()
        market_score = 0
        doc_score = 0
        data_insight_score = 0

        rc = self.routing_config
        di_cfg = rc.get('data_insight', {})
        mkt_cfg = rc.get('market', {})
        doc_cfg = rc.get('document', {})

        # --- DATA INSIGHT scoring from YAML config (split by tier) ---
        di_strong_score = self._score_signals(
            query_lower, di_cfg.get('strong_signals', []), di_cfg.get('strong_score', 25))
        di_medium_score = self._score_signals(
            query_lower, di_cfg.get('medium_signals', []), di_cfg.get('medium_score', 10))
        di_weak_score = self._score_signals(
            query_lower, di_cfg.get('weak_signals', []), di_cfg.get('weak_score', 5))
        data_insight_score = di_strong_score + di_medium_score + di_weak_score

        # --- LOCATION / COMPETITOR override logic ---
        has_strict_location = any(city in query_lower for city in self.known_cities)
        has_regex_location = bool(fast_results.get('locations')) and not (
            data_insight_score > 0 and not has_strict_location
        )
        has_location = has_strict_location or has_regex_location

        competitor_triggers = mkt_cfg.get('competitor_triggers', [])
        is_comparison = any(trigger in query_lower for trigger in competitor_triggers) or 'comparison' in fast_results['intents']

        has_strong_data_insight = data_insight_score >= 25
        competitor_providers = [p for p in fast_results.get('providers', []) if p.lower() not in OWN_BRAND_PROVIDERS]
        # "bandingkan" without competitor providers + strong DI signals = data comparison
        # (e.g., comparing time periods or regions), not a competitor comparison
        has_explicit_market_signal = bool(competitor_providers) or (
            is_comparison and not (has_strong_data_insight and not competitor_providers)
        )
        own_brand_only = (
            bool(fast_results.get('providers', []))
            and not competitor_providers
            and not has_explicit_market_signal
        )

        if has_location or is_comparison:
            if has_strong_data_insight and not has_explicit_market_signal:
                data_insight_score += 10
                market_score += 10
            else:
                market_score += 50

        # --- DOCUMENT: policy triggers ---
        policy_triggers = doc_cfg.get('policy_triggers', [])
        is_policy_query = any(trigger in query_lower for trigger in policy_triggers)
        if is_policy_query:
            if market_score > 0 or data_insight_score >= 25:
                pass  # let scores decide
            else:
                return 'document', 0, 0

        # --- MARKET scoring ---
        if 'comparison' in fast_results['intents'] or 'price_inquiry' in fast_results['intents']:
            market_score += 25
        if 'local_provider_inquiry' in fast_results['intents']:
            market_score += 30
        if competitor_providers:
            market_score += 20
        if has_location:
            market_score += 15
        if any(word in query_lower for word in mkt_cfg.get('generic_signals', [])):
            market_score += mkt_cfg.get('generic_score', 5)

        # --- DOCUMENT scoring ---
        if any(trigger in query_lower for trigger in doc_cfg.get('doc_triggers', [])):
            doc_score += doc_cfg.get('doc_score', 15)
        if any(trigger in query_lower for trigger in doc_cfg.get('knowledge_triggers', [])):
            doc_score += doc_cfg.get('knowledge_score', 40)

        # --- OWN-BRAND BIAS: queries mentioning only our brands → document ---
        if own_brand_only:
            doc_score += 10

        # --- Final Decision ---
        max_score = max(market_score, doc_score, data_insight_score)
        if max_score == 0:
            return 'document', 0, 0
        if data_insight_score > max(market_score, doc_score):
            return 'data_insight', data_insight_score, di_strong_score
        if market_score > doc_score:
            return 'market', market_score, 0
        return 'document', doc_score, 0

    def analyze_fast(self, query: str) -> Dict:
        """
        Single-pass analysis.
        """
        query_lower = query.lower()
        result = {
            'asking_for_locations': False, 
            'asking_for_providers': False,
            'is_local_search': False,
            'providers': [], 
            'speeds': [], 
            'locations': [], 
            'keywords': [],
            'search_mode': 'standard', 
            'intents': [], 
            'confidence': 1.0
        }
        
        # 1. Detect Intents
        detected_intents = []
        for intent, pattern in self.compiled_patterns.items():
            if intent in INTENT_PATTERNS and pattern.search(query_lower):
                detected_intents.append(intent)
        result['intents'] = detected_intents
        
        # 2. Set Flags based on Intent
        is_all_providers = 'all_providers_inquiry' in detected_intents
        is_local_inquiry = 'local_provider_inquiry' in detected_intents
        
        if is_local_inquiry:
            result['is_local_search'] = True
            result['asking_for_providers'] = True # Implicitly asking for providers
            result['search_mode'] = 'provider_discovery'
        elif 'location_inquiry' in detected_intents:
            result['asking_for_locations'] = True
            result['search_mode'] = 'location_discovery'
        elif 'provider_discovery' in detected_intents:
            result['asking_for_providers'] = True
            result['search_mode'] = 'provider_discovery'
        elif 'comparison' in detected_intents:
            result['search_mode'] = 'comparison'
        elif 'price_inquiry' in detected_intents:
            result['search_mode'] = 'price_focused'
        
        # 3. Extract Providers
        # Don't extract if user asks "Apa saja provider" (generic), unless they specify others
        if not (result['asking_for_providers'] and len(detected_intents) == 1):
            provider_matches = self.compiled_patterns['providers'].findall(query_lower)
            if provider_matches:
                flat_matches = [match for group in provider_matches for match in group if match]
                for match in flat_matches:
                    if match.lower() not in self.provider_exclusions:
                        canonical = self._fuzzy_match_provider(match)
                        if canonical and canonical not in result['providers']:
                            result['providers'].append(canonical)
        
        # 4. Handle "Local" Logic (The 'Lokal' Filter)
        # If user asks for "Provider Lokal", we purposefully EXCLUDE the big national ones
        # from the search filter to ensure the RAG retrieves the smaller local players.
        if result['is_local_search']:
            # Filter out national providers from the extracted list (if any were found)
            result['providers'] = [
                p for p in result['providers'] 
                if p not in self.national_exclusions
            ]
        
        # 5. Extract Speeds
        speed_matches = self.compiled_patterns['speeds'].findall(query_lower)
        if speed_matches:
            flat_speeds = [match for group in speed_matches for match in group if match]
            for speed_str in flat_speeds:
                if speed_str.isdigit():
                    speed = int(speed_str)
                    if (self.limits['speed_range_min'] <= speed <= self.limits['speed_range_max'] 
                        and speed not in result['speeds']):
                        result['speeds'].append(speed)
            result['speeds'] = result['speeds'][:self.limits['max_speeds']]
        
        # 6. Extract Locations
        if not result['asking_for_locations']:
            location_matches = self.compiled_patterns['locations'].findall(query_lower)
            # Strategy: Trust Regex first, then fuzzy fallback
            found_locations = []
            
            # Regex Matches
            if location_matches:
                flat_locations = [match for group in location_matches for match in group if match]
                for loc in flat_locations:
                    canonical = self._fuzzy_match_location(loc.strip())
                    if canonical and canonical not in found_locations:
                        found_locations.append(canonical)
            
            # Fallback: Scan words if no regex match (for simple queries like "harga di jember")
            if not found_locations:
                words = re.findall(r'\b[a-zA-Z\s\-]{3,20}\b', query_lower)
                for word in words:
                    # Skip stop words
                    if word.lower() in self.stop_words: continue
                    canonical = self._fuzzy_match_location(word.strip())
                    if canonical and canonical not in found_locations:
                        found_locations.append(canonical)
            
            result['locations'] = found_locations[:self.limits['max_locations']]
        
        # 7. Extract Keywords (Fallback)
        if not any([result['providers'], result['speeds'], result['locations']]):
            words = re.findall(r'\b[a-zA-Z]{3,}\b', query_lower)
            keywords = []
            for word in words:
                if (word not in self.stop_words and 
                    word not in self.provider_exclusions and
                    self.limits['min_keyword_length'] <= len(word) <= self.limits['max_keyword_length'] and
                    word not in keywords):
                    keywords.append(word)
                    if len(keywords) >= self.limits['max_keywords']:
                        break
            result['keywords'] = keywords
        
        # 8. Calculate Confidence
        confidence_factors = []
        if result['providers']: confidence_factors.append(0.4)
        if result['speeds']: confidence_factors.append(0.3)  
        if result['locations']: confidence_factors.append(0.2)
        if result['intents']: confidence_factors.append(0.1)
        
        result['confidence'] = sum(confidence_factors) if confidence_factors else 0.1
        
        return result

    def extract_single_matching_location(db_locations_str: str, query_location: str, processor) -> Optional[str]:
        """
        From a comma-separated list of DB locations, find the first segment
        that matches the user's query_location.

        Args:
            db_locations_str (str): "KOTA SURABAYA, KOTA MALANG, KABUPATEN SIDOARJO"
            query_location (str): "surabaya"
            processor (FastQueryProcessor): The processor instance.

        Returns:
            Optional[str]: "KOTA SURABAYA" if a match is found, otherwise None.
        """
        if not db_locations_str or not query_location:
            return None

        # Use the processor's robust matching logic
        db_location_segments = [loc.strip() for loc in db_locations_str.split(',')]
        for segment in db_location_segments:
            if processor.check_location_match(segment, query_location):
                return segment  # Return the original cased segment from the DB
                
        return None


def normalize_location_data_for_llm(products, query_location=None, processor=None, analysis=None):
    if not products: return []
    if not processor: processor = get_processor_instance()
    
    # Handle backward compatibility for location input
    if analysis and 'location' in analysis:
        query_locations = analysis['location']
    else:
        query_locations = [query_location] if query_location else []
        
    normalized_products = []
    
    for product in products:
        normalized_product = product.copy()
        is_available = False
        display_location_for_llm = "Area cakupan tidak disebutkan"

        # --- DPD Table Logic (Comma-Separated) ---
        if 'original_locations_dpd' in product and pd.notna(product.get('original_locations_dpd')):
            locations_raw = str(product.get('original_locations_dpd', ''))
            if query_locations and locations_raw:
                matched_cities = []
                for query_loc in query_locations:
                    # Uses the new method you added in Step 1
                    matched_city = processor.find_matching_segment(locations_raw, query_loc)
                    if matched_city and matched_city not in matched_cities:
                        matched_cities.append(matched_city)
                
                if matched_cities:
                    if len(matched_cities) == 1:
                        display_location_for_llm = f"Tersedia di {matched_cities[0]}"
                    else:
                        display_location_for_llm = f"Tersedia di {', '.join(matched_cities[:-1])} dan {matched_cities[-1]}"
                    is_available = True
            elif locations_raw:
                display_location_for_llm = processor.format_locations_for_llm(locations_raw, None)

        # --- DPM Table Logic (Single Location) ---
        elif 'original_locations_dpm' in product and pd.notna(product.get('original_locations_dpm')):
            location_raw = str(product.get('original_locations_dpm', ''))
            if query_locations and location_raw:
                matched_cities = []
                for query_loc in query_locations:
                    if processor.check_location_match(location_raw, query_loc):
                        if location_raw not in matched_cities:
                            matched_cities.append(location_raw)
                
                if matched_cities:
                    display_location_for_llm = f"Tersedia di {matched_cities[0]}"
                    is_available = True
            elif location_raw:
                display_location_for_llm = processor.format_locations_for_llm(location_raw, None)
        
        normalized_product['location_display'] = display_location_for_llm
        normalized_product['location_available'] = is_available
        
        # Standardize Price and Name
        price = product.get('price')
        price_unit = product.get('price_unit', 'Bulan')
        if price:
            try: 
                normalized_product['price_formatted'] = f"Rp {int(price):,}/{price_unit}"
            except (ValueError, TypeError): 
                normalized_product['price_formatted'] = f"{price}/{price_unit}"
        else: 
            normalized_product['price_formatted'] = "Harga tidak disebutkan"
        
        product_name = product.get('product_name', '')
        cleaned_name = re.sub(r'\s*\([^)]*\)\s*', ' ', product_name).strip()
        normalized_product['product_name_clean'] = re.sub(r'\s+', ' ', cleaned_name) if len(cleaned_name) >= 10 else product_name
        
        # --- FIXED LOCATION SUMMARY LOGIC ---
        if query_locations:
            # We must check DPD (using find_matching_segment) AND DPM (using check_location_match)
            available_in = []
            for loc in query_locations:
                is_in_dpd = False
                is_in_dpm = False
                
                # Check DPD (split check)
                if 'original_locations_dpd' in product:
                    if processor.find_matching_segment(str(product.get('original_locations_dpd') or ''), loc):
                        is_in_dpd = True

                # Check DPM (direct check)
                if 'original_locations_dpm' in product:
                    if processor.check_location_match(str(product.get('original_locations_dpm') or ''), loc):
                        is_in_dpm = True
                
                if is_in_dpd or is_in_dpm:
                    available_in.append(loc.title())

            if available_in:
                if len(available_in) == 1:
                    normalized_product['location_summary'] = f"✓ Tersedia di {available_in[0]}"
                else:
                    normalized_product['location_summary'] = f"✓ Tersedia di {' dan '.join(available_in)}"
            else:
                normalized_product['location_summary'] = f"✗ Tidak tersedia di {' atau '.join(loc.title() for loc in query_locations)}"
        else:
            normalized_product['location_summary'] = display_location_for_llm

        normalized_products.append(normalized_product)
    
    return normalized_products


def process_database_results_for_llm(products_raw, analysis):
    """
    Main function to process database results before sending to LLM.
    Now supports multi-location queries while maintaining backward compatibility.
    """
    if not products_raw:
        return []
    
    processor = get_processor_instance()
    
    # BACKWARD COMPATIBLE: Pass full locations list to the context
    locations_list = analysis.get('location', [])
    query_location = locations_list[0] if locations_list else None  # Keep for backward compatibility
    
    # Pass the analysis context so normalization can access all locations
    normalized_products = normalize_location_data_for_llm(products_raw, query_location, processor, analysis)
    
    logger.debug(f"Processed {len(products_raw)} products for LLM. Query location: {query_location}")
    if normalized_products:
        sample = normalized_products[0]
        logger.debug(f"Sample normalized product: available={sample.get('location_available')}, summary='{sample.get('location_summary')}', display='{sample.get('location_display')}'")
    
    return normalized_products


_processor_instance = None
_processor_lock = threading.Lock()

def get_processor_instance():
    """Get singleton processor instance to avoid re-compilation overhead."""
    global _processor_instance
    if _processor_instance is None:
        with _processor_lock:
            if _processor_instance is None:  # double-checked locking
                _processor_instance = FastQueryProcessor()
    return _processor_instance


# --- Data Insight Engine Singleton --- #
_data_insight_engine_instance = None
_data_insight_engine_lock = Lock()

def get_data_insight_engine() -> DataInsightEngine:
    """Get singleton DataInsightEngine instance."""
    global _data_insight_engine_instance
    if _data_insight_engine_instance is None:
        with _data_insight_engine_lock:
            if _data_insight_engine_instance is None:
                _data_insight_engine_instance = DataInsightEngine()
    return _data_insight_engine_instance


def _detect_followup_query(query_lower: str) -> bool:
    """
    Detect if a query is a follow-up that references previous conversation context.
    Uses word-boundary-aware matching to avoid false positives.

    Returns True if the query contains anaphoric references, drill-down commands,
    or other follow-up indicators.
    """
    import re

    # --- Group 1: Short tokens that need word-boundary matching (avoid substring false positives) ---
    # e.g. "it" must not match "item", "audit"; "nya" must not match "kenya"
    boundary_tokens = [
        r'\bthem\b', r'\btheir\b', r'\bthose\b', r'\bthese\b',
        r'\bits\b', r'\bit\b',
        # Indonesian pronouns/suffixes (avoid standalone "itu"/"ini" — too common in first-turn queries)
        r'\bmereka\b',
        r'nya\b',  # -nya suffix: "datanya", "hasilnya", "tabelnya" (must end at word boundary)
    ]
    for pattern in boundary_tokens:
        if re.search(pattern, query_lower):
            return True

    # --- Group 2: Multi-word phrases (safe for substring match — low false positive risk) ---
    phrase_triggers = [
        # English follow-up phrases
        'group them', 'sort them', 'filter them', 'show them',
        'based on the', 'from the previous', 'from earlier',
        'the same data', 'the same query', 'the result',
        # Indonesian follow-up phrases
        'data tadi', 'hasil tadi', 'tabel tadi', 'grafik tadi',
        'yang tadi', 'yang sebelumnya', 'yang barusan',
        'dari data', 'dari hasil', 'dari tabel',
        'tampilkan ulang', 'coba tampilkan', 'coba kelompokkan',
        'sekarang coba', 'sekarang tampilkan', 'sekarang filter',
        'gimana kalau', 'bagaimana kalau', 'bagaimana jika',
        'tambahkan filter', 'tambah filter', 'ganti ke',
        'ubah jadi', 'ubah ke', 'format ulang',
        'lanjutkan', 'teruskan', 'lanjut ke',
        'data di atas', 'tabel di atas', 'grafik di atas',
        'yang di atas', 'hasil di atas',
    ]
    for phrase in phrase_triggers:
        if phrase in query_lower:
            return True

    # --- Group 3: Drill-down / manipulation commands (often follow-ups) ---
    # These are actions that typically operate on previously shown data
    drilldown_triggers = [
        'kelompokkan', 'grupkan', 'group by',
        'urutkan', 'sort by', 'order by',
        'filter by', 'filter berdasarkan',
        'breakdown per', 'detail per', 'rincian per',
        'drill down', 'lebih detail', 'lebih spesifik',
        'per region', 'per branch',  # often follow-ups to aggregate data
        'bandingkan dengan', 'compare with', 'compare to',
        'exclude', 'kecualikan', 'hapus yang',
        'top 5 dari', 'top 10 dari',
        'export', 'download',
    ]
    for trigger in drilldown_triggers:
        if trigger in query_lower:
            return True

    return False


async def llm_fallback_routing(query: str) -> str:
    """
    LLM fallback for ambiguous routing when regex scores are close.
    Uses a fast Gemini model to classify the query.
    """
    try:
        prompt = f"""Given this user query: "{query}"

First, identify the SUBJECT MATTER of the query (product details, policies, metrics, competitive landscape, etc.)
Then classify into exactly one category based on which data source would answer it:

- "document": Internal knowledge base — policies, SOPs, regulations, strategic info, product catalog details, technical guides, internal documents, program/marketing materials. This includes questions about our own brands (IndiHome, Telkomsel, Tsel One, EZnet, IH Lite) product offerings, bundling, add-ons, and internal procedures.
- "market": Scraped market intelligence database — competitor product listings, pricing/speed comparisons across ISPs, coverage data, gimmick comparisons, market landscape analysis.
- "data_insight": Internal analytics database — customer counts, revenue, sales figures, churn rates, performance metrics, regional breakdowns.

Reply with only the category name."""

        response = await asyncio.to_thread(
            genai.GenerativeModel("gemini-2.0-flash").generate_content, prompt
        )
        result = response.text.strip().lower().replace('"', '').replace("'", "")
        if result in ('document', 'market', 'data_insight'):
            logger.info(f"LLM fallback routing: '{query[:50]}' -> {result}")
            return result
    except Exception as e:
        logger.warning(f"LLM fallback routing failed: {e}")
    return 'document'  # safe default


# Low-confidence threshold: if the winning score is below this, use LLM to verify
_ROUTING_CONFIDENCE_THRESHOLD = 30


async def semantic_routing_boost(query: str) -> Optional[str]:
    """
    Intermediate routing tier between keyword scoring and LLM fallback.
    Embeds the query and checks tbl_doc_summaries for high-confidence document match.
    Returns 'document' if confident, None to fall through to LLM.
    """
    try:
        if not tbl_doc_summaries:
            return None
        api_key = get_effective_gemini_api_key()
        if not api_key:
            return None
        logger.info("Semantic routing embedding call starting")
        genai.configure(api_key=api_key)
        q_vec = genai.embed_content(
            model=EMBEDDING_MODEL,
            content=query,
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=768
        )['embedding']
        results = tbl_doc_summaries.search(q_vec).limit(3).to_list()
        if results:
            top_score = 1 - results[0]['_distance']
            if top_score >= 0.78:
                logger.info(f"Semantic routing boost: top doc sem={top_score:.4f} → document")
                return 'document'
    except Exception as e:
        logger.warning(f"Semantic routing boost failed: {e}")
    return None


async def smart_route(query: str, processor, fast_analysis: Dict) -> tuple:
    """
    Three-stage router:
    1. Fast regex scoring via determine_routing()
    2. If winning score < threshold (low confidence) and no market signals: semantic embedding check
    3. If market signals present or semantic confidence insufficient: LLM fallback
    Returns: (mode, score, strong_score, original_mode)
        original_mode is the regex-detected mode before LLM override (None if no override)
    """
    mode, score, strong_score = processor.determine_routing(query, fast_analysis)

    if query.strip().lower() in {"test", "tes", "testing", "hello", "hi", "halo", "ping", "cek", "check"}:
        logger.info(f"Smart Router: placeholder query '{query}' detected, defaulting to market without LLM escalation")
        return 'market', score, strong_score, None

    if score < _ROUTING_CONFIDENCE_THRESHOLD:
        # Guard: skip semantic boost if there are meaningful market signals
        competitor_providers_check = [
            p for p in fast_analysis.get('providers', [])
            if p.lower() not in OWN_BRAND_PROVIDERS
        ]
        has_market_signal = (
            bool(competitor_providers_check) or
            bool(fast_analysis.get('locations', [])) or
            'local_provider_inquiry' in fast_analysis.get('intents', [])
        )

        if not has_market_signal:
            semantic_mode = await semantic_routing_boost(query)
            if semantic_mode:
                logger.info(f"Smart Router: Semantic boost '{mode}' -> '{semantic_mode}'")
                return semantic_mode, 0, 0, mode  # no LLM needed

        # Fall back to LLM (market queries or low semantic confidence)
        logger.info(f"Smart Router: low confidence (score={score}, mode={mode}). "
                    f"Escalating to LLM for: '{query[:50]}'")
        llm_mode = await llm_fallback_routing(query)
        logger.info(f"Smart Router: LLM override '{mode}' -> '{llm_mode}'")
        return llm_mode, 0, 0, mode

    return mode, score, strong_score, None  # no override


def _detect_query_language(query: str) -> str:
    """If query contains any Indonesian marker words → 'id', else 'en'."""
    id_markers = {'berapa', 'dari', 'untuk', 'per', 'bulan', 'tahun', 'yang',
                  'dan', 'ini', 'itu', 'apa', 'bagaimana', 'tampilkan', 'tolong',
                  'lihat', 'cari', 'saya', 'bisa', 'ingin', 'apakah', 'mana'}
    return 'id' if any(w in query.lower().split() for w in id_markers) else 'en'


def _build_confirmation_html(query: str, lang: str) -> str:
    """Build confirmation prompt HTML with interactive mode-selection buttons."""
    labels = {
        'id': {
            'msg': 'Kami mendeteksi Anda ingin melakukan <strong>Analisis Data</strong>. Apakah benar?',
            'yes': 'Ya, Lanjutkan', 'alt': 'Bukan, saya ingin...',
            'doc': 'Pencarian Dokumen', 'mkt': 'Market Intelligence'
        },
        'en': {
            'msg': 'It looks like you want to perform <strong>Data Analysis</strong>. Is that correct?',
            'yes': 'Yes, Continue', 'alt': 'No, I want...',
            'doc': 'Document Search', 'mkt': 'Market Intelligence'
        }
    }[lang]

    import html as _html
    safe_query = _html.escape(query, quote=True)

    return f'''<!-- ROUTE_CONFIRM:data_insight -->
<div class="route-confirm-card" data-original-query="{safe_query}">
  <div class="route-confirm-icon"><i class="fas fa-database"></i></div>
  <p class="route-confirm-message">{labels['msg']}</p>
  <div class="route-confirm-actions">
    <button class="route-confirm-btn route-confirm-yes" data-mode="data_insight">
      <i class="fas fa-check"></i> {labels['yes']}
    </button>
    <div class="route-confirm-alt">
      <button class="route-confirm-btn route-confirm-other" data-mode="document">
        <i class="fas fa-file-alt"></i> {labels['doc']}
      </button>
      <button class="route-confirm-btn route-confirm-other" data-mode="market">
        <i class="fas fa-store"></i> {labels['mkt']}
      </button>
    </div>
  </div>
</div>'''


# --- Model Selection for Market Mode --- #
LLM_TO_USE_MARKET = STANDARD_MODEL


# --- Prompt Templates --- #
GEMINI_CONSOLIDATED_MARKET_PROMPT_TEMPLATE = """
**Role:** Senior Telecommunications Market Analyst (Indonesia).
**Objective:** Help users understand competitive landscape, uncover insights, and think strategically about the ISP/telco market.

**⚠️ PRIORITY OVERRIDE:** 
If the user asks for a **simple comparison, direct answer, or general industry question**, respond naturally without forcing deep analysis structure.

**Input Context:**
*   `QUERY`: {query}
*   `HISTORY`: {conversation_history}
*   `MARKET DATA`:
{multi_provider_data_section}
=== END DATA ===

**Core Analysis Protocols:**

1.  **Data-First, Context-Enhanced:**
    - Use provided `MARKET DATA` as primary source for specific claims (prices, speeds, packages)
    - You MAY add general Indonesian telco industry context (market trends, typical practices) when helpful
    - **NEVER fabricate specific product data** not present in the provided data

2.  **Honesty About Gaps:**
    - If data is incomplete or missing, say so directly
    - Don't pretend certainty when data doesn't support it
    - Phrases like "berdasarkan data yang tersedia" or "data menunjukkan" are good

3.  **Talk Like a Colleague:**
    - AVOID introductory sentences like "Sebagai Senior Strategic Business Analyst..."
    - Be insightful, not robotic
    - Use strategic framing: implications, opportunities, risks

**Output Requirements:**

1.  **Analysis (STRICT HTML ONLY - NO MARKDOWN):**
    - Wrap your response in `<div class="market-analysis-container"><div class="analysis-summary">...</div></div>`
    - **FORBIDDEN:** Do NOT use any Markdown syntax:
      - NO `#`, `##`, `###`, `####` for headers → Use `<h3>`, `<h4>` instead
      - NO `*` or `-` for bullet points → Use `<ul><li>...</li></ul>` instead
      - NO `|` pipe tables → Use `<table><tr><td>...</td></tr></table>` instead
      - NO `**text**` for bold → Use `<strong>` or `<b>` instead
      - NO `*text*` for italic → Use `<em>` or `<i>` instead
    - Use proper HTML tags: `<p>`, `<h3>`, `<h4>`, `<ul>`, `<li>`, `<table>`, `<tr>`, `<td>`, `<th>`, `<strong>`, `<em>`
    - **Be Concistent** with inline styles. Keep table-border thickness uniform and light (e.g: 1px)
    - Tables MUST use inline styles for borders: `<table style="width:100%; border-collapse: collapse;"><tr><th style="border: 1px solid #ccc; padding: 8px;">...</th></tr></table>`

2.  **Structured Data Blocks (when applicable):**
    - If your analysis references specific products/promos from the data, output them after your HTML using this format:
    
    For products:
    ```
    ITEM_START
    Provider: [name]
    Nama Paket: [package name]
    Kecepatan: [number] Mbps
    Harga: [number only]
    Price Unit: [/bulan or other]
    Area Cakupan: [location]
    Benefits/Gimmicks: [benefits]
    Event Date: [YYYY-MM-DD]
    File Type: product
    ITEM_END
    ```
    
    For promos:
    ```
    ITEM_START
    Provider: [name]
    Promo Title: [title]
    Content Summary: [summary]
    Start Date: [YYYY-MM-DD]
    End Date: [YYYY-MM-DD]
    Promo URL: [url]
    File Type: promo
    ITEM_END
    ```
    
    - **Skip this section entirely** if no specific data items are relevant to the query

3.  **Key Findings (for system memory):**
    - Always end with this block:
    ```
    KEY_FINDINGS_START
    - [Specific finding with numbers when available]
    - [2-5 bullet points total]
    KEY_FINDINGS_END
    ```

**Response:**
"""


GEMINI_DOCUMENT_PROMPT_TEMPLATE = r"""
**Role:** Senior Strategic Business Analyst (Telkomsel).
**Objective:** Provide a deep-dive analysis that synthesizes hidden details and strategic implications.

**TODAY'S DATE: {current_date}**

**⚠️ PRIORITY OVERRIDE:**
If the user asks for a **calculation, formula correction, direct answer, or out-of-persona questions**, you MUST **STOP** the "Deep-dive Analysis" behavior. Simply execute the user's specific request precisely.

**Input Context:**
*   `QUERY`: {query}
*   `HISTORY`: {conversation_history}
*   `DOCUMENTS`:
{document_context_section}
=== END DOCUMENTS ===

**Core Analysis Protocols:**

1.  **Relevance Check:**
    - **VERIFY** if `DOCUMENTS` answer the `QUERY`.
    - **If Irrelevant:** State clearly: "Dokumen yang tersedia membahas [Topik Dokumen], namun tidak memuat informasi spesifik mengenai [Query User]." Do not hallucinate.

2.  **Data Extraction:**
    - **Exhaustive Detail:** Extract every relevant number, table row, and nuance.
    - **Visual Analysis:** Scrutinize images/charts for hard data.

3.  **Strict Citation & Hallucination Control (CRITICAL):**
    - **POLICY EXPIRY CHECK:** If a document's title or content explicitly states a program period (e.g., "Program GTM Juli 2025", "Kebijakan Februari 2026"), compare it against today's date ({current_date}). If the program period has ended, include a warning: ⚠️ *Catatan: [Nama Program] berlaku untuk [periode]. Per {current_date}, program ini kemungkinan telah berakhir. Konfirmasi ke kebijakan terbaru sebelum menggunakannya.*
    - **FILENAME TRUTH:** You MUST use the **exact filename** provided in the `FILE:` field of the Source Block. **NEVER** use the document Title (e.g., 'Playbook', 'Presentation') for citations.
    - **PAGE TRUTH:** You will see lines like `!!! SYSTEM TRUTH: ... Page X !!!`. You MUST obey this.
    - **ANTI-HALLUCINATION:** If you find product details in a block labeled Page 4, you MUST cite Page 4. NEVER cite a different page number (like Page 6) just because it appears nearby in the prompt or is mentioned in the text.
    - **ATOMIC ATTRIBUTION (TABLES):** In HTML tables, you must verify the source for *each specific row and cell*. If Row 1 data (Specs) comes from Page 4, and Row 2 data (Price) comes from Page 6, you MUST cite Page 4 in Row 1 and Page 6 in Row 2. **DO NOT** "carry over" the citation from a previous or subsequent row.
    - **DOCUMENT ISOLATION:** NEVER cite page number from Document A when data came from Document B.
    - **EXACT FILENAME RULE:** ALWAYS use the EXACT filename from the `<<< SOURCE: ... >>>` headers. NEVER reconstruct, infer, or modify filenames based on document content. If content mentions "Playbook", but the header says "Sosialisasi", cite "Sosialisasi".
    - **SYNTAX:** Cite every fact immediately: `[[Source: <exact_filename> | Page: <page_num>]]`.
    - **CHANNEL MESSAGES:** If data comes from the TELEGRAM CHANNEL CONTEXT section, cite as: `[[Source: Channel | Message: <message_id> | Date: <date>]]`. Do NOT mix channel citations with document citations.

**Technical Formatting Rules (STRICT):**
1.  **HTML ONLY:** You must **ONLY** use HTML tags for formatting (`<table>`, `<tr>`, `<td>`, `<ul>`, `<li>`).
2.  **TABLE FORMAT:** Use HTML tables (`<table>`, `<tr>`, `<td>`) for best rendering. Avoid Markdown pipe syntax.
3.  **INTRA-CELL CITATIONS:** When creating tables, place the citation `[[Source...]]` **INSIDE** the specific `<td>` tag that contains the data. 
    *   *Correct:* `<td>Gratis 2 SIMCard... [[Source: File.pdf | Page: 4]]</td>`
    *   *Wrong:* `<td>Gratis 2 SIMCard...</td> ... [[Source: File.pdf | Page: 6]]`
4.  **Multiple Citations:** If citing multiple sources, separate them with a SPACE, not a comma. Example: `[[Source: A | Page: 1]] [[Source: B | Page: 2]]`.
5.  **Structure:** Wrap output in `<div class="analysis-container">`.
6.  **MATH FORMATTING:** For calculations, **strictly** use LaTeX syntax:
    *   Inline math: `$formula$` (e.g., `$250.000 \times 2 = 500.000$`)
    *   Block math: `$$formula$$` for complex equations
    *   Example calculation block:
        ```
        $$\text{{Total}} = \text{{Harga Paket}} \times \text{{Bulan}} + \text{{PSB}}$$
        $$= Rp250.000 \times 2 + Rp89.000 = Rp589.000$$
        ```
7.  **AVOID** introductory sentences template like "Sebagai Senior Strategic Business Analyst..."
"""


# --- ConversationMemory initialization ---
conversation_memory = ConversationMemory(
    db_path=DB_PATH,
    max_history_turns=8,         # Max history turns to retrieve for prompt context (can be tuned)
    # Default timeouts and retry params are set in ConversationMemory's __init__
    # but can be overridden here if needed, e.g.:
    # default_connect_timeout=15, 
    # max_retries=2 
)
logger.info("Database-backed ConversationMemory initialized.")


# --- Conversation History Logger ---
def log_conversation_history(user_id, question, answer, log_file_path=None):
    """
    Appends a conversation turn (user question and assistant answer) to a dedicated log file.
    Args:
        user_id (str): The ID of the user.
        question (str): The user's question.
        answer (str): The assistant's answer.
        log_file_path (str, optional): Path to the log file. Defaults to 'conversation_history.log' in the current directory.
    """
    if log_file_path is None:
        log_file_path = 'conversation_history.log'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = (
        f"[{timestamp}] USER_ID: {user_id}\n"
        f"  Question: {question}\n"
        f"  Answer: {answer}\n"
        "---\n"
    )
    try:
        with open(log_file_path, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        logging.error(f"Failed to write conversation log: {e}")


# SQLite demo does not require production DB_CONFIG validation.


# --------------------------------------------------------------------------------- #
# --------------------------- Background Task Executer  --------------------------- #
# --------------------------------------------------------------------------------- #
# Updated async-compatible task executor
async def _execute_generation_task_async(job_id, target_function_name_str, args_tuple, kwargs_dict):
    """Async version of the generation task executor.
    
    Executes a specified async generation function as a background job with
    enhanced concurrency handling while maintaining backward compatibility.

    Args:
        job_id (str): The unique ID for the background job.
        target_function_name_str (str): The name of the function to execute.
        args_tuple (tuple): Positional arguments for the target function.
        kwargs_dict (dict): Keyword arguments for the target function.

    Side Effects:
        - Updates the job status ('running', 'finished', 'failed') in the DB.
        - Stores the final result payload or an error dictionary in the DB.
    """
    logger.info(f"[Async Job {job_id}] Starting execution for {target_function_name_str}")
    start_time = time.time()

    target_function = None
    if target_function_name_str == 'generate_document_response_selected':
        target_function = generate_document_response_selected
    elif target_function_name_str == 'generate_market_response_selected':
        target_function = generate_market_response_selected
    elif target_function_name_str == 'generate_data_insight_response':
        target_function = generate_data_insight_response

    if not target_function:
        error_message = f"Target function '{target_function_name_str}' not found."
        logger.error(f"[Async Job {job_id}] {error_message}")
        await asyncio.to_thread(_update_job_in_db, job_id, 'failed', {"error": error_message})
        return

    await asyncio.to_thread(_update_job_in_db, job_id, 'running', None, "🔍 Initializing request...", 5)

    final_convo_status = 'failed'
    result_payload_for_db = {"error": "An unexpected error occurred in the background task."}
    api_key_token = None

    try:
        api_key_override = kwargs_dict.get("api_key_override") if kwargs_dict else None
        if api_key_override:
            api_key_token = CURRENT_REQUEST_GEMINI_API_KEY.set(api_key_override)
        if target_function_name_str == 'generate_document_response_selected':
            await asyncio.to_thread(_update_job_in_db, job_id, 'running', None, "📄 Processing documents...", 15)
        elif target_function_name_str == 'generate_market_response_selected':
            await asyncio.to_thread(_update_job_in_db, job_id, 'running', None, "🏢 Analyzing market data...", 15)
        elif target_function_name_str == 'generate_data_insight_response':
            await asyncio.to_thread(_update_job_in_db, job_id, 'running', None, "📊 Generating data insight...", 15)
        
        # Prepare kwargs for the target function, including the job_id
        final_kwargs_for_target = kwargs_dict.copy()
        final_kwargs_for_target['job_id_for_logging'] = job_id

        # Call the async generator function
        raw_generation_result = target_function(*args_tuple, **final_kwargs_for_target)

        # Handle async generator results
        if hasattr(raw_generation_result, '__aiter__'):
            # Handle async generator case
            logger.info(f"[Async Job {job_id}] Function returned async generator - collecting response")
            await asyncio.to_thread(_update_job_in_db, job_id, 'running', None, "🔄 Collecting response...", 80)
            
            collected_chunks = []
            context_items = []
            next_suggestions = [] # <--- [FIX] Initialize suggestions container
            
            generator_return_value = None
            generator_exhausted = False
            
            try:
                logger.debug(f"[Async Job {job_id}] Starting to iterate over async generator")
                chunk_count = 0
                
                async for chunk in raw_generation_result:
                    chunk_count += 1
                    logger.debug(f"[Async Job {job_id}] Received chunk {chunk_count}: {type(chunk)} length={len(str(chunk)) if chunk else 0}")
                    
                    if isinstance(chunk, dict):
                        # Handle structured response (non-streaming final result)
                        final_response = chunk.get('answer_html', '')
                        context_items = chunk.get('context_items_for_memory', [])
                        
                        # <--- [FIX] Capture suggestions from generator output
                        next_suggestions = chunk.get('next_suggestions', []) 
                        
                        collected_chunks.append(final_response)
                        logger.info(f"[Async Job {job_id}] Received structured response")
                        break
                    elif chunk:
                        collected_chunks.append(str(chunk))
                
                logger.debug(f"[Async Job {job_id}] Async generator iteration completed normally, collected {len(collected_chunks)} chunks")
                generator_exhausted = True
                
            except StopAsyncIteration as e:
                # Capture the return value from the async generator
                generator_return_value = getattr(e, 'value', None)
                logger.info(f"[Async Job {job_id}] Async generator finished with StopAsyncIteration, return value type: {type(generator_return_value)}")
                if generator_return_value:
                    logger.debug(f"[Async Job {job_id}] Return value keys: {list(generator_return_value.keys()) if isinstance(generator_return_value, dict) else 'Not a dict'}")
                generator_exhausted = True
                
            except Exception as gen_iter_error:
                logger.error(f"[Async Job {job_id}] Exception while iterating async generator: {gen_iter_error}", exc_info=True)
                result_payload_for_db = {"error": f"Error iterating async generator: {str(gen_iter_error)}"}
            
            if generator_exhausted:
                try:
                    # Check if we got a return value (non-streaming case)
                    if generator_return_value and isinstance(generator_return_value, dict):
                        logger.info(f"[Async Job {job_id}] Using generator return value as result")
                        result_payload_for_db = generator_return_value
                        final_convo_status = 'finished'
                        
                    # Otherwise use collected chunks (streaming case)
                    elif collected_chunks:
                        final_answer_html = ''.join(collected_chunks)
                        logger.info(f"[Async Job {job_id}] Using collected chunks, total length: {len(final_answer_html)}")
                        
                        # Extract market sources from script tags and clean HTML
                        market_sources_data = None
                        cleaned_html = final_answer_html
                        
                        # Look for market sources script tags
                        script_pattern = r'<script>window\.handleMarketSources\s*&&\s*window\.handleMarketSources\s*\(\s*({.*?})\s*\);\s*</script>'
                        script_match = re.search(script_pattern, final_answer_html, re.DOTALL)
                        
                        if script_match:
                            try:
                                import json
                                market_sources_json = script_match.group(1)
                                market_sources_data = json.loads(market_sources_json)
                                # Remove the script tag from HTML
                                cleaned_html = re.sub(script_pattern, '', final_answer_html, flags=re.DOTALL).strip()
                                logger.info(f"[Async Job {job_id}] Extracted market sources data with {len(market_sources_data.get('sources', []))} sources")
                            except (json.JSONDecodeError, Exception) as e:
                                logger.warning(f"[Async Job {job_id}] Failed to parse market sources from script tag: {e}")
                        
                        result_payload_for_db = {
                            "answer_html": cleaned_html,
                            "context_items_for_memory": context_items,
                            "next_suggestions": next_suggestions # <--- [FIX] Include suggestions in final DB payload
                        }
                        
                        # Add market sources if found
                        if market_sources_data:
                            result_payload_for_db["market_sources"] = market_sources_data
                        
                        final_convo_status = 'finished'
                        
                    # No content generated
                    else:
                        logger.warning(f"[Async Job {job_id}] No content generated from async generator - no return value and no chunks")
                        logger.debug(f"[Async Job {job_id}] Generator debug: return_value={generator_return_value}, chunks_count={len(collected_chunks)}")
                        result_payload_for_db = {"error": "No content generated from generator"}
                    
                    if final_convo_status == 'finished':
                        logger.info(f"[Async Job {job_id}] Async generator execution finished successfully in {time.time() - start_time:.2f} seconds.")
                    
                except Exception as gen_error:
                    logger.error(f"[Async Job {job_id}] Error processing async generator results: {gen_error}", exc_info=True)
                    result_payload_for_db = {"error": f"Error processing async generator results: {str(gen_error)}"}
        
        elif isinstance(raw_generation_result, str) and raw_generation_result.strip().startswith("<p>"): 
            result_payload_for_db = {"error": raw_generation_result}
            logger.error(f"[Async Job {job_id}] Execution returned an error HTML string: {raw_generation_result[:200]}")
        
        elif isinstance(raw_generation_result, dict) and 'answer_html' in raw_generation_result: 
            result_payload_for_db = raw_generation_result 
            final_convo_status = 'finished'
            logger.info(f"[Async Job {job_id}] Execution finished successfully in {time.time() - start_time:.2f} seconds.")
        
        else: 
            result_payload_for_db = {"error": "Generation function returned an unexpected result format."}
            logger.error(f"[Async Job {job_id}] Generator function {target_function_name_str} returned unexpected format: {type(raw_generation_result)}")

    except Exception as e:
        logger.error(f"[Async Job {job_id}] Execution of {target_function_name_str} failed: {e}", exc_info=True)
        result_payload_for_db = {"error": f"Background task for {target_function_name_str} failed: {str(e)}"}

    finally:
        if api_key_token is not None:
            CURRENT_REQUEST_GEMINI_API_KEY.reset(api_key_token)
        if final_convo_status == 'finished':
            await asyncio.to_thread(_update_job_in_db, job_id, final_convo_status, result_payload_for_db, "✅ Response ready!", 100)
        else:
            await asyncio.to_thread(_update_job_in_db, job_id, final_convo_status, result_payload_for_db, "❌ Processing failed", 0)


def _execute_generation_task(job_id, target_function_name_str, args_tuple, kwargs_dict):
    """Backwards compatible sync wrapper for the async task executor.
    
    This maintains the original function signature while internally running
    the async version using asyncio.run() for full async compatibility.
    """
    try:
        # Create new event loop for this thread if one doesn't exist
        try:
            loop = asyncio.get_running_loop()
            # If we're already in an event loop, run in a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _execute_generation_task_async(job_id, target_function_name_str, args_tuple, kwargs_dict))
                future.result()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            asyncio.run(_execute_generation_task_async(job_id, target_function_name_str, args_tuple, kwargs_dict))
    except Exception as e:
        logger.error(f"[Thread Job {job_id}] Failed to execute async task: {e}", exc_info=True)
        # Fallback error handling
        try:
            _update_job_in_db(job_id, 'failed', {"error": f"Task execution failed: {str(e)}"}, "❌ Processing failed", 0)
        except Exception as db_error:
            logger.error(f"[Thread Job {job_id}] Failed to update job status after error: {db_error}", exc_info=True)



# --------------------------------------------------------------------------------- #
# ---------------------------  Main: Data Update in DB  --------------------------- #
# --------------------------------------------------------------------------------- #
def _update_job_in_db(job_id, convo_status, result_payload=None, thinking_step=None, progress_percentage=None):
    """
    Enhanced helper function - ONLY ADDS thinking_step and progress_percentage support
    to your existing function. All other logic remains the same.
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Your existing logic with minimal additions
        if thinking_step or progress_percentage is not None:
            # Extended query with new fields
            query = """
            UPDATE conversation_async_jobs 
            SET convo_status = ?, result = ?, thinking_step = ?, progress_percentage = ?, updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
            """
            
            # Convert result_payload to JSON if it's a dict/list (your existing logic)
            if isinstance(result_payload, (dict, list)):
                result_json = json.dumps(result_payload, ensure_ascii=False)
            else:
                result_json = result_payload
                
            cursor.execute(query, (convo_status, result_json, thinking_step, progress_percentage, job_id))
        else:
            # Fallback to your original behavior exactly as-is
            if result_payload is not None:
                query = """
                UPDATE conversation_async_jobs 
                SET convo_status = ?, result = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """
                if isinstance(result_payload, (dict, list)):
                    result_json = json.dumps(result_payload, ensure_ascii=False)
                else:
                    result_json = result_payload
                cursor.execute(query, (convo_status, result_json, job_id))
            else:
                query = """
                UPDATE conversation_async_jobs 
                SET convo_status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """
                cursor.execute(query, (convo_status, job_id))
                
        connection.commit()
        logger.info(f"Job {job_id} updated: status={convo_status}, thinking_step={thinking_step}")
        
    except Exception as e:
        logger.error(f"Error updating job {job_id} in database: {e}")
    finally:
        if 'connection' in locals():
            connection.close()


# ------------------------------------------------------------------------------------ #
# ---------------------------  Main: Local File Processor  --------------------------- #
# ------------------------------------------------------------------------------------ 

def _list_local_files(directory_path: str, pattern: str = "*") -> list[str]:
    """Lists files in a local directory, optionally filtering by glob pattern."""
    if not directory_path or not os.path.isdir(directory_path):
        logger.error(f"Directory not found or invalid: {directory_path}")
        return []
    try:
        # Use glob to find files matching the pattern within the directory
        search_path = os.path.join(directory_path, pattern)
        files = glob.glob(search_path)
        # Return only the basenames (filenames)
        basenames = [os.path.basename(f) for f in files if os.path.isfile(f)]
        logger.debug(f"Found {len(basenames)} files in {directory_path} matching '{pattern}'")
        return basenames
    except Exception as e:
        logger.error(f"Error listing files in local directory {directory_path}: {e}", exc_info=True)
        return []


# ------------------------------------------------------------------------------------------ #
# ---------------------------  Helper: Parse comma-page from DB  --------------------------- #
# ------------------------------------------------------------------------------------------ #
def _parse_page_list(page_str):
    """Helper to parse comma-separated page numbers from DB string."""
    if not page_str: # Handles None, NaN, empty string
        return []
    try:
        # Remove brackets if present, split by comma, strip whitespace, convert to int
        cleaned_str = str(page_str).strip('[] ')
        # Filter out empty strings after split before converting to int
        return [int(p.strip()) for p in cleaned_str.split(',') if p.strip().isdigit()]
    except ValueError:
        logger.warning(f"Could not parse page list string: '{page_str}'")
        return []


# ------------------------------------------------------------------------------------------------ #
# ---------------------------  Main: Load Document Data from Database  --------------------------- #
# ------------------------------------------------------------------------------------------------ #
def load_document_data_from_db():
    """Loads and aggregates document data from the MySQL database, validating local file existence."""
    logger.info("Attempting to load document data from database...")
    conn = None
    try:
        conn = get_db_connection()
        # Use pandas for easier data handling
        # Select all relevant columns
        query = """
        SELECT
            filename, document_summary, page_number, page_summary, page_content,
            page_visual_complexity, page_requires_advanced_processing,
            page_requires_experimental_processing, doc_has_complex_visuals,
            doc_has_highly_complex_visuals, doc_pages_req_advanced,
            doc_pages_req_experimental, prompt_suggestion_1, prompt_suggestion_2, prompt_suggestion_3
        FROM dashboard_summary_documents
        ORDER BY filename, page_number;
        """
        # Use a timeout for the query
        df = pd.read_sql(query, conn, chunksize=None) # Read all at once for now
        logger.info(f"Fetched {len(df)} rows from dashboard_summary_documents.")

        if df.empty:
            logger.warning("No data found in dashboard_summary_documents table.")
            return []

        # --- Aggregate data by filename ---
        aggregated_index = []
        # Group by the document filename
        for filename, group in df.groupby('filename'):
            if group.empty or not filename: # Also skip if filename is empty/null
                logger.warning(f"Skipping group with empty data or missing filename in DB.")
                continue

            # --- Path Validation (Check against POLICY_DIR FIRST) ---
            # Ensure the file actually exists locally relative to POLICY_DIR
            # Construct the full path safely
            try:
                # Basic check for invalid characters often found in bad data
                if any(c in filename for c in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']):
                     logger.warning(f"Skipping DB entry with potentially invalid characters in filename: '{filename}'")
                     continue
                # Normalize path separators just in case
                normalized_filename = os.path.normpath(filename)
                # Prevent path traversal attempt if normalized path tries to go up
                if normalized_filename.startswith("..") or os.path.isabs(normalized_filename):
                    logger.warning(f"Skipping DB entry with potential path traversal or absolute path: '{filename}'")
                    continue

                full_local_path = os.path.join(POLICY_DIR, normalized_filename)
                # Final check to ensure it's still within POLICY_DIR (though join usually handles this)
                abs_policy_dir = os.path.abspath(POLICY_DIR)
                abs_full_local_path = os.path.abspath(full_local_path)
                if not abs_full_local_path.startswith(abs_policy_dir):
                     logger.warning(f"Security Risk: Path resolved outside POLICY_DIR. Skipping DB entry: '{filename}' -> '{abs_full_local_path}'")
                     continue

            except Exception as path_e:
                logger.error(f"Error processing filename '{filename}' for path construction: {path_e}. Skipping.")
                continue

            if not os.path.exists(full_local_path):
                # Log a WARNING if the file listed in the DB doesn't exist locally
                logger.warning(f"DB entry references non-existent local file: '{filename}' (Expected path: {full_local_path}). Skipping this entry.")
                continue # Skip processing this filename group entirely
            elif not os.path.isfile(full_local_path):
                # Log a WARNING if the path exists but is not a file (e.g., a directory)
                logger.warning(f"DB entry references a path that is not a file: '{filename}' (Path: {full_local_path}). Skipping this entry.")
                continue
            else:
                # Log a DEBUG message if the file *is* found (optional, can be verbose)
                logger.debug(f"Validated local file existence for DB entry: '{filename}'")
            # --- End Path Validation ---

            # Get document-level info from the first row of the group
            # Ensure we handle potential NaNs gracefully
            first_row = group.iloc[0].replace({np.nan: None}) # Replace pandas NaN with Python None

            doc_entry = {
                # Use 'filename' from DB as the 'relative_path' for consistency
                'relative_path': filename, # Store the original filename from DB
                'source_path': first_row.get('source_path', ''), # Keep original source path if needed
                'title': filename, # Use filename as title by default
                'timestamp_utc': str(first_row.get('timestamp_utc')) if first_row.get('timestamp_utc') else None, # Convert to string if not None
                'model_used_in_generation': first_row.get('model_used', ''), # Model used for summary/analysis
                'document_summary': first_row.get('document_summary', ''),

                'prompt_suggestion_1': first_row.get('prompt_suggestion_1'),
                'prompt_suggestion_2': first_row.get('prompt_suggestion_2'),
                'prompt_suggestion_3': first_row.get('prompt_suggestion_3'),

                # --- Construct page_analysis from page-level data ---
                'page_analysis': {},

                # --- Construct visual_analysis_summary ---
                'visual_analysis_summary': {
                    'has_complex_visuals': bool(first_row.get('doc_has_complex_visuals', False)),
                    'has_highly_complex_visuals': bool(first_row.get('doc_has_highly_complex_visuals', False)),
                    'pages_requiring_advanced_model': _parse_page_list(first_row.get('doc_pages_req_advanced')),
                    'pages_requiring_experimental_model': _parse_page_list(first_row.get('doc_pages_req_experimental'))
                }
            }

            # Iterate through pages in the group
            for _, page_row_series in group.iterrows():
                page_row = page_row_series.replace({np.nan: None})
                page_num = page_row.get('page_number')
                if page_num is None or not str(page_num).isdigit():
                    logger.warning(f"Skipping page with invalid number '{page_num}' for file '{filename}'")
                    continue
                page_num_str = str(int(page_num))

                doc_entry['page_analysis'][page_num_str] = {
                    'page_number': int(page_num),
                    'summary': page_row.get('page_summary', ''),
                    'raw_content': page_row.get('page_content', ''),
                    'visual_complexity': page_row.get('page_visual_complexity', 'low'),
                    'requires_advanced': bool(page_row.get('page_requires_advanced_processing', False)),
                    'requires_experimental': bool(page_row.get('page_requires_experimental_processing', False)),
                }

            # Add the validated entry to the index
            aggregated_index.append(doc_entry)

        logger.info(f"Successfully loaded and aggregated data for {len(aggregated_index)} documents from the database (validated against local files).")
        return aggregated_index

    except (sqlite3.Error, pd.errors.DatabaseError) as db_err:
        logger.error(f"Database error during data loading: {db_err}", exc_info=True)
        return [] # Return empty list on DB error
    except FileNotFoundError as fnf_err:
        # This might catch errors if POLICY_DIR itself is invalid later, though checked at start
        logger.error(f"File system error during data loading (check POLICY_DIR: '{POLICY_DIR}'): {fnf_err}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error loading document data from database: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed.")
            
document_index = load_document_data_from_db()


# ------------------------------------------------------------------------------------------ #
# ---------------------------  Helper: Calculate Keyword Score  ---------------------------- #
# ------------------------------------------------------------------------------------------ #
def calculate_keyword_score(entry, query_terms):
    """Calculates keyword match score for a document index entry (DB version)."""
    if not query_terms or not entry:
        return 0, {}

    # --- Use 'relative_path' (which is the 'filename' from DB) ---
    filename = entry.get('relative_path', 'Unknown Title') # Use local relative path (filename)
    doc_summary = entry.get('document_summary', '')

    # --- Get page summaries from 'page_analysis' ---
    page_summaries = {}
    page_analysis = entry.get('page_analysis', {})
    if isinstance(page_analysis, dict):
         page_summaries = {
             page_num_str: details.get('summary', '')
             for page_num_str, details in page_analysis.items()
             if isinstance(details, dict) and details.get('summary')
         }

    # Scoring weights and parameters (Keep as is)
    TITLE_WEIGHT = 4.0
    DOC_SUMMARY_WEIGHT = 1.5
    PAGE_SUMMARY_WEIGHT = 1.0
    MAX_ESTIMATED_SCORE = 50.0

    title_score = 0
    doc_summary_score = 0
    total_page_score = 0
    matched_pages_count = 0
    matched_pages_scores = {} # Store scores per page

    title_lower = filename.lower() # Use relative_path here
    doc_summary_lower = doc_summary.lower() if doc_summary else ""

    # Calculate scores based on term occurrences
    for term in query_terms:
        term_lower = term.lower() # Ensure term is lowercase for matching
        if term_lower in title_lower:
            title_score += title_lower.count(term_lower) * TITLE_WEIGHT
        if term_lower in doc_summary_lower:
            doc_summary_score += doc_summary_lower.count(term_lower) * DOC_SUMMARY_WEIGHT

        for page_num_str, summary in page_summaries.items():
            page_lower = summary.lower()
            page_term_count = page_lower.count(term_lower)
            if page_term_count > 0:
                 current_page_score_for_term = page_term_count * PAGE_SUMMARY_WEIGHT
                 # Add to page's score, initialize if first match for this page
                 matched_pages_scores[page_num_str] = matched_pages_scores.get(page_num_str, 0) + current_page_score_for_term

    # Aggregate page scores
    for page_num_str, score in matched_pages_scores.items():
         if score > 0:
              total_page_score += score
              matched_pages_count += 1

    # Calculate final score components
    avg_page_score = (total_page_score / matched_pages_count) if matched_pages_count > 0 else 0
    # Give a base score if any page matches, plus the average
    page_component_score = (5 + avg_page_score) if matched_pages_count > 0 else 0

    total_score = title_score + doc_summary_score + page_component_score

    # Calculate confidence (normalized score, boosted by title match)
    confidence = 0.0
    if total_score > 0:
        # Normalize score to be between 0.1 and 0.95 (roughly)
        confidence = max(0.1, min(0.95, 0.1 + (total_score / MAX_ESTIMATED_SCORE) * 0.85))
        # Boost confidence slightly if title matched
        if title_score > 0:
            confidence = min(0.95, confidence + 0.05)

    return confidence, matched_pages_scores


# ------------------------------------------------------------------------------------- #
# ---------------------------  Helper: Find top Keywords  ---------------------------- #
# ------------------------------------------------------------------------------------ #
def find_top_keyword_sources(query, mode, limit=20):
    """Finds top N sources for the document using keyword matching (DB version)."""
    if not query:
        return []

    # Prepare query terms (Keep as is)
    query_terms_long = [term for term in query.lower().split() if len(term) > 2]
    query_terms = query_terms_long if query_terms_long else query.lower().split()
    if not query_terms:
        return []

    logger.info(f"Performing keyword search for '{query}' in mode '{mode}' with terms: {query_terms}")
    scored_sources = []

    # Document Mode Search
    if mode == 'document':
        if not document_index:
            logger.error("Document index is not loaded from DB. Cannot perform document search.")
            # Optionally try reloading here if it makes sense for your flow
            # document_index = load_document_data_from_db()
            # if not document_index: return []
            return []

        for entry in document_index:
            confidence, _ = calculate_keyword_score(entry, query_terms)
            if confidence >= 0.15:
                relative_path = entry.get('relative_path') # This is filename from DB
                if not relative_path: continue # Should not happen if DB load is correct

                preview_text = entry.get('document_summary', '')[:100]

                # Fallback to first page summary if doc summary empty
                if not preview_text and entry.get('page_analysis'):
                    try:
                        # Sort page numbers numerically
                        page_keys = sorted(
                             [k for k in entry['page_analysis'].keys() if k.isdigit()],
                             key=int
                        )
                        if page_keys:
                            first_page_key = str(page_keys[0])
                            first_page_details = entry['page_analysis'].get(first_page_key, {})
                            preview_text = first_page_details.get('summary', '')[:100]
                    except Exception as e:
                        logger.warning(f"Error getting first page summary for preview: {e}")

                if len(preview_text) >= 100: preview_text += "..."

                scored_sources.append({
                    "id": relative_path, # Use relative_path (filename) as ID
                    "name": os.path.basename(relative_path), # Display just the filename
                    "type": get_file_type_from_extension(relative_path),
                    "path": relative_path, # Store relative path (filename)
                    "preview": preview_text or "Dokumen internal relevan",
                    "confidence": confidence
                })

    # Market Mode Search
    elif mode == 'market':
        # 1. Score Provider Websites (Keep as is)
        for provider, url in PROVIDER_WEBSITES.items():
            provider_lower = provider.lower()
            score = sum(5 for term in query_terms if term == provider_lower or term in provider_lower)
            if score > 0:
                confidence = min(0.9, 0.4 + score * 0.1)
                scored_sources.append({
                    "id": url,
                    "name": provider.title() + " Website",
                    "type": "url",
                    "path": url,
                    "preview": f"Kunjungi situs web resmi {provider.title()}",
                    "confidence": confidence
                })

        # 2. Score Local Market Data Files
        logger.debug(f"Searching local directory '{BROADBAND_DIR}' for market data files...")
        # List JSON and CSV files specifically
        market_files = _list_local_files(BROADBAND_DIR, "*.json") + _list_local_files(BROADBAND_DIR, "*.csv")
        for filename in market_files:
            # Score based on query terms appearing in the filename
            score = sum(2 for term in query_terms if term in filename.lower())
            if score > 0:
                confidence = min(0.85, 0.3 + score * 0.15)
                file_type = get_file_type_from_extension(filename)
                scored_sources.append({
                    "id": filename, # Use filename as ID
                    "name": filename, # Display filename
                    "type": file_type,
                    "path": filename, # Store relative local path (relative to BROADBAND_DIR)
                    "preview": f"Data pasar relevan ({file_type.upper()})",
                    "confidence": confidence
                })

    # Sort results by confidence (highest first) and return top N
    scored_sources.sort(key=lambda x: x.get('confidence', 0), reverse=True)
    logger.info(f"Keyword search completed. Found {len(scored_sources)} potential matches. Returning top {limit}.")
    return scored_sources[:limit]


# --- Shared compensation intent detection ---
# Used by both Phase 1 (affinity bonus) and prompt assembly (domain knowledge injection).
# Regex word boundaries on short tokens prevent substring false positives (e.g. "sf" in "transfer").
_COMPENSATION_INTENT_PATTERNS = [
    r'\bsf\b', r'\bthp\b', r'take home pay', r'\bkomisi\b',
    r'\binsentif\b', r'\bgaji\b', r'penghasilan', r'pendapatan',
    r'sales force', r'hh agency', r'skema fee', r'fee skema',
]

def _is_compensation_intent(query_lower: str) -> bool:
    return any(re.search(p, query_lower) for p in _COMPENSATION_INTENT_PATTERNS)


# ===============================================
# RAG TRACE LOGGER — Structured JSON-lines telemetry
# ===============================================
_rag_trace_lock = threading.Lock()

def rag_trace(sid: str, stg: str, **kw):
    """Append one JSON-lines record to RAG trace log. Thread-safe."""
    record = {"ts": datetime.now().strftime("%m-%d %H:%M:%S"), "sid": sid, "stg": stg, **kw}
    line = json.dumps(record, ensure_ascii=False, separators=(',', ':'))
    with _rag_trace_lock:
        with open(RAG_TRACE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def auto_select_relevant_documents(query: str, limit: int = 4, initial_pool_size: int = 15, _rag_sid: str = "") -> list:
    """
    Hybrid-RAG Selection Engine:
    1. Phase 1 (Heuristic): Rapidly scores ALL documents based on Keywords, Exact Phrases, and Recency.
    2. Phase 2 (Semantic): Takes the Top N candidates and Embeds their SUMMARIES (low cost).
    3. Phase 3 (Fusion): Combines (Normalized Heuristic Score * 0.4) + (Semantic Score * 0.6).

    Returns: The Top N paths (default 4).
    """
    global document_index
    _t0 = time.time()
    if not query or not document_index:
        return []

    # --- PHASE 1: HEURISTIC SCORING (The "Broad Net") ---
    
    def normalize_text_docs(text):
        text = text.lower()
        text = re.sub(r'(\d+)([a-z]+)', r'\1 \2', text) # Split "30gb" -> "30 gb"
        return text

    query_norm = normalize_text_docs(query)
    
    # Weights for Heuristics
    W_PHRASE_MATCH = 30.0
    W_DOC_SUMMARY = 10.0    
    W_TITLE_EXACT = 20.0
    
    # Prepare Terms
    raw_terms = query_norm.split()
    stop_words = {'the', 'and', 'for', 'with', 'yang', 'dan', 'di', 'ke', 'dari', 'ini', 'itu', 'untuk', 'apakah'}
    query_terms = [t for t in raw_terms if t not in stop_words and (len(t) > 2 or t.isdigit())]
    
    query_phrases = []
    if len(query_terms) > 1:
        for i in range(len(query_terms)-1):
            query_phrases.append(f"{query_terms[i]} {query_terms[i+1]}")

    # --- Domain Synonym Expansion ---
    # Maps user-facing query vocabulary to document-vocabulary synonyms.
    # When a glossary key appears in the query, its synonym values are injected into
    # query_terms so Phase 1 heuristic scoring can find documents that use different terminology.
    # E.g. "take home pay" → adds ["komisi", "fee", "insentif", "skema"] to scoring terms.
    _PHASE1_GLOSSARY = {
        'take home pay': ['komisi', 'fee', 'insentif', 'skema', 'penetapan'],
        'thp':           ['komisi', 'fee', 'insentif', 'skema', 'penetapan'],
        'simulasi':      ['komisi', 'fee', 'insentif', 'perhitungan', 'skema'],
        'gaji':          ['komisi', 'fee', 'insentif', 'skema'],
        'penghasilan':   ['komisi', 'fee', 'insentif', 'skema'],
        'pendapatan':    ['komisi', 'fee', 'insentif', 'skema'],
        'sf':            ['sales force', 'hh agency', 'komisi', 'insentif'],
        'sales force':   ['hh agency', 'komisi', 'insentif', 'skema'],
        'komisi':        ['fee', 'insentif', 'skema', 'penetapan'],
        'insentif':      ['fee', 'komisi', 'skema', 'penetapan'],
        'upgrade speed': ['upsell', 'delta arpu', 'kenaikan harga', 'upgrade'],
        'cctv':          ['kamera', 'addon', 'add-on', 'perangkat'],
    }
    _expanded_terms = list(query_terms)
    for key, synonyms in _PHASE1_GLOSSARY.items():
        # Use word-boundary regex for short keys (≤3 chars) to avoid substring false positives
        # e.g. 'sf' must not match 'transfer', 'thp' must not match 'ththps'
        if len(key) <= 3:
            _match = bool(re.search(r'\b' + re.escape(key) + r'\b', query_norm))
        else:
            _match = key in query_norm
        if _match:
            for syn in synonyms:
                if syn not in _expanded_terms:
                    _expanded_terms.append(syn)
    # Use expanded terms for scoring; preserve original query_terms for phrase matching
    query_terms = _expanded_terms

    # --- Query-Intent Affinity Bonus ---
    # Problem: when query is about SF compensation/THP, it also contains many product terms
    # (indihome, cctv, telkomsel one, arpu, 220k...) that make product docs score 400-590.
    # The commission policy doc can't compete on keyword volume alone.
    # Fix: detect compensation intent → give a flat bonus to docs whose TITLE matches
    # compensation patterns, ensuring they survive into Phase 2 semantic re-ranking.
    _is_compensation_query = _is_compensation_intent(query_norm)
    _COMPENSATION_TITLE_PATTERNS = ['penetapan', 'kebijakan hh', 'hh agency',
                                    'skema fee', 'skema insentif', 'skema komisi']
    W_INTENT_AFFINITY = 200.0
            
    # Time Sensitivity — three-layer detection to avoid both over- and under-triggering.
    #
    # Layer 1 (Explicit): unambiguous recency keywords → strong recency weight.
    # Layer 2 (Implicit): contextual time signals → mild recency weight.
    # Layer 3 (Contextual Rescue): 'baru'/'kemarin' only count when paired with
    #          recency-indicating bigrams, avoiding false positives from standalone usage.
    #
    # Year mentions: only treated as recency if within 1 year of current date.
    #   "kebijakan 2024" (2+ years ago) → specificity signal, NOT recency.
    #   "kebijakan 2026" (current year) → implicit recency.
    current_date = datetime.now()

    # Layer 1: Explicit recency request
    is_explicit_time_sensitive = any(k in query_norm for k in [
        'terbaru', 'terupdate', 'terkini', 'terakhir', 'latest', 'most recent'
    ])

    # Layer 2: Implicit time context (non-year keywords)
    is_implicit_time_sensitive = False
    if not is_explicit_time_sensitive:
        if any(k in query_norm for k in ['saat ini', 'sekarang']):
            is_implicit_time_sensitive = True
        else:
            # Year mentions: only recent years (within 1 year) count as recency signal.
            # Older years are specificity signals ("show me the 2024 doc"), not recency.
            current_year = current_date.year
            for year_str in re.findall(r'\b(20\d{2})\b', query_norm):
                if abs(current_year - int(year_str)) <= 1:
                    is_implicit_time_sensitive = True
                    break

    # Layer 3: Contextual rescue for 'baru' and temporal words.
    # Standalone 'baru' is too common ("cara baru", "fitur baru" = general usage).
    # But "update baru", "dokumen baru", "kemarin" have clear recency intent.
    if not is_explicit_time_sensitive and not is_implicit_time_sensitive:
        _recency_bigrams = [
            'update baru', 'dokumen baru', 'kebijakan baru', 'peraturan baru',
            'info baru', 'data baru', 'baru saja', 'baru diupdate', 'baru dirilis'
        ]
        _temporal_words = ['kemarin', 'minggu lalu', 'bulan lalu', 'hari ini', 'barusan']
        if any(bg in query_norm for bg in _recency_bigrams):
            is_implicit_time_sensitive = True
        elif any(tw in query_norm for tw in _temporal_words):
            is_implicit_time_sensitive = True

    date_pattern = re.compile(r'^(\d{8})_')

    heuristic_results = [] # Stores (doc_entry, score)

    for entry in document_index:
        base_score = 0.0
        filename = os.path.basename(entry.get('relative_path', ''))
        title = normalize_text_docs(entry.get('title', filename))
        doc_summary = normalize_text_docs(entry.get('document_summary') or "")
        
        # A. Title & Phrase
        if query_norm in title: base_score += W_TITLE_EXACT
        for phrase in query_phrases:
            if phrase in title: base_score += W_PHRASE_MATCH
            if phrase in doc_summary: base_score += W_PHRASE_MATCH
        
        # B. Keywords (Capped)
        for term in query_terms:
            count = doc_summary.count(term)
            base_score += (min(count, 5) * W_DOC_SUMMARY)
            if term in title: base_score += 10.0

        # B2. Intent-Affinity Bonus — boost compensation docs when query is about THP/SF
        if _is_compensation_query:
            for pattern in _COMPENSATION_TITLE_PATTERNS:
                if pattern in title:
                    base_score += W_INTENT_AFFINITY
                    break

        # C. Extract days_old for Phase 3 — recency is NO LONGER a Phase 1 multiplier.
        #    Phase 1 uses pure keyword relevance so old but accurate documents are not
        #    gated out of the candidate pool before semantic search can evaluate them.
        days_old = None
        match = date_pattern.match(filename)
        if match:
            try:
                file_date = datetime.strptime(match.group(1), '%Y%m%d')
                days_old = max(0, (current_date - file_date).days)
            except ValueError:
                pass

        final_h_score = base_score  # Pure keyword relevance; no recency multiplier
        if final_h_score > 1.0:  # Only keep non-zero keyword matches
            heuristic_results.append({
                "entry": entry,
                "h_score": final_h_score,
                "days_old": days_old,  # Carried forward for Phase 3 fusion
                "path": entry.get('relative_path')
            })

    # Sort by Heuristic Score and take Top 15 (Initial Pool)
    heuristic_results.sort(key=lambda x: x["h_score"], reverse=True)
    candidates = heuristic_results[:initial_pool_size]

    if not candidates:
        return []

    logger.info(f"Hybrid-RAG Phase 1: Found {len(candidates)} heuristic candidates. Top: {candidates[0]['path']} ({candidates[0]['h_score']:.1f})")
    if _rag_sid:
        rag_trace(_rag_sid, "P1",
            pool=len(candidates), top=os.path.basename(candidates[0]['path'])[:60],
            top_s=round(candidates[0]['h_score'], 1), comp=_is_compensation_query,
            syn=len(query_terms) - len([t for t in raw_terms if t not in stop_words and (len(t) > 2 or t.isdigit())]),
            ms=round((time.time() - _t0) * 1000))

    # Layer 4 (Candidate-Driven): Detect time-sensitivity from what documents Phase 1 retrieved.
    # More robust than query keywords — the corpus signals what kind of doc is relevant.
    # Uses document_summary (already loaded from DB into document_index) for semantic content signals.
    if not is_explicit_time_sensitive and not is_implicit_time_sensitive and heuristic_results:
        _fn_policy_signals = [
            'kebijakan', 'penetapan', 'sk_', 'program', 'gtm', 'insentif',
            'surat', 'edaran', 'komisi', 'fee', 'skema', 'target', 'kinerja'
        ]
        _summary_policy_signals = [
            'berlaku', 'periode', 'skema fee', 'komisi', 'sales fee',
            'program insentif', 'management fee', 'target penjualan', 'insentif'
        ]
        top3_candidates = sorted(heuristic_results, key=lambda x: x['h_score'], reverse=True)[:3]
        for c in top3_candidates:
            fn = os.path.basename(c.get('path', '')).lower()
            summary = normalize_text_docs(c['entry'].get('document_summary', ''))
            fn_hit = any(s in fn for s in _fn_policy_signals)
            summary_hit = any(s in summary for s in _summary_policy_signals)
            if fn_hit or summary_hit:
                is_implicit_time_sensitive = True
                logger.debug(f"Hybrid-RAG Layer4 candidate-driven: implicit triggered by '{fn}' (fn={fn_hit}, summary={summary_hit})")
                break

    # --- Embed query once (shared between semantic rescue and Phase 2) ---
    q_vec = None
    try:
        api_key = get_effective_gemini_api_key()
        if not api_key:
            q_vec = None
            raise RuntimeError("Gemini API key not available for embeddings")
        logger.info("Hybrid-RAG query embedding call starting")
        genai.configure(api_key=api_key)
        q_vec = genai.embed_content(
            model=EMBEDDING_MODEL,
            content=query,
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=768
        )['embedding']
    except Exception as e:
        logger.warning(f"Query embedding failed: {e}")

    # --- SEMANTIC RESCUE: catch documents missed by keyword matching ---
    if q_vec and tbl_doc_summaries:
        try:
            semantic_rescue_results = tbl_doc_summaries.search(q_vec)\
                .limit(initial_pool_size)\
                .to_list()

            existing_filenames = {os.path.basename(c["entry"].get('relative_path', '')) for c in candidates}
            max_h_score = candidates[0]['h_score'] if candidates else 50.0

            for r in semantic_rescue_results:
                fname = r['filename']
                if fname not in existing_filenames:
                    sem_score = 1 - r['_distance']
                    if sem_score >= 0.80:
                        matching_entry = next(
                            (e for e in document_index if os.path.basename(e.get('relative_path', '')) == fname),
                            None
                        )
                        if matching_entry:
                            days_old = None
                            match = date_pattern.match(fname)
                            if match:
                                try:
                                    file_date = datetime.strptime(match.group(1), '%Y%m%d')
                                    days_old = max(0, (current_date - file_date).days)
                                except ValueError:
                                    pass

                            candidates.append({
                                "entry": matching_entry,
                                "h_score": max_h_score * 0.90,
                                "days_old": days_old,
                                "path": matching_entry.get('relative_path'),
                                "_semantic_rescue": True
                            })
                            existing_filenames.add(fname)

            rescue_count = sum(1 for c in candidates if c.get('_semantic_rescue'))
            if rescue_count > 0:
                logger.info(f"Hybrid-RAG Phase 1: Semantic rescue added {rescue_count} additional candidates")

        except Exception as e:
            logger.warning(f"Hybrid-RAG semantic rescue failed: {e}")

    # --- PHASE 2: SEMANTIC RE-RANKING (The "Smart Filter") ---
    # Strategy: search PAGE-LEVEL embeddings (tbl_doc_pages) instead of document-level
    # summaries. Document-level summaries are too generic for large comprehensive PDFs
    # (e.g., a 135-page Product Knowledge PDF scores moderately for ANY query because
    # its summary mentions every topic). By using the MAX page-level score per document,
    # a small specialized document with one very relevant page will correctly outscore
    # a large generic document where no single page is a strong match.
    #
    # Fallback: if tbl_doc_pages is unavailable, falls back to tbl_doc_summaries.

    _t2 = time.time()
    try:
        # q_vec already computed above; skip if embedding failed
        if not q_vec:
            raise ValueError("Query embedding not available")

        # 2. Map candidates to filenames for filtering
        candidate_filenames = [os.path.basename(c["entry"].get('relative_path', '')) for c in candidates]

        if not candidate_filenames:
            return [c["path"] for c in candidates[:limit]]

        # Escape single quotes in filenames to prevent SQL parsing errors.
        sql_list = ", ".join([f"'{f.replace(chr(39), chr(39)+chr(39))}'" for f in candidate_filenames])

        # 3. Search LanceDB — prefer page-level, fallback to summary-level
        score_map = {}

        if tbl_doc_pages:
            # PAGE-LEVEL search: returns multiple rows per document.
            # We take the MAX similarity score across all pages for each document.
            # This lets a document with one highly relevant page outscore a document
            # whose pages are all moderately relevant.
            page_results = tbl_doc_pages.search(q_vec)\
                .where(f"filename IN ({sql_list})")\
                .limit(len(candidates) * 5)\
                .to_list()

            for r in page_results:
                fname = r['filename']
                page_score = 1 - r['_distance']
                if fname not in score_map or page_score > score_map[fname]:
                    score_map[fname] = page_score

            logger.info(f"Hybrid-RAG Phase 2: Used PAGE-LEVEL search ({len(page_results)} page hits)")
            if _rag_sid:
                rag_trace(_rag_sid, "P2", method="page", hits=len(page_results), ms=round((time.time() - _t2) * 1000))

        elif tbl_doc_summaries:
            # Fallback: document-level summary search (less precise but still useful)
            search_results = tbl_doc_summaries.search(q_vec)\
                .where(f"filename IN ({sql_list})")\
                .limit(len(candidates))\
                .to_list()

            score_map = {r['filename']: (1 - r['_distance']) for r in search_results}
            logger.info(f"Hybrid-RAG Phase 2: Fallback to SUMMARY-LEVEL search")
            if _rag_sid:
                rag_trace(_rag_sid, "P2", method="summary", hits=len(search_results), ms=round((time.time() - _t2) * 1000))

        else:
            raise ValueError("Neither LanceDB 'document_pages' nor 'document_summaries' table accessible.")

        # 4. Map scores back to candidate order
        similarity_scores = []
        for c in candidates:
            fname = os.path.basename(c["entry"].get('relative_path', ''))
            similarity_scores.append(score_map.get(fname, 0.0))

    except Exception as e:
        logger.error(f"Hybrid-RAG Semantic Phase Failed (LanceDB): {e}. Falling back to Heuristic.")
        # When Phase 2 fails, we only have keyword scores from Phase 1.
        # Always apply recency tiebreaker; scale factor by query mode.
        import math
        for c in candidates:
            d = c.get("days_old")
            if d is not None:
                rec_weight = 0.25 if is_explicit_time_sensitive else 0.10 if is_implicit_time_sensitive else 0.03
                c["_fallback_score"] = c["h_score"] * (1.0 + rec_weight * math.exp(-0.001 * d))
            else:
                c["_fallback_score"] = c["h_score"]
        candidates.sort(key=lambda x: x["_fallback_score"], reverse=True)
        return [c["path"] for c in candidates[:limit]]

    # --- PHASE 3: FUSION & NORMALIZATION ---
    import math

    # Adaptive 3-component weights based on query time-sensitivity.
    # Explicit (e.g. "terbaru", "terkini"):  keyword=20%, semantic=55%, recency=25%
    # Implicit (e.g. "saat ini", "2026"):    keyword=30%, semantic=60%, recency=10%
    # Neutral  (factual/long-lasting query): keyword=29%, semantic=68%, recency=3%
    # Small recency floor ensures newer docs have a persistent edge even without explicit signals.
    if is_explicit_time_sensitive:
        W_heuristic, W_semantic, W_recency = 0.20, 0.55, 0.25
    elif is_implicit_time_sensitive:
        W_heuristic, W_semantic, W_recency = 0.30, 0.60, 0.10
    else:
        W_heuristic, W_semantic, W_recency = 0.29, 0.68, 0.03

    # Guard: if no candidate has date metadata, recency weight is useless (all would
    # score 0.0). Redistribute it proportionally to keyword + semantic so the full
    # 0–1.0 scoring range is utilized instead of being compressed to 0.75.
    if W_recency > 0:
        any_has_date = any(c.get("days_old") is not None for c in candidates)
        if not any_has_date:
            total_non_recency = W_heuristic + W_semantic
            W_heuristic = W_heuristic / total_non_recency
            W_semantic = W_semantic / total_non_recency
            W_recency = 0.0

    # Normalize heuristic scores (which can be 50–500) to 0.0–1.0.
    # Because Phase 1 no longer inflates scores via recency multiplier, max_h now
    # reflects pure keyword relevance and is a fairer normalization baseline.
    max_h = max(c["h_score"] for c in candidates) if candidates else 1.0

    final_ranked = []
    for i, candidate in enumerate(candidates):
        norm_h = candidate["h_score"] / max_h
        semantic_s = float(similarity_scores[i])

        # Recency score: soft exponential decay → 0 days=1.0, ~1yr≈0.70, ~2yr≈0.48
        # Deliberately gentle so semantic still dominates even in explicit mode.
        # Max recency advantage (new vs 2yr old, explicit mode): 0.25 × 0.52 = 0.13
        # Semantic needs only a 0.24 point gap to overcome it — achievable for relevant content.
        days_old = candidate.get("days_old")
        if days_old is not None and W_recency > 0:
            recency_s = math.exp(-0.001 * days_old)
        else:
            recency_s = 0.0

        final_score = (norm_h * W_heuristic) + (semantic_s * W_semantic) + (recency_s * W_recency)
        final_ranked.append((candidate["path"], final_score, semantic_s, recency_s, candidate["h_score"]))

    # Sort Final Results
    final_ranked.sort(key=lambda x: x[1], reverse=True)

    # Return Top N paths
    top_docs = [x[0] for x in final_ranked[:limit]]

    if final_ranked:
        mode_label = 'explicit' if is_explicit_time_sensitive else 'implicit' if is_implicit_time_sensitive else 'neutral'
        # Log ALL candidates' scores for diagnostic visibility — critical for debugging
        # why a correct document may rank below the limit cutoff.
        for rank, (path, fscore, sscore, rscore, hscore) in enumerate(final_ranked):
            marker = ">>>" if rank < limit else "   "
            logger.info(
                f"Hybrid-RAG #{rank+1} {marker} {os.path.basename(path)} "
                f"(Final={fscore:.4f}, Heur={hscore:.1f}, "
                f"Sem={sscore:.4f}, Rec={rscore:.4f}, Mode={mode_label})"
            )
        if _rag_sid:
            rag_trace(_rag_sid, "P3",
                mode=mode_label,
                sel=[os.path.basename(x[0])[:50] for x in final_ranked[:limit]],
                scores=[round(x[1], 3) for x in final_ranked[:limit]],
                total_ms=round((time.time() - _t0) * 1000))

    return top_docs

# -------------------------------------------------------------------------------------- #
# ---------------------------  Helper: Filetype Determiner  ---------------------------- #
# -------------------------------------------------------------------------------------- #
# --- get_file_type_from_extension --- #
def get_file_type_from_extension(filename):
    """Gets a simplified type based on file extension."""
    if not filename or '.' not in filename:
        return 'unknown'
    ext = filename.split('.')[-1].lower()
    type_map = {
        'pdf': 'pdf', 'pptx': 'pptx', 'ppt': 'ppt',
        'docx': 'docx', 'doc': 'doc', 'xlsx': 'xlsx',
        'xls': 'xls', 'csv': 'csv', 'json': 'json',
        'txt': 'txt',
    }
    return type_map.get(ext, 'file') # Default to 'file' if extension not in map



# --- get_mime_type --- #
def get_mime_type(file_path):
    """Get the standard MIME type based on file extension"""
    if not file_path or '.' not in file_path:
        return 'application/octet-stream' # Default binary type

    ext = file_path.split('.')[-1].lower()
    # Map common extensions to MIME types
    type_map = {
        'pdf': 'application/pdf',
        'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'ppt': 'application/vnd.ms-powerpoint',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'doc': 'application/msword',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'xls': 'application/vnd.ms-excel',
        'csv': 'text/csv',
        'json': 'application/json',
        'txt': 'text/plain',
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'gif': 'image/gif',
    }
    # Use mapped type or fallback to OS guess or default binary
    return type_map.get(ext, mimetypes.guess_type(file_path)[0] or 'application/octet-stream')

# --- determine_required_model (Logic remains same, relies on index structure) ---
def determine_required_model(documents):
    """Determines if a more complex model is needed based on document characteristics."""
    needs_experimental_model = False
    needs_advanced_model = False
    reason = "Default: No specific complexity triggers met." # Start with default reason

    if not documents:
        logger.warning("determine_required_model called with no documents. Using STANDARD model.")
        return STANDARD_MODEL, reason

    logger.debug(f"--- Entering determine_required_model for {len(documents)} document(s) ---")

    for i, doc in enumerate(documents):
        doc_title = doc.get('title', f'[Untitled Doc {i+1}]')
        # --- Use 'relative_path' ---
        doc_path = doc.get('relative_path', '')
        doc_path_lower = doc_path.lower()
        visual_analysis_summary = doc.get('visual_analysis_summary', {})

        logger.debug(f"Processing doc '{doc_title}': Path='{doc_path}', Visual Summary Type='{type(visual_analysis_summary)}'")

        # Ensure visual_analysis_summary is a dict before accessing keys
        if not isinstance(visual_analysis_summary, dict):
            logger.warning(f" visual_analysis_summary is not a dict for '{doc_title}'. Skipping visual checks for this doc.")
            continue

        # If experimental already triggered, no need to check further docs
        if needs_experimental_model:
            logger.debug(f" Skipping further checks for '{doc_title}', EXPERIMENTAL model already triggered.")
            continue

        # 1. Check explicit flags from visual analysis
        experimental_pages = visual_analysis_summary.get('pages_requiring_experimental_model', [])
        if isinstance(experimental_pages, list) and experimental_pages:
            needs_experimental_model = True
            reason = f"Doc '{doc_title}' flagged pages ({experimental_pages}) needing EXPERIMENTAL model."
            logger.info(f" Model Trigger: EXPERIMENTAL required by explicit flags in '{doc_title}'.")
            break # Experimental is highest priority, stop checking

        advanced_pages = visual_analysis_summary.get('pages_requiring_advanced_model', [])
        if isinstance(advanced_pages, list) and advanced_pages:
             # Only set advanced if experimental hasn't been set yet
            if not needs_experimental_model:
                 needs_advanced_model = True
                 reason = f"Doc '{doc_title}' flagged pages ({advanced_pages}) needing ADVANCED model."
                 logger.info(f" Model Trigger: ADVANCED required by explicit flags in '{doc_title}'.")
                 # Continue checking other docs in case one triggers experimental

        # 2. Heuristic: Check for complex keywords in title + visual flag
        if not needs_experimental_model: # Only check if experimental not already triggered
            complex_keywords = ['flowchart', 'diagram', 'architecture', 'schema', 'matrix', 'blueprint']
            has_visuals_flag = visual_analysis_summary.get('has_visuals', True) # Assume visuals if flag absent? safer to check
            title_lower = doc_title.lower()

            matched_keyword = next((k for k in complex_keywords if k in title_lower), None)
            if matched_keyword and has_visuals_flag:
                needs_experimental_model = True
                reason = f"Doc '{doc_title}' title keyword ('{matched_keyword}') suggests complexity & visual analysis confirms visuals."
                logger.info(f" Model Trigger: EXPERIMENTAL suggested by keywords/visuals flag in '{doc_title}'.")
                break # Experimental triggered, stop checking

        # 3. Heuristic: Check file type (e.g., PPTX often needs better visual understanding)
        if not needs_experimental_model and not needs_advanced_model: # Check only if nothing else triggered
            if doc_path_lower.endswith('.pptx'):
                needs_advanced_model = True
                reason = f"PowerPoint file type (.pptx) detected for '{doc_title}'."
                logger.info(f" Model Trigger: ADVANCED suggested by .pptx file type for '{doc_title}'.")
                 # Continue checking other docs

    # Determine final model based on flags
    if needs_experimental_model:
        model_name = EXPERIMENTAL_MODEL
        final_reason = f"EXPERIMENTAL model selected: {reason}"
    elif needs_advanced_model:
        model_name = ADVANCED_MODEL
        final_reason = f"ADVANCED model selected: {reason}"
    else:
        model_name = STANDARD_MODEL
        final_reason = f"STANDARD model selected: {reason}" # Use the initial or last updated reason

    logger.info(f"--- Exiting determine_required_model ---")
    logger.info(f"Final Model Decision: {model_name}. Reason: {final_reason}")
    return model_name, final_reason


# --- PDF Extractor to use Local Filesystem ---
def _extract_and_encode_pdf_page_local(pdf_relative_path: str, page_number: int) -> str | None:
    """
    Extracts a specific page from a local PDF as a base64 PNG using direct file access.
    Optimized to use fitz native file handling (lazy loading) rather than reading bytes into RAM.
    """
    # Construct the full path
    full_pdf_path = os.path.join(POLICY_DIR, pdf_relative_path)
    
    # Basic existence check
    if not os.path.exists(full_pdf_path):
        logger.error(f"Local PDF file not found: {full_pdf_path}")
        return None

    doc = None
    try:
        # Open the PDF directly from the file system
        # This is faster/lighter than reading bytes because it uses mmap/lazy loading
        doc = fitz.open(full_pdf_path)
        
        page_index = page_number - 1 # fitz uses 0-based index

        # Validate page number
        if 0 <= page_index < len(doc):
            page = doc.load_page(page_index)
            # Render page to pixmap (PNG format) with reasonable DPI
            # DPI 96 is good for LLM vision; if too slow, reduce to 72
            pix = page.get_pixmap(dpi=96)
            img_bytes_png = pix.tobytes("png")
            
            # Encode the PNG bytes to base64 string
            base64_image = base64.b64encode(img_bytes_png).decode('utf-8')
            logger.info(f"Successfully extracted and encoded page {page_number} from local PDF '{full_pdf_path}'")
            return base64_image
        else:
            logger.error(f"Invalid page number {page_number} requested for local PDF {full_pdf_path} which has {len(doc)} pages.")
            return None

    except Exception as e:
        logger.error(f"Error processing page {page_number} from local PDF {full_pdf_path}: {e}", exc_info=True)
        return None
    finally:
        # Ensure the Fitz document is closed
        if doc:
            try:
                doc.close()
            except Exception as e_close:
                logger.warning(f"Error closing Fitz document for {full_pdf_path}: {e_close}")


def refresh_document_index():
    """
    Reloads the document index from the database, ensuring it reflects
    the current state of local files in POLICY_DIR.
    This operation is made thread-safe using a lock.
    """
    global document_index # Declare intent to modify the global variable
    logger.info("Attempting to refresh document index...")
    try:
        # Call the function that loads from DB and validates local file existence
        new_index = load_document_data_from_db()
        # Acquire the lock before modifying the shared global variable
        with document_index_lock:
            document_index = new_index # Atomically update the global index
        logger.info(f"Document index refreshed successfully. New size: {len(document_index)} entries.")
        return True, f"Index refreshed. New size: {len(document_index)}"
    except Exception as e:
        logger.error(f"Failed to refresh document index: {e}", exc_info=True)
        # Do not replace the existing index if the refresh fails to avoid losing potentially working data
        return False, f"Error refreshing index: {e}"


# --- Normalization/Extraction Helpers --- #
def normalize_text(text):
    """Normalize text for comparison"""
    if not text: return ""
    return str(text).lower().strip()

def extract_numeric(text, default=None):
    """Extract numeric value from string"""
    if not text: return default
    clean_text = normalize_text(text).replace('.', '').replace(',', '')
    match = re.search(r'(\d+)', clean_text)
    if match:
        try:
            return float(match.group(1))
        except (ValueError, TypeError):
            pass
    return default

def normalize_location(text):
    """Normalize location text (remove prefixes like 'kota', lowercase)."""
    if not text: return ""
    normalized = normalize_text(text)
    prefixes = ['kota ', 'kabupaten ', 'kab. ', 'kab ', 'daerah ', 'kotamadya ', 'kt. ']
    suffixes = [' city', ' kota', ' area', ' district', ' wilayah']
    for prefix in prefixes:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
            break
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
            break
    return normalized.strip()

def are_numbers_close(num1, num2, field_type='generic', tolerance_percent=10.0):
    """Check if two numbers are within acceptable tolerance percentage."""
    if num1 is None or num2 is None or num1 == 0: return False # Avoid division by zero
    try:
        f_num1 = float(num1)
        f_num2 = float(num2)
        diff_percent = abs(f_num1 - f_num2) / f_num1 * 100
        return diff_percent <= tolerance_percent
    except (ValueError, TypeError):
        return False


# --- clean_response --- #
def clean_response(response_text: str) -> str:
    """
    Centralized response cleaner.
    1. Removes internal system blocks (KEY_FINDINGS).
    2. Cleans markdown code blocks.
    3. Normalizes Unicode spaces.
    4. Converts Markdown (Headers, Bold, Italics) to HTML.
    5. Removes meta-text/boilerplate.
    6. Fixes Invalid HTML Structures (e.g. <td> inside <ul>).
    7. Fixes HTML table structural whitespace.
    """
    if not response_text:
        return ""

    cleaned = response_text

    # 1. Remove KEY_FINDINGS_START...KEY_FINDINGS_END block
    start_marker = "KEY_FINDINGS_START"
    end_marker = "KEY_FINDINGS_END"
    
    while True:
        s_idx = cleaned.find(start_marker)
        e_idx = cleaned.find(end_marker)
        if s_idx != -1 and e_idx != -1:
            cleaned = cleaned[:s_idx] + cleaned[e_idx + len(end_marker):]
        else:
            break

    # 2. Normalize Non-Breaking Spaces (U+00A0) and others to Regular Spaces
    cleaned = cleaned.replace('\u00A0', ' ')
    cleaned = cleaned.replace('\u2002', ' ')
    cleaned = cleaned.replace('\u2003', ' ')
    cleaned = cleaned.replace('\u2009', ' ')

    # 3. Remove markdown code block fences (```html, ```)
    cleaned = re.sub(r'^```(?:[a-zA-Z]+)?\s*', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE)

    # 4. Convert Markdown to HTML
    # Convert Markdown Headers (Fixes the unrendered "###" syntax)
    # ### Title -> <h3>Title</h3>
    cleaned = re.sub(r'^###\s+(.*?)$', r'<h3>\1</h3>', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'^##\s+(.*?)$', r'<h2>\1</h2>', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'^#\s+(.*?)$', r'<h1>\1</h1>', cleaned, flags=re.MULTILINE)

    # Bold (**text**) -> <b>text</b>
    cleaned = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', cleaned)
    # Italics (*text* or _text_) -> <i>text</i>
    cleaned = re.sub(r'(?<![\\*_A-Za-z0-9])(?:\*|_)(?![*_\s])([^*_]+?)(?<!\s[*_])(?:\*|_)(?![\\*_A-Za-z0-9])', r'<i>\1</i>', cleaned)

    # incomplete progress signature
    cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)

    # 5. Remove predefined "meta-text" patterns (Boilerplate removal)
    meta_patterns = [
        r"^\s*Here'?s the HTML response based on the provided data:?\s*",
        r"^\s*Okay, I will analyze.*?based on the provided context.*?\n",
        r"^\s*Thinking process complete.*?\n",
        r"^\s*Mohon berikan pertanyaan spesifik.*\n?",
        r"^\s*Based on the provided context.*?\n",
        r"^\s*Answer:\s*",
        r"^\s*Okay, here is the HTML based on your request:?\s*",
        r"^\s*\*\*YOUR RESPONSE \(Ask for clarification OR provide the answer based on data, HTML Only\):\*\*\s*$",
    ]
    
    temp_cleaned = cleaned.strip()
    for pattern in meta_patterns:
        temp_cleaned = re.sub(pattern, '', temp_cleaned, flags=re.IGNORECASE | re.MULTILINE).strip()
    cleaned = temp_cleaned

    # 6. Fix Invalid HTML Structures (Fixes the broken table layout)
    # Sometimes LLMs put <td> inside <ul> which breaks rendering. We convert them to <li>.
    # <ul>...<td>...</td>...</ul> -> <ul>...<li>...</li>...</ul>
    cleaned = re.sub(r'(<ul[^>]*>)\s*<td[^>]*>', r'\1<li>', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'</td>\s*</ul>', r'</li></ul>', cleaned, flags=re.IGNORECASE)
    
    # <ol>...<td>...</td>...</ol> -> <ol>...<li>...</li>...</ol>
    cleaned = re.sub(r'(<ol[^>]*>)\s*<td[^>]*>', r'\1<li>', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'</td>\s*</ol>', r'</li></ol>', cleaned, flags=re.IGNORECASE)

    # 7. Clean whitespace between critical HTML table structural tags
    if re.search(r"<table", cleaned, flags=re.IGNORECASE):
        tags_to_fix = ['caption', 'colgroup', 'thead', 'tbody', 'tfoot', 'tr', 'td', 'th', 'col']
        tag_pattern = "|".join(tags_to_fix)
        
        cleaned = re.sub(
            fr'(</(?:{tag_pattern})>)\s+(<(?:{tag_pattern}|table)\b)', 
            r'\1\2', 
            cleaned, 
            flags=re.IGNORECASE
        )
        cleaned = re.sub(
            fr'(<(?:{tag_pattern}|table)\b[^>]*>)\s+(<(?:{tag_pattern})\b)', 
            r'\1\2', 
            cleaned, 
            flags=re.IGNORECASE
        )

    return cleaned.strip()


def safe_extract_locations(product):
    """Safely extract location data with fallbacks"""
    try:
        # Try both fields
        locations = product.get('original_locations_dpd', '') or product.get('original_locations_dpm', '')
        
        # Validate it's a string
        if not isinstance(locations, str):
            logger.warning(f"Invalid location data type: {type(locations)} for product {product.get('product_name')}")
            return ""
            
        return locations
    except Exception as e:
        logger.error(f"Error extracting locations: {e}")
        return ""


# ----------------------------------------------------------------------------------------------------------------------------------- #
# ---------------------------------------- MAIN: Generate Dynamic SQL (Product, Promo, Both) ---------------------------------------- #
# ----------------------------------------------------------------------------------------------------------------------------------- #
def build_market_sql_filters(
    extracted_filters: dict,
    data_preference: str,
    job_id_for_logging: str = "N/A"
):
    """
    Builds a comprehensive SQL filter set based on a pre-extracted, context-aware dictionary.
    This version handles provider logic (inclusion/exclusion), location, speed ranges, and price ranges
    by safely formatting values directly into the SQL string.
    """
    log_prefix = f"[StatefulSQL Job {job_id_for_logging}]"
    
    dpd_conditions = []
    dpm_conditions = []
    promo_conditions = []

    # --- 1. PROVIDER FILTERING ---
    search_type = extracted_filters.get('search_type')
    if search_type == 'local_providers_only':
        logging.info(f"{log_prefix} Applying 'local_providers_only' exclusion filter.")
        exclusions = list(NATIONAL_PROVIDER_EXCLUSIONS)
        if exclusions:
            # Safely format strings for the NOT IN clause
            formatted_exclusions = ', '.join([f"'{ex.replace("'", "''")}'" for ex in exclusions])
            dpd_conditions.append(f"LOWER(dpd.provider) NOT IN ({formatted_exclusions})")
            dpm_conditions.append(f"LOWER(dpm.provider) NOT IN ({formatted_exclusions})")
            promo_conditions.append(f"LOWER(promo.provider) NOT IN ({formatted_exclusions})")

    elif search_type == 'national_providers_only':  # ADD THIS LOGIC
        # Include ONLY national providers
        inclusions = list(NATIONAL_PROVIDER_EXCLUSIONS)
        if inclusions:
            formatted_inclusions = ', '.join([f"'{inc.replace("'", "''")}'" for inc in inclusions])
            dpd_conditions.append(f"LOWER(dpd.provider) IN ({formatted_inclusions})")
            dpm_conditions.append(f"LOWER(dpm.provider) IN ({formatted_inclusions})")
            promo_conditions.append(f"LOWER(promo.provider) IN ({formatted_inclusions})")

    else:
        # Standard provider inclusion logic
        providers_to_include = extracted_filters.get('providers', [])
        if isinstance(providers_to_include, str) and providers_to_include.upper() == "ALL_PROVIDERS_FLAG":
            logging.info(f"{log_prefix} Searching all providers (no provider filter).")
        elif providers_to_include:
            logging.info(f"{log_prefix} Applying IN-filter for providers: {providers_to_include}")
            formatted_providers = ', '.join([f"'{p.lower().replace("'", "''")}'" for p in providers_to_include])
            dpd_conditions.append(f"LOWER(dpd.provider) IN ({formatted_providers})")
            dpm_conditions.append(f"LOWER(dpm.provider) IN ({formatted_providers})")
            if data_preference in ["promo", "both"]:
                promo_conditions.append(f"LOWER(promo.provider) IN ({formatted_providers})")

    # --- 2. LOCATION FILTERING ---
    # 🎯 Changed key from 'locations' to 'location' to match LLM output.
    locations = extracted_filters.get('location', [])
    if locations:
        loc_dpd_parts, loc_dpm_parts, loc_promo_parts = [], [], []
        for loc in locations:
            safe_loc = loc.lower().replace("'", "''")
            safe_kota_loc = f"kota {safe_loc}"
            loc_dpd_parts.append(f"(LOWER(dpd.locations) LIKE '%{safe_loc}%' OR LOWER(dpd.locations) LIKE '%{safe_kota_loc}%')")
            loc_dpm_parts.append(f"(LOWER(dpm.found) LIKE '%{safe_loc}%' OR LOWER(dpm.found) LIKE '%{safe_kota_loc}%')")
        
        # 🎯 Changed joiner from 'AND' to 'OR' for multi-location queries.
        # This finds products available in *any* of the specified locations, not all of them.
        if loc_dpd_parts: dpd_conditions.append(f"({' OR '.join(loc_dpd_parts)})")
        if loc_dpm_parts: dpm_conditions.append(f"({' OR '.join(loc_dpm_parts)})")

    # --- 3. SPEED & PRICE FILTERS ---
    if 'speed_exact' in extracted_filters:
        try:
            speed = int(extracted_filters['speed_exact'])
            dpd_conditions.append(f"CAST(dpd.speed_mbps AS UNSIGNED) = {speed}")
            dpm_conditions.append(f"CAST(dpm.speed AS UNSIGNED) = {speed}")
        except (ValueError, TypeError):
            logging.warning(f"Could not parse 'speed_exact' filter: {extracted_filters.get('speed_exact')}")

    if 'speed_min' in extracted_filters:
        try:
            min_speed = int(extracted_filters['speed_min'])
            dpd_conditions.append(f"CAST(dpd.speed_mbps AS UNSIGNED) >= {min_speed}")
            dpm_conditions.append(f"CAST(dpm.speed AS UNSIGNED) >= {min_speed}")
        except (ValueError, TypeError):
            logging.warning(f"Could not parse 'speed_min' filter: {extracted_filters.get('speed_min')}")
            
    if 'speed_max' in extracted_filters:
        try:
            max_speed = int(extracted_filters['speed_max'])
            dpd_conditions.append(f"CAST(dpd.speed_mbps AS UNSIGNED) <= {max_speed}")
            dpm_conditions.append(f"CAST(dpm.speed AS UNSIGNED) <= {max_speed}")
        except (ValueError, TypeError):
            logging.warning(f"Could not parse 'speed_max' filter: {extracted_filters.get('speed_max')}")

    if 'price_max' in extracted_filters:
        try:
            max_price = float(extracted_filters['price_max'])
            dpd_conditions.append(f"dpd.price <= {max_price}")
            dpm_conditions.append(f"dpm.price <= {max_price}")
        except (ValueError, TypeError):
            logging.warning(f"Could not parse 'price_max' filter: {extracted_filters.get('price_max')}")

    # --- 4. ASSEMBLE FINAL COMPONENTS ---
    source_details_dpd = r"""'{"table":"dpd"}' AS _source_details_dpd"""
    source_details_dpm = r"""'{"table":"dpm"}' AS _source_details_dpm"""
    source_details_promo = r"""'{"table":"promo"}' AS _source_details_promo"""

    product_sql_components = {
        "dpd": {
            "select_fields": [
                "provider", 
                "product_name", 
                "speed_mbps", 
                "price", 
                "price_unit", 
                "locations AS original_locations_dpd", 
                "gimmicks", 
                "event_date",  # ADDED: Include date field explicitly
                source_details_dpd
            ],
            "where_conditions": dpd_conditions,
            "parameters": [],
            "table_name": "dashboard_product_detail"
        },
        "dpm": {
            "select_fields": [
                "provider", 
                "package_name AS product_name", 
                "speed AS speed_mbps", 
                "price", 
                "NULL AS price_unit", 
                "found AS original_locations_dpm", 
                "gimmicks", 
                "timestamp",
                "source",
                source_details_dpm
            ],
            "where_conditions": dpm_conditions,
            "parameters": [],
            "table_name": "dashboard_provider_matpro"
        }
    }
    promo_sql_components = {
        "promo_detail": {
            "select_fields": [
                "provider", 
                "promo_title", 
                "content_summary", 
                "start_date", 
                "end_date", 
                "promo_url", 
                source_details_promo
            ],
            "where_conditions": promo_conditions,
            "parameters": [],
            "table_name": "dashboard_promo_detail"
        }
    }
    
    logging.info(f"{log_prefix} Built SQL filters. DPD Conds: {len(dpd_conditions)}, DPM Conds: {len(dpm_conditions)}")
    
    return product_sql_components, promo_sql_components


# ----------------------------------------------------------------------------------------------------------------------------- #
# ---------------------------- HELPER: DYNAMIC QUERY BUILDER to load consolidated ISP data from DB ---------------------------- #
# ----------------------------------------------------------------------------------------------------------------------------- #
async def load_consolidated_isp_data_from_db_product_async(
    dynamic_sql_filters_product: dict,
    original_query_text_for_logging: str,
    logger_instance=None,
    query_analysis=None
):
    """
    Enhanced async version with date column support.
    Purely async implementation (removed synchronous fallbacks).
    """
    global logger, async_db_pool
    current_logger = logger_instance if logger_instance else logger
    log_prefix_load = f"[LoadProductDB Query: '{original_query_text_for_logging[:30]}...'] "

    current_logger.info(f"DEBUG: Enhanced async function called with filters: {dynamic_sql_filters_product}")

    # 1. Check Cache
    filters_str = json.dumps(dynamic_sql_filters_product, sort_keys=True)
    cache_key = f"market_products:{hashlib.md5(filters_str.encode()).hexdigest()}"

    cached_result = market_data_cache.get(cache_key)
    if cached_result:
        current_logger.info(f"IN-MEMORY CACHE HIT for products: {cache_key}")
        return cached_result, len(cached_result)

    current_logger.info(f"IN-MEMORY CACHE MISS for products: {cache_key}. Querying database...")
    
    # 2. Validation
    if not dynamic_sql_filters_product or dynamic_sql_filters_product.get("status") == "error": 
        return [], 0
        
    sql_components = dynamic_sql_filters_product.get('sql_components', {})
    if not sql_components: 
        return [], 0

    if not async_db_pool:
        current_logger.error(f"{log_prefix_load} Critical: Async DB pool is not initialized.")
        return [], 0

    product_data_list = []
    product_count = 0

    try:
        async with async_db_pool.acquire() as conn:
            df_products_list_temp = []
            
            # Iterate through DPD and DPM tables
            for table_key in ['dpd', 'dpm']:
                table_sql_info = sql_components.get(table_key)
                if not table_sql_info: continue
                
                select_fields = table_sql_info.get("select_fields", [])
                table_name = table_sql_info.get("table_name")
                where_conditions_list = table_sql_info.get("where_conditions", [])
                params_list = table_sql_info.get("parameters", [])

                if not table_name or not select_fields: continue

                select_fields_str = ", ".join(select_fields)
                sql_query_string = f"SELECT {select_fields_str} FROM {table_name} AS {table_key}"
                if where_conditions_list: 
                    sql_query_string += " WHERE " + " AND ".join(where_conditions_list)

                current_logger.debug(f"{log_prefix_load}Async Product SQL ({table_key}): {sql_query_string}")
                
                async with conn.cursor() as cursor:
                    await cursor.execute(sql_query_string, params_list if params_list else None)
                    # Fetch headers and rows
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        rows = await cursor.fetchall()
                        
                        if rows:
                            df_table = pd.DataFrame(rows, columns=columns)
                            df_table['source_table'] = table_key
                            df_products_list_temp.append(df_table)
                            current_logger.debug(f"{log_prefix_load}Async retrieved {len(df_table)} rows from {table_key}")

        # Process results if we have data
        if df_products_list_temp:
            df_products_combined = pd.concat(df_products_list_temp, ignore_index=True)
                        
            # Ensure all potentially missing columns exist
            for col in ['original_locations_dpd', 'original_locations_dpm', 'gimmicks', 'price_unit', 'event_date', 'timestamp']:
                if col not in df_products_combined.columns:
                    df_products_combined[col] = None

            # Unify Location Columns
            df_products_combined['location_display_for_llm'] = df_products_combined['original_locations_dpd'].fillna(
                df_products_combined['original_locations_dpm']
            )
            df_products_combined['original_locations'] = df_products_combined['location_display_for_llm']

            # Normalize date fields to a unified 'unified_date' column
            def normalize_date_fields(row):
                unified_date = None
                if row['source_table'] == 'dpd' and pd.notna(row.get('event_date')):
                    unified_date = str(row['event_date'])
                elif row['source_table'] == 'dpm' and pd.notna(row.get('timestamp')):
                    timestamp_str = str(row['timestamp'])
                    if len(timestamp_str) >= 8 and '_' in timestamp_str:
                        date_part = timestamp_str.split('_')[0]
                        if len(date_part) == 8:
                            unified_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                row['unified_date'] = unified_date
                return row

            df_products_combined = df_products_combined.apply(normalize_date_fields, axis=1)

            # Clean and Standardize Price Unit & Gimmicks for 'dpm'
            def clean_dpm_data(row):
                if row['source_table'] == 'dpm':
                    gimmick_str = str(row.get('gimmicks', ''))
                    price_unit_keywords = ['/bulan', 'per bulan', '/tahun', 'per tahun']
                    for keyword in price_unit_keywords:
                        if keyword in gimmick_str.lower():
                            row['price_unit'] = 'per bulan' if 'bulan' in keyword else 'per tahun'
                            row['gimmicks'] = gimmick_str.replace(keyword, '').strip()
                            break
                return row
            
            df_products_combined = df_products_combined.apply(clean_dpm_data, axis=1)

            product_count = len(df_products_combined)
            product_data_list = df_products_combined.to_dict('records')
            
            # Final loop to ensure no NaN values and add file_type
            for entry in product_data_list:
                entry['file_type'] = 'product_detail' if entry.get('source_table') == 'dpd' else 'product_matpro'
                for key, value in entry.items():
                    if pd.isna(value):
                        entry[key] = None
            
            debug_log_product_data(product_data_list, "AFTER_CLEANING_WITH_DATES", current_logger)

    except Exception as e:
        current_logger.error(f"{log_prefix_load}Unexpected error during data loading: {e}", exc_info=True)

    if product_data_list:
        if query_analysis:
            product_data_list = process_database_results_for_llm(product_data_list, query_analysis)
            current_logger.debug(f"Performed additional normalization on {len(product_data_list)} products.")
        
        current_logger.info(f"Storing {len(product_data_list)} items in cache for key: {cache_key}")
        market_data_cache[cache_key] = product_data_list

    return product_data_list, product_count


async def load_consolidated_isp_data_from_db_promo_async(
    dynamic_sql_filters_promo: dict,
    original_query_text_for_logging: str,
    logger_instance=None,
    query_analysis=None
):
    """
    Enhanced async version with timestamp column support and date normalization.
    Purely async implementation (removed synchronous fallbacks).
    """
    global logger, async_db_pool
    current_logger = logger_instance if logger_instance else logger
    log_prefix_load = f"[LoadPromoDB Query: '{original_query_text_for_logging[:30]}...'] "

    if not dynamic_sql_filters_promo or dynamic_sql_filters_promo.get("status") == "error":
        current_logger.error(f"{log_prefix_load}Received error or empty dynamic_sql_filters for promos.")
        return [], 0

    sql_components = dynamic_sql_filters_promo.get('sql_components', {})
    promo_sql_info = sql_components.get('promo_detail')

    if not promo_sql_info or not isinstance(promo_sql_info, dict):
        current_logger.warning(f"{log_prefix_load}No 'promo_detail' sql_components found for promo loading.")
        return [], 0

    if not async_db_pool:
        current_logger.error(f"{log_prefix_load} Critical: Async DB pool is not initialized.")
        return [], 0

    promo_data_list = []
    promo_count = 0

    def normalize_promo_date_fields(entry):
        """Normalize date fields for promotional data."""
        import datetime
        
        def safe_date_parse(date_value):
            if not date_value or pd.isna(date_value): return None
            date_str = str(date_value).strip()
            if date_str in ['0000-00-00', '0000-00-00 00:00:00', 'NULL', 'None', '']: return None
            try:
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%m/%d/%Y']:
                    try:
                        parsed_date = datetime.datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
                return None
            except Exception:
                return None
        
        normalized_start = safe_date_parse(entry.get('start_date'))
        normalized_end = safe_date_parse(entry.get('end_date'))
        unified_date = normalized_end or normalized_start
        
        entry['start_date'] = normalized_start
        entry['end_date'] = normalized_end
        entry['unified_date'] = unified_date
        entry['has_valid_dates'] = bool(normalized_start or normalized_end)
        return entry

    try:
        async with async_db_pool.acquire() as conn:
            select_fields = promo_sql_info.get("select_fields", [])
            table_name = promo_sql_info.get("table_name")
            where_conditions_list = promo_sql_info.get("where_conditions", [])
            params_list = promo_sql_info.get("parameters", [])

            # Simple SQL Construction
            select_fields_str = ", ".join(select_fields)
            sql_query_string = f"SELECT {select_fields_str} FROM {table_name} AS promo"

            if where_conditions_list:
                # Basic safety check on conditions (optional, but good practice if strings come from outside)
                safe_conditions = [cond for cond in where_conditions_list 
                                 if re.match(r"^[a-zA-Z0-9_.*,'() =<>!%+-]+$", cond.replace('%s','').strip())]
                if safe_conditions:
                    sql_query_string += " WHERE " + " AND ".join(safe_conditions)

            current_logger.debug(f"{log_prefix_load}Async Promo SQL: {sql_query_string}")
            
            async with conn.cursor() as cursor:
                await cursor.execute(sql_query_string, params_list if params_list else None)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = await cursor.fetchall()
                    df_promo = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
                else:
                    df_promo = pd.DataFrame()

        promo_count = len(df_promo)
        current_logger.info(f"{log_prefix_load}Promo table fetched. Rows: {promo_count}")
        
        if not df_promo.empty:
            for _, row_series in df_promo.iterrows():
                entry = row_series.to_dict()
                entry['source_table'] = promo_sql_info.get("table_name", "dashboard_provider_matpro")
                entry['file_type'] = 'promo_detail'
                entry['original_locations'] = entry.get('original_locations_promo', "")
                
                # Handle NaN values
                for key, value in entry.items():
                    if pd.isna(value):
                        entry[key] = None
                
                entry = normalize_promo_date_fields(entry)
                promo_data_list.append(entry)

    except Exception as e:
        current_logger.error(f"{log_prefix_load}Unexpected error in promo loading: {e}", exc_info=True)

    return promo_data_list, promo_count


# Direct async replacements - no wrappers needed
async def load_consolidated_isp_data_from_db_product(
    dynamic_sql_filters_product: dict,
    original_query_text_for_logging: str,
    logger_instance=None,
    query_analysis=None
):
    """
    Enhanced async version with date column support.
    Direct replacement - no wrapper complexity.
    """
    return await load_consolidated_isp_data_from_db_product_async(
        dynamic_sql_filters_product,
        original_query_text_for_logging,
        logger_instance,
        query_analysis
    )


async def load_consolidated_isp_data_from_db_promo(
    dynamic_sql_filters_promo: dict,
    original_query_text_for_logging: str,
    logger_instance=None,
    query_analysis=None
):
    """
    Enhanced async version with timestamp column support.
    Direct replacement - no wrapper complexity.
    """
    return await load_consolidated_isp_data_from_db_promo_async(
        dynamic_sql_filters_promo,
        original_query_text_for_logging,
        logger_instance,
        query_analysis
    )


# ------------------------------ HELPER: Normalize ------------------------------ #
def parse_speed_range(speed_value):
    """
    Parse speed values that could be single values or ranges.
    
    Args:
        speed_value (str): Speed value from filter extraction
        
    Returns:
        dict: Contains min_speed and/or max_speed if applicable
    """
    if not speed_value:
        return {}
    
    result = {}
    
    # Check if it's a range (contains hyphen)
    if '-' in speed_value:
        parts = speed_value.split('-')
        if len(parts) == 2:
            try:
                min_val = int(parts[0].strip())
                max_val = int(parts[1].strip())
                result['min_speed'] = min_val
                result['max_speed'] = max_val
                return result
            except ValueError:
                pass  # Fall through to other parsing if range format is invalid
    
    # Check for comparison operators
    if speed_value.startswith('>=') or speed_value.startswith('='):
        try:
            min_val = int(speed_value[2:].strip())
            result['min_speed'] = min_val
            return result
        except ValueError:
            pass
            
    if speed_value.startswith('<=') or speed_value.startswith('='):
        try:
            max_val = int(speed_value[2:].strip())
            result['max_speed'] = max_val
            return result
        except ValueError:
            pass
            
    if speed_value.startswith('>'):
        try:
            min_val = int(speed_value[1:].strip())
            result['min_speed'] = min_val + 1  # Exclusive lower bound
            return result
        except ValueError:
            pass
            
    if speed_value.startswith('<'):
        try:
            max_val = int(speed_value[1:].strip())
            result['max_speed'] = max_val - 1  # Exclusive upper bound
            return result
        except ValueError:
            pass
    
    # Try to parse as a single value
    try:
        exact_val = int(speed_value.strip())
        result['exact_speed'] = exact_val
        return result
    except ValueError:
        pass
        
    # If we get here, parsing failed
    return {}


# ----------------------------------------------------------------------------------------------------------------------- #
# -------------------------------------------- HELPER: Debugger              -------------------------------------------- #
# ----------------------------------------------------------------------------------------------------------------------- #
def debug_log_product_data(products, stage_name, logger):
    """Debug logger to trace product data through processing stages"""
    if not logger: 
        return
    
    logger.debug(f"\n=== DEBUG: {stage_name} ===")
    logger.debug(f"Total products: {len(products)}")
    
    if products and len(products) > 0:
        # Log first product as sample
        sample = products[0]
        logger.debug(f"Sample product structure:")
        for key, value in sample.items():
            # Defensively convert any value to string before slicing
            value_str = str(value) if value is not None else ""
            if 'location' in key.lower():
                logger.debug(f"  {key}: {value_str[:100]}...")
            else:
                logger.debug(f"  {key}: {value_str}") # Log full value if not location
    
    # Check location data specifically for the first 5 products
    for idx, product in enumerate(products[:5]):
        # --- ROBUST DATA GETTING AND CONVERSION ---
        # Get the value and immediately ensure it's a string.
        # This handles cases where the value is None, a float (nan), or an int.
        locations_dpd_str = str(product.get('original_locations_dpd', ''))
        locations_dpm_str = str(product.get('original_locations_dpm', ''))
        location_display_str = str(product.get('location_display', ''))
        location_available = product.get('location_available', None) # This can remain as boolean/None
        
        logger.debug(f"\nProduct {idx + 1}:")
        # Now, slice the guaranteed string variables
        logger.debug(f"  original_locations_dpd: {locations_dpd_str[:50]}..." if locations_dpd_str else "  original_locations_dpd: EMPTY")
        logger.debug(f"  original_locations_dpm: {locations_dpm_str[:50]}..." if locations_dpm_str else "  original_locations_dpm: EMPTY")
        logger.debug(f"  location_display: {location_display_str}")
        logger.debug(f"  location_available: {location_available}")
    
    logger.debug(f"=== END DEBUG: {stage_name} ===\n")

# ----------------------------------------------------------------------------------------------------------------------- #
# -------------------------------------------- HELPER: Tabulator formulating -------------------------------------------- #
# ----------------------------------------------------------------------------------------------------------------------- #
def _parse_llm_final_response(llm_response_text: str, log_prefix: str = ""):
    """
    Parses the final LLM response text to separate natural language summary
    from structured data (for Tabulator) marked by ITEM_START/ITEM_END.
    Dynamically sets Tabulator columns based on content type (product vs. promo).
    Enhanced with event_date and timestamp columns for data freshness tracking.
    Optimized for high-concurrency async usage.

    Args:
        llm_response_text: The raw text output from the LLM.
        log_prefix: Prefix for log messages.

    Returns:
        A tuple: (natural_language_summary_html, final_json_for_tabulator)
    """
    global logger # Assuming logger is globally defined

    def _convert_timestamp_to_readable(timestamp_str):
        """
        Convert timestamp format '20250516_144853' to date format (daily basis).
        Returns formatted date string or original if conversion fails.
        """
        try:
            if isinstance(timestamp_str, str) and len(timestamp_str) == 15 and '_' in timestamp_str:
                date_part, time_part = timestamp_str.split('_')
                if len(date_part) == 8 and len(time_part) == 6:
                    year = date_part[:4]
                    month = date_part[4:6]
                    day = date_part[6:8]
                    return f"{year}-{month}-{day}"
        except Exception as e:
            logger.debug(f"{log_prefix} Failed to convert timestamp '{timestamp_str}': {e}")
        return timestamp_str  # Return original if conversion fails

    def _is_promo_item(item):
        """
        FIXED: Detect promo items by checking for promo-specific fields,
        not just FileType which might be "N/A"
        """
        # Check for promo-specific fields
        has_promo_title = bool(item.get("Promo_Title"))
        has_content_summary = bool(item.get("Content_Summary")) 
        has_promo_url = bool(item.get("Promo_URL"))
        has_start_or_end_date = bool(item.get("Start_Date")) or bool(item.get("End_Date"))
        
        # Also check FileType as fallback
        has_promo_filetype = "promo" in str(item.get("FileType", "")).lower()
        
        # Item is promo if it has promo fields OR explicit promo filetype
        return has_promo_title or has_content_summary or has_promo_url or has_start_or_end_date or has_promo_filetype

    def _is_product_item(item):
        """
        Detect product items by checking for product-specific fields
        """
        has_product_name = bool(item.get("Name/Title"))
        has_speed = bool(item.get("Speed"))
        has_price = bool(item.get("Price"))
        
        # Also check FileType
        has_product_filetype = "product" in str(item.get("FileType", "")).lower()
        
        return has_product_name or has_speed or has_price or has_product_filetype

    natural_language_summary_html = ""
    final_json_for_tabulator = None
    parsed_items_for_tabulator = []

    item_start_marker = "ITEM_START"
    item_end_marker = "ITEM_END"

    all_item_blocks_matches = list(re.finditer(
        f"^{re.escape(item_start_marker)}(.*?)^{re.escape(item_end_marker)}", # Added ^ to ensure start/end of line for markers
        llm_response_text,
        re.DOTALL | re.MULTILINE | re.IGNORECASE # Added MULTILINE
    ))

    if all_item_blocks_matches:
        logger.info(f"{log_prefix} Found {len(all_item_blocks_matches)} ITEM_START...ITEM_END blocks.")
        first_block_start_offset = all_item_blocks_matches[0].start()
        summary_part_text = llm_response_text[:first_block_start_offset].strip()
        natural_language_summary_html = clean_response(summary_part_text) # Assuming clean_response is defined

        for match_obj in all_item_blocks_matches:
            block_content = match_obj.group(1).strip()
            item_data = {}

            current_key = None
            accumulated_value = []
            # Regex to capture "Key Name: Value" allowing for multi-line values if no new key starts
            line_parser_regex = re.compile(r'^\s*([^:]+?)\s*:\s*(.*)$', re.MULTILINE)

            lines_in_block = block_content.split('\n')
            i = 0
            while i < len(lines_in_block):
                line = lines_in_block[i].strip()
                if not line:
                    i += 1
                    continue
                
                match = line_parser_regex.match(line)
                if match:
                    # If a previous key was being processed, store its accumulated value
                    if current_key and accumulated_value:
                        item_data[current_key.strip()] = "\n".join(accumulated_value).strip()
                    
                    current_key = match.group(1).strip()
                    value_part = match.group(2).strip()
                    accumulated_value = [value_part] if value_part else [] # Start new accumulation
                elif current_key: # This line is a continuation of the previous key's value
                    accumulated_value.append(line)
                i += 1
            
            # Store the last accumulated key-value pair
            if current_key and accumulated_value:
                item_data[current_key.strip()] = "\n".join(accumulated_value).strip()
            elif current_key and not accumulated_value: # Key with empty value
                 item_data[current_key.strip()] = ""

            if item_data:
                # Normalize keys and structure the item
                normalized_item = {}
                for k, v_str in item_data.items():
                    k_lower_normalized = k.lower().replace("_", " ").strip()
                    v_str = str(v_str).strip() # Ensure value is string and stripped

                    if "provider" in k_lower_normalized: 
                        normalized_item["Provider"] = v_str
                    
                    # PRODUCT FIELDS
                    elif "nama paket" in k_lower_normalized or "product name" in k_lower_normalized:
                        normalized_item["Name/Title"] = v_str # For products
                    elif "kecepatan" in k_lower_normalized or "speed" in k_lower_normalized:
                        normalized_item["Speed"] = v_str
                    elif "harga" == k_lower_normalized or "price" == k_lower_normalized:
                        normalized_item["Price"] = v_str # e.g., "350000" or "N/A"
                    elif "price unit" in k_lower_normalized:
                        normalized_item["Price Unit"] = v_str
                    elif "area cakupan" in k_lower_normalized or "locations" in k_lower_normalized or "coverage" in k_lower_normalized:
                        normalized_item["Coverage Area"] = v_str
                    elif "benefit" in k_lower_normalized or "gimmicks" in k_lower_normalized or "fitur" in k_lower_normalized:
                        normalized_item["Benefits/Gimmicks"] = v_str # Primarily for products
                    elif "event date" in k_lower_normalized or "eventdate" in k_lower_normalized:
                        normalized_item["EventDate"] = v_str # From dashboard_product_detail
                    elif "timestamp" in k_lower_normalized:
                        # Convert timestamp format 20250516_144853 to date format
                        readable_timestamp = _convert_timestamp_to_readable(v_str)
                        normalized_item["Timestamp"] = readable_timestamp # From dashboard_provider_matpro
                    
                    # PROMO FIELDS - Updated mapping
                    elif "promo title" in k_lower_normalized or "judul promo" in k_lower_normalized:
                        normalized_item["Promo_Title"] = v_str
                    elif "content summary" in k_lower_normalized or "ringkasan" in k_lower_normalized or "summary" in k_lower_normalized:
                        normalized_item["Content_Summary"] = v_str
                    elif "start date" in k_lower_normalized or "tanggal mulai" in k_lower_normalized:
                        normalized_item["Start_Date"] = v_str
                    elif "end date" in k_lower_normalized or "tanggal akhir" in k_lower_normalized:
                        normalized_item["End_Date"] = v_str
                    elif "promo url" in k_lower_normalized or "link promo" in k_lower_normalized or "promo link" in k_lower_normalized:
                        normalized_item["Promo_URL"] = v_str
                    elif "periode" in k_lower_normalized or "validity" in k_lower_normalized:
                        normalized_item["Period"] = v_str # LLM might provide this as a string summary

                    # METADATA FIELDS
                    elif "file type" in k_lower_normalized: # Crucial for type detection
                        normalized_item["FileType"] = v_str.lower() # e.g., "promo_detail", "product_detail"
                    elif "data source table" in k_lower_normalized:
                        normalized_item["SourceTable"] = v_str

                    else: # Keep unmapped keys if any, title-cased
                        normalized_item[k.replace(" ", "_").title()] = v_str
                
                # Add item if it has proper identifying fields
                has_product_fields = normalized_item.get("Name/Title")
                has_promo_fields = normalized_item.get("Provider") and normalized_item.get("Promo_Title")
                
                if has_product_fields or has_promo_fields:
                    parsed_items_for_tabulator.append(normalized_item)
                else:
                    logger.warning(f"{log_prefix} Item block parsed but lacks required fields. Item_data: {item_data}")
            else:
                logger.warning(f"{log_prefix} Failed to parse key-value pairs from ITEM block or item_data remained empty. Content: {block_content[:200]}...")

        # ---- FIXED: Dynamic Column Definition based on content ----
        if parsed_items_for_tabulator:
            # FIXED: Use the new detection functions instead of FileType only
            num_promo_items = sum(1 for item in parsed_items_for_tabulator if _is_promo_item(item))
            num_product_items = sum(1 for item in parsed_items_for_tabulator if _is_product_item(item))
            
            is_exclusively_promo = num_promo_items > 0 and num_product_items == 0
            is_exclusively_product = num_product_items > 0 and num_promo_items == 0
            is_mixed_primarily_promo = num_promo_items > 0 and num_promo_items >= num_product_items

            logger.info(f"{log_prefix} Item type check for Tabulator: Promos={num_promo_items}, Products={num_product_items}. Exclusively Promo: {is_exclusively_promo}")

            tabulator_cols_definitions = []

            if is_exclusively_promo:
                logger.info(f"{log_prefix} Defining columns EXCLUSIVELY for PROMO details.")
                tabulator_cols_definitions = [
                    {"title": "Provider", "field": "Provider", "headerFilter": "input", "minWidth": 120, "tooltip": True, "sorter": "string"},
                    {"title": "Promo Title", "field": "Promo_Title", "headerFilter": "input", "minWidth": 200, "tooltip": True, "sorter": "string", "formatter": "textarea"},
                    {"title": "Content Summary", "field": "Content_Summary", "headerFilter": "input", "minWidth": 250, "tooltip": True, "formatter": "textarea", "sorter": "string"},
                    {"title": "Start Date", "field": "Start_Date", "minWidth": 110, "tooltip": True, "sorter": "date", "sorterParams": {"format": "YYYY-MM-DD", "alignEmptyValues": "bottom"}, "hozAlign": "center"},
                    {"title": "End Date", "field": "End_Date", "minWidth": 110, "tooltip": True, "sorter": "date", "sorterParams": {"format": "YYYY-MM-DD", "alignEmptyValues": "bottom"}, "hozAlign": "center"},
                    {
                        "title": "Promo URL", 
                        "field": "Promo_URL", 
                        "minWidth": 120, 
                        "formatter": "link", 
                        "formatterParams": { 
                            "label": "View Promo",     
                            "target": "_blank"
                        }, 
                        "tooltip": True, 
                        "sorter": "string", 
                        "hozAlign": "center"
                    }
                ]

            else: # Default to product-centric view or a mixed view
                tabulator_cols_definitions = [
                    {"title": "Provider", "field": "Provider", "headerFilter": "input", "minWidth": 120, "tooltip": True, "sorter": "string"},
                    {"title": "Name/Title", "field": "Name/Title", "headerFilter": "input", "minWidth": 200, "tooltip": True, "sorter": "string", "formatter": "textarea"},
                    {"title": "Speed (Mbps)", "field": "Speed", "minWidth": 100, "tooltip": True, "sorter": "number", "sorterParams": {"alignEmptyValues": "bottom"}, "hozAlign": "center"},
                    {"title": "Price", "field": "Price", "minWidth": 120, "tooltip": True, "sorter": "number", "sorterParams": {"alignEmptyValues": "bottom", "thousandSeparator": ",", "decimalSeparator": "."}, "hozAlign": "right", "formatter": "money", "formatterParams": {"symbol": "Rp ", "thousand": ",", "precision": 0}},
                    {"title": "Unit", "field": "Price Unit", "minWidth": 80, "tooltip": True, "sorter": "string"},
                    {"title": "Coverage Area", "field": "Coverage Area", "minWidth": 150, "tooltip": True, "formatter": "textarea", "sorter": "string"},
                    {"title": "Benefits/Gimmicks", "field": "Benefits/Gimmicks", "minWidth": 180, "tooltip": True, "formatter": "textarea", "sorter": "string"},
                    {"title": "Event Date", "field": "EventDate", "minWidth": 110, "tooltip": True, "sorter": "date", "sorterParams": {"format": "YYYY-MM-DD", "alignEmptyValues": "bottom"}, "hozAlign": "center"},
                ]
                
                if num_promo_items > 0: # If mixed content, add relevant promo columns
                    logger.info(f"{log_prefix} Adding supplementary promo columns for mixed view.")

                    # Add promo-specific columns for mixed view
                    promo_columns_to_add = [
                        {"title": "Promo Title", "field": "Promo_Title", "headerFilter": "input", "minWidth": 180, "tooltip": True, "formatter": "textarea", "sorter": "string"},
                        {"title": "Content Summary", "field": "Content_Summary", "headerFilter": "input", "minWidth": 200, "tooltip": True, "formatter": "textarea", "sorter": "string"},
                        {"title": "Start Date", "field": "Start_Date", "minWidth": 100, "tooltip": True, "sorter": "date", "sorterParams": {"format": "YYYY-MM-DD"}, "hozAlign": "center"},
                        {"title": "End Date", "field": "End_Date", "minWidth": 100, "tooltip": True, "sorter": "date", "sorterParams": {"format": "YYYY-MM-DD"}, "hozAlign": "center"},
                        {
                            "title": "Promo URL", 
                            "field": "Promo_URL", 
                            "minWidth": 100, 
                            "formatter": "link", 
                            "formatterParams": {
                                "label": "View Promo",
                                "target": "_blank"
                            }, 
                            "tooltip": True, 
                            "sorter": "string", 
                            "hozAlign": "center"
                        }
                    ]
                    
                    for col in promo_columns_to_add:
                        if not any(existing_col['field'] == col['field'] for existing_col in tabulator_cols_definitions):
                            tabulator_cols_definitions.append(col)

            # --- Populate Rows based on defined columns ---
            tabulator_data_rows = []
            for item in parsed_items_for_tabulator:
                row_data = {}
                for col_def in tabulator_cols_definitions:
                    field_value = item.get(col_def["field"], "")
                    # Attempt to convert to number for sorting/formatting if applicable
                    if col_def["field"] in ["Price", "Speed"]:
                        try:
                            # Remove commas for price, handle "N/A" or non-numeric for speed
                            if col_def["field"] == "Price" and isinstance(field_value, str):
                                cleaned_val = field_value.replace(",", "").replace("Rp", "").strip()
                                if cleaned_val.isdigit(): 
                                    field_value = int(cleaned_val)
                                else: 
                                    field_value = None # Or keep as string if formatting handles it
                            elif col_def["field"] == "Speed" and isinstance(field_value, str):
                                speed_match = re.match(r"(\d+)", field_value) # Extract leading numbers
                                if speed_match: 
                                    field_value = int(speed_match.group(1))
                                else: 
                                    field_value = None # Indicates N/A or non-standard speed for sorting
                        except ValueError:
                            field_value = None # Fallback for sorting if conversion fails
                    row_data[col_def["field"]] = field_value if field_value is not None else ""

                # Handle promo URL formatting
                if "Promo_URL" in row_data and row_data["Promo_URL"]:
                    link_text = str(row_data["Promo_URL"]).strip()
                    if link_text and not link_text.lower().startswith(('http://', 'https://')):
                        if "@" in link_text or "." not in link_text or " " in link_text: # If it doesn't look like a domain
                             pass # Keep as is, might be just text
                        else: 
                            row_data["Promo_URL"] = 'http://' + link_text # Prepend http if it looks like a domain
                    elif not link_text: 
                        row_data["Promo_URL"] = ""
                        
                tabulator_data_rows.append(row_data)

            tabulator_payload = {"type": "tabulator_data", "columns": tabulator_cols_definitions, "data": tabulator_data_rows}
            try:
                final_json_for_tabulator = json.dumps(tabulator_payload)
                logger.info(f"{log_prefix} Constructed Tabulator JSON. Rows: {len(tabulator_data_rows)}, Cols: {len(tabulator_cols_definitions)}. Promo view: {is_exclusively_promo}")
            except TypeError as te:
                logger.error(f"{log_prefix} TypeError during json.dumps: {te}. Payload keys: {list(tabulator_payload.keys())}", exc_info=True)
                natural_language_summary_html += "<br><p><em>Error: Tipe data tidak valid saat memformat tabel.</em></p>"
                final_json_for_tabulator = None # Important to nullify on error
            except Exception as e_dump:
                logger.error(f"{log_prefix} Exception during json.dumps: {e_dump}.", exc_info=True)
                natural_language_summary_html += "<br><p><em>Error: Gagal memformat data tabel karena kesalahan tak terduga.</em></p>"
                final_json_for_tabulator = None

        else: # No items were parsed from ITEM_START/END blocks
            logger.warning(f"{log_prefix} parsed_items_for_tabulator was empty. No table data generated.")
            # natural_language_summary_html is already set from before block processing
            # final_json_for_tabulator remains None

    else: # No ITEM_START...ITEM_END blocks in LLM response
        logger.info(f"{log_prefix} No ITEM_START...ITEM_END blocks in LLM response. Treating as full HTML response.")
        natural_language_summary_html = clean_response(llm_response_text) # clean_response should handle if text is empty
        # final_json_for_tabulator remains None

    return natural_language_summary_html, final_json_for_tabulator


def rank_and_filter_results(results: list, query: str, top_k: int = 10) -> list:
    """
    Scores and ranks database results based on relevance to the query.
    ENHANCED: Ensures fair provider representation when multiple providers are present.
    """
    if not results:
        return []

    query_lower = query.lower()
    # A simple set of stop words for ranking purposes
    stop_words_for_ranking = {'di', 'yang', 'dan', 'dari', 'ke', 'ini', 'itu', 'untuk', 'dengan'}
    query_words = {word for word in query_lower.split() if word not in stop_words_for_ranking}

    # Group results by provider first
    results_by_provider = {}
    for item in results:
        provider = item.get('provider', 'unknown').lower().strip()
        if provider not in results_by_provider:
            results_by_provider[provider] = []
        results_by_provider[provider].append(item)

    # If only one provider, use original logic
    if len(results_by_provider) == 1:
        return _score_and_rank_single_provider(results, query_words, top_k)
    
    # Multi-provider scenario: ensure balanced representation
    return _score_and_rank_multi_provider(results_by_provider, query_words, top_k)


def _score_and_rank_single_provider(results: list, query_words: set, top_k: int) -> list:
    """Original scoring logic for single provider scenarios"""
    scored_results = []
    for item in results:
        score = _calculate_relevance_score(item, query_words)
        item['_relevance_score'] = score
        scored_results.append(item)

    scored_results.sort(key=lambda x: x.get('_relevance_score', 0), reverse=True)
    return scored_results[:top_k]


def _score_and_rank_multi_provider(results_by_provider: dict, query_words: set, top_k: int) -> list:
    """Enhanced scoring logic ensuring fair provider representation"""
    
    # Step 1: Score items within each provider
    provider_scored_results = {}
    for provider, items in results_by_provider.items():
        scored_items = []
        for item in items:
            score = _calculate_relevance_score(item, query_words)
            item['_relevance_score'] = score
            scored_items.append(item)
        
        # Sort within provider by score
        scored_items.sort(key=lambda x: x.get('_relevance_score', 0), reverse=True)
        provider_scored_results[provider] = scored_items

    # Step 2: Determine fair allocation per provider
    num_providers = len(provider_scored_results)
    base_items_per_provider = max(1, top_k // num_providers)  # At least 1 per provider
    remaining_slots = top_k - (base_items_per_provider * num_providers)
    
    final_results = []
    
    # Step 3: Take base allocation from each provider
    provider_taken = {}
    for provider, scored_items in provider_scored_results.items():
        taken = min(base_items_per_provider, len(scored_items))
        final_results.extend(scored_items[:taken])
        provider_taken[provider] = taken
    
    # Step 4: Distribute remaining slots to providers with highest-scoring remaining items
    if remaining_slots > 0:
        # Create a pool of remaining items with their scores
        remaining_pool = []
        for provider, scored_items in provider_scored_results.items():
            taken_count = provider_taken[provider]
            remaining_items = scored_items[taken_count:]
            for item in remaining_items:
                remaining_pool.append((item, provider))
        
        # Sort remaining pool by score and take top items
        remaining_pool.sort(key=lambda x: x[0].get('_relevance_score', 0), reverse=True)
        
        for i in range(min(remaining_slots, len(remaining_pool))):
            final_results.append(remaining_pool[i][0])
    
    # Step 5: Final sort by score for consistent ordering
    final_results.sort(key=lambda x: x.get('_relevance_score', 0), reverse=True)
    
    # Debug logging (use your logger instead of print)
    provider_counts = {}
    for item in final_results:
        provider = item.get('provider', 'unknown').lower()
        provider_counts[provider] = provider_counts.get(provider, 0) + 1
    
    # Use your existing logger instance
    try:
        logger.info(f"BALANCED RANKING: Distributed {len(final_results)} items across {len(provider_counts)} providers: {provider_counts}")
    except:
        pass  # Fallback if logger not available
    
    return final_results[:top_k]


def _calculate_relevance_score(item: dict, query_words: set) -> float:
    """Calculate relevance score for a single item"""
    score = 0.0
    
    # --- Location Match Boost ---
    if item.get('location_available') is True:
        score += 10.0

    # Combine relevant text fields for keyword scoring
    item_name = str(item.get("product_name") or item.get("promo_title") or "").lower()
    item_details = str(item.get("gimmicks") or item.get("content_summary") or "").lower()
    provider_name = str(item.get("provider", "")).lower()

    # --- Keyword Scoring ---
    # +3 points for each query word found in the item's name/title
    score += sum(3 for word in query_words if word in item_name)
    
    # +2 points if the provider name itself was in the query
    if provider_name and provider_name in query_words:
        score += 2.0
        
    # +1 point for each query word found in the details/gimmicks
    score += sum(1 for word in query_words if word in item_details)

    return score


def detect_chart_request(query):
    """Detect if user is requesting charts/visualizations"""
    chart_keywords = ['chart', 'graph', 'visualization', 'plot', 'grafik', 'diagram', 'visualisasi', 'bar', 'pie', 'line']
    return any(keyword in query.lower() for keyword in chart_keywords)

def prioritize_content_for_charts(doc_input_data, query, chars_budget):
    """Prioritize raw content over summaries for chart requests"""
    
    is_chart_request = detect_chart_request(query)
    if not is_chart_request:
        return None  # Use existing logic
    
    # For chart requests, prioritize pages likely to contain numerical data
    page_analysis = doc_input_data.get('page_analysis', {})
    numerical_keywords = ['%', 'percent', 'number', 'data', 'statistics', 'results', 'metrics', 'total', 'amount', 'revenue', 'profit', 'loss', 'growth']
    
    prioritized_pages = []
    for page_num, page_data in page_analysis.items():
        raw_content = page_data.get('raw_content', '')
        summary = page_data.get('summary', '')
        
        # Check if page likely contains numerical data
        has_numbers = any(char.isdigit() for char in raw_content)
        has_keywords = any(keyword in (raw_content + summary).lower() for keyword in numerical_keywords)
        
        score = 0
        if has_numbers: score += 2
        if has_keywords: score += 1
        if len(raw_content) > 100: score += 1  # Prefer pages with substantial content
        
        if score > 0:
            prioritized_pages.append((int(page_num), score, page_data))
    
    # Sort by score (highest first)
    prioritized_pages.sort(key=lambda x: x[1], reverse=True)
    
    return prioritized_pages[:5]  # Return top 5 relevant pages


def should_use_expert_mode(query: str) -> bool:
    """Quick detection for queries needing expert knowledge"""
    expert_keywords = [
        # English keywords
        'recommend', 'suggest', 'best practice', 'expert', 'opinion',
        'successful elsewhere', 'industry standard', 'benchmark', 
        'lessons learned', 'risk', 'challenge', 'effective',
        'compare to', 'similar initiative', 'what would you',
        'analysis', 'assess', 'evaluate', 'judgment', 'advice',
        'strategy', 'approach', 'methodology', 'framework',
        'success rate', 'failure rate', 'case study',
        'competitor', 'market leader', 'industry leader',
        'best case', 'worst case', 'scenario', 'alternative',
        'pros and cons', 'advantages', 'disadvantages',
        'implementation', 'execution', 'rollout',
        'key success factors', 'critical success factors',
        'pitfall', 'mistake', 'lesson', 'experience',
        
        # Indonesian keywords
        'rekomendasi', 'saran', 'usul', 'anjuran', 'masukan',
        'praktik terbaik', 'best practice', 'standar industri',
        'ahli', 'pakar', 'expert', 'spesialis',
        'pendapat', 'opini', 'pandangan', 'perspektif',
        'penilaian', 'evaluasi', 'analisis', 'kajian',
        'strategi', 'pendekatan', 'metodologi', 'kerangka kerja',
        'berhasil', 'sukses', 'gagal', 'kegagalan',
        'risiko', 'tantangan', 'kendala', 'hambatan',
        'efektif', 'efisien', 'optimal', 'maksimal',
        'bandingkan', 'perbandingan', 'komparasi',
        'inisiatif serupa', 'program serupa', 'proyek serupa',
        'pengalaman', 'pelajaran', 'hikmah', 'pembelajaran',
        'faktor kunci', 'faktor penting', 'faktor sukses',
        'implementasi', 'penerapan', 'pelaksanaan', 'eksekusi',
        'studi kasus', 'case study', 'contoh kasus',
        'kompetitor', 'pesaing', 'pemimpin pasar',
        'alternatif', 'opsi lain', 'pilihan lain',
        'kelebihan', 'kekurangan', 'keuntungan', 'kerugian',
        'skenario', 'kemungkinan', 'proyeksi',
        'tingkat keberhasilan', 'tingkat kegagalan',
        'standar', 'acuan', 'benchmark', 'patokan',
        'industri', 'sektor', 'bidang usaha',
        'bagaimana jika', 'apa bila', 'seandainya',
        'sebaiknya', 'seharusnya', 'lebih baik',
        'disarankan', 'dianjurkan', 'direkomendasikan'
    ]
    
    return any(keyword in query.lower() for keyword in expert_keywords)



# ===================================
# Market Mode Helpers
# ===================================
def extract_market_sources_from_data(product_data: List[Dict], promo_data: List[Dict]) -> List[Dict[str, Any]]:
    """
    Extract unique source paths from market data and format them for frontend display.
    """
    source_set = set()  # For deduplication
    sources_list = []
    
    # Process product data (dashboard_provider_matpro has source column)
    for item in product_data:  # ← FIXED: Look in product_data instead
        source_path = item.get('source')
        if not source_path or source_path.strip() == '':
            continue
            
        # Deduplicate by source path
        if source_path in source_set:
            continue
        source_set.add(source_path)
        
        # Extract filename from path for display name
        filename = os.path.basename(source_path)
        provider = item.get('provider', 'Unknown')
        timestamp = item.get('timestamp', 'Unknown')
        
        # Create source object
        source_obj = {
            'name': f"{provider} Brochure - {filename}",
            'type': 'image',
            'path': source_path,
            'provider': provider,
            'timestamp': timestamp
        }
        sources_list.append(source_obj)
    
    logger.info(f"Extracted {len(sources_list)} unique market sources")
    return sources_list


def build_demo_market_context_sources(
    query_text: str,
    extracted_filters: Dict[str, Any],
    sampled_products_for_llm: List[Dict[str, Any]],
    ranked_promos: List[Dict[str, Any]],
    channel_matches_for_memory: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Build synthetic source references for portfolio demo UX when no concrete market
    source artifacts were attached to the response payload.
    """
    locations = extracted_filters.get("location") or extracted_filters.get("locations") or []
    providers = extracted_filters.get("providers") or []
    speed_exact = extracted_filters.get("speed_exact")
    speed_min = extracted_filters.get("speed_min")
    speed_max = extracted_filters.get("speed_max")

    location_label = ", ".join(locations[:2]) if locations else "Target Area"
    if speed_exact:
        speed_label = f"{speed_exact} Mbps"
    elif speed_min and speed_max:
        speed_label = f"{speed_min}-{speed_max} Mbps"
    elif speed_min:
        speed_label = f">= {speed_min} Mbps"
    elif speed_max:
        speed_label = f"<= {speed_max} Mbps"
    else:
        speed_label = "Mixed Speed Tiers"

    provider_label = ", ".join([str(p).title() for p in providers[:2]]) if providers else "Multi-Provider"
    timestamp_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    demo_sources = [
        {
            "path": "#",
            "title": f"Market Overview - {location_label}.pdf",
            "type": "pdf",
            "relevant_pages": [1, 2],
            "timestamp": timestamp_label
        },
        {
            "path": "#",
            "title": f"Package Matrix - {speed_label}.pdf",
            "type": "pdf",
            "relevant_pages": [1],
            "timestamp": timestamp_label
        },
        {
            "path": "#",
            "title": f"Channel: Market watch - {provider_label}",
            "type": "channel",
            "message_id": None,
            "timestamp": timestamp_label
        }
    ]

    if sampled_products_for_llm:
        first_product = sampled_products_for_llm[0]
        demo_sources[0]["title"] = f"Market Overview - {first_product.get('provider', provider_label).title()}.pdf"
    elif ranked_promos:
        first_promo = ranked_promos[0]
        demo_sources[1]["title"] = f"Promo Brief - {first_promo.get('provider', provider_label).title()}.pdf"

    if channel_matches_for_memory:
        first_channel = channel_matches_for_memory[0]
        summary = (first_channel.get("message_summary") or "Market signal")[:40]
        demo_sources[2]["title"] = f"Channel: {summary}"
        demo_sources[2]["message_id"] = first_channel.get("message_id")

    logger.info(
        "Built %s demo market context sources for query '%s'",
        len(demo_sources),
        query_text[:60]
    )
    return demo_sources


def validate_source_path(relative_path: str) -> str:
    """
    Validate and sanitize source path to prevent directory traversal attacks.
    
    Args:
        relative_path: The relative path from the request
        
    Returns:
        Full validated path
        
    Raises:
        HTTPException: If path is invalid or insecure
    """
    # Decode URL encoding
    decoded_path = unquote(relative_path)
    
    # Security checks
    if ".." in decoded_path or decoded_path.startswith("/"):
        logger.warning(f"Potential directory traversal attempt blocked: {decoded_path}")
        raise HTTPException(status_code=400, detail="Invalid filename provided")
    
    # Construct full path
    full_path = os.path.abspath(os.path.join(MARKET_SOURCES_BASE_DIR, decoded_path))
    
    # Ensure path is within allowed directory
    abs_base_dir = os.path.abspath(MARKET_SOURCES_BASE_DIR)
    if not full_path.startswith(abs_base_dir):
        logger.critical(f"SECURITY ALERT: Attempt to access file outside base directory blocked. Requested: '{decoded_path}', Resolved: '{full_path}'")
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Check if file exists
    if not os.path.exists(full_path) or not os.path.isfile(full_path):
        logger.warning(f"Market source file not found: {full_path}")
        raise HTTPException(status_code=404, detail="Source file not found")
    
    return full_path

def resize_image(image_path: str, size: tuple, maintain_aspect_ratio: bool = True) -> bytes:
    """
    Resize image to specified dimensions and return as bytes.
    
    Args:
        image_path: Path to the source image
        size: Target size as (width, height)
        maintain_aspect_ratio: Whether to maintain aspect ratio
        
    Returns:
        Image bytes in JPEG format
    """
    try:
        with Image.open(image_path) as img:
            # Convert to RGB if necessary (for JPEG output)
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            if maintain_aspect_ratio:
                img.thumbnail(size, Image.Resampling.LANCZOS)
            else:
                img = img.resize(size, Image.Resampling.LANCZOS)
            
            # Save to bytes buffer
            img_buffer = io.BytesIO()
            img.save(img_buffer, format='JPEG', quality=85, optimize=True)
            img_buffer.seek(0)
            
            return img_buffer.getvalue()
    except Exception as e:
        logger.error(f"Error resizing image {image_path}: {e}")
        raise HTTPException(status_code=500, detail="Failed to process image")



# ===================================
# Helper: Document mode
# ===================================
def find_relevant_pages_semantic(query: str, doc_entry: dict, embedding_model_name: str = EMBEDDING_MODEL, limit: int = 10, threshold: float = 0.0, _rag_sid: str = "") -> dict:
    """
    STRENGTHENED Semantic Search with Query Token Anchoring:
     - PHASE 1 (Token Anchoring): Find pages containing EXACT query tokens (numbers, codes, prices)
     - PHASE 2 (Semantic): Embed summaries + raw content snippets for semantic matching
     - PHASE 3 (Fusion): Combine token-anchored pages with HIGHLY relevant semantic pages
    This prevents the LLM from hallucinating citations by ensuring pages with
    exact query matches are ALWAYS included, regardless of semantic score.

    Returns only genuinely relevant pages (no padding to limit).
    Display count reflects actual relevance, not an arbitrary cap.
    """
    _tpg = time.time()
    doc_path_for_log = doc_entry.get('relative_path', 'Unknown')
    logger.info(f"[Semantic] Starting enhanced search for '{doc_path_for_log}' with query: '{query[:50]}...'")
    
    result = {
        'pages': [],
        'fallback_mode': None,
        'fallback_reason': None,
        'token_anchored_pages': [],
        'semantic_pages': []
    }
    
    page_analysis_data = doc_entry.get('page_analysis', {})
    if not query or not page_analysis_data:
        result['fallback_reason'] = "missing_data"
        return result

    # =========================================================================
    # PHASE 1: QUERY TOKEN ANCHORING (Critical for citation accuracy)
    # =========================================================================
    
    def extract_anchor_tokens(query_str: str) -> list:
        """Extract tokens that are likely specific identifiers requiring exact page match"""
        tokens = []
        
        # Pattern 1: Numbers with units (e.g., "100mbps", "50gb", "300ribu")
        tokens.extend(re.findall(r'\b\d+(?:\s*(?:mbps|gbps|gb|tb|ribu|juta|rb|jt|%|rupiah|rp))\b', query_str, re.IGNORECASE))
        
        # Pattern 2: Standalone significant numbers (prices, speeds, percentages)
        tokens.extend(re.findall(r'\b\d{2,}\b', query_str))
        
        # Pattern 3: Product codes / SKUs (alphanumeric patterns)
        tokens.extend(re.findall(r'\b[A-Z]{2,}\d+[A-Z0-9]*\b', query_str, re.IGNORECASE))
        
        # Pattern 4: Price patterns (e.g., "350.000", "1,500,000")
        tokens.extend(re.findall(r'\b\d{1,3}(?:[.,]\d{3})+\b', query_str))
        
        # Pattern 5: Formulas or special patterns (e.g., "2+0", "50/50")
        tokens.extend(re.findall(r'\b\d+[+\-x/]\d+\b', query_str))
        
        # Deduplicate and clean
        cleaned_tokens = list(set([t.lower().strip() for t in tokens if len(t) >= 2]))
        logger.debug(f"[Semantic] Extracted anchor tokens: {cleaned_tokens}")
        return cleaned_tokens
    
    anchor_tokens = extract_anchor_tokens(query)
    token_anchored_pages = []
    
    if anchor_tokens:
        logger.info(f"[Semantic] Phase 1: Searching for anchor tokens: {anchor_tokens}")
        
        for page_num_str, details in page_analysis_data.items():
            if not page_num_str.isdigit():
                continue
                
            raw_content = (details.get('raw_content') or "").lower()
            summary = (details.get('summary') or "").lower()
            combined_content = f"{raw_content} {summary}"
            
            match_count = 0
            matched_tokens = []
            for token in anchor_tokens:
                if re.search(r'\b' + re.escape(token) + r'\b', combined_content, re.IGNORECASE):
                    match_count += 1
                    matched_tokens.append(token)
                elif token.isdigit() and token in combined_content:
                    match_count += 1
                    matched_tokens.append(token)
            
            if match_count > 0:
                token_anchored_pages.append({
                    'page': int(page_num_str),
                    'match_count': match_count,
                    'matched_tokens': matched_tokens
                })
        
        token_anchored_pages.sort(key=lambda x: x['match_count'], reverse=True)
        result['token_anchored_pages'] = [p['page'] for p in token_anchored_pages[:3]]
        
        logger.info(f"[Semantic] Phase 1 Complete: Found {len(token_anchored_pages)} pages with anchor tokens. "
                   f"Top matches: {result['token_anchored_pages']}")

    # =========================================================================
    # PHASE 2: SEMANTIC EMBEDDING (LanceDB Optimized)
    # =========================================================================
    
    # Clean query for embedding
    stop_phrases = ["can you", "please", "tell me", "what is", "how to", "describe", 
                    "summarize", "find", "search", "tolong", "bisa", "apa", "bagaimana"]
    clean_query = query.lower()
    for phrase in stop_phrases:
        clean_query = clean_query.replace(phrase, "")
    clean_query = clean_query.strip()
    if len(clean_query) < 3:
        clean_query = query

    try:
        if not tbl_doc_pages:
            raise ValueError("LanceDB 'document_pages' table not accessible.")

        api_key = get_effective_gemini_api_key()
        if not api_key:
            raise ValueError("Gemini API key not available for semantic page search.")
        logger.info("Semantic page search embedding call starting")
        genai.configure(api_key=api_key)
        
        # 1. Embed Query (Single API Call)
        query_embedding = genai.embed_content(
            model=embedding_model_name,
            content=clean_query,
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=768
        )['embedding']

        # 2. Search LanceDB for pages belonging to this document
        filename = os.path.basename(doc_entry.get('relative_path', ''))
        
        # Search for top matches within this specific document
        # Escape single quotes in filename for SQL safety (e.g. "Okt '24")
        safe_filename = filename.replace("'", "''")
        search_results = tbl_doc_pages.search(query_embedding)\
            .where(f"filename = '{safe_filename}'")\
            .limit(limit * 2)\
            .to_list()

        if not search_results:
             if result['token_anchored_pages']:
                result['pages'] = result['token_anchored_pages']
                result['fallback_mode'] = 'token_anchor_only'
                return result
             result['fallback_reason'] = "zero_scores_lancedb"
             return result

        # 3. Format results: [(page_num, score), ...]
        # LanceDB returns distance. Score = 1 - distance.
        scored_pages = [(r['page_number'], 1 - r['_distance']) for r in search_results]
        scored_pages.sort(key=lambda x: x[1], reverse=True)

        # =========================================================================
        # PHASE 3: ADAPTIVE THRESHOLD (Lowered to reduce aggressive dropping)
        # =========================================================================
        
        best_score = scored_pages[0][1]
        relevance_cutoff = best_score * 0.80
        hard_floor = 0.45
        final_cutoff = max(relevance_cutoff, hard_floor)
        
        semantic_pages = []
        for pn, score in scored_pages:
            if score >= final_cutoff and len(semantic_pages) < limit:
                semantic_pages.append(pn)
        
        result['semantic_pages'] = semantic_pages
        
        logger.info(f"[Semantic] Phase 2 Complete: Best_Score={best_score:.4f}, "
                   f"Cutoff={final_cutoff:.4f}, Semantic Pages={semantic_pages}")

        # =========================================================================
        # PHASE 4: FUSION (Combine Token-Anchored + HIGH Semantic)
        # =========================================================================
        # RELEVANCE THRESHOLDS:
        # - Token-anchored pages: ALWAYS included (exact query match = high relevance)
        # - Semantic pages score >= 0.50: HIGH relevance (strong semantic match)
        # - Semantic pages score >= 0.35: MID relevance (contextual match)
        # NO PADDING: Only return genuinely relevant pages, display count reflects reality

        HIGH_SEMANTIC_THRESHOLD = 0.75  # Strong semantic match (increased)
        MID_SEMANTIC_THRESHOLD = 0.60    # Contextual match (increased)

        final_pages = []
        seen = set()

        # FIRST: Add all token-anchored pages (CRITICAL for citation accuracy)
        for pn in result['token_anchored_pages']:
            if pn not in seen:
                final_pages.append(pn)
                seen.add(pn)

        # SECOND: Add HIGH relevance semantic pages (strong match)
        for pn, score in scored_pages:
            if pn not in seen and score >= HIGH_SEMANTIC_THRESHOLD:
                final_pages.append(pn)
                seen.add(pn)

        # THIRD: Add MID relevance semantic pages (contextual) - only if not too many already
        # Cap at reasonable limit to avoid overwhelming context
        if len(final_pages) < 8:
            for pn, score in scored_pages:
                if pn not in seen and score >= MID_SEMANTIC_THRESHOLD and len(final_pages) < limit:
                    final_pages.append(pn)
                    seen.add(pn)

        final_pages.sort()
        result['pages'] = final_pages
        
        logger.info(f"[Semantic] FINAL: {len(final_pages)} pages selected. "
                   f"Token-Anchored: {result['token_anchored_pages']}, "
                   f"Semantic: {result['semantic_pages']}, "
                   f"Final: {final_pages}")
        if _rag_sid:
            rag_trace(_rag_sid, "PG",
                doc=os.path.basename(doc_path_for_log)[:50], pages=final_pages,
                best=round(best_score, 3), ms=round((time.time() - _tpg) * 1000))

        return result

    except Exception as e:
        logger.error(f"[Semantic] Embedding error: {e}", exc_info=True)
        
        if result['token_anchored_pages']:
            result['pages'] = result['token_anchored_pages']
            result['fallback_mode'] = 'token_anchor_after_error'
            return result
        
        result['fallback_reason'] = "exception_in_embedding"
        
        # 'page_numbers_map' no longer exists in this scope.
        # Fallback to getting all page numbers from the doc_entry
        all_pgs = sorted([int(k) for k in page_analysis_data.keys() if k.isdigit()])
        result['pages'] = all_pgs[:3]
        return result


def clean_document_footers(raw_content: str) -> str:
    """
    Aggressively remove footer page numbers that could confuse the LLM.
    These footers often show numbers like "8" or "Page 12" which the LLM
    might incorrectly use as citation page numbers.
    """
    if not raw_content:
        return ""
    
    cleaned = raw_content
    
    # Pattern 1: "Internal 12", "Confidential 8", "Page 8", "Halaman 12"
    cleaned = re.sub(
        r'(?im)^\s*(?:Internal|Confidential|Private|Restricted|Page|Halaman|Hal\.?)\s*\d+\s*$', 
        '', 
        cleaned
    )
    
    # Pattern 2: Standalone numbers at end of lines (likely footer page numbers)
    cleaned = re.sub(r'(?m)^\s*\d{1,3}\s*$', '', cleaned)
    
    # Pattern 3: Footer patterns like "Page 8 of 20" or "8/20"
    cleaned = re.sub(r'(?im)^\s*(?:Page\s*)?\d+\s*(?:of|/|dari)\s*\d+\s*$', '', cleaned)
    
    # Pattern 4: Copyright footers with page numbers
    cleaned = re.sub(r'(?im)^\s*©.*\d+\s*$', '', cleaned)
    
    # Pattern 5: Date + page number footers
    cleaned = re.sub(r'(?im)^\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s*\d+\s*$', '', cleaned)
    
    # Cleanup excessive newlines
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    
    return cleaned.strip()


def format_source_block(filename: str, page_num: int, content: str, content_type: str = "RAW") -> str:
    """Format a source block with strong filename anchoring to prevent hallucination."""
    cleaned = clean_document_footers(content) if content_type == "RAW" else content
    
    # CRITICAL: Repeat exact filename multiple times to prevent LLM from inventing filenames
    return (
        f"\n<<< SOURCE: {filename} | SYSTEM_INDEX_PAGE {page_num} | [{content_type}] >>>\n"
        f"[CITATION ANCHOR: Use EXACTLY \"{filename}\" as source - do NOT modify or infer filename]\n"
        f"{cleaned}\n"
        f"<<< END SOURCE: {filename} | PAGE {page_num} >>>\n"
    )


def format_document_header(doc_index: int, title: str, filename: str, pages: list) -> str:
    """
    Format document header with strong isolation warnings.
    Prevents the LLM from confusing pages across different documents.
    """
    pages_str = ', '.join(map(str, sorted(pages))) if pages else 'None'
    
    return f"""

########## DOCUMENT BOUNDARY ##########
########## START OF DOCUMENT {doc_index + 1} ##########

**DOCUMENT {doc_index + 1} METADATA**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Internal Title: {title}
• Official Filename: {filename}
• Pages Loaded: [{pages_str}]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ **CITATION SCOPE FOR THIS DOCUMENT:**
• When citing data from THIS document, use: [[Source: {filename} | Page: X]]
• Valid page numbers for THIS document: {pages_str}
• DO NOT use page numbers from other documents when citing data found here!

"""


def format_document_footer(doc_index: int, filename: str) -> str:
    """Format document footer with boundary marker"""
    return f"""

########## END OF DOCUMENT {doc_index + 1} ({filename}) ##########
########## DOCUMENT BOUNDARY ##########

"""

# ===================================
# Helper: Document & Market Mode (Both)
# ===================================
async def trigger_summarization_task(user_id: str, session_id: str):
    """
    Checks if conversation needs summarization and runs it in background.
    UPDATED: Uses direct DB count to ignore 'get_context' limits.
    """
    conn = None
    try:
        # 1. Get Actual Turn Count (Direct DB Query)
        total_turns = 0
        conn = get_db_connection() # Uses your local definition
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM conversation_memory WHERE user_id = ? AND session_id = ?",
            (user_id, session_id)
        )
        result = cursor.fetchone()
        if result:
            total_turns = result[0]
        
        logger.info(f"DEBUG: Session {session_id} has {total_turns} total turns.")

        # 2. Threshold Check (Modulo Logic)
        # Trigger every 5 turns (5, 10, 15...)
        if total_turns > 0 and total_turns % 1 == 0:
            logger.info(f"Summarization TRIGGERED for User {user_id} Session {session_id} (Turn {total_turns})")
            
            # 3. Get Context for Summarization
            context_data = await asyncio.to_thread(conversation_memory.get_context, user_id, session_id)
            turns = context_data.get('turns', [])
            
            # Sort by turn order to ensure chronological summary
            turns_sorted = sorted(turns, key=lambda x: x['turn_order'])
            
            if not turns_sorted:
                return

            text_block = ""
            for t in turns_sorted:
                text_block += f"User: {t['query_text']}\nAI: {t['response_text']}\n\n"
            
            prompt = f"""
            Summarize the following conversation history into a concise paragraph.
            Retain key facts: e.g: User Name, Location, Budget, Preferences, Numbers, and specific technical constraints.
            Discard pleasantries.
            
            Conversation:
            {text_block}
            
            Summary:
            """
            
            logger.info("Summarization Gemini call starting")
            genai.configure(api_key=get_effective_gemini_api_key())
            model = genai.GenerativeModel("gemini-flash-lite-latest") # Or your preferred model
            response = await model.generate_content_async(prompt)
            summary_text = response.text.strip()
            
            # 4. Save to DB
            last_turn_id = turns_sorted[-1]['turn_order']
            
            await asyncio.to_thread(
                conversation_memory.update_summary,
                user_id,
                summary_text,
                last_turn_id,
                session_id
            )
            logger.info(f"Summary successfully updated for {user_id} at turn {last_turn_id}")
        else:
            logger.info(f"Summarization skipped. (Turns: {total_turns} % 5 != 0)")

    except Exception as e:
        logger.error(f"Summarization failed: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


# --- Channel Search Configuration ---
# Keywords that trigger channel context injection (IndiHome internal channel)
INDIHOME_KEYWORDS = {
    'indihome', 'indihom', 'indi home', 'indihme', 'fiber home', 'myindihome',
    'my indihome', 'indihomeone', 'indihome one', 'triple play', 'dual play',
    'set top box', 'stb', 'indihome tv', 'useetv', 'usee tv', 'usee infinity',
    # Sales Force / Compensation keywords
    'sf', 'sales force', 'insentif', 'fee', 'komisi', 'thp', 'take home pay',
    'gaji', 'bonus', 'penjualan', 'psb', 'pasang baru', 'put in service'
}

# Minimum similarity threshold for channel message retrieval
CHANNEL_SEARCH_THRESHOLD = 0.75

# Adaptive retrieval configuration
# Fetches more results when quality is consistent, stops when score drops sharply
CHANNEL_SEARCH_CONFIG = {
    'min_limit': 2,           # Minimum results to return (guard against steep early drop)
    'max_limit': 6,           # Maximum results (matches initial_limit)
    'initial_limit': 6,       # How many to fetch from LanceDB
    'score_gap_threshold': 0.15,  # Stop if absolute gap > 0.15 (not percentage) AND have min_limit results
    'minimum_floor': 0.55,    # Reject all if top score < this (prevents weak results)
}


# ===================================
# Channel Message Search (LanceDB)
# ===================================
def search_channel_messages(query: str, limit: int = 5, threshold: float = CHANNEL_SEARCH_THRESHOLD) -> list:
    """
    Searches the channel_messages LanceDB table for semantically relevant
    Telegram channel messages to enrich RAG context.
    
    Returns messages with timestamps so the LLM can determine relevance
    based on recency. No pre-filtering on age - let the LLM decide.
    
    Args:
        query: Search query string
        limit: Maximum number of results to return
        threshold: Minimum similarity score (0-1)
    
    Returns:
        List of dicts with message_id, message_summary, content_for_rag, timestamp, score.
        Returns empty list if table unavailable or search fails.
    """
    # Early returns for edge cases
    if not query or not query.strip():
        return []
    
    if not tbl_channel_messages:
        return []
    
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logging.warning("Channel search skipped: GEMINI_API_KEY not set")
        return []

    try:
        logger.info("Channel search embedding call starting")
        genai.configure(api_key=api_key)
        q_vec = genai.embed_content(
            model=EMBEDDING_MODEL,
            content=query,
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=768
        )['embedding']

        # Fetch more than needed to account for threshold filtering
        # Some results may fall below CHANNEL_SEARCH_THRESHOLD (0.75)
        results = tbl_channel_messages.search(q_vec)\
            .limit(limit * 2)\
            .to_list()

        matched = []
        for r in results:
            score = 1 - r['_distance']
            if score >= threshold:
                matched.append({
                    'message_id': r.get('message_id'),
                    'message_summary': r.get('message_summary', ''),
                    'content_for_rag': r.get('content_for_rag', ''),
                    'timestamp': r.get('timestamp', ''),
                    'score': score
                })

        # Pure semantic ranking - LLM handles recency filtering via timestamps
        matched.sort(key=lambda x: x['score'], reverse=True)
        return matched[:limit]

    except Exception as e:
        logging.warning(f"Channel message search failed: {e}")
        return []


async def search_channel_messages_adaptive(query: str) -> list[dict[str, Any]]:
    """
    Adaptive retrieval: returns high-quality matches, stopping when relevance drops sharply.

    Guards:
        - minimum_floor: Reject all if top score < threshold (prevents weak results)
        - score_gap_threshold: Stop when absolute gap > threshold AND have minimum results
        - max_limit: Safety ceiling

    Examples:
        - High consistency [0.91, 0.89, 0.87]: Returns all 3 (small gaps)
        - Steep drop [0.92, 0.75]: Returns only 1st (gap 0.17 > 0.15 threshold)
        - All weak [0.45, 0.40]: Returns [] (below floor 0.55)
        - Single result [0.80]: Returns [{score: 0.80}] (floor passed)

    Returns:
        List of matched channel messages (empty list if none meet quality threshold)
    """
    try:
        config = CHANNEL_SEARCH_CONFIG

        # Fetch initial batch from LanceDB
        results = await asyncio.to_thread(
            search_channel_messages,
            query,
            limit=config['initial_limit']
        )

        if not results:
            return []

        # Guard: minimum quality floor
        top_score = results[0].get('score', 0)
        if top_score < config['minimum_floor']:
            logging.info(f"Adaptive retrieval: top score {top_score:.2f} below floor {config['minimum_floor']}, returning empty")
            return []

        # Apply gap-based filtering
        # Check gap BEFORE appending to exclude the result that triggered the gap
        kept = []
        prev_score = top_score

        for match in results:
            score = match.get('score', 0)

            # Guard: stop if gap threshold exceeded AND we have minimum results
            if len(kept) >= config['min_limit']:
                gap = prev_score - score
                if gap > config['score_gap_threshold']:
                    logging.debug(f"Adaptive retrieval: stopping at {len(kept)} results, gap {gap:.2f}")
                    break

            kept.append(match)
            prev_score = score

            # Safety ceiling
            if len(kept) >= config['max_limit']:
                break

        logging.info(f"Adaptive retrieval: {len(kept)} results (fetched {len(results)}, top={top_score:.2f})")
        return kept

    except Exception as e:
        logging.info(f"Adaptive channel search failed: {e}")
        return []


def format_channel_context(channel_matches: list) -> str:
    """Formats matched channel messages into a context section for the prompt."""
    if not channel_matches:
        return ""

    parts = ["\n=== TELEGRAM CHANNEL CONTEXT ===\n"]
    parts.append("(Informasi tambahan dari channel Telegram internal. Kutip dengan format: "
                 "[[Source: Channel | Message: <id> | Date: <tanggal>]])\n")

    for m in channel_matches:
        msg_id = m.get('message_id', 'N/A')
        timestamp = m.get('timestamp', 'N/A')
        summary = m.get('message_summary', '')
        content = m.get('content_for_rag', '')

        parts.append(f"\n[Message ID: {msg_id} | Date: {timestamp}]")
        if summary:
            parts.append(f"Summary: {summary}")
        if content:
            parts.append(f"Content: {content[:1500]}")
        parts.append("")

    parts.append("=== END CHANNEL CONTEXT ===\n")
    return "\n".join(parts)


async def get_channel_context_if_relevant(query: str) -> tuple:
    """
    Returns formatted channel context and raw matches if query is IndiHome-related.

    Consolidates the keyword check and adaptive retrieval into a single call.
    Use this instead of calling search_channel_messages_adaptive directly.

    Args:
        query: The user query string

    Returns:
        Tuple of (formatted_context_string, matches_list)
        - formatted_context_string: str to inject into prompt
        - matches_list: list of channel message dicts for frontend tracking
    """
    if not any(kw in query.lower() for kw in INDIHOME_KEYWORDS):
        return "", []
    matches = await search_channel_messages_adaptive(query)
    if not matches:
        return "", []
    return format_channel_context(matches), matches


# ===================================
# Generate Response: Documents
# ===================================
async def generate_document_response_selected(query, selected_documents_data, user_id, history_string_for_prompt, job_id_for_logging: str = None, stream_response: bool = False, session_id: str = None, model_override: str = None, api_key_override: str = None):
    """
    Orchestrates RAG generation with Domain Knowledge Injection, Top-Doc Immunity, and Regex Rescue.
    UPDATED: Exclusively uses page_content for generation (when available) to prevent citation hallucinations.
    """
    current_job_id_str = job_id_for_logging if job_id_for_logging else ("N/A_STREAM" if stream_response else "N/A_ASYNC")
    log_prefix = f"[Job {current_job_id_str} User {user_id} Session {session_id[:8] if session_id else 'N/A'}]"
    
    # 0. DOMAIN KNOWLEDGE INJECTION
    original_user_query_for_db = query 
    acronyms = {
        "sobi": "Sobat IndiHome", "lp": "landing page", "pdd": "Pembayaran Di Depan",
        "wok": "wilayah operasional kerja", "ps": "put in service", "psb": "pasang baru",
        "byod": "buy your own device", "ta": "telkom akses", "tif": "telkom infrastrukur",
        "tsel": "telkomsel", "gtm": "Go-To-Market", "fmc": "Fixed Mobile Convergence",
        "hbb": "Home Broadband", "fbb": "fixed broadband", "sf": "sales force",
        "src": "sampoerna retail community", "sbp": "sales business partner", "bw": "bandwidth"
    }
    
    for short, full in acronyms.items():
        query = re.sub(r'\b' + re.escape(short) + r'\b', f"{full} ({short})", query, flags=re.IGNORECASE)
    
    if query != original_user_query_for_db:
        logger.info(f"{log_prefix} Query Expanded (Late-Stage): '{original_user_query_for_db}' -> '{query}'")

    logger.info(f"{log_prefix} DocGen Start. Stream: {stream_response}. Query='{query[:70]}...', #Docs={len(selected_documents_data)}")

    # --- HELPER: REGEX RESCUE (Finds '2+0', '150Mbps' when vectors fail) ---
    def _find_pages_by_regex(query_str, doc_data_entry):
        tokens = re.findall(r'\b[A-Za-z0-9\+\-]{2,}\b', query_str)
        # Look for tokens with digits or symbols (codes/prices/formulas)
        priority_tokens = [t for t in tokens if any(char.isdigit() for char in t) or "+" in t or "%" in t]
        
        if not priority_tokens: return []
        
        hits = []
        for pn, det in doc_data_entry.get('page_analysis', {}).items():
            content = (det.get('raw_content') or "") + " " + (det.get('summary') or "")
            content_lower = content.lower()
            
            score = 0
            for t in priority_tokens:
                if t.lower() in content_lower: score += 1
            if score > 0: hits.append((int(pn), score))
        
        hits.sort(key=lambda x: x[1], reverse=True)
        return [h[0] for h in hits[:3]]

    # --- HELPER: CITATION SANITIZER (Aggressive) ---
    def _sanitize_citation_syntax(text: str) -> str:
        if not text: return ""
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            if re.match(r'^\s{4,}', line) or line.startswith('\t'):
                cleaned_lines.append(line.lstrip()) 
            else:
                cleaned_lines.append(line)
        text = '\n'.join(cleaned_lines)
        
        # 1. Standardize the opening tag
        text = re.sub(r'(?<!\[)\[Source:', r'[[Source:', text)
        
        # 2. Fix the "SUMMARY], " artifact you saw in the logs
        text = re.sub(r'(Page:\s*SUMMARY)[,.]?\s*\]+[,.]?', r'\1]]', text, flags=re.IGNORECASE)

        # 3. Standardize Page Numbers (Integers)
        text = re.sub(r'(Page:\s*\d+)(?<!\])\](?![\]])', r'\1]]', text)
        text = re.sub(r'(Page:\s*\d+)[,.]\s*\]\]', r'\1]]', text)

        # 4. Clean Comma Artifacts between citations
        text = re.sub(r'\]\]\s*,?\s*\[\[', r']] [[', text)

        # 5. Convert "SUMMARY" citations to PLAIN TEXT (No Button)
        def _summary_replacer(match):
            content = match.group(1) 
            return f"({content.replace('Page: SUMMARY', 'Global Summary')})"

        text = re.sub(r'\[\[(Source:.*?\|\s*Page:\s*SUMMARY)\]\]', _summary_replacer, text, flags=re.IGNORECASE)

        # 6. Cleanup HTML artifacts
        text = re.sub(r'(\[\[Source:.*?)\\_(.*?\]\])', r'\1_\2', text)
        text = re.sub(r'\s*(?:<>|&lt;&gt;)?\s*(?:</div>|</ div>)\s*$', '', text, flags=re.IGNORECASE)
        
        return text

    # --- HELPER: KEY FINDINGS ---
    def _parse_key_findings_from_response(response_text: str) -> list:
        try:
            start, end = "KEY_FINDINGS_START", "KEY_FINDINGS_END"
            s_idx, e_idx = response_text.find(start), response_text.find(end)
            if s_idx == -1 or e_idx == -1: return []
            content = response_text[s_idx + len(start):e_idx].strip()
            return [line[2:].strip() for line in content.split('\n') if line.strip().startswith('- ')]
        except Exception: return []

    # --- HELPER: MEMORY LOG ---
    def _create_memory_log(turn_num, user_query, processed_docs, chart_detected, expert_mode, model, total_imgs, findings, job_id):
        from datetime import datetime
        doc_summaries = [{
            "path": d.get('path'), "title": d.get('title'),
            "pages_analyzed": d.get('relevant_pages', []),
            "contributed_content": len(d.get('relevant_pages', [])) > 0
        } for d in processed_docs]
        return {
            "version": "1.0", "metadata": {"turn_number": turn_num, "timestamp_utc": datetime.now().isoformat() + "Z", "job_id": job_id or "N/A"},
            "input_analysis": {"user_query": user_query, "document_analysis_context": {"chart_request_detected": chart_detected, "expert_mode_enabled": expert_mode, "model_selected": model, "total_documents_processed": len(processed_docs), "documents_with_content": len([d for d in doc_summaries if d["contributed_content"]])}},
            "system_action": {"documents_processed": doc_summaries, "visual_processing_summary": {"images_sent_to_llm": total_imgs, "visual_analysis_enabled": total_imgs > 0}},
            "llm_output": {"key_findings": findings}
        }

    # 1. Validation & Config
    is_chart_request = detect_chart_request(query)
    if is_chart_request: logger.info(f"{log_prefix} Chart request detected.")

    if not selected_documents_data:
        yield {"error": "<p>Tidak ada dokumen yang dipilih.</p>", "answer_html": "<p>Tidak ada dokumen yang dipilih.</p>", "context_items_for_memory": []}; return
    if not POLICY_DIR or not os.path.isdir(POLICY_DIR):
        yield {"error": "<p>Config Error: POLICY_DIR.</p>", "answer_html": "<p>Config Error: POLICY_DIR.</p>", "context_items_for_memory": []}; return
    api_key = get_effective_gemini_api_key(api_key_override)
    if not api_key:
        yield {"error": "<p>Config Error: API Key.</p>", "answer_html": "<p>Config Error: API Key.</p>", "context_items_for_memory": []}; return

    processed_docs_for_memory_output = []
    complete_llm_response = ""

    try:
        genai.configure(api_key=api_key)
        if job_id_for_logging: await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "🤖 Selecting AI model...", 40)
        
        model_name, reason = await asyncio.to_thread(determine_required_model, selected_documents_data)
        logger.info(f"{log_prefix} Model: {model_name} ({reason})")

        if model_override:
            logger.info(f"{log_prefix} Model override: {model_override} (was: {model_name})")
            model_name = model_override

        if job_id_for_logging: await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "🔎 Processing content...", 60)

        use_thinking = model_supports_thinking(model_name)
        gen_config = GenerationConfig(temperature=0.3, max_output_tokens=8192*4)
        safety_settings = [{"category": c, "threshold": "BLOCK_MEDIUM_AND_ABOVE"} for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]]

        # --- FIX: Define prepared_history here ---
        prepared_history = await asyncio.to_thread(
            conversation_memory.get_hybrid_history_for_prompt, 
            user_id, 
            session_id, 
            turn_threshold=3, 
            char_threshold=15000
        )
        final_history_str = prepared_history if isinstance(prepared_history, str) and prepared_history.strip() else "(Tidak ada riwayat percakapan)"

        base_template = GEMINI_DOCUMENT_PROMPT_TEMPLATE.format(query="", conversation_history="", document_context_section="", current_date="")
        
        overhead = len(base_template) + len(query) + len(final_history_str) + 3000
        max_ctx = int(os.environ.get("MAX_TOTAL_CONTEXT_CHARS_DOC", 60000))
        avail_chars = max(7500, max_ctx - overhead)
        chars_per_doc = max(5000, (avail_chars // len(selected_documents_data))) if selected_documents_data else 0
        
        doc_context_parts = []
        image_parts = []
        processed_img_keys = set()
        valid_doc_count = 0

        # --- 5. DOCUMENT PROCESSING ---
        async def process_single_document(i, doc_data):
            rel_path = doc_data.get('path')
            clean_fn = os.path.basename(rel_path) if rel_path else f"Doc_{i+1}"
            title = doc_data.get('title', clean_fn)
            
            if not rel_path or not await asyncio.to_thread(os.path.exists, os.path.join(POLICY_DIR, rel_path)):
                return {'path': rel_path, 'title': title, 'relevant_pages': []}, None, []

            segments = [f"\n--- DOCUMENT {i+1}: {title} ({clean_fn}) ---\n"]
            used_chars = sum(len(s) for s in segments)
            
            is_summary = any(k in query.lower() for k in ["summarize", "summary", "ringkas", "rangkum"])
            pages = doc_data.get('semantically_relevant_pages', [])
            
            if is_chart_request:
                prio_chart_pages = prioritize_content_for_charts(doc_data, query, chars_per_doc)
                if prio_chart_pages:
                    pages = list(set(pages + [p[0] for p in prio_chart_pages]))
            
            if not pages and 'semantically_relevant_pages' in doc_data:
                if not is_summary:
                    if i == 0:
                        rescue_pages = _find_pages_by_regex(query, doc_data)
                        if rescue_pages: pages = rescue_pages
                        else:
                            all_pgs = sorted([int(p) for p in doc_data.get('page_analysis', {}).keys() if str(p).isdigit()])
                            pages = all_pgs[:5] 
                    else: return {'path': rel_path, 'title': title, 'relevant_pages': []}, None, []
                else:
                    all_pgs = sorted([int(p) for p in doc_data.get('page_analysis', {}).keys() if str(p).isdigit()])
                    pages = all_pgs[:MAX_PAGES_PER_DOC_CONTEXT] if all_pgs else [1, 2, 3]
            elif not pages:
                 all_pgs = sorted([int(p) for p in doc_data.get('page_analysis', {}).keys() if str(p).isdigit()])
                 pages = all_pgs[:MAX_PAGES_PER_DOC_CONTEXT] if all_pgs else [1, 2, 3]

            pg_summaries = {}
            for pn, det in doc_data.get('page_analysis', {}).items():
                pg_summaries[str(pn)] = det.get('summary', '')
                if det.get('raw_content'): pg_summaries[f"{pn}_raw"] = det.get('raw_content')

            vis_summary = doc_data.get('visual_analysis_summary', {})
            adv_pgs = set(map(str, vis_summary.get('pages_requiring_advanced_model', [])))
            exp_pgs = set(map(str, vis_summary.get('pages_requiring_experimental_model', [])))
            img_target_pgs = adv_pgs.union(exp_pgs)
            
            img_pgs_sent, text_pgs_sent = [], []
            doc_contributed = False
            limited_pages = pages[:MAX_PAGES_PER_DOC_CONTEXT]
            img_tasks = []

            for pn in limited_pages:
                if str(pn) in img_target_pgs and (rel_path, pn) not in processed_img_keys and len(image_parts) < MAX_IMAGES_PER_DOC:
                    img_tasks.append(asyncio.to_thread(_extract_and_encode_pdf_page_local, rel_path, pn))
                else: text_pgs_sent.append(pn)

            if img_tasks:
                results = await asyncio.gather(*img_tasks, return_exceptions=True)
                for pn, res in zip(limited_pages, results):
                    if isinstance(res, str) and re.match(r"^[A-Za-z0-9+/=]+$", res):
                        decoded = base64.b64decode(res)
                        ref = f"* [Image Included: Page {pn}]\n"
                        if used_chars + len(ref) <= chars_per_doc:
                            image_parts.append({"mime_type": "image/png", "data": decoded})
                            segments.append(ref); used_chars += len(ref)
                            img_pgs_sent.append(pn); processed_img_keys.add((rel_path, pn))
                            doc_contributed = True
                        else: text_pgs_sent.append(pn)
                    else: text_pgs_sent.append(pn)

            final_text_pgs = []
            if text_pgs_sent:
                temp_chars = used_chars + 600
                seen_pgs = set()
                ordered_unique_pgs = []
                for p in text_pgs_sent:
                    if p not in seen_pgs:
                        ordered_unique_pgs.append(p)
                        seen_pgs.add(p)

                pages_to_include = []
                temp_budget_check = used_chars + 600
                
                for pn in ordered_unique_pgs:
                    raw_content = pg_summaries.get(f"{pn}_raw", '')
                    summary_content = pg_summaries.get(str(pn), '')
                    if raw_content and len(raw_content.strip()) > 50: est_len = len(raw_content) + 200
                    else: est_len = len(summary_content) + 200
                    
                    if temp_budget_check + est_len <= chars_per_doc:
                        pages_to_include.append(pn)
                        temp_budget_check += est_len
                
                pages_to_include.sort(key=int)

                # Add document header with isolation warning FIRST (once per document)
                doc_header = format_document_header(i, title, clean_fn, pages_to_include)
                segments.append(doc_header)
                temp_chars += len(doc_header)

                for pn in pages_to_include:
                    raw = pg_summaries.get(f"{pn}_raw", '')
                    summ = pg_summaries.get(str(pn), f"[Summary N/A]")

                    # Use format_source_block which handles footer cleaning internally
                    if raw and len(raw.strip()) > 50:
                        content_block = format_source_block(clean_fn, pn, raw, "RAW")
                    else:
                        content_block = format_source_block(clean_fn, pn, summ, "SUMMARY")
                    
                    if temp_chars + len(content_block) <= chars_per_doc:
                        segments.append(content_block)
                        final_text_pgs.append(pn)
                        temp_chars += len(content_block)
                    else:
                        break
                
                # Add document footer with boundary marker (once per document)
                doc_footer = format_document_footer(i, clean_fn)
                segments.append(doc_footer)

            all_header_pgs = sorted(list(set(img_pgs_sent + final_text_pgs)))
            if all_header_pgs:
                doc_contributed = True
                pg_list = ', '.join(map(str, all_header_pgs))
                
                segments.insert(1, f"""
                **DOCUMENT METADATA**
                **Internal Title:** {title}
                **Official Filename:** {clean_fn}
                **Pages Loaded:** {pg_list}

                ⚠️ **STRICT CITATION RULE:**
                Always cite the 'System Index Page' number provided in the `<<< ... >>>` headers.
                NEVER cite the page numbers found inside the document text/images (e.g. footers).
                """)

            return {'path': rel_path, 'title': title, 'relevant_pages': sorted(list(set(img_pgs_sent + final_text_pgs)))}, segments if doc_contributed else None, image_parts

        doc_tasks = [process_single_document(i, d) for i, d in enumerate(selected_documents_data)]
        results = await asyncio.gather(*doc_tasks, return_exceptions=True)

        for i, res in enumerate(results):
            if isinstance(res, Exception): logger.error(f"{log_prefix} Doc {i} Error: {res}"); continue
            mem, segs, _ = res
            processed_docs_for_memory_output.append(mem)
            if segs:
                doc_context_parts.extend(segs)
                valid_doc_count += 1
            else:
                doc_context_parts.append(f"\n--- DOCUMENT {i+1} ---\n(Content excluded/not found)\n")

        if valid_doc_count == 0 and selected_documents_data:
            err = "<p>Dokumen tidak dapat diproses (Content limit/relevance).</p>"
            if stream_response: yield err; return
            yield {"error": err, "answer_html": err, "context_items_for_memory": processed_docs_for_memory_output}; return

        doc_ctx_str = "".join(doc_context_parts)
        expert_mode = should_use_expert_mode(query)
        if expert_mode: doc_ctx_str += "\n**EXPERT MODE**: Provide industry insights and comparative analysis.\n"

        # Inject SF/compensation domain knowledge only when query is THP/incentive-related
        if _is_compensation_intent(query.lower()):
            doc_ctx_str += (
                "\n**Domain Knowledge — SF/HH Agency Compensation (Telkomsel):**\n"
                "- Take Home Pay (THP) Sales Force = Gaji Pokok + Total Insentif Variabel\n"
                "- Insentif variabel dihitung per produk terjual sesuai skema fee yang ditetapkan dalam dokumen \"Surat Penetapan Kebijakan\" atau \"SK\" periode aktif\n"
                "- Produk yang umumnya memiliki skema insentif: IndiHome (new sales), Upgrade Speed/Upsell, CCTV, Telkomsel One, EZnet\n"
                "- **CRITICAL:** Jika dokumen skema fee/komisi TIDAK tersedia dalam `DOCUMENTS`, JANGAN gunakan asumsi industri. "
                "Nyatakan: \"Dokumen skema komisi/fee periode aktif tidak tersedia — silakan rujuk ke dokumen Surat Penetapan Kebijakan terkini.\"\n"
            )

        # --- Channel Message Context Injection ---
        # Only inject channel context for IndiHome-related queries (channel is IndiHome internal)
        channel_matches_for_memory = []  # Track for frontend source display
        try:
            channel_ctx, channel_matches = await get_channel_context_if_relevant(query)
            if channel_ctx:
                doc_ctx_str += channel_ctx
                channel_matches_for_memory = channel_matches  # Store for memory output
                logger.info(f"{log_prefix} Injected channel context for IndiHome query ({len(channel_matches)} messages)")
        except Exception as ch_err:
            logger.warning(f"{log_prefix} Channel context injection failed: {ch_err}")

        final_prompt = GEMINI_DOCUMENT_PROMPT_TEMPLATE.format(query=query, conversation_history=final_history_str, document_context_section=doc_ctx_str, current_date=datetime.now().strftime("%d %B %Y"))
        final_contents = [final_prompt] + image_parts

        logger.info(f"{log_prefix} Sending to Gemini. Prompt len: {len(final_prompt)}")
        rag_trace(session_id[:8] if session_id else "na", "GEN",
            model=model_name, docs=valid_doc_count,
            prompt_len=len(final_prompt), comp_dk=_is_compensation_intent(query.lower()))
        if job_id_for_logging: await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "Generating response...", 80)

        generation_model = genai.GenerativeModel(model_name)

        if stream_response:
            yield f'<!-- PROGRESS:{{"step": "Menghasilkan respons...", "progress": 80, "type": "generating"}} -->'

            chunks = []
            chunk_count = 0
            finish_reason = None
            thinking_complete_sent = False

            try:
                # Use google.genai SDK for thinking-capable models (exposes thought parts)
                if use_thinking:
                    logger.info(f"{log_prefix} Using google.genai SDK with thinking for model {model_name}")
                    genai_client = create_google_genai_client(api_key)
                    thinking_gen_config = genai_new_types.GenerateContentConfig(
                        temperature=0.3,
                        max_output_tokens=8192*4,
                        thinking_config=genai_new_types.ThinkingConfig(
                            thinking_level="HIGH",
                            include_thoughts=True,
                        ),
                        safety_settings=[genai_new_types.SafetySetting(category=c, threshold="BLOCK_MEDIUM_AND_ABOVE") for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]],
                    )
                    # Convert old-style image dicts to google.genai Part objects
                    genai_new_contents = [final_prompt]
                    for img in image_parts:
                        genai_new_contents.append(genai_new_types.Part.from_bytes(data=img["data"], mime_type=img["mime_type"]))
                    stream = await asyncio.to_thread(
                        lambda: genai_client.models.generate_content_stream(
                            model=model_name,
                            contents=genai_new_contents,
                            config=thinking_gen_config,
                        )
                    )
                else:
                    def run_document_stream():
                        logger.info(f"{log_prefix} Using google.generativeai document streaming fallback")
                        genai.configure(api_key=api_key)
                        return generation_model.generate_content(final_contents, generation_config=gen_config, safety_settings=safety_settings, stream=True)
                    stream = await asyncio.to_thread(run_document_stream)

                for chunk in stream:
                    chunk_count += 1

                    if use_thinking:
                        # google.genai SDK response format
                        if not chunk.candidates:
                            continue
                        candidate = chunk.candidates[0]
                        if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                            finish_reason = candidate.finish_reason
                            logger.info(f"{log_prefix} Chunk {chunk_count}: finish_reason={finish_reason}")
                        if not candidate.content or not candidate.content.parts:
                            continue

                        thought_parts = []
                        text_parts = []
                        for p in candidate.content.parts:
                            if hasattr(p, 'thought') and p.thought and hasattr(p, 'text') and p.text:
                                thought_parts.append(p.text)
                            elif hasattr(p, 'text') and p.text:
                                text_parts.append(p.text)

                        if chunk_count <= 5 or thought_parts:
                            parts_info = ', '.join(
                                'thought={} text_len={}'.format(getattr(p, 'thought', None), len(getattr(p, 'text', '') or ''))
                                for p in candidate.content.parts
                            )
                            logger.info(f"{log_prefix} Chunk {chunk_count}: {len(thought_parts)} thought parts, {len(text_parts)} text parts, parts_detail=[{parts_info}]")

                        # Emit thought chunks as THINKING markers
                        for thought_text in thought_parts:
                            encoded = base64.b64encode(thought_text.encode('utf-8')).decode('ascii')
                            yield f'<!-- THINKING:{encoded} -->'

                        # Transition marker: first real text after thinking
                        if text_parts and not thinking_complete_sent:
                            yield f'<!-- PROGRESS:{{"type":"thinking_done","progress":85}} -->'
                            thinking_complete_sent = True

                        txt = "".join(text_parts)
                    else:
                        # Legacy google.generativeai SDK format
                        # Check for blocking
                        if chunk.prompt_feedback and chunk.prompt_feedback.block_reason:
                            logger.error(f"{log_prefix} STREAM BLOCKED at chunk {chunk_count}: {chunk.prompt_feedback.block_reason}")
                            yield "<p>Blocked by AI Safety Filter.</p>"
                            return
                        if not chunk.candidates:
                            logger.warning(f"{log_prefix} Chunk {chunk_count}: No candidates (empty chunk)")
                            continue
                        candidate = chunk.candidates[0]
                        if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                            finish_reason = candidate.finish_reason
                            logger.info(f"{log_prefix} Chunk {chunk_count}: finish_reason={finish_reason}")
                        if not candidate.content:
                            logger.warning(f"{log_prefix} Chunk {chunk_count}: Candidate has no content")
                            continue
                        if not candidate.content.parts:
                            logger.warning(f"{log_prefix} Chunk {chunk_count}: Content has no parts")
                            continue
                        txt = "".join(p.text for p in candidate.content.parts if hasattr(p, 'text'))

                    if txt:
                        chunks.append(txt)
                        # NOTE: Do NOT call clean_response() on individual chunks.
                        # clean_response() calls .strip() which removes leading/trailing
                        # whitespace from each chunk, causing spaces at chunk boundaries
                        # to be lost (e.g. "<div " + "class=" becomes "<divclass=").
                        # Only remove code fences inline; full cleanup happens on the
                        # accumulated response after streaming completes (line ~4744).
                        stream_txt = re.sub(r'^```(?:[a-zA-Z]+)?\s*', '', txt, flags=re.MULTILINE)
                        stream_txt = re.sub(r'\s*```$', '', stream_txt, flags=re.MULTILINE)
                        if "[CHART:" not in txt and stream_txt:
                            yield stream_txt
                    else:
                        logger.warning(f"{log_prefix} Chunk {chunk_count}: Parts had no text")
                
                logger.info(f"{log_prefix} Stream complete. Total chunks: {chunk_count}, Final finish_reason: {finish_reason}")

            except Exception as stream_error:
                error_str = str(stream_error)
                logger.error(f"{log_prefix} STREAM EXCEPTION after {chunk_count} chunks: {error_str}", exc_info=True)

                # Provide user-friendly error message based on error type
                if "getaddrinfo" in error_str or "ConnectError" in error_str or "11001" in error_str:
                    yield "<p><strong>Koneksi ke AI gagal.</strong> Mohon periksa koneksi internet dan API Key Gemini.</p>"
                else:
                    yield f"<p>Stream error: {error_str}</p>"
                return

            if chunks:
                complete_llm_response = "".join(chunks)
                complete_llm_response = _sanitize_citation_syntax(complete_llm_response)
                try:
                    processed = await asyncio.to_thread(process_charts_in_response, complete_llm_response)
                    if "[CHART:" in complete_llm_response: yield clean_response(processed)
                except Exception: yield clean_response(complete_llm_response)

        else:
            resp = await generation_model.generate_content_async(final_contents, generation_config=gen_config, safety_settings=safety_settings)
            raw_text = resp.text if hasattr(resp, 'text') else "".join(p.text for p in resp.candidates[0].content.parts)
            if raw_text:
                sanitized_text = await asyncio.to_thread(clean_response, raw_text)
                sanitized_text = _sanitize_citation_syntax(sanitized_text)
                complete_llm_response = sanitized_text
                final_answer_html = await asyncio.to_thread(process_charts_in_response, sanitized_text)
            else: final_answer_html = "<p>No text response generated.</p>"

        # --- DEBUG: LOG RAW LLM OUTPUT ---
        logger.info(f"{log_prefix} RAW LLM OUTPUT START:\n{complete_llm_response}\n{log_prefix} RAW LLM OUTPUT END")

        key_findings = _parse_key_findings_from_response(complete_llm_response)
        
        # ASYNC WRAPPER: Wrap blocking memory calls
        context_data = await asyncio.to_thread(conversation_memory.get_context, user_id, session_id)
        turn_num = (max([t['turn_order'] for t in context_data.get('turns', [])]) if context_data.get('turns') else 0) + 1
        
        mem_log = _create_memory_log(turn_num, original_user_query_for_db, processed_docs_for_memory_output, is_chart_request, expert_mode, model_name, len(image_parts), key_findings, current_job_id_str)
        
        # --- GENERATE EMBEDDING FOR HYBRID MEMORY ---
        # Critical: Must be generated BEFORE update_context
        current_embedding = None
        try:
            embedding_result = await asyncio.to_thread(
                genai.embed_content,
                model="models/gemini-embedding-001",
                content=original_user_query_for_db,
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=768
            )
            current_embedding = embedding_result['embedding']
        except Exception as e:
            logger.warning(f"{log_prefix} Failed to generate embedding for memory: {e}")
            
            # --- TRIGGER BACKGROUND SUMMARIZATION ---
            logger.info(f"{log_prefix} Dispatching background summarizer task...")
            asyncio.create_task(trigger_summarization_task(user_id, session_id))

        suggs = []
        raw_suggs = [s for d in selected_documents_data for s in d.get('suggestions', []) if s]
        if raw_suggs:
            import random
            random.shuffle(raw_suggs)
            suggs = list(set(raw_suggs))[:4]

        # Add channel sources to memory output for frontend display
        for cm in channel_matches_for_memory:
            processed_docs_for_memory_output.append({
                "path": f"channel_{cm.get('message_id', 'unknown')}",
                "title": f"Channel: {cm.get('message_summary', '')[:50]}{'...' if len(cm.get('message_summary', '')) > 50 else ''}",
                "type": "channel",
                "message_id": cm.get('message_id'),
                "timestamp": cm.get('timestamp')
            })

        metadata_payload = {
            "context_items_for_memory": processed_docs_for_memory_output,
            "next_suggestions": suggs
        }
        yield f"<!-- STREAM_END_METADATA:{json.dumps(metadata_payload)} -->"

    except Exception as e:
        logger.error(f"{log_prefix} Error: {e}", exc_info=True)
        err = f"<p>Error: {str(e)}</p>"
        if stream_response: yield err; return
        yield {"error": str(e), "answer_html": err, "context_items_for_memory": processed_docs_for_memory_output}
    

# ===================================
# Generate Response: Market
# ===================================
async def generate_market_response_selected(
    original_query: str,
    selected_providers_for_initial_scope: list,
    conversation_history_str: str,
    user_id: str,
    job_id_for_logging: str = None,
    stream_response: bool = False,
    session_id: str = None,  # Required for memory management
    model_override: str = None,
    api_key_override: str = None
):
    """
    Asynchronously generates a market analysis by processing data from multiple sources.
    This enhanced RAG pipeline prioritizes fetching a comprehensive set of relevant data,
    then intelligently ranks and samples it, with a key focus on prioritizing the most
    recent product information before sending it to the LLM for analysis and presentation.
    
    NEW: Implements Hybrid Conversation Memory Management System.
    """
    start_time = time.time()
    current_job_id_str = job_id_for_logging or ("N/A_STREAM" if stream_response else "N/A_ASYNC")
    log_prefix = f"[Job {current_job_id_str} User {user_id} MarketGen]"
    logger.info(f"{log_prefix} Start. Query='{original_query[:70]}...'")
    logger.info(f"{log_prefix} Initial context: providers={selected_providers_for_initial_scope}")
    rag_trace(session_id[:8] if session_id else "na", "MKT",
        providers=selected_providers_for_initial_scope,
        q=original_query[:80])

    # Validate API key early
    api_key = get_effective_gemini_api_key(api_key_override)
    logger.info(f"{log_prefix} [DEBUG] api_key_override present: {bool(api_key_override)} | api_key present: {bool(api_key)}")
    if not api_key:
        logger.error(f"{log_prefix} No Gemini API key available")
        err_msg = "<p><strong>Error:</strong> API Key Gemini tidak tersedia. Mohon:</p><ul><li>Masukkan API key di modal pengaturan (tombol 'API Key' di header)</li><li>Atau set environment variable GEMINI_API_KEY</li></ul>"
        if stream_response:
            yield err_msg
        else:
            yield {"error": err_msg, "answer_html": err_msg, "context_items_for_memory": []}
        return

    # Helper for fallback sync DB query (if async pool fails)
    def _sync_suggestion_query(queried_location):
        """Fallback sync method for suggestion query when async pool unavailable"""
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            provider_clause, provider_params = build_sqlite_in_clause(
                [provider.lower() for provider in NATIONAL_PROVIDER_EXCLUSIONS]
            )
            sql_suggestion_query = f"""
                SELECT DISTINCT provider
                FROM (
                    SELECT provider FROM dashboard_product_detail
                    WHERE LOWER(locations) LIKE ? AND LOWER(provider) {provider_clause}
                    UNION
                    SELECT provider FROM dashboard_provider_matpro
                    WHERE LOWER(found) LIKE ? AND LOWER(provider) {provider_clause}
                )
                LIMIT 5
            """
            loc_param = f"%{queried_location[0].lower()}%"
            params = [loc_param, *provider_params, loc_param, *provider_params]
            cursor.execute(sql_suggestion_query, params)
            results = cursor.fetchall()
            return [row[0] for row in results] if results else []
        finally:
            if conn: conn.close()

    # Initialize ConversationMemory instance for hybrid memory management
    # Uses the global conversation_memory instance that's already initialized

    # Helper function to parse key findings from LLM response
    def _parse_key_findings_from_response(response_text: str) -> list:
        """Extract key findings from the LLM response between KEY_FINDINGS_START and KEY_FINDINGS_END"""
        try:
            start_marker = "KEY_FINDINGS_START"
            end_marker = "KEY_FINDINGS_END"
            
            start_index = response_text.find(start_marker)
            end_index = response_text.find(end_marker)
            
            if start_index == -1 or end_index == -1:
                logger.warning(f"{log_prefix} Key findings markers not found in LLM response")
                return []
            
            # Extract content between markers
            findings_content = response_text[start_index + len(start_marker):end_index].strip()
            
            # Parse bullet points that start with "- "
            findings_lines = [line.strip() for line in findings_content.split('\n') if line.strip()]
            key_findings = []
            
            for line in findings_lines:
                if line.startswith('- '):
                    # Remove the "- " prefix and clean up the finding
                    finding = line[2:].strip()
                    if finding:  # Only add non-empty findings
                        key_findings.append(finding)
            
            logger.info(f"{log_prefix} Parsed {len(key_findings)} key findings from LLM response")
            return key_findings
            
        except Exception as e:
            logger.error(f"{log_prefix} Error parsing key findings: {e}")
            return []

    # Helper function to create memory log
    def _create_memory_log(
        turn_number: int, 
        user_query: str, 
        fast_analysis: dict, 
        extracted_filters: dict, 
        product_count: int, 
        promo_count: int, 
        products_sent: int, 
        promos_sent: int, 
        key_findings: list,
        job_id: str = None
    ) -> dict:
        """Create structured memory log for the current turn"""
        from datetime import datetime
        
        return {
            "version": "1.0",
            "metadata": {
                "turn_number": turn_number,
                "timestamp_utc": datetime.now().isoformat() + "Z",
                "job_id": job_id or "N/A"
            },
            "input_analysis": {
                "user_query": user_query,
                "fast_analysis_results": {
                    "providers": fast_analysis.get('providers', []),
                    "locations": fast_analysis.get('locations', []),
                    "speeds": fast_analysis.get('speeds', []),
                    "intents": fast_analysis.get('intents', []),
                    "search_mode": fast_analysis.get('search_mode', 'standard')
                }
            },
            "system_action": {
                "final_filters_applied": extracted_filters,
                "data_retrieval_summary": {
                    "products_found_in_db": product_count,
                    "promos_found_in_db": promo_count,
                    "products_sent_to_llm": products_sent,
                    "promos_sent_to_llm": promos_sent
                }
            },
            "llm_output": {
                "key_findings": key_findings
            }
        }

    try:
        # --- STEP 0: FAST QUERY ANALYSIS ---
        # Quickly analyzes the query for key entities like locations, speeds, etc., to inform subsequent steps.
        fast_analysis_start = time.time()
        processor = await asyncio.to_thread(get_processor_instance)
        fast_analysis = await asyncio.to_thread(processor.analyze_fast, original_query)
        fast_analysis_duration = time.time() - fast_analysis_start
        logger.info(f"{log_prefix} Step 0 (Fast Analysis) Complete in {fast_analysis_duration:.3f}s. Results: {fast_analysis}")
        
        # --- STEP 1: CONTEXT-AWARE FILTER EXTRACTION ---
        # Uses the query, conversation history, and fast analysis to determine precise database filters.
        logger.info(f"{log_prefix} Step 1: Extracting context-aware filters...")
        if job_id_for_logging: 
            await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "🔍 Menganalisis Konteks Percakapan...", 15)

        # ASYNC CALL: Directly await the async function (removed asyncio.to_thread)
        extracted_filters, _ = await extract_filters_for_db_enhanced(
            original_query,
            conversation_history_str,
            selected_providers_for_initial_scope,
            fast_analysis,
            api_key_override=api_key_override
        )
        logger.info(f"{log_prefix} Context-aware filters extracted: {extracted_filters}")
        determined_preference = extracted_filters.get('data_type_preference', 'both')

        # --- STEP 2: DYNAMIC SQL CONSTRUCTION ---
        # Builds the appropriate SQL WHERE clauses based on the extracted filters.
        product_filters, promo_filters = await asyncio.to_thread(
            build_market_sql_filters,
            extracted_filters,
            determined_preference,
            current_job_id_str
        )
        logger.info(f"{log_prefix} Step 2 (Build SQL) Complete.")
        if job_id_for_logging: 
            await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "⚙️ Membangun filter pencarian...", 30)

        # --- STEP 3: CONCURRENT DATA LOADING ---
        # Fetches product and promotion data from the database in parallel for maximum efficiency.
        async def load_product_data():
            if determined_preference in ["product", "both"]:
                data, _ = await load_consolidated_isp_data_from_db_product(
                    dynamic_sql_filters_product={"sql_components": product_filters},
                    original_query_text_for_logging=original_query,
                    logger_instance=logger,
                    query_analysis=extracted_filters
                )
                return data
            return []

        async def load_promo_data():
            if determined_preference in ["promo", "both"]:
                raw_data, _ = await load_consolidated_isp_data_from_db_promo(
                    {"sql_components": promo_filters}, original_query, logger, query_analysis=extracted_filters
                )
                if raw_data:
                    return await asyncio.to_thread(process_database_results_for_llm, raw_data, fast_analysis)
            return []

        results = await asyncio.gather(load_product_data(), load_promo_data(), return_exceptions=True)
        
        product_data_from_db = results[0] if not isinstance(results[0], Exception) else []
        promo_data_from_db = results[1] if not isinstance(results[1], Exception) else []

        if isinstance(results[0], Exception): logger.error(f"{log_prefix} Product data loading failed: {results[0]}")
        if isinstance(results[1], Exception): logger.error(f"{log_prefix} Promo data loading failed: {results[1]}")

        logger.info(f"{log_prefix} Step 3 (Load Data) Complete. Found {len(product_data_from_db)} products, {len(promo_data_from_db)} promos.")
        if job_id_for_logging: 
            await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "📊 Memuat data...", 50)

        # --- STEP 4: RANK & SAMPLE WITH RECENCY PRIORITY ---
        # Processes fetched data to create a concise, relevant, and up-to-date context for the LLM.
        async def process_products():
            if not product_data_from_db:
                return []
            
            all_products_by_provider = defaultdict(list)
            for p in product_data_from_db:
                all_products_by_provider[p.get('provider', 'Unknown').lower()].append(p)

            # This function ranks, sorts by recency, and truncates products for a single provider.
            async def process_provider(provider, products):
                # First, rank all products by relevance to the query to get a good initial subset.
                ranked_provider_products = await asyncio.to_thread(
                    rank_and_filter_results, 
                    products, 
                    original_query, 
                    100  # Keep a larger intermediate pool for sorting
                )

                # This is the critical step to ensure the newest products are prioritized.
                def get_sort_key(product):
                    """Robustly parses event_date for sorting, handling missing or invalid formats."""
                    date_str = product.get('event_date') or product.get('unified_date')
                    if not date_str or pd.isna(date_str):
                        return datetime.min # Push items without a date to the end
                    try:
                        # Handle both 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM:SS' formats
                        return datetime.strptime(str(date_str).split(" ")[0], '%Y-%m-%d')
                    except (ValueError, TypeError):
                        return datetime.min # Treat parsing errors as the oldest date

                # Primary Sort: by event_date (most recent first).
                # The initial relevance ranking acts as a secondary sort key for items with the same date.
                ranked_provider_products.sort(key=get_sort_key, reverse=True)
                
                newest_date = ranked_provider_products[0].get('event_date') if ranked_provider_products else 'N/A'
                logger.info(f"{log_prefix} Provider '{provider}': Sorted {len(ranked_provider_products)} products by recency. Newest event_date: {newest_date}")

                # Finally, truncate the date-sorted list to the maximum allowed size.
                final_sample = ranked_provider_products[:MAX_PRODUCTS_PER_PROVIDER_TO_LLM]
                
                logger.info(f"{log_prefix} RECENCY-FOCUSED SAMPLING: Provider '{provider}' processed {len(products)} initial -> {len(final_sample)} final (limit: {MAX_PRODUCTS_PER_PROVIDER_TO_LLM})")
                
                return final_sample

            provider_tasks = [process_provider(p, prod_list) for p, prod_list in all_products_by_provider.items()]
            provider_results = await asyncio.gather(*provider_tasks, return_exceptions=True)
            
            sampled_products_for_llm = []
            for result in provider_results:
                if isinstance(result, Exception):
                    logger.error(f"{log_prefix} A provider processing task failed: {result}")
                else:
                    sampled_products_for_llm.extend(result)
                    
            return sampled_products_for_llm
        
        async def process_promos():
            if not promo_data_from_db:
                return []
            return await asyncio.to_thread(rank_and_filter_results, promo_data_from_db, original_query, MAX_PROMOS_PER_PROVIDER_TO_LLM)

        processing_results = await asyncio.gather(process_products(), process_promos(), return_exceptions=True)
        
        sampled_products_for_llm = processing_results[0] if not isinstance(processing_results[0], Exception) else []
        ranked_promos = processing_results[1] if not isinstance(processing_results[1], Exception) else []

        if isinstance(processing_results[0], Exception): logger.error(f"{log_prefix} Product processing failed: {processing_results[0]}")
        if isinstance(processing_results[1], Exception): logger.error(f"{log_prefix} Promo processing failed: {processing_results[1]}")

        logger.info(f"{log_prefix} Step 4 (Rank & Sample) Complete. Kept {len(sampled_products_for_llm)} products and {len(ranked_promos)} promos.")
        if job_id_for_logging: 
            await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "🔄 Memproses hasil...", 70)

        # --- STEP 5: LLM PROMPT CONSTRUCTION ---
        # Assembles the final prompt with system instructions, conversation history, and the curated data context.
        async def build_prompt_context():
            data_context_parts = []
            if sampled_products_for_llm:
                data_context_parts.append("--- DATA PRODUK PALING RELEVAN (DIPRIORITASKAN BERDASARKAN TANGGAL TERBARU) ---")
                products_by_provider_for_prompt = defaultdict(list)
                for p in sampled_products_for_llm:
                    products_by_provider_for_prompt[p.get('provider', 'Unknown').lower()].append(p)
                
                for provider, products in sorted(products_by_provider_for_prompt.items()):
                    data_context_parts.append(f"\n## Provider: {provider.title()}")
                    for p in products:
                        location_display = p.get('location_display', p.get('location_summary', 'N/A'))
                        gimmicks_info = p.get('gimmicks', '') or 'Tidak disebutkan'
                        
                        product_lines = [
                            f"  - Nama Paket: {p.get('product_name', 'N/A')}",
                            f"    Kecepatan: {p.get('speed_mbps', 'N/A')} Mbps",
                            f"    Harga: {p.get('price_formatted', 'N/A')}",
                            f"    Lokasi: {location_display}",
                            f"    Benefits/Gimmicks: {gimmicks_info}",
                            f"    Event Date: {p.get('unified_date', 'N/A')}",
                            f"    Sumber: {p.get('source_table', 'N/A')}\n"
                        ]
                        data_context_parts.extend(product_lines)

            if ranked_promos:
                data_context_parts.append("--- DATA PROMOSI PALING RELEVAN ---")
                for p in ranked_promos:
                    promo_lines = [
                        f"\n## Promosi dari Provider: {p.get('provider', 'N/A').title()}",
                        f"  - Judul Promo: {p.get('promo_title', 'N/A')}",        
                        f"    Ringkasan: {p.get('content_summary', 'N/A')}",  
                        f"    Tanggal Mulai: {p.get('start_date', 'N/A')}",          
                        f"    Tanggal Selesai: {p.get('end_date', 'N/A')}",              
                        f"    URL: {p.get('promo_url', 'N/A')}",            
                        f"    Sumber Data: {p.get('source_table', 'N/A')}\n"
                    ]
                    data_context_parts.extend(promo_lines)
            return data_context_parts

        async def handle_suggestion_query():
            if sampled_products_for_llm or ranked_promos: return []
            
            data_context_parts = ["(Tidak ada data produk maupun promosi yang relevan ditemukan berdasarkan kriteria pencarian.)"]
            queried_locations = fast_analysis.get('locations', []) or extracted_filters.get('location', [])
            is_local_provider_query = (extracted_filters.get('search_type') == 'local_providers_only' or fast_analysis.get('search_mode') == 'location_discovery')

            if queried_locations and is_local_provider_query:
                primary_location = queried_locations[0]
                logger.info(f"{log_prefix} No local providers found for '{primary_location}'. Finding national providers as fallback.")
                
                national_providers_in_area = await asyncio.to_thread(_sync_suggestion_query, queried_locations)

                if national_providers_in_area:
                    suggestion_text = (
                        "\n\n--- PETUNJUK TAMBAHAN UNTUK ANALIS ---\n"
                        "Anda tidak menemukan data untuk 'provider lokal' sesuai permintaan. "
                        "Namun, sebagai gantinya, Anda telah menemukan bahwa provider NASIONAL berikut tersedia di lokasi yang diminta. "
                        "Sebutkan bahwa Anda tidak menemukan provider lokal, tetapi tawarkan provider nasional ini sebagai alternatif.\n"
                        f"Provider Nasional yang Tersedia: {', '.join(national_providers_in_area)}\n"
                    )
                    data_context_parts.append(suggestion_text)
            return data_context_parts

        context_parts, suggestion_parts = await asyncio.gather(build_prompt_context(), handle_suggestion_query())

        multi_provider_data_section_for_llm = "\n".join(suggestion_parts or context_parts)

        # --- Channel Message Context Injection (Market Mode) ---
        # Only inject channel context for IndiHome-related queries (channel is IndiHome internal)
        channel_matches_for_memory = []  # Track for frontend source display
        try:
            channel_ctx, channel_matches = await get_channel_context_if_relevant(original_query)
            if channel_ctx:
                multi_provider_data_section_for_llm += channel_ctx
                channel_matches_for_memory = channel_matches  # Store for memory output
                logger.info(f"{log_prefix} Injected channel context for IndiHome query in Market mode ({len(channel_matches)} messages)")
        except Exception as ch_err:
            logger.warning(f"{log_prefix} Channel context injection failed in Market mode: {ch_err}")

        provisional_market_sources = await asyncio.to_thread(
            extract_market_sources_from_data,
            sampled_products_for_llm,
            ranked_promos
        )
        provisional_context_items_for_memory = []
        for cm in channel_matches_for_memory:
            provisional_context_items_for_memory.append({
                "path": f"channel_{cm.get('message_id', 'unknown')}",
                "title": f"Channel: {cm.get('message_summary', '')[:50]}{'...' if len(cm.get('message_summary', '')) > 50 else ''}",
                "type": "channel",
                "message_id": cm.get('message_id'),
                "timestamp": cm.get('timestamp')
            })

        if not provisional_market_sources and not provisional_context_items_for_memory:
            provisional_context_items_for_memory = build_demo_market_context_sources(
                original_query,
                extracted_filters,
                sampled_products_for_llm,
                ranked_promos,
                channel_matches_for_memory
            )

        if stream_response:
            source_chip_payload = []
            source_chip_payload.extend(provisional_market_sources)
            source_chip_payload.extend(provisional_context_items_for_memory)
            if source_chip_payload:
                yield f'<!-- PROGRESS:{json.dumps({"type": "source_chips", "sources": source_chip_payload}, ensure_ascii=False)} -->'
        
        if len(multi_provider_data_section_for_llm) > MAX_DATA_SECTION_CHARS_MARKET:
            multi_provider_data_section_for_llm = multi_provider_data_section_for_llm[:MAX_DATA_SECTION_CHARS_MARKET] + "\n\n--- (DATA CONTEXT DIPOTONG KARENA TERLALU PANJANG) ---"
        
        prepared_history = await asyncio.to_thread(
            conversation_memory.get_hybrid_history_for_prompt,
            user_id=user_id, 
            session_id=session_id,
            turn_threshold=3,
            char_threshold=15000
        )

        # Use the prepared history instead of the raw conversation_history_str
        current_llm_prompt_text = GEMINI_CONSOLIDATED_MARKET_PROMPT_TEMPLATE.format(
            query=original_query, 
            conversation_history=prepared_history,
            multi_provider_data_section=multi_provider_data_section_for_llm
        )
        
        logger.info(f"{log_prefix} Step 5 (Construct Prompt) Complete.")
        if job_id_for_logging: 
            await asyncio.to_thread(_update_job_in_db, job_id_for_logging, 'running', None, "Menghasilkan analisis...", 85)

        # --- STEP 6: LLM INFERENCE & RESPONSE HANDLING ---
        # Executes the call to the generative AI model and streams/yields the response.
        effective_market_model = model_override or LLM_TO_USE_MARKET
        rag_trace(session_id[:8] if session_id else "na", "GEN",
            model=effective_market_model, docs=len(selected_providers_for_initial_scope),
            prompt_len=len(current_llm_prompt_text), comp_dk=False)
        use_thinking_market = model_supports_thinking(effective_market_model)
        main_generation_model = genai.GenerativeModel(effective_market_model)
        gen_config = GenerationConfig(temperature=0.20, max_output_tokens=MARKET_OUTPUT_TOKEN)
        safety_settings = [{"category": c, "threshold": "BLOCK_MEDIUM_AND_ABOVE"} for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]]

        logger.debug(f"{log_prefix} FINAL PROMPT length: {len(current_llm_prompt_text)} chars")
        
        async def collect_and_format_market_sources(products, promos):
            """Async wrapper for source collection"""
            return await asyncio.to_thread(extract_market_sources_from_data, products, promos)

        # Variable to store the complete LLM response for memory log creation
        complete_llm_response = ""

        if stream_response:
            yield f'<!-- PROGRESS:{{"step": "Menghasilkan analisis...", "progress": 85, "type": "generating"}} -->'

            chunk_count = 0
            finish_reason = None
            thinking_complete_sent_market = False

            try:
                if use_thinking_market:
                    api_key = get_effective_gemini_api_key(api_key_override)
                    logger.info(f"{log_prefix} [DEBUG] API Key override received: {bool(api_key_override)} | Effective key: {bool(api_key)}")
                    if not api_key:
                        logger.error(f"{log_prefix} [DEBUG] NO API KEY AVAILABLE - cannot create genai client")
                        yield "<p><strong>Error:</strong> API Key tidak tersedia. Mohon masukkan API key di modal pengaturan.</p>"
                        return
                    logger.info(f"{log_prefix} Using google.genai SDK with thinking for market model {effective_market_model}")
                    genai_client = create_google_genai_client(api_key)
                    thinking_gen_config = genai_new_types.GenerateContentConfig(
                        temperature=0.20,
                        max_output_tokens=MARKET_OUTPUT_TOKEN,
                        thinking_config=genai_new_types.ThinkingConfig(
                            thinking_level="HIGH",
                            include_thoughts=True,
                        ),
                        safety_settings=[genai_new_types.SafetySetting(category=c, threshold="BLOCK_MEDIUM_AND_ABOVE") for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]],
                    )
                    stream = await asyncio.to_thread(
                        lambda: genai_client.models.generate_content_stream(
                            model=effective_market_model,
                            contents=current_llm_prompt_text,
                            config=thinking_gen_config,
                        )
                    )
                else:
                    def run_streaming():
                        api_key = get_effective_gemini_api_key(api_key_override)
                        logger.info(f"{log_prefix} Using google.generativeai market streaming fallback")
                        if api_key:
                            genai.configure(api_key=api_key)
                        return main_generation_model.generate_content(current_llm_prompt_text, generation_config=gen_config, safety_settings=safety_settings, stream=True)
                    stream = await asyncio.to_thread(run_streaming)

                for chunk in stream:
                        chunk_count += 1

                        if use_thinking_market:
                            # google.genai SDK response format
                            if not chunk.candidates:
                                continue
                            candidate = chunk.candidates[0]
                            if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                                finish_reason = candidate.finish_reason
                                logger.info(f"{log_prefix} Chunk {chunk_count}: finish_reason={finish_reason}")
                            if not candidate.content or not candidate.content.parts:
                                continue

                            thought_parts = []
                            text_parts = []
                            for p in candidate.content.parts:
                                if hasattr(p, 'thought') and p.thought and hasattr(p, 'text') and p.text:
                                    thought_parts.append(p.text)
                                elif hasattr(p, 'text') and p.text:
                                    text_parts.append(p.text)

                            for thought_text in thought_parts:
                                encoded = base64.b64encode(thought_text.encode('utf-8')).decode('ascii')
                                yield f'<!-- THINKING:{encoded} -->'

                            if text_parts and not thinking_complete_sent_market:
                                yield f'<!-- PROGRESS:{{"type":"thinking_done","progress":85}} -->'
                                thinking_complete_sent_market = True

                            chunk_text = "".join(text_parts)
                        else:
                            # Legacy google.generativeai SDK format
                            if hasattr(chunk, 'prompt_feedback') and chunk.prompt_feedback.block_reason:
                                logger.error(f"{log_prefix} STREAM PROMPT BLOCKED at chunk {chunk_count}. Reason: {chunk.prompt_feedback.block_reason.name}")
                                yield f"<p>Permintaan Anda diblokir oleh filter keamanan AI ({chunk.prompt_feedback.block_reason.name}).</p>"
                                return
                            if chunk.candidates and hasattr(chunk.candidates[0], 'finish_reason') and chunk.candidates[0].finish_reason:
                                finish_reason = chunk.candidates[0].finish_reason
                                logger.info(f"{log_prefix} Chunk {chunk_count}: finish_reason={finish_reason}")
                            chunk_text = ""
                            if hasattr(chunk, 'text'):
                                chunk_text = chunk.text
                            elif chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                                chunk_text = "".join(p.text for p in chunk.candidates[0].content.parts if hasattr(p, 'text'))

                        if chunk_text:
                            complete_llm_response += chunk_text
                            # NOTE: Do NOT call clean_response() on individual chunks.
                            # It strips whitespace at chunk boundaries, causing broken
                            # output like "<divclass=" or "masaberlangganan".
                            stream_txt = re.sub(r'^```(?:[a-zA-Z]+)?\s*', '', chunk_text, flags=re.MULTILINE)
                            stream_txt = re.sub(r'\s*```$', '', stream_txt, flags=re.MULTILINE)
                            if stream_txt:
                                yield stream_txt

                logger.info(f"{log_prefix} Stream complete. Total chunks: {chunk_count}, Final finish_reason: {finish_reason}")

            except Exception as stream_error:
                error_str = str(stream_error)
                logger.error(f"{log_prefix} STREAM EXCEPTION after {chunk_count} chunks: {error_str}", exc_info=True)

                # Provide user-friendly error message based on error type
                if "getaddrinfo" in error_str or "ConnectError" in error_str or "11001" in error_str:
                    yield "<p><strong>Koneksi ke AI gagal.</strong> Mohon periksa:</p>"
                    yield "<ul><li>Koneksi internet Anda aktif</li><li>Tidak ada firewall yang memblokir aplikasi</li><li>API Key Gemini valid</li></ul>"
                    yield "<p>Anda dapat menggunakan mode demo tanpa AI untuk melihat data.</p>"
                else:
                    yield f"<p>Stream error: {error_str}</p>"
                return

        else: # Non-streaming async mode
            logger.info(f"{log_prefix} Using google.generativeai market async fallback")
            api_key = get_effective_gemini_api_key(api_key_override)
            if api_key:
                genai.configure(api_key=api_key)
            response = await main_generation_model.generate_content_async(current_llm_prompt_text, generation_config=gen_config, safety_settings=safety_settings)
            
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
                raise RuntimeError(f"Permintaan diblokir oleh filter keamanan AI ({response.prompt_feedback.block_reason.name}).")
            
            if not response.text.strip():
                yield "<p>AI tidak memberikan respons yang valid.</p>"
                return

            complete_llm_response = response.text  # Store complete response

            # Clean the response using global clean_response before processing for user display
            cleaned_response = clean_response(response.text)

            natural_language_summary_html, final_json_for_tabulator = await asyncio.to_thread(
                _parse_llm_final_response, cleaned_response, log_prefix
            )
            
            if natural_language_summary_html: yield natural_language_summary_html
            if final_json_for_tabulator: yield final_json_for_tabulator

        # Parse tabulator data from accumulated response
        tabulator_data = None
        if complete_llm_response:
            _, tabulator_json = await asyncio.to_thread(
                _parse_llm_final_response, complete_llm_response, log_prefix
            )
            if tabulator_json:
                try:
                    tabulator_data = json.loads(tabulator_json)
                except json.JSONDecodeError:
                    pass

        # Filter product data to only include items selected by LLM (reduce KV sample count)
        # Extract unique providers and product names from tabulator data
        selected_providers = set()
        selected_product_names = set()
        if tabulator_data and 'data' in tabulator_data:
            for row in tabulator_data['data']:
                provider = row.get('Provider', '').lower().strip()
                product_name = row.get('Name/Title', '').lower().strip()
                if provider:
                    selected_providers.add(provider)
                if product_name:
                    selected_product_names.add(product_name)

        # Filter product_data_from_db to only include items matching selected providers
        # AND (provider + product_name) combination for more precise matching
        filtered_product_data = []
        if selected_providers and product_data_from_db:
            for item in product_data_from_db:
                item_provider = str(item.get('provider', '')).lower().strip()
                item_name = str(item.get('product_name', '')).lower().strip()

                # Include if provider matches AND (name matches OR no name constraint)
                if item_provider in selected_providers:
                    # If we have specific product names, check for match
                    if selected_product_names:
                        # Check if any selected product name is contained in or matches the item name
                        name_matches = any(
                            sel_name in item_name or item_name in sel_name
                            for sel_name in selected_product_names
                        )
                        if name_matches:
                            filtered_product_data.append(item)
                    else:
                        # No specific product names, include all items from selected providers
                        filtered_product_data.append(item)

            logger.info(f"{log_prefix} Filtered sources: {len(product_data_from_db)} → {len(filtered_product_data)} products from {len(selected_providers)} selected providers")
        else:
            # Fallback to all products if no tabulator data
            filtered_product_data = product_data_from_db if product_data_from_db else []

        # Yield sources + tabulator as metadata marker
        market_sources = await collect_and_format_market_sources(filtered_product_data, promo_data_from_db)

        # Build context_items for memory - include channel sources
        context_items_for_memory = []
        for cm in channel_matches_for_memory:
            context_items_for_memory.append({
                "path": f"channel_{cm.get('message_id', 'unknown')}",
                "title": f"Channel: {cm.get('message_summary', '')[:50]}{'...' if len(cm.get('message_summary', '')) > 50 else ''}",
                "type": "channel",
                "message_id": cm.get('message_id'),
                "timestamp": cm.get('timestamp')
            })

        if not market_sources and not context_items_for_memory:
            context_items_for_memory = build_demo_market_context_sources(
                original_query,
                extracted_filters,
                sampled_products_for_llm,
                ranked_promos,
                channel_matches_for_memory
            )
            logger.info(f"{log_prefix} No grounded market source artifacts found. Using demo context sources ({len(context_items_for_memory)} items).")

        metadata_payload = {
            "market_sources": market_sources if market_sources else [],
            "context_items_for_memory": context_items_for_memory,
            "next_suggestions": [],
            "tabulator_data": tabulator_data
        }
        yield f"<!-- STREAM_END_METADATA:{json.dumps(metadata_payload)} -->"

        # --- STEP 7: TRIGGER BACKGROUND SUMMARIZATION ---
        logger.info(f"{log_prefix} Step 7 Start...")

        if session_id:
            logger.info(f"{log_prefix} Dispatching background summarizer task...")
            asyncio.create_task(trigger_summarization_task(user_id, session_id))
        else:
            logger.warning(f"{log_prefix} session_id is falsy: {session_id}")

        logger.info(f"{log_prefix} Step 7 Complete.")

        total_duration = time.time() - start_time
        logger.info(f"{log_prefix} Request completed successfully. Total time: {total_duration:.4f}s.")

    except Exception as e:
        logger.error(f"{log_prefix} An exception occurred in the main generation pipeline: {e}", exc_info=True)
        yield f"<p>Terjadi masalah internal: {e}</p>"

# --- Data Insight Response Generator --- #
async def generate_data_insight_response(
    original_query: str,
    user_id: str,
    history_string_for_prompt: str,
    job_id_for_logging: str = None,
    session_id: str = None
):
    """
    Async generator for the Data Insight pipeline.
    Follows the same pattern as generate_document_response_selected / generate_market_response_selected.
    """
    log_prefix = f"[DataInsight Job {job_id_for_logging} User {user_id} Session {session_id}]"
    logger.info(f"{log_prefix} Starting data insight generation for: '{original_query[:80]}'")

    try:
        engine = get_data_insight_engine()
        result = await engine.process(original_query, session_id or "unknown", history_string_for_prompt)

        answer_html = result.get('answer_html', '<p>Tidak ada hasil.</p>')
        charts = result.get('charts', [])
        sql_used = result.get('sql_used', '')
        query_intent = result.get('query_intent', '')
        context_items = result.get('context_items_for_memory', [])
        suggestions = result.get('next_suggestions', [])

        logger.info(f"{log_prefix} Data insight generated: intent={query_intent}, "
                     f"charts={len(charts)}, sql_len={len(sql_used)}")

        # Save to conversation memory
        if session_id and conversation_memory:
            try:
                # Insert user turn
                turn_id = await asyncio.to_thread(
                    conversation_memory.insert_user_turn,
                    user_id=user_id,
                    session_id=session_id,
                    query_text=original_query
                )
                # Update bot response
                await asyncio.to_thread(
                    conversation_memory.update_bot_response,
                    turn_id=turn_id,
                    response_text=answer_html,
                    rag_mode='data_insight',
                    context_items=context_items
                )
            except Exception as mem_err:
                logger.error(f"{log_prefix} Memory save failed: {mem_err}")

        # Yield structured response (matching the dict pattern used by _execute_generation_task_async)
        yield {
            'answer_html': answer_html,
            'context_items_for_memory': context_items,
            'next_suggestions': suggestions
        }

    except Exception as e:
        logger.error(f"{log_prefix} Data insight generation failed: {e}", exc_info=True)
        yield {
            'answer_html': f"<p>Maaf, terjadi kesalahan saat memproses permintaan data insight: {e}</p>",
            'context_items_for_memory': [],
            'next_suggestions': []
        }


# ---------------------------- HELPER: extract_filters_for_db ---------------------------- #
# Enhanced extract_filters_for_db with FastQueryProcessor integration
async def extract_filters_for_db_enhanced(query_text: str, conversation_history_input, selected_providers_from_frontend=None, fast_analysis=None, api_key_override: str = None):
    """
    Enhanced version that leverages FastQueryProcessor results for better performance and accuracy.
    Refactored to ASYNC to prevent blocking on LLM API calls.
    """
    global ADVANCED_MODEL, logger

    formatted_history_for_prompt = ""
    if isinstance(conversation_history_input, str):
        formatted_history_for_prompt = conversation_history_input
    elif isinstance(conversation_history_input, list):
        formatted_history_for_prompt = "\n".join(map(str, conversation_history_input))

    log_prefix_for_func = f"extract_filters_for_db_enhanced (Query: '{query_text[:30]}...'): "
    logger.info(f"{log_prefix_for_func}Start. Frontend selection: {selected_providers_from_frontend}, Fast analysis available: {fast_analysis is not None}")

    # Start with fast analysis results as baseline
    final_filters = {}
    if fast_analysis:
        # Convert fast analysis to filter format
        if fast_analysis.get('providers'):
            final_filters['providers'] = fast_analysis['providers']
        if fast_analysis.get('locations'):
            final_filters['location'] = fast_analysis['locations']
        if fast_analysis.get('speeds'):
            if len(fast_analysis['speeds']) == 1:
                final_filters['speed_exact'] = fast_analysis['speeds'][0]
            else:
                final_filters['speed_min'] = min(fast_analysis['speeds'])
                final_filters['speed_max'] = max(fast_analysis['speeds'])
        
        # Set search type based on fast analysis
        if fast_analysis.get('search_mode') == 'location_discovery':
            final_filters['search_type'] = 'local_providers_only'
        elif fast_analysis.get('asking_for_providers'):
            final_filters['search_type'] = 'provider_discovery'

    raw_llm_filters = {}
    llm_extraction_status_message = "OK_LLM_EMPTY_FILTERS"

    # Heuristic fallback for obvious city names missed by fast analysis.
    if 'location' not in final_filters and 'locations' not in final_filters:
        query_locations = []
        normalized_query = normalize_text(query_text)
        for city_name in KNOWN_DEMO_LOCATIONS:
            if re.search(rf'\b{re.escape(city_name)}\b', normalized_query):
                query_locations.append(city_name.title())
        if query_locations:
            final_filters['location'] = query_locations
            logger.info(f"{log_prefix_for_func}Heuristic location fallback detected: {query_locations}")

    # Enhanced LLM extraction with fast analysis context
    api_key = get_effective_gemini_api_key(api_key_override)
    if api_key:
        try:
            logger.info(f"{log_prefix_for_func}Preparing Gemini filter extraction")
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(ADVANCED_MODEL)
            history_content_for_llm = formatted_history_for_prompt.strip() or "No relevant history."

            # Enhanced prompt with fast analysis context
            fast_analysis_context = ""
            if fast_analysis:
                fast_analysis_context = f"""
                
**Quick Analysis Results (for context):**
- Detected providers: {fast_analysis.get('providers', [])}
- Detected locations: {fast_analysis.get('locations', [])}
- Detected speeds: {fast_analysis.get('speeds', [])}
- Search mode: {fast_analysis.get('search_mode', 'standard')}
- Confidence: {fast_analysis.get('confidence', 0):.2f}
"""

            llm_prompt_for_db = f"""
            You are an expert system that intelligently merges conversation history, quick analysis results, and new queries to extract database filter criteria.
            Your main goal is to understand the user's FULL intent and refine the quick analysis when needed.

            **Conversation History (Previous Context):**
            ---
            {history_content_for_llm}
            ---
            **Current User Query (New Information / Changes):**
            ---
            "{query_text}"
            ---{fast_analysis_context}

            **Instructions:**
            1.  **Analyze History:** What was the user's original request?
            2.  **Analyze Current Query:** What new information or changes is the user providing?
            3.  **Refine Quick Analysis:** Use the quick analysis as a starting point, but correct or enhance it based on conversation context.
            4.  **Merge and Synthesize:** Combine all information to create complete, accurate filters.

            **CRITICAL: Indonesian Comparative Terms Understanding**

            **Speed Filter Keywords (Indonesian):**
            - **Below/Under**: "dibawah X mbps" / "kurang dari X mbps" / "maksimal X mbps" / "tidak lebih dari X mbps" → "speed_max": X
            - **Above/Over**: "diatas X mbps" / "lebih dari X mbps" / "minimal X mbps" / "setidaknya X mbps" / "tidak kurang dari X mbps" → "speed_min": X
            - **Exactly**: "tepat X mbps" / "sekitar X mbps" / "X mbps" (without comparative words) → "speed_exact": X
            - **Between**: "antara X dan Y mbps" / "X sampai Y mbps" / "X - Y mbps" → "speed_min": X, "speed_max": Y

            **Price Filter Keywords (Indonesian):**
            - **Below/Under**: "dibawah Rp X" / "kurang dari Rp X" / "maksimal Rp X" / "budget maksimal X" → "price_max": X
            - **Above/Over**: "diatas Rp X" / "lebih dari Rp X" / "minimal Rp X" / "setidaknya Rp X" → "price_min": X
            - **Exactly**: "tepat Rp X" / "sekitar Rp X" / "Rp X" (without comparative words) → "price_exact": X
            - **Between**: "antara Rp X dan Rp Y" / "Rp X sampai Rp Y" → "price_min": X, "price_max": Y

            **Provider Scope Keywords (Indonesian):**
            - **National**: "provider nasional" / "ISP besar" / "telco utama" / "operator besar" / "provider utama" → "search_type": "national_providers_only"
            - **Local**: "provider lokal" / "ISP daerah" / "provider kecil" / "operator lokal" / "ISP setempat" → "search_type": "local_providers_only"
            - **General**: "provider" / "ISP" (without scope qualifier) → "search_type": "provider_discovery"

            **Examples of Correct Interpretation:**
            - "dibawah 100mbps" → {{"speed_max": 100}}
            - "minimal 50mbps" → {{"speed_min": 50}}
            - "antara 20 sampai 100 mbps" → {{"speed_min": 20, "speed_max": 100}}
            - "budget maksimal 500ribu" → {{"price_max": 500000}}
            - "provider nasional di jakarta" → {{"location": ["Jakarta"], "search_type": "national_providers_only"}}
            - "ISP lokal surabaya" → {{"location": ["Surabaya"], "search_type": "local_providers_only"}}

            **Output ONLY a valid JSON object with the MERGED and REFINED criteria.** Omit keys if no value is applicable.

            **Available Keys for JSON:**
            - "data_type_preference": "product", "promo", or "both".
            - "providers": An array of provider names to INCLUDE, or the string "ALL_PROVIDERS_FLAG".
            - "location": An array of standardized city names.
            - "speed_min", "speed_max", "speed_exact": Numeric speed values (in Mbps).
            - "price_min", "price_max", "price_exact": Numeric price values (in Rupiah, without currency symbols).
            - "gimmick_keywords": An array of feature keywords.
            - "search_type": 
            - "local_providers_only" = find only local/regional providers
            - "national_providers_only" = find only major national providers  
            - "provider_discovery" = general provider search

            **IMPORTANT PARSING NOTES:**
            - Convert all speed values to numeric (remove "mbps", "Mbps" suffixes)
            - Convert all price values to numeric Rupiah (remove "Rp", "ribu", "juta" - convert thousands/millions)
            - "ribu" = multiply by 1,000, "juta" = multiply by 1,000,000
            - Always use lowercase for location names initially, system will normalize them
            - Pay close attention to comparative words - they determine which filter type to use

            **Example (Refinement):**
            - Quick Analysis detected: providers=["biznet"], locations=["jakarta"]
            - History: "User: provider lokal di jakarta"
            - Current Query: "bagaimana dengan yang dibawah 50mbps?"
            - Your JSON Output: {{"location": ["Jakarta"], "search_type": "local_providers_only", "speed_max": 50}}

            **Another Example (Price with Indonesian terms):**
            - Query: "paket internet dibawah 300ribu di surabaya"
            - Your JSON Output: {{"location": ["Surabaya"], "price_max": 300000}}
            """
            
            generation_config = genai.types.GenerationConfig(temperature=0.0, max_output_tokens=1024*4)
            logger.info(
                f"{log_prefix_for_func}Calling Gemini filter extraction "
                f"(timeout={FILTER_EXTRACTION_TIMEOUT_SECONDS}s, key_present={bool(api_key)})"
            )
            llm_start_time = time.time()
            response = await asyncio.wait_for(
                model.generate_content_async(llm_prompt_for_db, generation_config=generation_config),
                timeout=FILTER_EXTRACTION_TIMEOUT_SECONDS
            )
            logger.info(
                f"{log_prefix_for_func}Gemini filter extraction completed in "
                f"{time.time() - llm_start_time:.2f}s"
            )
            
            # Response parsing logic
            criteria_text = ""
            if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                criteria_text = "".join(part.text for part in response.candidates[0].content.parts if hasattr(part, 'text')).strip()
            
            if criteria_text:
                json_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', criteria_text, re.DOTALL) or re.search(r'(\{.*\})', criteria_text, re.DOTALL)
                if json_match:
                    try:
                        parsed_json = json.loads(json_match.group(1))
                        raw_llm_filters = {k.lower(): v for k, v in parsed_json.items() if v is not None}
                        logger.info(f"{log_prefix_for_func}LLM extracted raw filters: {raw_llm_filters}")
                        llm_extraction_status_message = "OK_LLM"
                    except json.JSONDecodeError as json_err:
                        logger.error(f"{log_prefix_for_func}JSONDecodeError: {json_err}. Text: {criteria_text}")
                        llm_extraction_status_message = "JSON_DECODE_ERROR"
                else:
                    logger.warning(f"{log_prefix_for_func}No JSON found in LLM response: {criteria_text[:200]}")
                    llm_extraction_status_message = "NO_JSON_IN_RESPONSE"
            else:
                 logger.warning(f"{log_prefix_for_func}LLM response was empty or invalid. Full response: {response}")
                 llm_extraction_status_message = "EMPTY_OR_INVALID_LLM_RESPONSE"

        except asyncio.TimeoutError:
            logger.warning(
                f"{log_prefix_for_func}Gemini filter extraction timed out after "
                f"{FILTER_EXTRACTION_TIMEOUT_SECONDS}s. Falling back to fast analysis only."
            )
            llm_extraction_status_message = "TIMEOUT_FALLBACK_FAST_ANALYSIS"
        except Exception as e:
            logger.error(f"{log_prefix_for_func}Error in LLM filter extraction: {e}", exc_info=True)
            llm_extraction_status_message = f"EXCEPTION_IN_FILTER_EXTRACTION: {type(e).__name__}"
    else:
        logger.warning(f"{log_prefix_for_func}GEMINI_API_KEY not set. Using fast analysis only.")
        llm_extraction_status_message = "API_KEY_MISSING"

    # Merge LLM results with fast analysis baseline
    for key in ['location', 'locations', 'search_type', 'providers', 'speed_min', 'speed_max', 'speed_exact', 'price_min', 'price_max', 'price_exact', 'gimmick_keywords']:
        if key in raw_llm_filters:
            final_filters[key] = raw_llm_filters[key]
        elif key == 'location' and 'locations' in raw_llm_filters:
            final_filters['locations'] = raw_llm_filters['locations']

    # Trust the LLM's context-aware decision
    if 'data_type_preference' in raw_llm_filters:
        # LLM has considered conversation history and made a decision - trust it
        final_filters['data_type_preference'] = raw_llm_filters['data_type_preference']
    else:
        # Only use keyword fallback if LLM didn't provide a preference
        promo_trigger_keywords = ["promo", "promosi", "diskon", "penawaran", "cashback"]
        is_explicit_promo_request = any(kw in query_text.lower() for kw in promo_trigger_keywords)
        final_filters['data_type_preference'] = 'promo' if is_explicit_promo_request else 'product'

    # Enhanced fallback using fast analysis
    if 'providers' not in final_filters and final_filters.get('search_type') != 'local_providers_only':
        if fast_analysis and fast_analysis.get('providers'):
            final_filters['providers'] = fast_analysis['providers']
            logger.info(f"{log_prefix_for_func}Using fast analysis providers: {fast_analysis['providers']}")
        else:
            logger.info(f"{log_prefix_for_func}No providers from LLM or fast analysis. Running rule-based fallback.")
            fallback_providers = detect_isp_names(query_text)
            if fallback_providers:
                final_filters['providers'] = fallback_providers

    logger.info(f"{log_prefix_for_func}Final enhanced filters: {final_filters} (LLM status: {llm_extraction_status_message})")
    
    return final_filters, llm_extraction_status_message


# Keep the original function for backward compatibility
def extract_filters_for_db(query_text: str, conversation_history_input, selected_providers_from_frontend=None):
    """Original function maintained for backward compatibility"""
    return extract_filters_for_db_enhanced(query_text, conversation_history_input, selected_providers_from_frontend, None)


# Fallback logic for extract_filters_for_db
def detect_isp_names(query_text):
    """Detects Indonesian ISP names within a given text using multiple methods.

    This function identifies mentions of ISPs through a three-step process:
    1.  Exact matching against a list of official ISP names.
    2.  Alias matching for common nicknames, abbreviations, and typos.
    3.  Fuzzy string matching on individual words to catch other misspellings.

    It returns a list of unique, normalized ISP names found in the text.

    Args:
        query_text (str): The input text to search for ISP names.

    Returns:
        list[str]: A list of unique, standardized ISP names detected in the text.
    """
    if not query_text:
        return []

    query_lower = query_text.lower()
    detected_isps_set = set()

    aliases = {
        "indihome": "indihome", "indi home": "indihome", "indihom": "indihome", "indihome revamp": "indihome",
        "fm": "firstmedia", "first media": "firstmedia", "first": "firstmedia",
        "mr": "myrepublic", "myrep": "myrepublic", "my republic": "myrepublic",
        "biz": "biznet", "bizz": "biznet", "bizn": "biznet", "biznet home": "biznet",
        "xl home": "xlhome", "xlh": "xlhome", "xl.satu": "xl home", "xl satu": "xl home", "x.satu": "xl home",
        "icon+": "iconnet", "iconplus": "iconnet", "icon net": "iconnet", "icon": "iconnet",
        "mnc": "mncplay", "mnc play": "mncplay",
        "oxygen.id": "oxygen", "oxy": "oxygen",
        "firtmedia": "firstmedia", "firstmedya": "firstmedia", "1stmedia": "firstmedia", "fm media": "firstmedia",
        "myrepublik": "myrepublic", "myrepubliq": "myrepublic",
        "bizet": "biznet", "beznet": "biznet", "bizmed": "biznet",
        "exel": "xlhome", "exelhome": "xlhome",
        "iconet": "iconnet", "ikonet": "iconnet"
    }

    # Step 1: Exact full ISP name check (from INDONESIAN_ISPS)
    for isp in INDONESIAN_ISPS:
        if isp in query_lower: # Simple substring check
            detected_isps_set.add(isp)

    # Step 2: Alias check (longer aliases first)
    sorted_alias_keys = sorted(aliases.keys(), key=len, reverse=True)
    for alias_key in sorted_alias_keys:
        if alias_key in query_lower:
            mapped_isp = aliases[alias_key]
            if mapped_isp in INDONESIAN_ISPS:
                detected_isps_set.add(mapped_isp)

    # Step 3: Fuzzy matching for typos on individual words
    words = query_lower.split()
    for word in words:
        if len(word) < 4: # Minimum word length for fuzzy matching
            continue

        best_fuzzy_match_isp = None
        highest_similarity_score = 0.75  # Initial threshold

        for isp_candidate in INDONESIAN_ISPS:
            if isp_candidate in detected_isps_set: # Skip already found ISPs
                continue

            similarity = SequenceMatcher(None, word, isp_candidate).ratio()

            if similarity > highest_similarity_score:
                highest_similarity_score = similarity
                best_fuzzy_match_isp = isp_candidate
            elif similarity >= 0.70 and isp_candidate.startswith(word) and similarity > highest_similarity_score - 0.05 :
                highest_similarity_score = similarity
                best_fuzzy_match_isp = isp_candidate

        if best_fuzzy_match_isp and highest_similarity_score >= 0.80: # Final stricter threshold
            detected_isps_set.add(best_fuzzy_match_isp)

    return list(detected_isps_set)



# ------------------------------ HELPER: GET UNIQUE SPEED (MARKET MODE) ---------------------------------- #
def get_unique_speeds_in_scope(scope_filters=None, logger_instance=None):
    global logger # Assuming logger is globally defined
    current_logger = logger_instance if logger_instance else logger

    query = """
    SELECT DISTINCT CAST(t.speed_mbps_cleaned AS INTEGER) AS distinct_numeric_speed
    FROM (
        SELECT 
            TRIM(LOWER(REPLACE(CAST(speed_mbps AS TEXT), 'mbps', ''))) AS speed_mbps_cleaned,
            provider,
            locations
        FROM dashboard_product_detail
        WHERE speed_mbps IS NOT NULL AND TRIM(CAST(speed_mbps AS TEXT)) != ''
        UNION
        SELECT 
            TRIM(LOWER(REPLACE(CAST(speed AS TEXT), 'mbps', ''))) AS speed_mbps_cleaned,
            provider,
            found AS locations
        FROM dashboard_provider_matpro
        WHERE speed IS NOT NULL AND TRIM(CAST(speed AS TEXT)) != ''
    ) AS t
    """
    where_clauses = ["t.speed_mbps_cleaned GLOB '[0-9]*'"]
    params = []

    if scope_filters:
        location_filter_data = scope_filters.get('location')
        if location_filter_data:
            user_queried_normalized_list = []
            if isinstance(location_filter_data, str):
                user_queried_normalized_list.extend(
                    normalize_location(loc.strip()) for loc in location_filter_data.split(',') if loc.strip()
                )
            elif isinstance(location_filter_data, list):
                user_queried_normalized_list.extend(
                    normalize_location(str(loc).strip()) for loc in location_filter_data if str(loc).strip()
                )
            
            if user_queried_normalized_list:
                location_sql_parts = []
                for norm_loc in user_queried_normalized_list:
                    location_sql_parts.append("LOWER(t.locations) LIKE ?")
                    params.append(f"%{norm_loc}%")
                if location_sql_parts:
                    where_clauses.append(f"({ ' OR '.join(location_sql_parts) })")

        provider_filter_data = scope_filters.get('provider')
        if provider_filter_data:
            valid_providers = []
            if isinstance(provider_filter_data, str):
                if provider_filter_data.strip(): valid_providers = [provider_filter_data.lower().strip()]
            elif isinstance(provider_filter_data, list):
                valid_providers = [str(p).lower().strip() for p in provider_filter_data if str(p).strip()]
            
            if valid_providers:
                provider_clause, provider_params = build_sqlite_in_clause(valid_providers)
                where_clauses.append(f"LOWER(t.provider) {provider_clause}")
                params.extend(provider_params)
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    query += " ORDER BY distinct_numeric_speed ASC LIMIT 5;" # Get a few relevant alternative speeds

    conn = None
    distinct_speeds = []
    try:
        conn = get_db_connection()
        current_logger.debug(f"Get Unique Speeds Query: {query} with Params: {params}")
        df = pd.read_sql(query, conn, params=params if params else None)
        if not df.empty:
            distinct_speeds = sorted([
                int(s) for s in df['distinct_numeric_speed'].dropna().astype(float).unique() 
                if s is not None and not np.isnan(s) and float(s).is_integer() and float(s) > 0
            ])
        current_logger.info(f"Found distinct speeds in scope {scope_filters}: {distinct_speeds}")
    except Exception as e:
        current_logger.error(f"Error fetching unique speeds for scope {scope_filters}: {e}", exc_info=True)
    finally:
        if conn and conn.open:
            conn.close()
    return distinct_speeds


# ------------------------------ HELPER: GET RECENT DOCUMENTS (DOCUMENTS MODE) ---------------------------------- #
def get_recent_documents_from_index(limit=25):
    """
    Finds documents in the index with a YYYYMMDD_ prefix in their filename,
    sorts them by date, and returns the most recent ones.
    """
    logger.info(f"Attempting to get {limit} most recent documents from index...")
    recent_docs_with_dates = []
    date_prefix_pattern = re.compile(r'^(\d{8})_') # Regex to capture the YYYYMMDD part

    # Access the document index thread-safely
    current_index = None
    with document_index_lock:
        # Create a copy of the index to process outside the lock
        current_index = list(document_index)

    if not current_index:
        logger.warning("Document index is empty. Cannot get recent documents.")
        return [] # Return empty list if index is empty

    for entry in current_index:
        relative_path = entry.get('relative_path')
        if not relative_path:
            logger.debug(f"Skipping index entry with no relative_path: {entry.get('title', 'N/A')}")
            continue

        match = date_prefix_pattern.match(os.path.basename(relative_path))
        if match:
            date_str = match.group(1)
            try:
                # Validate the date format (optional but good practice)
                # If we just need to sort, the string YYYYMMDD is sufficient
                # but parsing ensures it's actually a calendar date format
                doc_date = datetime.strptime(date_str, '%Y%m%d')

                # Keep necessary info for frontend, adding the parsed date for sorting
                doc_info = {
                    'id': relative_path, # Use relative_path as ID
                    'name': os.path.basename(relative_path), # Display just the filename
                    'type': get_file_type_from_extension(relative_path),
                    'path': relative_path, # Store relative path
                    # Use document summary as preview if available, otherwise generic
                    'preview': entry.get('document_summary', 'Dokumen internal terbaru')[:100] + '...' if entry.get('document_summary') else 'Dokumen internal terbaru',
                    'confidence': 0.9, # Assign high confidence as they are user-requested 'recent'
                    '_doc_date': doc_date # Store the parsed date for sorting
                }
                recent_docs_with_dates.append(doc_info)

            except ValueError:
                logger.warning(f"Filename '{os.path.basename(relative_path)}' has YYYYMMDD_ prefix but date '{date_str}' is invalid. Skipping.")
                # Skip if the date format is invalid
            except Exception as e:
                logger.error(f"Unexpected error processing recent doc entry '{os.path.basename(relative_path)}': {e}", exc_info=True)
                # Log and skip on other errors


    # Sort by date in descending order (most recent first)
    recent_docs_with_dates.sort(key=lambda x: x.get('_doc_date', datetime.min), reverse=True)

    # Select the top N and remove the temporary '_doc_date' key
    top_recent_docs = recent_docs_with_dates[:limit]
    for doc in top_recent_docs:
        doc.pop('_doc_date', None)

    logger.info(f"Found and returning {len(top_recent_docs)} recent documents with valid date prefixes.")
    return top_recent_docs


def get_synthetic_documents_from_filesystem(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fallback document catalog for the portfolio demo.
    Reads synthetic files directly from POLICY_DIR / SYNTHETIC_DOCS_DIR so the
    document finder works even when dashboard_summary_documents is empty.
    """
    docs_dir = POLICY_DIR if os.path.isdir(POLICY_DIR) else SYNTHETIC_DOCS_DIR
    if not os.path.isdir(docs_dir):
        logger.warning(f"Synthetic documents directory not found: {docs_dir}")
        return []

    documents = []
    for entry in sorted(os.scandir(docs_dir), key=lambda item: item.name.lower()):
        if not entry.is_file():
            continue
        rel_path = os.path.relpath(entry.path, docs_dir).replace("\\", "/")
        ext = get_file_type_from_extension(entry.name)
        preview = "Synthetic portfolio demo document"
        try:
            with open(entry.path, "r", encoding="utf-8") as f:
                preview_text = f.read(220).strip().replace("\n", " ")
                if preview_text:
                    preview = preview_text[:140] + ("..." if len(preview_text) > 140 else "")
        except Exception as preview_err:
            logger.debug(f"Preview read skipped for {entry.name}: {preview_err}")

        documents.append({
            "id": rel_path,
            "name": entry.name,
            "type": ext,
            "path": rel_path,
            "preview": preview,
            "confidence": 0.95,
            "timestamp": datetime.fromtimestamp(entry.stat().st_mtime).isoformat(),
            "is_demo_document": True
        })

    logger.info(f"Loaded {len(documents)} synthetic documents from filesystem fallback.")
    return documents[:limit] if limit else documents


async def run_ingestion_task(file_path: str):
    """
    Native Async task.
    It keeps the event loop free by awaiting the blocking parts 
    in a separate thread context.
    """
    try:
        filename = os.path.basename(file_path)
        logger.info(f"⚙️ [Background] Starting AI processing for: {filename}")
        
        # 1. Heavy AI Processing (Run in Thread)
        # We await the thread, releasing the event loop for other users
        generated_data = await asyncio.to_thread(process_document_and_generate_data, file_path)
        
        if generated_data:
            # 2. Database Operations (Run in Thread)
            # We define a small inner helper to group the blocking DB calls
            def _blocking_db_operations(data):
                conn = get_pipeline_db_conn()
                try:
                    # MySQL
                    if insert_document_data_to_db(conn, data['mysql_rows']):
                        logger.info(f"✅ [Background] MySQL Insert success for {filename}")
                        
                        # LanceDB
                        if insert_data_to_lancedb(data['lancedb_rows']):
                            logger.info(f"✅ [Background] LanceDB Insert success for {filename}")
                            refresh_document_index()
                            logger.info(f"🔄 [Background] Index refreshed.")
                        else:
                            logger.error(f"❌ [Background] LanceDB Insert FAILED for {filename}")
                    else:
                        logger.error(f"❌ [Background] MySQL Insert failed for {filename}")
                finally:
                    conn.close()

            # Await the DB operations
            await asyncio.to_thread(_blocking_db_operations, generated_data)
            
        else:
            logger.warning(f"⚠️ [Background] No data generated for {filename}")

    except Exception as e:
        logger.error(f"❌ [Background] Ingestion failed: {e}", exc_info=True)



# ================================================================= #
#              DATA INSIGHT: SQL EDIT / CHART PICKER API
# ================================================================= #

@api_router.post("/api/data_insight/execute_sql")
async def execute_raw_sql_endpoint(request: Request):
    """Execute user-edited SQL (SELECT only) and return raw columnar data."""
    try:
        body = await request.json()
        sql = body.get('sql', '').strip()
        session_id = body.get('session_id', 'unknown')

        if not sql:
            raise HTTPException(status_code=400, detail="SQL query is required")

        engine = get_data_insight_engine()
        result = await engine.execute_raw_sql(sql, session_id)
        return result

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"[execute_raw_sql] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {str(e)}")


@api_router.post("/api/data_insight/generate_chart")
async def generate_chart_endpoint(request: Request):
    """Render a chart with user-specified config (chart type, axes, etc.)."""
    try:
        body = await request.json()
        sql = body.get('sql', '').strip()
        chart_type = body.get('chart_type', 'bar')
        x_column = body.get('x_column')
        y_column = body.get('y_column')
        color_column = body.get('color_column')
        aggregation = body.get('aggregation')
        session_id = body.get('session_id', 'unknown')

        if not sql:
            raise HTTPException(status_code=400, detail="SQL query is required")

        engine = get_data_insight_engine()
        result = await engine.render_with_config(
            sql, chart_type, x_column, y_column,
            color_column, aggregation, session_id
        )
        return result

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"[generate_chart] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chart generation failed: {str(e)}")


@api_router.post("/api/data_insight/ai_recommend")
async def ai_recommend_endpoint(request: Request):
    """Let AI pick the best visualization(s) for the given SQL."""
    try:
        body = await request.json()
        sql = body.get('sql', '').strip()
        session_id = body.get('session_id', 'unknown')

        if not sql:
            raise HTTPException(status_code=400, detail="SQL query is required")

        engine = get_data_insight_engine()
        result = await engine.render_with_ai(sql, session_id)
        return result

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"[ai_recommend] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"AI recommendation failed: {str(e)}")


# ================================================================= #
#                              API
# ================================================================= #

# --- Sorting document based on most recent --- #
@api_router.get("/api/recent_documents")
async def get_recent_documents(limit: int = Query(20, ge=1, le=100)):
    """Get recent documents (Async wrapper)."""
    # Accessing the index in memory is fast, but sorting might take time if list is huge.
    # Wrapping in thread is safer for concurrency.
    try:
        recent_docs = await asyncio.to_thread(get_recent_documents_from_index, limit)
        return {"message": "Success", "documents": recent_docs or []}
    except Exception as e:
        logger.error(f"Error getting recent docs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Error")

# --- Get ALL documents for Document Finder --- #
@api_router.get("/api/all_documents")
async def get_all_documents():
    """Get all documents for the document finder (Async wrapper)."""
    try:
        all_docs = []
        with document_index_lock:
            current_index = list(document_index)

        if current_index:
            all_docs = [
                {
                    "id": entry.get("relative_path"),
                    "name": os.path.basename(entry.get("relative_path", "")),
                    "type": get_file_type_from_extension(entry.get("relative_path", "")),
                    "path": entry.get("relative_path"),
                    "preview": (entry.get("document_summary") or "Synthetic portfolio demo document")[:140],
                    "confidence": 0.95,
                    "timestamp": entry.get("timestamp_utc")
                }
                for entry in current_index
                if entry.get("relative_path")
            ]

        if not all_docs:
            all_docs = await asyncio.to_thread(get_synthetic_documents_from_filesystem)

        return {"message": "Success", "documents": all_docs or []}
    except Exception as e:
        logger.error(f"Error getting all docs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Error")


# --- Providing prompt suggestion for frontend --- #
@api_router.post("/api/get_prompt_suggestions")
async def get_prompt_suggestions(
    payload: SuggestionRequest,
    token: str = Depends(get_current_user)  # Add auth
):
    suggestions = []
    
    try:
        def _load_prompt_suggestions():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                if payload.selected_paths:
                    filenames = [os.path.basename(p) for p in payload.selected_paths if p]
                    if filenames:
                        placeholders = ','.join(['?'] * len(filenames))
                        query = f"""
                            SELECT prompt_suggestion_1, prompt_suggestion_2, prompt_suggestion_3
                            FROM dashboard_summary_documents
                            WHERE filename IN ({placeholders})
                            AND (prompt_suggestion_1 IS NOT NULL OR prompt_suggestion_2 IS NOT NULL)
                            LIMIT 15
                        """
                        cursor.execute(query, tuple(filenames))
                        rows = cursor.fetchall()
                        if rows:
                            return rows

                cursor.execute("""
                    SELECT prompt_suggestion_1, prompt_suggestion_2, prompt_suggestion_3
                    FROM dashboard_summary_documents
                    WHERE prompt_suggestion_1 IS NOT NULL
                    AND prompt_suggestion_1 != ''
                    ORDER BY filename ASC
                    LIMIT 15
                """)
                return cursor.fetchall()
            finally:
                conn.close()

        rows = await asyncio.to_thread(_load_prompt_suggestions)

        raw_suggestions = []
        for row in rows:
            raw_suggestions.extend([s for s in row if s and str(s).strip()])

        seen = set()
        suggestions = [x for x in raw_suggestions if not (x in seen or seen.add(x))][:payload.limit]

        return {"status": "success", "suggestions": suggestions}

    except Exception as e:
        logger.error(f"Error fetching prompt suggestions: {e}", exc_info=True)
        return {"status": "error", "suggestions": []}


# ---------------------
# Chat History API
# ---------------------
@api_router.get("/api/chat_history")
async def get_chat_history(current_user: str = Depends(get_current_user)):
    """
    Retrieves chat history sessions grouped by date for the sidebar.
    """
    user_id = current_user 
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        sql = """
            SELECT 
                session_id, 
                MAX(timestamp) as last_ts,
                (SELECT query_text FROM conversation_memory cm2 
                 WHERE cm2.session_id = cm1.session_id
                 AND cm2.user_id = cm1.user_id
                 ORDER BY turn_order ASC LIMIT 1) as title
            FROM conversation_memory cm1
            WHERE user_id = ?
            GROUP BY session_id
            ORDER BY MAX(id) DESC
        """
        cursor.execute(sql, (user_id,))
        rows = cursor.fetchall()
        
        history = []
        for row in rows:
            history.append({
                "session_id": row[0],
                "timestamp": row[1] if row[1] else None,
                "title": row[2] if row[2] else "New Chat"
            })
        return history
    except Exception as e:
        logger.error(f"Error fetching history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@api_router.get("/api/chat_session/{session_id}")
async def get_chat_session(session_id: str, current_user: str = Depends(get_current_user)):
    """
    Retrieves all messages for a specific session to reload the chat window.
    Ensures users can only access their own sessions.
    """
    user_id = current_user
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        sql = """
            SELECT query_text, response_text, context_items, rag_mode, market_sources, timestamp, thinking_text
            FROM conversation_memory
            WHERE session_id = ? AND user_id = ?
            ORDER BY turn_order ASC
        """
        cursor.execute(sql, (session_id, user_id))
        rows = cursor.fetchall()

        STALE_THRESHOLD_SECONDS = 180
        messages = []
        for row in rows:
            messages.append({
                "type": "user",
                "content": row[0]
            })

            response_text = row[1]
            turn_timestamp = row[5]

            if response_text == '[Generating...]':
                is_stale = False
                if not turn_timestamp:
                    is_stale = True
                else:
                    try:
                        turn_dt = datetime.fromisoformat(str(turn_timestamp))
                        is_stale = (datetime.now() - turn_dt).total_seconds() > STALE_THRESHOLD_SECONDS
                    except ValueError:
                        is_stale = True
                if is_stale:
                    response_text = (
                        '<p>Proses sebelumnya terputus atau melebihi batas waktu.</p>'
                        '<p><em>Silakan kirim ulang pertanyaan Anda.</em></p>'
                    )

            sources = []
            if row[2]:
                try:
                    sources = json.loads(row[2]) if isinstance(row[2], str) else row[2]
                except Exception:
                    sources = []

            market_sources = []
            if row[4]:
                try:
                    market_sources = json.loads(row[4]) if isinstance(row[4], str) else row[4]
                except Exception:
                    market_sources = []

            messages.append({
                "type": "bot",
                "content": response_text,
                "sources": sources,
                "mode": row[3],
                "market_sources": market_sources,
                "thinking_text": row[6] if row[6] else None
            })
        return {"messages": messages}
    except Exception as e:
        logger.error(f"Error fetching session: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
        

@api_router.delete("/api/chat_session/{session_id}")
async def delete_chat_session(session_id: str, current_user: str = Depends(get_current_user)):
    """
    Deletes a specific chat session and clears it from V2 memory cache.
    """
    user_id = current_user
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        sql = "DELETE FROM conversation_memory WHERE session_id = ? AND user_id = ?"
        cursor.execute(sql, (session_id, user_id))
        conn.commit()
        
        rows_deleted = cursor.rowcount
        
        if rows_deleted == 0:
            raise HTTPException(status_code=404, detail="Session not found or access denied")

        if 'conversation_memory' in globals():
            conversation_memory.clear_cache(user_id, session_id)
            
        logger.info(f"User {user_id} deleted session {session_id}")
        return {"status": "success", "message": f"Session {session_id} deleted"}
            
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error deleting session: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# --- /api/chat Endpoint ---
@api_router.post("/api/chat", status_code=status.HTTP_202_ACCEPTED, response_model=JobSubmissionResponse)
async def async_chat(
    request: ChatRequest, 
    background_tasks: BackgroundTasks,
    current_user: str = Depends(get_current_user),
    x_gemini_api_key: Optional[str] = Header(None)
):
    global document_index, INDONESIAN_ISPS, conversation_memory, _execute_generation_task

    try:
        api_key_override = x_gemini_api_key.strip() if x_gemini_api_key else None

        # 1. SETUP & ID GENERATION (Must be first)
        original_query = request.query.strip()
        user_id = current_user
        
        user_session_id_from_client = request.session_id.strip() if request.session_id else None
        user_session_id_to_return = user_session_id_from_client or str(uuid.uuid4().hex)

        if not original_query:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"answer": "<p>Pertanyaan tidak boleh kosong.</p>", "session_id": user_session_id_to_return})

        # 2. SEAMLESS ROUTING LOGIC
        # Determines 'rag_mode' before we process sources
        processor = get_processor_instance()
        fast_analysis = processor.analyze_fast(original_query)
        
        if request.mode.lower() == "auto":
            # Check for follow-up queries that should inherit previous mode.
            # Two signals: (A) short query < 4 words, (B) anaphoric/follow-up references
            is_thin_query = len(original_query.split()) < 4
            query_lower_rt = original_query.lower()
            has_anaphora = _detect_followup_query(query_lower_rt)
            is_followup = is_thin_query or has_anaphora

            if is_followup:
                # Retrieve history to check previous mode
                # ASYNC WRAPPER: Wrap blocking memory call
                last_context = await asyncio.to_thread(conversation_memory.get_context, user_id, user_session_id_to_return)
                if last_context.get('turns'):
                    last_turn = sorted(last_context['turns'], key=lambda x: x['turn_order'])[-1]
                    rag_mode = last_turn.get('rag_mode', 'document') # Default to document if missing
                    logger.info(f"Seamless Router: Follow-up detected ({'thin' if is_thin_query else 'anaphora'}), sticking to mode '{rag_mode}'")
                else:
                    # No history, use smart routing
                    rag_mode, _, _, _ = await smart_route(original_query, processor, fast_analysis)
            else:
                # Normal query, use smart routing
                rag_mode, _, _, _ = await smart_route(original_query, processor, fast_analysis)

            logger.info(f"Seamless Router: Auto-selected mode '{rag_mode}' for query: '{original_query[:50]}'")
        else:
            # Manual override
            rag_mode = request.mode.lower()

        # 3. SOURCE SELECTION & AUTO-DISCOVERY
        selected_source_paths = request.selected_sources
        model_override = request.model_override if request.model_override in ALLOWED_MODEL_OVERRIDES else DEFAULT_LLM_MODEL

        # Rate limit check for Pro models
        if model_override in RATE_LIMITED_MODELS:
            allowed, count, limit = await check_and_increment_model_quota(user_id, model_override)
            if not allowed:
                logger.info(f"Rate limit: {user_id} exceeded {model_override} quota ({count}/{limit})")
                model_override = DEFAULT_LLM_MODEL

        logger.info(f"Chat Request: User='{user_id}', Session='{user_session_id_to_return}', Mode='{rag_mode}', Model={model_override}")

        # Contextual Fallback for Document Mode
        if rag_mode == 'document' and not selected_source_paths:
            # A. Attempt automatic selection based on CURRENT query
            selected_source_paths = await asyncio.to_thread(auto_select_relevant_documents, original_query, limit=4, _rag_sid=user_session_id_to_return[:8])

            # B. Contextual Fallback: If nothing found, check history (e.g. user asks "Buatkan tabel" referring to prev doc)
            if not selected_source_paths:
                logger.info(f"Auto-RAG found nothing for '{original_query}'. Checking conversation history for context...")
                try:
                    # ASYNC WRAPPER: Wrap blocking memory call
                    context_data = await asyncio.to_thread(conversation_memory.get_context, user_id, user_session_id_to_return)
                    past_turns = context_data.get('turns', [])
                    
                    if past_turns:
                        last_turn = sorted(past_turns, key=lambda x: x['turn_order'])[-1]
                        last_items = last_turn.get('context_items', [])
                        
                        # Handle JSON string vs List
                        if isinstance(last_items, str):
                            try: last_items = json.loads(last_items)
                            except: last_items = []
                        
                        previous_paths = [item.get('path') for item in last_items if isinstance(item, dict) and item.get('path')]
                        
                        if previous_paths:
                            selected_source_paths = list(set(previous_paths))
                            logger.info(f"Contextual Fallback: Re-using {len(selected_source_paths)} docs from previous turn")
                except Exception as e:
                    logger.warning(f"Contextual fallback failed: {e}")

            if not selected_source_paths:
                msg = "<p>Saya tidak menemukan dokumen yang relevan di database, dan tidak ada konteks dokumen sebelumnya.</p>"
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"answer": msg, "session_id": user_session_id_to_return})

        # 4. PREPARE EXECUTION ARGS
        query_for_matching = original_query
        # ASYNC WRAPPER: Wrap blocking memory call
        history_string_for_prompt = await asyncio.to_thread(
            conversation_memory.get_last_query_history_str, 
            user_id, 
            session_id=user_session_id_to_return, 
            num_turns_for_prompt=5
        )
        job_id = str(uuid.uuid4().hex)

        args_for_task = ()
        kwargs_for_task = {}
        target_function_name = ""

        # 5. MODE-SPECIFIC SETUP
        if rag_mode == "document":
            target_function_name = 'generate_document_response_selected'
            
            # Ensure index is loaded
            if not document_index:
                 await asyncio.to_thread(refresh_document_index)
                 if not document_index:
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail={"error": "Indeks dokumen tidak tersedia.", "session_id": user_session_id_to_return})
            
            # Fetch Suggestions from DB (Using Async Pool if available)
            doc_suggestions_map = {}
            if selected_source_paths:
                try:
                    if async_db_pool:
                        async with async_db_pool.acquire() as conn:
                            async with conn.cursor() as cursor:
                                format_strings = ','.join(['%s'] * len(selected_source_paths))
                                sql = f"""
                                    SELECT filename, prompt_suggestion_1, prompt_suggestion_2, prompt_suggestion_3 
                                    FROM dashboard_summary_documents 
                                    WHERE filename IN ({format_strings})
                                    GROUP BY filename
                                """
                                await cursor.execute(sql, tuple(selected_source_paths))
                                rows = await cursor.fetchall()
                                for row in rows:
                                    suggs = [s for s in row[1:] if s and str(s).strip()]
                                    doc_suggestions_map[row[0]] = suggs
                    else:
                        # Fallback to sync connection in thread if pool failed
                        def _fetch_suggestions_sync():
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as cursor:
                                    format_strings = ','.join(['%s'] * len(selected_source_paths))
                                    sql = f"""
                                        SELECT filename, prompt_suggestion_1, prompt_suggestion_2, prompt_suggestion_3 
                                        FROM dashboard_summary_documents 
                                        WHERE filename IN ({format_strings})
                                        GROUP BY filename
                                    """
                                    cursor.execute(sql, tuple(selected_source_paths))
                                    return cursor.fetchall()
                            finally:
                                conn.close()
                        
                        rows = await asyncio.to_thread(_fetch_suggestions_sync)
                        for row in rows:
                            suggs = [s for s in row[1:] if s and str(s).strip()]
                            doc_suggestions_map[row[0]] = suggs
                            
                except Exception as e:
                    logger.error(f"Error fetching suggestions: {e}")

            # Re-Hydrate Document Details from Index
            unsorted_docs_map = {}
            selected_paths_set = set(selected_source_paths)
            
            for doc_entry in document_index:
                path = doc_entry.get('relative_path', '')
                if path in selected_paths_set:
                    # Semantic search for specific pages
                    semantic_result = find_relevant_pages_semantic(query_for_matching, doc_entry, _rag_sid=user_session_id_to_return[:8])
                    
                    # Suggestions logic
                    suggestions = doc_suggestions_map.get(path, [])
                    if not suggestions:
                        suggestions = [
                            doc_entry.get('prompt_suggestion_1'),
                            doc_entry.get('prompt_suggestion_2'),
                            doc_entry.get('prompt_suggestion_3')
                        ]
                        suggestions = [s for s in suggestions if s and str(s).strip()]

                    unsorted_docs_map[path] = {
                        'path': path, 
                        'title': doc_entry.get('title', os.path.basename(path)),
                        'document_summary': doc_entry.get('document_summary', ''), 
                        'page_analysis': doc_entry.get('page_analysis', {}),
                        'visual_analysis_summary': doc_entry.get('visual_analysis_summary', {}), 
                        'semantically_relevant_pages': semantic_result.get('pages', []),
                        'suggestions': suggestions
                    }

            # Filter valid docs
            current_selected_docs_details = []
            for path in selected_source_paths:
                if path in unsorted_docs_map:
                    current_selected_docs_details.append(unsorted_docs_map[path])
            
            if not current_selected_docs_details:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"answer": "<p>Dokumen yang dipilih tidak ditemukan.</p>", "session_id": user_session_id_to_return})
            
            args_for_task = (original_query, current_selected_docs_details, user_id, history_string_for_prompt)
            kwargs_for_task = {'job_id_for_logging': job_id, 'stream_response': False, 'session_id': user_session_id_to_return, 'model_override': model_override, 'api_key_override': api_key_override}

        elif rag_mode == "market":
            target_function_name = 'generate_market_response_selected'

            # Extract filters specifically for Market logic
            # ASYNC CALL: Directly await the async function
            db_filters_dict, _ = await extract_filters_for_db_enhanced(query_for_matching, history_string_for_prompt, selected_source_paths, fast_analysis, api_key_override=api_key_override)

            market_selected_providers = db_filters_dict.get('provider', [])
            if not market_selected_providers and not db_filters_dict.get('search_type'):
                # Fallback to simple detection if LLM extraction missed providers
                market_selected_providers = detect_isp_names(original_query)

            args_for_task = (original_query, market_selected_providers, history_string_for_prompt, user_id)
            kwargs_for_task = {'job_id_for_logging': job_id, 'stream_response': False, 'session_id': user_session_id_to_return, 'model_override': model_override, 'api_key_override': api_key_override}

        elif rag_mode == "data_insight":
            target_function_name = 'generate_data_insight_response'
            args_for_task = (original_query, user_id, history_string_for_prompt)
            kwargs_for_task = {'job_id_for_logging': job_id, 'session_id': user_session_id_to_return, 'api_key_override': api_key_override}

        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Mode '{rag_mode}' tidak valid.", "session_id": user_session_id_to_return})

        # 6. DB INSERT & BACKGROUND TASK
        input_params = {"user_id": user_id, "session_id": user_session_id_to_return, "rag_mode": rag_mode}
        
        # Use Async Pool for non-blocking insert
        try:
            if async_db_pool:
                async with async_db_pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            "INSERT INTO conversation_async_jobs (job_id, convo_status, input_params, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())", 
                            (job_id, 'pending', json.dumps(input_params))
                        )
                        await conn.commit()
            else:
                # Fallback to sync insert in thread
                def _insert_job_sync():
                    conn = get_db_connection()
                    try:
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            INSERT INTO conversation_async_jobs
                            (job_id, convo_status, input_params, created_at, updated_at)
                            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """,
                            (job_id, 'pending', json.dumps(input_params))
                        )
                        conn.commit()
                    finally:
                        conn.close()
                await asyncio.to_thread(_insert_job_sync)
                
        except Exception as db_err:
            logger.error(f"Failed to submit job {job_id}: {db_err}")
            raise HTTPException(status_code=500, detail="Database Error")
        
        background_tasks.add_task(_execute_generation_task_async, job_id, target_function_name, args_for_task, kwargs_for_task)
        
        return JobSubmissionResponse(job_id=job_id, session_id=user_session_id_to_return, message="Processing...")

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Chat API Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")


# --- Streaming Chat Endpoint --- #
@api_router.post("/api/stream_chat")
async def async_stream_chat(
    request: ChatRequest, 
    current_user: str = Depends(get_current_user),
    x_gemini_api_key: Optional[str] = Header(None)
):
    """
    Real-time streaming endpoint with seamless routing and sticky session memory.
    """
    global document_index, conversation_memory, job_storage, INDONESIAN_ISPS, job_storage_lock

    # 1. SETUP & ID GENERATION
    original_query = request.query.strip()
    user_id = current_user
    user_session_id = request.session_id.strip() or str(uuid.uuid4().hex)
    api_key_override = x_gemini_api_key.strip() if x_gemini_api_key else None
    api_key_token = CURRENT_REQUEST_GEMINI_API_KEY.set(api_key_override) if api_key_override else None

    # BYOK Debug Logging
    logger.info(f"[BYOK-DEBUG] X-Gemini-API-Key header present: {bool(x_gemini_api_key)} | Length: {len(x_gemini_api_key) if x_gemini_api_key else 0}")
    if api_key_override:
        logger.info(f"[BYOK-DEBUG] API Key override set: {api_key_override[:4]}...{api_key_override[-4:] if len(api_key_override) > 8 else ''}")
    
    # 2. SEAMLESS ROUTING LOGIC
    processor = get_processor_instance()
    fast_analysis = processor.analyze_fast(original_query)
    
    needs_confirmation = False
    if request.mode.lower() == "auto":
        is_thin_query = len(original_query.split()) < 4
        query_lower_rt = original_query.lower()
        has_anaphora = _detect_followup_query(query_lower_rt)
        is_followup = is_thin_query or has_anaphora

        if is_followup and user_session_id:
            # Check history for "Sticky Mode"
            # ASYNC WRAPPER: Wrap blocking memory call
            last_context = await asyncio.to_thread(conversation_memory.get_context, user_id, user_session_id)
            if last_context.get('turns'):
                last_turn = sorted(last_context['turns'], key=lambda x: x['turn_order'])[-1]
                rag_mode = last_turn.get('rag_mode')
                if not rag_mode:
                    rag_mode, _, _, _ = await smart_route(original_query, processor, fast_analysis)
                    logger.info(f"Seamless Stream Router: No mode stored for last turn, smart_route selected '{rag_mode}'")
                else:
                    logger.info(f"Seamless Stream Router: Inheriting mode '{rag_mode}' for follow-up ({'thin' if is_thin_query else 'anaphora'})")
            else:
                rag_mode, _, _, _ = await smart_route(original_query, processor, fast_analysis)
        else:
            rag_mode, route_score, strong_score, original_mode = await smart_route(original_query, processor, fast_analysis)
            # Confirm when data_insight selected but no strong signal contributed,
            # OR when LLM overrode data_insight to something else (let user choose)
            di_confirm_cfg = processor.routing_config.get('data_insight', {}).get('confirmation', {})
            if di_confirm_cfg.get('enabled', True) and (
                (rag_mode == 'data_insight' and strong_score == 0)
                or (original_mode == 'data_insight' and rag_mode != 'data_insight')
            ):
                needs_confirmation = True
                rag_mode = 'data_insight'  # Reset to data_insight for confirmation prompt

        logger.info(f"Seamless Stream Router: Auto-selected '{rag_mode}'")
    else:
        rag_mode = request.mode.lower()

    rag_trace(user_session_id[:8], "RT", mode=rag_mode, followup=is_followup if 'is_followup' in dir() else False)

    selected_source_paths = request.selected_sources
    model_override = request.model_override if request.model_override in ALLOWED_MODEL_OVERRIDES else DEFAULT_LLM_MODEL
    import uuid
    request_id = str(uuid.uuid4())[:8]
    log_prefix = f"[req={request_id} User={user_id} Session={user_session_id[:8]} Mode={rag_mode}]"

    # Rate limit check for Pro models
    quota_fallback_notice = None
    if model_override in RATE_LIMITED_MODELS:
        allowed, count, limit = await check_and_increment_model_quota(user_id, model_override)
        if not allowed:
            logger.info(f"{log_prefix} Rate limit: {user_id} exceeded {model_override} quota ({count}/{limit})")
            quota_fallback_notice = model_override
            model_override = DEFAULT_LLM_MODEL

    # Helper for errors
    async def error_stream(message: str) -> AsyncGenerator[str, None]:
        yield message

    if not original_query:
        return StreamingResponse(error_stream("<p>Pertanyaan tidak boleh kosong.</p>"), media_type="text/html; charset=utf-8")

    # 3. AUTO-DISCOVERY (Document Mode)
    if rag_mode == 'document' and not selected_source_paths:
        selected_source_paths = await asyncio.to_thread(auto_select_relevant_documents, original_query, limit=4, _rag_sid=user_session_id[:8])
        if not selected_source_paths:
            logger.warning(f"{log_prefix} Auto-RAG found no documents.")
            return StreamingResponse(error_stream("<p>Tidak ada dokumen yang relevan ditemukan untuk pertanyaan ini. Coba kata kunci lain.</p>"), media_type="text/html; charset=utf-8")

    # 3.5. CONFIRMATION SHORT-CIRCUIT (no pipeline, no DB save)
    if needs_confirmation:
        lang = _detect_query_language(original_query)
        confirm_html = _build_confirmation_html(original_query, lang)
        async def confirmation_streamer():
            yield "<!-- ROUTED_MODE: data_insight -->"
            yield confirm_html
        logger.info(f"{log_prefix} Route confirmation requested (data_insight, no strong signal)")
        return StreamingResponse(confirmation_streamer(), media_type="text/html; charset=utf-8")

    # 4. MAIN STREAM GENERATOR (2-PHASE SAVE IMPLEMENTATION)
    async def combined_streamer() -> AsyncGenerator[str, None]:
        _t_stream_start = time.time()
        context_items_for_memory_update = []
        llm_generator = None
        full_response_accumulator = "" # Track full text for DB update
        current_turn_id = None  # Initialize early so CancelledError handler is safe
        
        # Define Job ID early
        if rag_mode == 'document':
            stream_job_id = f"STREAM_DOC_{user_session_id[:8]}"
        elif rag_mode == 'data_insight':
            stream_job_id = f"STREAM_DI_{user_session_id[:8]}"
        else:
            stream_job_id = f"STREAM_MKT_{user_session_id[:8]}"

        # --- PHASE 1: IMMEDIATE SAVE (The "Promise") ---
        # Save user query NOW. Even if the stream crashes 1ms later, this persists.
        try:
            current_turn_id = await asyncio.to_thread(
                conversation_memory.insert_user_turn,
                user_id=user_id, 
                session_id=user_session_id, 
                query_text=original_query
            )
        except Exception as db_e:
            logger.error(f"{log_prefix} Failed to save initial user turn: {db_e}")
            yield "<p>Gagal menyimpan sesi percakapan.</p>"
            return

        def build_source_cache():
            source_info_for_cache = []
            if rag_mode == 'document':
                for doc_info in context_items_for_memory_update:
                     if isinstance(doc_info, dict) and 'path' in doc_info:
                        source_info_for_cache.append({
                            "name": doc_info.get('title', os.path.basename(doc_info.get('path',''))),
                            "type": get_file_type_from_extension(doc_info.get('path','')),
                            "path": doc_info.get('path'),
                            "relevant_pages": doc_info.get('relevant_pages', []) 
                        })
            elif rag_mode == 'market':
                for provider_name in context_items_for_memory_update:
                    if isinstance(provider_name, str):
                        title_cased = {"xlhome": "XL Home", "firstmedia": "First Media"}.get(provider_name.lower(), provider_name.title())
                        source_info_for_cache.append({
                            "name": f"{title_cased} Services", "type": "data",
                            "path": provider_name.lower(), "relevant_pages": []
                        })
            elif rag_mode == 'data_insight':
                for item in context_items_for_memory_update:
                    if isinstance(item, dict):
                        source_info_for_cache.append({
                            "name": f"Data Insight: {item.get('intent', 'query')}",
                            "type": "data_insight",
                            "path": "data_insight",
                            "relevant_pages": []
                        })
            return source_info_for_cache

        try:
            # Signal the mode to frontend immediately
            yield f"<!-- ROUTED_MODE: {rag_mode} -->"
            # Notify frontend if model was downgraded due to quota
            if quota_fallback_notice:
                yield f'<!-- MODEL_FALLBACK:{{"original":"{quota_fallback_notice}","fallback":"{DEFAULT_LLM_MODEL}","reason":"daily_quota_exceeded"}} -->'
            yield f'<!-- PROGRESS:{{"step": "Memuat riwayat percakapan...", "progress": 10, "type": "init"}} -->'
            
            # Get History
            history_string_for_prompt = await asyncio.to_thread(
                conversation_memory.get_last_query_history_str, 
                user_id, 
                session_id=user_session_id, 
                num_turns_for_prompt=5
            )
            
            # --- SETUP SOURCES BASED ON MODE ---
            if rag_mode == 'document':
                yield f'<!-- PROGRESS:{{"step": "Memuat indeks dokumen...", "progress": 20, "type": "loading_index"}} -->'
                if not document_index: await asyncio.to_thread(refresh_document_index)
                if not document_index: raise RuntimeError("Indeks dokumen tidak tersedia.")
                
                # Fetch & Filter Docs
                unsorted_docs_map = {}
                selected_paths_set = set(selected_source_paths)

                _docs_info = []
                for _de in document_index:
                    _rp = _de.get('relative_path', '')
                    if _rp in selected_paths_set:
                        _docs_info.append({"name": _de.get('title', os.path.basename(_rp))[:50], "ext": os.path.splitext(_rp)[1].lstrip('.')})
                yield f'<!-- PROGRESS:{json.dumps({"step": f"Menganalisis {len(selected_source_paths)} dokumen...", "progress": 30, "type": "docs_found", "docs": _docs_info}, ensure_ascii=False)} -->'
                
                for doc_entry in document_index:
                    if (entry_path := doc_entry.get('relative_path', '')) in selected_paths_set:
                        semantic_res = await asyncio.to_thread(find_relevant_pages_semantic, original_query, doc_entry, _rag_sid=user_session_id[:8])
                        unsorted_docs_map[entry_path] = {
                            'path': entry_path, 'title': doc_entry.get('title', os.path.basename(entry_path)),
                            'document_summary': doc_entry.get('document_summary', ''), 'page_analysis': doc_entry.get('page_analysis', {}),
                            'visual_analysis_summary': doc_entry.get('visual_analysis_summary', {}),
                            'semantically_relevant_pages': semantic_res.get('pages', [])
                        }
                        _pg_count = len(semantic_res.get('pages', []))
                        _doc_title = doc_entry.get('title', os.path.basename(entry_path))
                        _doc_ext = os.path.splitext(entry_path)[1].lstrip('.')
                        yield f'<!-- PROGRESS:{json.dumps({"step": f"Memproses {_doc_title[:40]}...", "progress": 35, "type": "doc_pages", "doc": _doc_title[:50], "ext": _doc_ext, "pages": _pg_count, "status": "done"}, ensure_ascii=False)} -->'

                if not unsorted_docs_map: raise FileNotFoundError("Dokumen yang dipilih tidak ditemukan.")

                # Preserve ranking order
                current_docs = []
                for path in selected_source_paths:
                    if path in unsorted_docs_map:
                        current_docs.append(unsorted_docs_map[path])
                
                context_items_for_memory_update = current_docs

                yield f'<!-- PROGRESS:{{"step": "Menyiapkan model AI...", "progress": 50, "type": "model_ready"}} -->'

                args = (original_query, current_docs, user_id, history_string_for_prompt)
                kwargs = {'stream_response': True, 'job_id_for_logging': stream_job_id, 'session_id': user_session_id, 'model_override': model_override, 'api_key_override': api_key_override}
                llm_generator = generate_document_response_selected(*args, **kwargs)

            elif rag_mode == 'market':
                # Use filters OR detection
                providers = selected_source_paths
                if not providers:
                    yield f'<!-- PROGRESS:{{"step": "Menganalisis query market...", "progress": 25, "type": "init"}} -->'
                    providers = await asyncio.to_thread(detect_isp_names, original_query)

                yield f'<!-- PROGRESS:{json.dumps({"step": f"Menganalisis {len(providers)} provider...", "progress": 30, "type": "providers_found", "providers": providers if isinstance(providers, list) else []}, ensure_ascii=False)} -->'

                yield f'<!-- PROGRESS:{{"step": "Menyiapkan model AI...", "progress": 50, "type": "model_ready"}} -->'

                context_items_for_memory_update = providers
                args = (original_query, providers, history_string_for_prompt, user_id)
                kwargs = {'stream_response': True, 'job_id_for_logging': stream_job_id, 'session_id': user_session_id, 'model_override': model_override, 'api_key_override': api_key_override}
                llm_generator = generate_market_response_selected(*args, **kwargs)

            elif rag_mode == 'data_insight':
                yield f'<!-- PROGRESS:{{"step": "Menganalisis pertanyaan data...", "progress": 15, "type": "di_phase", "phase": "analyzing"}} -->'
                rag_trace(user_session_id[:8], "DI", q=original_query[:80])

                engine = get_data_insight_engine()
                agent = engine.create_analytical_agent()
                context_items_for_memory_update = [{'type': 'data_insight'}]

                # ── Agentic streaming: classify → plan → execute → synthesize ──
                # The agent yields PROGRESS markers + final HTML.
                # We collect the full response for DB save.
                agent_full_html = ""
                try:
                    async for chunk in agent.analyze_streaming(
                        user_query=original_query,
                        session_id=user_session_id,
                        history=history_string_for_prompt
                    ):
                        chunk_str = str(chunk)
                        yield chunk_str
                        # Only accumulate non-progress chunks for DB
                        if not chunk_str.startswith('<!-- PROGRESS:'):
                            agent_full_html += chunk_str

                    # Save final result to DB
                    if agent_full_html.strip():
                        await asyncio.to_thread(
                            conversation_memory.update_bot_response,
                            turn_id=current_turn_id,
                            response_text=agent_full_html,
                            rag_mode='data_insight',
                            context_items=context_items_for_memory_update
                        )
                        logger.info(f"[DataInsight Agent] Turn {current_turn_id} saved ({len(agent_full_html)} chars).")
                    else:
                        await asyncio.to_thread(
                            conversation_memory.update_bot_response,
                            turn_id=current_turn_id,
                            response_text="<p>Analisis tidak menghasilkan data.</p>",
                            rag_mode='data_insight'
                        )
                    rag_trace(user_session_id[:8], "END",
                        mode="data_insight", chunks=0,
                        resp_len=len(agent_full_html),
                        ms=round((time.time() - _t_stream_start) * 1000))
                except Exception as agent_err:
                    logger.error(f"[DataInsight Agent] Turn {current_turn_id} failed: {agent_err}",
                                 exc_info=True)
                    err_html = f"<p>Terjadi kesalahan saat analisis: {agent_err}</p>"
                    yield err_html
                    try:
                        await asyncio.to_thread(
                            conversation_memory.update_bot_response,
                            turn_id=current_turn_id,
                            response_text=err_html,
                            rag_mode='data_insight'
                        )
                    except Exception as save_err:
                        logger.error(f"[DataInsight Agent] Error save also failed: {save_err}",
                                     exc_info=True)
                return

            else:
                yield f"<p>Mode '{rag_mode}' tidak valid.</p>"
                return

        except Exception as setup_exc:
            logger.error(f"{log_prefix} Error during stream setup: {setup_exc}", exc_info=True)
            yield f"<p>Terjadi kesalahan internal saat persiapan: {setup_exc}</p>"
            # Update DB with error
            await asyncio.to_thread(
                conversation_memory.update_bot_response, 
                current_turn_id, 
                f"<p>System Error: {setup_exc}</p>", 
                rag_mode
            )
            return

        # --- STREAM EXECUTION ---
        source_cached = False
        chunk_count = 0
        SAVE_EVERY_N_CHUNKS = 20  # Adjust based on your avg chunk size
        
        try:
            async for chunk in llm_generator:
                chunk_str = str(chunk)
                full_response_accumulator += chunk_str
                chunk_count += 1
                yield chunk_str

                # Incremental save: first save early, then every N chunks
                should_save = (chunk_count == 5) or (chunk_count > 5 and chunk_count % SAVE_EVERY_N_CHUNKS == 0)
                if should_save:
                    clean_partial = re.sub(r'<!--.*?-->', '', full_response_accumulator, flags=re.DOTALL).strip()
                    if clean_partial:
                        asyncio.create_task(asyncio.to_thread(
                            conversation_memory.update_bot_response,
                            turn_id=current_turn_id,
                            response_text=clean_partial,
                            rag_mode=rag_mode,
                            context_items=context_items_for_memory_update
                        ))

                if not source_cached and full_response_accumulator:
                    try:
                        source_info = build_source_cache()
                        source_cache_key = f"{user_id}:{user_session_id}:{original_query}"
                        with job_storage_lock:
                            job_storage.setdefault('source_stream_cache', {})[source_cache_key] = {
                                'data': source_info,
                                'timestamp': time.time()
                            }
                        source_cached = True
                    except Exception: pass
            
            # --- PHASE 2: SUCCESSFUL FINISH ---
            rag_trace(user_session_id[:8], "END",
                mode=rag_mode, chunks=chunk_count,
                resp_len=len(full_response_accumulator),
                ms=round((time.time() - _t_stream_start) * 1000))
            # The stream finished normally. Save the complete answer to DB.
            # 0. Extract thinking text from THINKING markers before stripping
            thinking_markers = re.findall(r'<!-- THINKING:([\w+/=]+) -->', full_response_accumulator)
            accumulated_thinking_text = None
            if thinking_markers:
                try:
                    decoded_parts = [base64.b64decode(m).decode('utf-8') for m in thinking_markers]
                    accumulated_thinking_text = ''.join(decoded_parts)
                except Exception as e:
                    logger.warning(f"{log_prefix} Failed to decode thinking markers: {e}")

            # 1. Clean response (remove metadata tags if needed)
            # Strip all HTML comments (progress markers, metadata, etc.) before cleaning
            stripped_text = re.sub(r'<!--.*?-->', '', full_response_accumulator, flags=re.DOTALL).strip()
            clean_text = clean_response(stripped_text)
            
            # 2. Extract market sources if in market mode (optional enhancement)
            market_srcs = None
            if rag_mode == 'market' and '<!-- STREAM_END_METADATA' in full_response_accumulator:
                try:
                    # Quick regex to grab metadata json if present for market sources
                    match = re.search(r'<!-- STREAM_END_METADATA:(.*?) -->', full_response_accumulator)
                    if match:
                        meta = json.loads(match.group(1))
                        market_srcs = meta.get('market_sources')
                except: pass

            # 3. Create Embedding (Optional, prevents blocking)
            current_embedding = None
            try:
                emb_res = await asyncio.to_thread(
                    genai.embed_content,
                    model="models/gemini-embedding-001",
                    content=original_query,
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=768
                )
                current_embedding = emb_res['embedding']
            except: pass

            # 4. Update the row we created in Phase 1
            await asyncio.to_thread(
                conversation_memory.update_bot_response,
                turn_id=current_turn_id,
                response_text=clean_text,
                rag_mode=rag_mode,
                context_items=context_items_for_memory_update,
                query_embedding=current_embedding,
                market_sources=market_srcs,
                thinking_text=accumulated_thinking_text
            )
        
        except asyncio.CancelledError:
            logger.warning(f"{log_prefix} Client disconnected (Refresh). Saving partial response.")

            # Hard-stop: kill the MySQL query still running in the worker thread
            try:
                _sql_gen = get_data_insight_engine().get_sql_generator()
                await asyncio.to_thread(_sql_gen.kill_session_query, user_session_id)
            except Exception as _e:
                logger.error(f"[HardStop] kill_session_query error: {_e}")

            # Strip progress/metadata comments from accumulated text
            partial_text = re.sub(r'<!--.*?-->', '', full_response_accumulator, flags=re.DOTALL).strip()
            
            if not partial_text:
                partial_text = "[Percakapan terputus sebelum jawaban dihasilkan]"
            
            if current_turn_id is not None:
                try:
                    await asyncio.shield(asyncio.to_thread(
                        conversation_memory.update_bot_response,
                        turn_id=current_turn_id,
                        response_text=partial_text,
                        rag_mode=rag_mode,
                        context_items=context_items_for_memory_update
                    ))
                except asyncio.CancelledError:
                    pass

            raise

        except Exception as stream_ex:
            logger.error(f"{log_prefix} Exception during streaming generator: {stream_ex}", exc_info=True)
            yield "<p>Terjadi kesalahan internal saat streaming respons.</p>"
            
            # Save error state to DB
            await asyncio.to_thread(
                conversation_memory.update_bot_response,
                turn_id=current_turn_id,
                response_text=clean_response(full_response_accumulator) + f"\n<br><em>Error: {str(stream_ex)}</em>",
                rag_mode=rag_mode
            )

    async def request_scoped_streamer():
        try:
            async for chunk in combined_streamer():
                yield chunk
        finally:
            # reset() requires the original context, but the streaming generator
            # runs in a different one. Just set back to None instead.
            if api_key_token is not None:
                CURRENT_REQUEST_GEMINI_API_KEY.set(None)

    return StreamingResponse(request_scoped_streamer(), media_type="text/html; charset=utf-8")


# Ensure /api/get_sources uses the same cache key structure and lock
@api_router.post("/api/get_sources")
async def get_sources(payload: GetSourcesRequest):
    """
    Retrieves cached sources for a specific chat session.
    Converted to FastAPI with Pydantic validation.
    """
    # Pydantic ensures these fields exist, so no manual check needed
    user_id = payload.user_id
    session_id = payload.session_id
    query = payload.query
    
    sources = []
    source_cache_key = f"{user_id}:{session_id}:{query}"
    
    # Accessing in-memory dict is fast, safe to do directly
    # Note: job_storage and job_storage_lock must be global variables
    with job_storage_lock: 
        if 'source_stream_cache' in job_storage:
            cached_entry = job_storage['source_stream_cache'].get(source_cache_key)
            
            # Handle both new format (dict with data/timestamp) and potential legacy format
            if cached_entry:
                if isinstance(cached_entry, dict) and 'data' in cached_entry:
                    sources = cached_entry['data']
                    # Optional: Refresh timestamp on access (LRU style)
                    cached_entry['timestamp'] = time.time() 
                elif isinstance(cached_entry, list):
                    # Fallback for any old data lingering in memory
                    sources = cached_entry
    
    logger.debug(f"[GetSourcesAPI] Retrieving sources for key '{source_cache_key}', found: {len(sources)}")
    
    return {"sources": sources, "session_id": session_id}


# ==========================================
# 🛡️ SECURITY MIDDLEWARE (INITIALIZATION)
# ==========================================
# This decorator registers the function to run on every request
@async_app.middleware("http") 
async def ban_mystery_caller(request: Request, call_next):
    
    # 1. Define the IP causing the "geospatial" log spam
    BANNED_IP = "10.98.0.156" 

    # 2. Check if the request comes from that IP
    if request.client.host == BANNED_IP:
        # 3. Check if they are accessing the missing path
        if "geospatial" in request.url.path:
            logger.warning(f"⛔ BLOCKED suspicious request from {BANNED_IP} to {request.url.path}")
            # Stop the request here (return 403 Forbidden)
            return Response(content="Access Denied", status_code=403)

    # 4. Otherwise, let the request pass through to your app
    return await call_next(request)


# ============================================================================
# FASTAPI: STATIC PAGES & HTML SERVING
# ============================================================================

@api_router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serves the main HTML page using FastAPI Templates."""
    frontend_build = "index.html consolidated-v1"
    logger.info(f"[BYOK-DEBUG] Rendering template: {frontend_build} via FastAPI (single template architecture)")
    return templates.TemplateResponse("index.html", {"request": request, "frontend_build": frontend_build})


@api_router.get("/multi_page_viewer/{relative_filename:path}", response_class=HTMLResponse)
async def multi_page_viewer(request: Request, relative_filename: str, pages: str = Query(None)):
    """Serves the PDF viewer page."""
    # Decode filename
    decoded_relative_filename = unquote(relative_filename)
    logger.info(f"Multi-page viewer request: '{decoded_relative_filename}'")

    # Security Check
    if ".." in decoded_relative_filename or decoded_relative_filename.startswith("/"):
        logger.warning(f"Path traversal blocked: {decoded_relative_filename}")
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Existence Check (Non-blocking check is hard, using thread for safety on slow disks)
    full_local_path = os.path.join(POLICY_DIR, decoded_relative_filename)
    exists = await asyncio.to_thread(os.path.exists, full_local_path)
    
    if not exists:
        raise HTTPException(status_code=404, detail="Document not found")
    if not decoded_relative_filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files supported")

    # Parse Pages
    relevant_pages = []
    if pages:
        try:
            relevant_pages = sorted([int(p) for p in pages.split(',') if p.strip().isdigit() and int(p) > 0])
        except ValueError:
            pass

    return templates.TemplateResponse(
        "multi_page_viewer.html",
        {
            "request": request,
            "document_filename": decoded_relative_filename,
            "relevant_pages_json": json.dumps(relevant_pages)
        }
    )


@api_router.get("/api/view_page/{relative_filename:path}/{page_number}")
async def view_page_local_endpoint(relative_filename: str, page_number: int):
    """
    FastAPI endpoint that serves a specific PDF page as a PNG image.
    Connects the HTML frontend to the _extract_and_encode_pdf_page_local helper.
    """
    # 1. Decode the URL-encoded filename (e.g., "Folder%2FFile.pdf" -> "Folder/File.pdf")
    decoded_filename = unquote(relative_filename)

    # Defensive cleanup
    if "\\" in decoded_filename:
        decoded_filename = decoded_filename.replace("\\", "")

    # 2. Security Check (Prevent hacking via "../")
    if ".." in decoded_filename or decoded_filename.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    # 3. Call your existing helper function
    base64_image = await asyncio.to_thread(
        _extract_and_encode_pdf_page_local, 
        decoded_filename, 
        page_number
    )

    # 4. Handle the result
    if base64_image:
        try:
            # Your helper returns Base64 (string), but the Browser <img> tag wants Raw Bytes.
            # We simply decode it back here.
            image_bytes = base64.b64decode(base64_image)
            
            # Return as a standard image response
            return Response(content=image_bytes, media_type="image/png")
        except Exception as e:
            logger.error(f"Error decoding base64 from helper: {e}")
            raise HTTPException(status_code=500, detail="Image processing error")
    else:
        # If helper returns None, it means the page or file doesn't exist
        raise HTTPException(status_code=404, detail="Page not found")


# --- Source Peeker Endpoint (Perplexity-style) ---
@api_router.get("/source_peek/{relative_filename:path}", response_class=HTMLResponse)
async def source_peek(request: Request, relative_filename: str, page: int = Query(1, ge=1)):
    """
    Dedicated endpoint for the 'Citation Peeking' UX.
    Renders the 'source_peek.html' template.
    """
    # 1. Decode and Validate
    decoded_filename = unquote(relative_filename)

    # Always use the /rajawaliai prefix for asset URLs in the template.
    # This works in ALL environments because the app registers routes at both
    # "/" (unprefixed) and "/rajawaliai" (prefixed) via include_router.
    # - Prod (nginx strips prefix): browser requests /rajawaliai/api/... → nginx proxies it
    # - Dev (direct access): browser requests /rajawaliai/api/... → hits prefixed router
    dynamic_prefix = request.scope.get("root_path", "") or "/rajawaliai"

    # Defensive cleanup
    if "\\" in decoded_filename:
        decoded_filename = decoded_filename.replace("\\", "")

    if ".." in decoded_filename or decoded_filename.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid filename security check")

    # 2. Check Existence (Optional, but good for speed)
    full_local_path = os.path.join(POLICY_DIR, decoded_filename)
    exists = await asyncio.to_thread(os.path.exists, full_local_path)

    if not exists:
        return HTMLResponse(content="<h3>Document not found on server.</h3>", status_code=404)

    # 3. Render Template
    # We explicitly pass the prefix so the HTML knows where to find the API
    return templates.TemplateResponse(
        "source_peek.html",
        {
            "request": request,
            "filename": os.path.basename(decoded_filename),
            "quoted_filename": quote(decoded_filename),
            "page": page,
            "api_prefix": dynamic_prefix
        }
    )

# --- Channel Message Peek Endpoint ---
@api_router.get("/api/channel/message/{message_id}", response_class=HTMLResponse)
async def channel_message_peek(request: Request, message_id: int):
    """
    Fetches a channel message by ID and renders it in a peeker modal.
    Used for citation peeking of Telegram channel sources.
    """
    if not tbl_channel_messages:
        return HTMLResponse(content="<h3>Channel database unavailable.</h3>", status_code=503)

    try:
        # Use a zero vector for search (we only care about filtering by message_id)
        # The embedding dimension is 768 based on EMBEDDING_MODEL
        zero_vec = [0.0] * 768
        results = tbl_channel_messages.search(zero_vec).where(f"message_id = {message_id}").limit(1).to_list()

        if not results:
            return HTMLResponse(content=f"<h3>Message {message_id} not found.</h3>", status_code=404)

        msg = results[0]

        # Check for media file
        downloaded_media_dir = "/home/rajawalia3/radityayud/python_project/rajawali_intelligence/downloaded_media"
        media_url = None

        # First check if LanceDB has a media_path field
        media_path = msg.get('media_path') or msg.get('file_path') or msg.get('image_path')
        if media_path and os.path.exists(media_path):
            media_url = f"/rajawaliai/api/channel/media/{message_id}"
        elif os.path.isdir(downloaded_media_dir):
            # Check downloaded_media directory for files matching message_id
            for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.JPG', '.JPEG', '.PNG']:
                potential_file = os.path.join(downloaded_media_dir, f"{message_id}{ext}")
                if os.path.exists(potential_file):
                    media_url = f"/rajawaliai/api/channel/media/{message_id}"
                    break

        # Dynamic prefix for template
        dynamic_prefix = request.scope.get("root_path", "") or "/rajawaliai"

        return templates.TemplateResponse(
            "channel_peek.html",
            {
                "request": request,
                "message_id": msg.get('message_id', 'N/A'),
                "message_summary": msg.get('message_summary', ''),
                "content_for_rag": msg.get('content_for_rag', ''),
                "timestamp": msg.get('timestamp', 'N/A'),
                "media_url": media_url,
                "api_prefix": dynamic_prefix
            }
        )

    except Exception as e:
        logging.warning(f"Channel message peek failed: {e}")
        return HTMLResponse(content=f"<h3>Error fetching message: {e}</h3>", status_code=500)


# --- Channel Media Endpoint ---
@api_router.get("/api/channel/media/{message_id}")
async def channel_media_endpoint(message_id: int):
    """
    Serves media files for channel messages from downloaded_media directory.
    """
    from fastapi.responses import FileResponse

    downloaded_media_dir = "/home/rajawalia3/radityayud/python_project/rajawali_intelligence/downloaded_media"

    if not os.path.isdir(downloaded_media_dir):
        raise HTTPException(status_code=404, detail="Media directory not found")

    # Look for files matching message_id with various extensions
    for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.JPG', '.JPEG', '.PNG', '.GIF', '.WEBP']:
        potential_file = os.path.join(downloaded_media_dir, f"{message_id}{ext}")
        if os.path.exists(potential_file):
            media_type = "image/jpeg" if ext.lower() in ['.jpg', '.jpeg'] else f"image/{ext[1:].lower()}"
            return FileResponse(potential_file, media_type=media_type)

    raise HTTPException(status_code=404, detail="Media file not found")


# --- /api/search_sources (Modified for Local) ---
@api_router.get("/api/search_sources")
async def search_sources_api(
    query: str = Query(..., min_length=1),
    mode: str = Query("document", regex="^(document|market)$"), 
    limit: int = Query(20, ge=1, le=50)
):
    """Search sources (Async wrapper for CPU-bound search)."""
    try:
        # String matching (fuzzy search) is CPU bound, so we offload to thread
        results = await asyncio.to_thread(find_top_keyword_sources, query, mode, limit)
        
        # Add market logic if needed (Ported from your previous logic)
        if mode == 'market' and len(results) < limit:
            # This is lightweight enough to run here or can be moved to the helper
            pass 
            
        return results
    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Search failed")

   
def allowed_file(filename):
    """Checks if the file extension is allowed."""
    ALLOWED_EXTENSIONS = {'pdf'}
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS    


    
    
# Ensure the directory exists
os.makedirs("generated_charts", exist_ok=True)
os.makedirs(os.path.join("generated_charts", "data_insight"), exist_ok=True)

# Mount the static files to 'async_app' (FastAPI), NOT 'app' (Flask).
# We mount both paths to ensure it works regardless of how your reverse proxy handles the prefix.
# This automatically handles serving the files, so no manual serve_chart function is needed.
async_app.mount("/generated_charts", StaticFiles(directory="generated_charts"), name="generated_charts")
async_app.mount("/rajawaliai/generated_charts", StaticFiles(directory="generated_charts"), name="generated_charts_prefixed")

# Optional: Also add a route to list available charts (for debugging)
@api_router.get("/debug/charts")
async def list_charts():
    """Debug endpoint to list generated charts"""
    charts_dir = os.path.join(os.getcwd(), "generated_charts")
    if os.path.exists(charts_dir):
        charts = [f for f in os.listdir(charts_dir) if f.endswith('.png')]
        return {"charts": charts, "count": len(charts)}
    else:
        return {"charts": [], "count": 0}


@api_router.get("/api/chat_result/{job_id}")
async def async_chat_result(job_id: str = Path(..., title="The ID of the job to retrieve.")):
    """
    (FastAPI Version) Polls the database for the result of a submitted job.
    Uses the async DB pool to prevent blocking the event loop.
    """
    global conversation_memory, async_db_pool

    job_details = None
    try:
        if async_db_pool:
            async with async_db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(
                        "SELECT job_id, convo_status, input_params, result, thinking_step, progress_percentage FROM conversation_async_jobs WHERE job_id = %s",
                        (job_id,)
                    )
                    job_details = await cursor.fetchone()
        else:
            def _fetch_job_sync():
                conn = get_db_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        SELECT job_id, convo_status, input_params, result, thinking_step, progress_percentage
                        FROM conversation_async_jobs
                        WHERE job_id = ?
                        """,
                        (job_id,)
                    )
                    row = cursor.fetchone()
                    return dict(row) if row else None
                finally:
                    conn.close()

            job_details = await asyncio.to_thread(_fetch_job_sync)
    except Exception as db_err:
        logger.error(f"DB error fetching job {job_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail={"job_id": job_id, "status": "error", "message": "Gagal mengambil status pekerjaan."})

    if not job_details:
        raise HTTPException(status_code=404, detail={"job_id": job_id, "status": "not_found", "message": "Pekerjaan tidak ditemukan."})

    input_params = {}
    user_session_id_for_response = None
    if raw_input_params := job_details.get('input_params'):
        try:
            input_params = json.loads(raw_input_params)
            user_session_id_for_response = input_params.get('session_id')
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail={"job_id": job_id, "status": "error", "message": "Kesalahan data pekerjaan internal."})

    convo_status = job_details.get('convo_status')
    if convo_status in ['pending', 'running']:
        return {
            "job_id": job_id, "status": convo_status,
            "message": job_details.get('thinking_step') or "Jawaban Anda masih diproses...",
            "session_id": user_session_id_for_response,
            "thinking_step": job_details.get('thinking_step') or "⏳ Processing...",
            "progress_percentage": job_details.get('progress_percentage') or 0
        }

    raw_job_result = job_details.get('result')
    if convo_status == 'failed':
        error_message = "Terjadi kegagalan saat memproses permintaan Anda."
        if raw_job_result:
            try:
                error_data = json.loads(raw_job_result)
                error_message = error_data.get('error', error_message)
            except (json.JSONDecodeError, AttributeError):
                error_message = raw_job_result if isinstance(raw_job_result, str) else error_message
        return {
            "job_id": job_id, "status": "error", "message": error_message, "sources": [], "suggestions": [],
            "session_id": user_session_id_for_response, "thinking_step": "❌ Processing failed", "progress_percentage": 0
        }

    if convo_status == 'finished':
        if not raw_job_result:
            raise HTTPException(status_code=500, detail={"job_id": job_id, "status": "error", "message": "Hasil pekerjaan tidak lengkap."})
        try:
            job_result_data = json.loads(raw_job_result)
        except json.JSONDecodeError:
             raise HTTPException(status_code=500, detail={"job_id": job_id, "status": "error", "message": "Format hasil pekerjaan tidak valid."})


        final_answer_html = job_result_data.get('answer_html', "<p>Tidak ada jawaban yang dihasilkan.</p>")
        final_tabulator_json = job_result_data.get('tabulator_json')

        rag_mode_from_job = input_params.get('rag_mode')

        final_sources_list = []
        
        context_items_for_memory = job_result_data.get('context_items_for_memory', [])
        if rag_mode_from_job == 'document':
            for doc_info in context_items_for_memory:
                if isinstance(doc_info, dict) and (path_value := doc_info.get('path')):
                    final_sources_list.append({"name": doc_info.get('title', os.path.basename(path_value)), "type": get_file_type_from_extension(path_value), "path": path_value, "relevant_pages": doc_info.get('relevant_pages', [])})
        elif rag_mode_from_job == 'market':
            for provider_name in context_items_for_memory:
                if isinstance(provider_name, str):
                    title_cased = {"xlhome": "XL Home", "firstmedia": "First Media"}.get(provider_name.lower(), provider_name.title())
                    final_sources_list.append({"name": f"{title_cased} Services", "type": "data", "path": provider_name.lower(), "relevant_pages": []})

        # Extract suggestions from the saved job result
        final_suggestions = job_result_data.get('next_suggestions', [])

        response_payload = {
            "job_id": job_id, 
            "status": "success", 
            "answer": final_answer_html, 
            "sources": final_sources_list,
            "next_suggestions": final_suggestions, 
            "suggestions": final_suggestions, # Keep both keys for compatibility
            "session_id": user_session_id_for_response,
            "thinking_step": "✅ Complete!", 
            "progress_percentage": 100
        }
        if final_tabulator_json:
            response_payload["tabulator_data"] = final_tabulator_json

        # Add market sources if available
        market_sources_from_result = job_result_data.get('market_sources')
        if market_sources_from_result:
            response_payload["market_sources"] = market_sources_from_result

        return response_payload

    raise HTTPException(status_code=500, detail={"job_id": job_id, "status": "error", "message": f"Status pekerjaan tidak diketahui: {convo_status}"})


# --- Market Source API Call ---
@api_router.get("/api/market/source/thumbnail/{filename:path}")
async def get_market_source_thumbnail(filename: str):
    """
    Serve thumbnail version of market source image for grid display.
    
    Args:
        filename: Relative path to the source file
        
    Returns:
        Thumbnail image as JPEG
    """
    logger.info(f"Thumbnail request for market source: {filename}")
    
    try:
        full_path = validate_source_path(filename)
        thumbnail_bytes = resize_image(full_path, THUMBNAIL_SIZE, maintain_aspect_ratio=True)
        
        return Response(
            content=thumbnail_bytes,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=3600"}  # Cache for 1 hour
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error serving thumbnail for {filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    

@api_router.get("/api/market/source/full/{filename:path}")
async def get_market_source_full(filename: str):
    """
    Serve full-size version of market source image for detailed viewing.
    
    Args:
        filename: Relative path to the source file
        
    Returns:
        Full-size image (resized if too large)
    """
    logger.info(f"Full-size request for market source: {filename}")
    
    try:
        full_path = validate_source_path(filename)
        
        # Check original image size
        with Image.open(full_path) as img:
            original_size = img.size
            
        # Only resize if image is too large
        if original_size[0] > MAX_IMAGE_SIZE[0] or original_size[1] > MAX_IMAGE_SIZE[1]:
            image_bytes = resize_image(full_path, MAX_IMAGE_SIZE, maintain_aspect_ratio=True)
            media_type = "image/jpeg"
        else:
            # Serve original file
            with open(full_path, 'rb') as f:
                image_bytes = f.read()
            
            # Determine media type based on file extension
            ext = os.path.splitext(filename)[1].lower()
            media_type_map = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.webp': 'image/webp'
            }
            media_type = media_type_map.get(ext, 'image/jpeg')
        
        return Response(
            content=image_bytes,
            media_type=media_type,
            headers={"Cache-Control": "public, max-age=7200"}  # Cache for 2 hours
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error serving full image for {filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@api_router.get("/api/market/source/download/{filename:path}")
async def download_market_source(filename: str):
    """
    Download market source image file.
    
    Args:
        filename: Relative path to the source file
        
    Returns:
        File download response
    """
    logger.info(f"Download request for market source: {filename}")
    
    try:
        full_path = validate_source_path(filename)
        
        def iter_file(file_path: str):
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    yield chunk
        
        # Get file size for content-length header
        file_size = os.path.getsize(full_path)
        
        return StreamingResponse(
            iter_file(full_path),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename=\"{os.path.basename(filename)}\"",
                "Content-Length": str(file_size)
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error downloading {filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# --- View Page Route (Serves single page image from Local) ---
@api_router.get("/api/view_page/{relative_filename:path}/{page_number}")
async def view_page_local(relative_filename: str, page_number: int):
    """
    Serves a single PDF page as an image.
    Wraps the synchronous Fitz (PyMuPDF) extraction in a thread.
    """
    decoded_relative_filename = unquote(relative_filename)
    
    if ".." in decoded_relative_filename or page_number <= 0:
        raise HTTPException(status_code=400, detail="Invalid request parameters")

    # Run CPU-heavy image extraction in thread pool
    # Note: _extract_and_encode_pdf_page_local returns base64 string
    base64_png = await asyncio.to_thread(
        _extract_and_encode_pdf_page_local, 
        decoded_relative_filename, 
        page_number
    )

    if not base64_png:
        raise HTTPException(status_code=404, detail="Page not found or extraction failed")

    try:
        # Decode back to bytes for efficient serving
        image_bytes = base64.b64decode(base64_png)
        return Response(content=image_bytes, media_type="image/png")
    except Exception as e:
        logger.error(f"Error serving page image: {e}")
        raise HTTPException(status_code=500, detail="Image processing error")


# --- Download Route ---
@api_router.get("/api/download/{relative_filename:path}")
async def download_file_local(relative_filename: str):
    """Downloads file using FastAPI FileResponse (Optimized streaming)."""
    decoded_relative_filename = unquote(relative_filename)
    
    if ".." in decoded_relative_filename or decoded_relative_filename.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    full_local_path = os.path.abspath(os.path.join(POLICY_DIR, decoded_relative_filename))
    abs_policy_dir = os.path.abspath(POLICY_DIR)

    if not full_local_path.startswith(abs_policy_dir):
        logger.critical(f"Security Alert: Download traversal attempt: {full_local_path}")
        raise HTTPException(status_code=403, detail="Access denied")

    exists = await asyncio.to_thread(os.path.exists, full_local_path)
    if not exists:
        raise HTTPException(status_code=404, detail="File not found")

    # FileResponse handles ranges and streaming automatically
    return FileResponse(
        path=full_local_path, 
        filename=os.path.basename(decoded_relative_filename),
        media_type='application/octet-stream'
    )


# --- File Upload Endpoint ---
@api_router.post("/api/upload_files")
async def upload_files(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...), 
    uploadDate: str = Form(...)
):
    """Handles file uploads and schedules background ingestion."""
    
    # 1. Validate Date
    try:
        dt = datetime.strptime(uploadDate, '%Y-%m-%d')
        date_prefix = dt.strftime('%Y%m%d')
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid date format. Use YYYY-MM-DD."})

    # 2. Validate Directory
    if not POLICY_DIR or not os.path.isdir(POLICY_DIR):
        return JSONResponse(status_code=500, content={"error": "Server config error: POLICY_DIR invalid."})

    uploaded_files_info = []
    errors = []

    # 3. Process Files
    for file in files:
        if not file.filename.lower().endswith('.pdf'):
            errors.append({"original": file.filename, "error": "Only PDF files allowed"})
            continue
            
        try:
            original_filename = os.path.basename(file.filename)
            safe_filename = "".join(c for c in original_filename if c.isalnum() or c in (' ', '.', '_', '-')).strip().replace(' ', '_')
            new_filename = f"{date_prefix}_{safe_filename}"
            save_path = os.path.join(POLICY_DIR, new_filename)

            # 4. Handle Collisions
            if os.path.exists(save_path):
                counter = 1
                base, ext = os.path.splitext(new_filename)
                while os.path.exists(save_path):
                    new_filename = f"{base}_{counter}{ext}"
                    save_path = os.path.join(POLICY_DIR, new_filename)
                    counter += 1

            # 5. Save File (Async)
            async with aiofiles.open(save_path, 'wb') as out_file:
                while content := await file.read(1024 * 1024):
                    await out_file.write(content)
            
            # 6. TRIGGER BACKGROUND TASK Async
            background_tasks.add_task(run_ingestion_task, save_path)
            
            logger.info(f"Saved '{original_filename}' as '{new_filename}'. Scheduled ingestion.")
            uploaded_files_info.append({"original": original_filename, "saved_as": new_filename})

        except Exception as e:
            logger.error(f"Failed to save {file.filename}: {e}", exc_info=True)
            errors.append({"original": file.filename, "error": str(e)})
        finally:
            await file.close()

    # 7. Response
    if not uploaded_files_info and errors:
        status_code = 500
    elif errors:
        status_code = 207
    else:
        status_code = 200

    return JSONResponse(
        status_code=status_code,
        content={
            "message": "Upload successful. AI processing started in background.",
            "uploaded": uploaded_files_info,
            "errors": errors
        }
    )


# --- Login/Logout Handler ---
# PORTFOLIO DEMO: Mock authentication (replaces LDAP)
def authenticate_with_ldap(username: str, password: str) -> dict:
    """
    Mock authentication for portfolio demo.
    Accepts demo users: demo/demo123, reviewer/reviewer123, test/test123
    """
    # Validate input length
    if len(username) < 4 or len(username) > 42 or len(password) < 4 or len(password) > 42:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username and password must be between 4 and 42 characters"
        )

    # Check against demo users
    if username in DEMO_USERS and DEMO_USERS[username]['password'] == password:
        full_name = DEMO_USERS[username]['display_name']
        logger.info(f"✅ Mock auth successful for: {username}")
        return {
            "username": username,
            "name": full_name,
            "email": f"{username}@portfolio.demo"
        }

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password"
    )

def generate_session_token() -> str:
    """Generate secure session token using built-in libraries"""
    random_bytes = secrets.token_bytes(32)
    timestamp = str(time.time()).encode()
    combined = random_bytes + timestamp
    return base64.urlsafe_b64encode(hashlib.sha256(combined).digest()).decode()


def create_session_in_db(user_data: dict) -> tuple[str, datetime]:
    """Store session and return token AND expiry time"""
    session_token = generate_session_token()
    expire_time = datetime.now() + timedelta(minutes=SESSION_TIMEOUT_MINUTES)

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO user_sessions (session_token, username, user_email, user_name, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (session_token, user_data["username"], user_data["email"], user_data["name"], datetime.now(), expire_time))
        conn.commit()
    finally:
        if conn: conn.close()

    return session_token, expire_time


def get_user_from_db_session(session_token: str) -> Optional[str]:
    """Get username from database session and check if valid"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Get session and update last_accessed
        cursor.execute("""
            SELECT username FROM user_sessions
            WHERE session_token = ? AND expires_at > datetime('now')
        """, (session_token,))

        result = cursor.fetchone()
        if result:
            # Update last accessed time
            cursor.execute("""
                UPDATE user_sessions
                SET last_accessed = datetime('now')
                WHERE session_token = ?
            """, (session_token,))
            conn.commit()
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Error checking session: {e}")
        return None
    finally:
        if conn: conn.close()

def invalidate_session(session_token: str) -> bool:
    """Remove session from database"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM user_sessions WHERE session_token = ?", (session_token,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error invalidating session: {e}")
        return False
    finally:
        if conn: conn.close()

@api_router.post("/api/login")
async def login(request: LoginRequest):
    username = request.username
    password = request.password
    try:
        user_data = authenticate_with_ldap(username, password)
        
        # Updated to receive expiry time
        session_token, expires_at = create_session_in_db(user_data)
        
        return {
            "status": "success",
            "session_token": session_token,
            "expires_at": expires_at.isoformat(), # Added for auto-logout
            "user_info": user_data,
            "expires_in_minutes": SESSION_TIMEOUT_MINUTES
        }
        
    except HTTPException as http_exc:
        # Re-raise LDAP authentication errors
        logger.warning(f"Login failed for user {username}: {http_exc.detail}")
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during login for {username}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Terjadi kesalahan sistem saat login"
        )

@api_router.post("/api/logout")
async def logout(
    current_user: str = Depends(get_current_user),
    authorization: Optional[str] = Header(None)
):
    """
    Logout user and invalidate session
    """
    try:
        if authorization and authorization.startswith("Bearer "):
            session_token = authorization.replace("Bearer ", "")
            success = invalidate_session(session_token)
            
            if success:
                logger.info(f"User {current_user} successfully logged out")
                return {
                    "status": "success",
                    "message": "Logout berhasil"
                }
            else:
                logger.warning(f"Failed to invalidate session for user {current_user}")
        
        return {
            "status": "success", 
            "message": "Logout berhasil"
        }
        
    except HTTPException:
        # Even if there's an auth error, we can still return success for logout
        return {
            "status": "success",
            "message": "Logout berhasil"
        }
    except Exception as e:
        logger.error(f"Error during logout: {e}", exc_info=True)
        return {
            "status": "success",
            "message": "Logout berhasil"
        }


@api_router.get("/api/check-auth")
async def check_authentication_status(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401)

    token = authorization.split(" ")[1]
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT username, user_name, user_email, expires_at
            FROM user_sessions
            WHERE session_token = ? AND expires_at > datetime('now')
        """, (token,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=401)

        return {
            "authenticated": True,
            "user": {"username": row[0], "name": row[1], "email": row[2]},
            "expires_at": row[3]
        }
    finally:
        if conn: conn.close()


# ===============================================
# API KEY ENDPOINT (Portfolio Demo)
# ===============================================
@api_router.get("/api/demo-key")
async def get_demo_key():
    """
    Return demo API key if configured.
    This allows reviewers without their own API key to test the demo.
    """
    demo_key = os.environ.get("GEMINI_API_KEY_DEMO")
    if demo_key:
        return {"key": demo_key}
    return {"error": "Demo key not configured"}


# 1. Support the new prefixed paths (Fixes your 404)
async_app.include_router(api_router, prefix="/rajawaliai")

# 2. Support the original paths (Backward Compatibility)
async_app.include_router(api_router)

logger.info("Mounting legacy Flask app into the main FastAPI application.")
