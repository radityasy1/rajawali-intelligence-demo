import os
import io
import json
import time
import logging
import mimetypes
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
from datetime import datetime, timezone
import sys
import sqlite3
import google.generativeai as genai
from google.generativeai.types import GenerationConfig
import google.api_core.exceptions as google_exceptions
import lancedb
import pyarrow as pa

# Attempt to import colorlog for colored logging
try:
    import colorlog
except ImportError:
    colorlog = None
    print("Warning: colorlog library not found. Console logs will not be colored. "
          "Install with 'pip install colorlog'")

# Attempt to import httplib2 for proxy configuration
try:
    import httplib2
    try:
        ProxyTypes = httplib2.ProxyTypes
    except AttributeError:
        ProxyTypes = None
        # This initial logging will use the default basicConfig if setup_logging isn't called yet
        # or will be captured by the new logger if setup_logging is called before this.
        logging.warning("httplib2.ProxyTypes not found. Proxy configuration via client_options may be limited.")
except ImportError:
    httplib2 = None
    ProxyTypes = None
    logging.warning("httplib2 not found. Explicit proxy configuration via client_options may not work. "
                    "Install with 'pip install httplib2'.")

# --- Load Environment Variables FIRST ---
load_dotenv()

# --- Enhanced Logging Setup ---
def setup_logging(log_level_str="INFO", log_file="pipeline.log"):
    """
    Sets up sophisticated logging with colored console output and rotating file logs.
    """
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicate messages
    # or conflicts if basicConfig was called implicitly.
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # General log format
    log_format_str = "%(asctime)s - %(name)s - %(levelname)-8s - [%(filename)s:%(lineno)d - %(funcName)s()] - %(message)s"
    
    # Console Handler (Colored)
    if colorlog:
        console_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s - %(name)s - %(levelname)-8s%(reset)s - [%(filename)s:%(lineno)d - %(funcName)s()] - %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S',
            reset=True,
            log_colors={
                'DEBUG':    'cyan',
                'INFO':     'green',
                'WARNING':  'yellow',
                'ERROR':    'red',
                'CRITICAL': 'red,bg_white',
            },
            secondary_log_colors={},
            style='%'
        )
    else:
        console_formatter = logging.Formatter(log_format_str, datefmt='%Y-%m-%d %H:%M:%S')
        
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File Handler (Rotating)
    # Ensure 'logs' directory exists or adjust path as needed
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file_path = log_dir / log_file

    file_formatter = logging.Formatter(log_format_str, datefmt='%Y-%m-%d %H:%M:%S')
    # Rotate logs at 10MB, keep 5 backup files
    file_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    logging.info("Logging setup complete. Console and file handlers configured.")

# Call logging setup as early as possible
setup_logging(log_level_str=os.getenv("LOG_LEVEL", "INFO"), log_file="summarizer_pipeline.log")

# Get a logger for the current module
logger = logging.getLogger(__name__)


# --- Configuration ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_API_KEY_2 = os.getenv("GEMINI_API_KEY_2")
GEMINI_MODEL = os.getenv("GEMINI_PROCESSING_MODEL", "gemini-2.5-flash-lite") # Using a more recent model
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DEFAULT_POLICY_DIR = os.path.join(DATA_DIR, "synthetic", "documents")


def _resolve_demo_dir(env_value: str | None, fallback_dir: str) -> str:
    if env_value:
        normalized = env_value.strip()
        if os.name == "nt" and normalized.startswith("/"):
            return fallback_dir
        return normalized
    return fallback_dir


POLICY_DIR = _resolve_demo_dir(os.getenv("POLICY_DIR"), DEFAULT_POLICY_DIR)
DB_PATH = os.getenv("DB_PATH", os.path.join(DATA_DIR, "demo.db"))
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

# --- LanceDB Configuration ---
# Portfolio Demo: Use relative path
LANCEDB_PATH = os.environ.get("LANCEDB_PATH", os.path.join(os.path.dirname(__file__), "data", "lancedb"))
EMBEDDING_MODEL = "models/gemini-embedding-001"

try:
    ldb = lancedb.connect(LANCEDB_PATH)
    summary_tables = set(ldb.table_names())
    if "document_summaries" in summary_tables:
        tbl_doc_summaries = ldb.open_table("document_summaries")
    else:
        schema_summaries = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 768)),
            pa.field("filename", pa.string()),
            pa.field("document_summary", pa.string()),
            pa.field("timestamp", pa.string())
        ])
        tbl_doc_summaries = ldb.create_table("document_summaries", schema=schema_summaries)

    if "document_pages" in summary_tables:
        tbl_doc_pages = ldb.open_table("document_pages")
    else:
        schema_pages = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 768)),
            pa.field("filename", pa.string()),
            pa.field("page_number", pa.int32()),
            pa.field("text_content", pa.string())
        ])
        tbl_doc_pages = ldb.create_table("document_pages", schema=schema_pages)
except Exception as e:
    logger.critical(f"Failed to initialize LanceDB: {e}")
    tbl_doc_summaries = None
    tbl_doc_pages = None

REPLACE_OLD_CONTENT = False
#REPLACE_OLD_CONTENT = True

# REPROCESS_MISSING_SUMMARIES = False
REPROCESS_MISSING_SUMMARIES = True


# --- Constants ---
API_CALL_DELAY_SECONDS = 2
MAX_RETRIES = 2
REQUEST_TIMEOUT_SECONDS = 300 # Increased timeout for potentially larger content
MODEL_CONTEXT_LIMIT_TOKENS = int(os.getenv("MODEL_CONTEXT_LIMIT_TOKENS", 1048576))
MAX_PAGE_WORDS = 4000 # Upper limit for page text length in words

SAFETY_SETTINGS = [
    {"category": c, "threshold": "BLOCK_MEDIUM_AND_ABOVE"}
    for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
              "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]
]
GENERATION_CONFIG = GenerationConfig(
    temperature=0.1, 
    response_mime_type="application/json",
    max_output_tokens=8192*8
) # Ensure JSON output

# API Key Management State Variables
_active_api_key = None
_api_key_cooldowns = {}
API_KEY_COOLDOWN_SECONDS = 3600


PIPELINE_READY = bool(GEMINI_API_KEY)
if not PIPELINE_READY:
    logger.warning("GEMINI_API_KEY not set. Document processing functions will be unavailable until configured.")
os.makedirs(POLICY_DIR, exist_ok=True)


def ensure_pipeline_ready():
    """Fail lazily at runtime instead of exiting during module import."""
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set for document processing")
    if not tbl_doc_summaries or not tbl_doc_pages:
        raise RuntimeError("LanceDB is not available for document processing")


def ensure_sqlite_document_schema():
    """Create the SQLite table used by document indexing and upload ingestion."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
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
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        logger.warning(f"Could not ensure dashboard_summary_documents schema at startup: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


ensure_sqlite_document_schema()


# --- Modified Gemini Prompt (With Page Summary) ---
GEMINI_DOCUMENT_PROMPT = r"""
Analyze the provided file ({filename}).

**OUTPUT: BAHASA INDONESIA (Strictly)**
**FORMAT: Single Valid JSON Object**

### INSTRUCTIONS
1.  **Document Summary:** 8-12 sentence executive summary of the entire document.
2.  **Page-by-Page Analysis:** For every page/slide:
    *   **page_content**: Extract **ALL information** comprehensively but in **High-Density / Telegraphic Style**.
        *   **NO META-TEXT:** BANNED PHRASES: "Halaman ini...", "Dokumen ini...", "Gambar ini menunjukkan...", "Nota dinas ini berisi...".
        *   **Start DIRECTLY with data:** E.g., "Judul: X. Paragraf 1: [Isi]. Tabel: [Data]."
        *   **Tables:** Convert to dense text formats. (e.g., "Col A: 10, Col B: 20" instead of "Kolom A memiliki nilai 10...").
        *   **Images:** Describe details directly. (e.g., "Grafik Batang: Tren naik 20% di Q1.")
    *   **page_summary**: 2-3 sentences. **Direct Subject-Verb style.**
        *   *Bad:* "Halaman ini menjelaskan prosedur login."
        *   *Good:* "Prosedur login mencakup input username dan password."
    *   **visual_complexity**: Choose EXACTLY ONE:
        *   "text_dominant" (Mostly text, simple headers)
        *   "moderately_complex" (Contains tables, simple charts, or diagrams)
        *   "highly_complex" (Dense infographics, complex blueprints, handwritten notes, or overlapping elements)

### CRITICAL JSON RULES
*   **Quotes:** Use SINGLE quotes (') inside text to avoid breaking JSON. Ex: "Program 'Super' dimulai."
*   **Backslashes:** Double-escape them: "C:\\Path".
*   **Newlines:** Use "\\n" instead of real line breaks.
*   **Completeness:** You MUST finish the JSON with "}}".

### JSON EXAMPLE
{{
  "document_summary": "Ringkasan eksekutif dokumen...",
  "pages": {{
    "1": {{
      "page_content": "Judul: Laporan Q1. Poin Utama: Pendapatan naik 10% menjadi 50M. Tabel Pendapatan: Jan (10M), Feb (15M), Mar (25M). Grafik: Tren positif di sektor ritel.",
      "page_summary": "Laporan pendapatan Q1 menunjukkan pertumbuhan 10% didorong sektor ritel.",
      "visual_complexity": "text_dominant"
    }}
  }}
}}
"""

# --- API Key Management Functions (Keep existing - Copied from original for completeness) ---
def initialize_api_keys():
    """Initializes the module-level API key variables and cooldowns."""
    global _active_api_key, _api_key_cooldowns
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not found in environment variables.")
    _active_api_key = GEMINI_API_KEY
    _api_key_cooldowns = {
        GEMINI_API_KEY: 0,
        GEMINI_API_KEY_2: 0 if GEMINI_API_KEY_2 else None
    }
    key_status = "Primary only"
    if GEMINI_API_KEY_2: key_status = "Primary and Secondary"
    logger.info(f"API Key Management Initialized ({key_status}). Cooldown: {API_KEY_COOLDOWN_SECONDS}s.")

def get_active_api_key():
    """Returns the currently active API key, switching if necessary due to cooldown."""
    global _active_api_key, _api_key_cooldowns
    if not _api_key_cooldowns: initialize_api_keys() # Ensure initialized
    if not GEMINI_API_KEY_2: return GEMINI_API_KEY # Only primary key available

    current_time = time.time()
    main_cooldown_end = _api_key_cooldowns.get(GEMINI_API_KEY, 0)
    secondary_cooldown_end = _api_key_cooldowns.get(GEMINI_API_KEY_2, 0)

    is_main_active = (_active_api_key == GEMINI_API_KEY)
    is_main_ready = main_cooldown_end <= current_time
    is_secondary_ready = secondary_cooldown_end <= current_time

    if is_main_active and not is_main_ready: # Main is active but in cooldown
        if is_secondary_ready:
            logger.info("Switching to secondary API key (Primary in cooldown).")
            _active_api_key = GEMINI_API_KEY_2
        else:
            # Both in cooldown, stick with main if it was active, or secondary if it was. Log warning.
            logger.warning(f"Primary API key is active but in cooldown ({main_cooldown_end - current_time:.0f}s left). Secondary also in cooldown ({secondary_cooldown_end - current_time:.0f}s left). Sticking with current active key.")
    elif not is_main_active and not is_secondary_ready: # Secondary is active but in cooldown
        if is_main_ready:
            logger.info("Switching back to primary API key (Secondary in cooldown).")
            _active_api_key = GEMINI_API_KEY
        else:
            logger.warning(f"Secondary API key is active but in cooldown ({secondary_cooldown_end - current_time:.0f}s left). Primary also in cooldown ({main_cooldown_end - current_time:.0f}s left). Sticking with current active key.")
    elif not is_main_active and is_main_ready: # Secondary is active, but primary is now ready (prefer primary)
         logger.info("Switching back to primary API key (preferred and available).")
         _active_api_key = GEMINI_API_KEY
    # If main is active and ready, or secondary is active and ready (and main not preferred or not ready), no change needed.
    return _active_api_key

def handle_rate_limit(rate_limited_key):
    """Marks a key as rate limited and attempts to switch."""
    global _active_api_key, _api_key_cooldowns
    if not _api_key_cooldowns: initialize_api_keys() # Ensure initialized
    logger.warning(f"Rate limit encountered for key ending in '...{rate_limited_key[-4:]}'. Applying cooldown.")
    _api_key_cooldowns[rate_limited_key] = time.time() + API_KEY_COOLDOWN_SECONDS

    alternate_key = None
    if GEMINI_API_KEY_2: # Check if secondary key exists
        alternate_key = GEMINI_API_KEY_2 if rate_limited_key == GEMINI_API_KEY else GEMINI_API_KEY

    if alternate_key:
        alt_cooldown_end = _api_key_cooldowns.get(alternate_key, 0)
        if alt_cooldown_end <= time.time():
            logger.info(f"Switching active API key to '...{alternate_key[-4:]}' due to rate limit on previous key.")
            _active_api_key = alternate_key
            return True
        else:
            logger.warning(f"Could not switch API key. Alternate key '...{alternate_key[-4:]}' is also in cooldown ({alt_cooldown_end - time.time():.0f}s left).")
            return False
    else:
        logger.warning("Rate limit encountered, but no secondary API key available to switch to.")
        return False

# --- Helper Functions (configure_gemini, fix_and_validate_page_data, generate_prompt_suggestions - Modified) ---
def configure_gemini():
    """Configures the genai client with the CURRENTLY active API key."""
    active_key = get_active_api_key()
    client_options = {}
    if httplib2 and ProxyTypes is not None and (HTTP_PROXY or HTTPS_PROXY):
        proxy_url = HTTP_PROXY if HTTP_PROXY else HTTPS_PROXY # Prefer HTTP_PROXY if both defined
        try:
            # Basic parsing, assuming format like http://host:port or https://host:port
            protocol, rest = proxy_url.split('://', 1)
            host_port_parts = rest.split(':')
            proxy_host = host_port_parts[0]
            proxy_port = int(host_port_parts[1]) if len(host_port_parts) > 1 else (80 if protocol == 'http' else 443)

            proxy_info = httplib2.ProxyInfo(
                proxy_type=ProxyTypes.HTTP, # Gemini SDK typically uses HTTPS, but httplib2 proxy type is for the proxy server itself
                proxy_host=proxy_host,
                proxy_port=proxy_port,
                proxy_user=None, # Add user/pass if your proxy needs auth
                proxy_pass=None
            )
            # Create a new Http object with proxy_info for each configuration
            # This avoids issues if the underlying Http object is stateful or shared unexpectedly.
            http_client = httplib2.Http(proxy_info=proxy_info)
            client_options['transport'] = http_client # For google-auth & requests, not directly for genai client_options
                                                      # genai might use standard env vars (HTTPS_PROXY) if this doesn't work.
                                                      # The genai library's direct proxy support via client_options is less clear.
                                                      # Often, setting HTTPS_PROXY env var is more reliable.
            logger.debug(f"Attempting to configure Gemini with httplib2 proxy: {proxy_host}:{proxy_port} (via client_options if supported, or env vars).")
        except Exception as proxy_parse_err:
             logger.error(f"Failed to parse proxy URL '{proxy_url}' or create httplib2 client: {proxy_parse_err}. Relying on OS environment variables for proxy.", exc_info=True)
             client_options = {} # Fallback
    elif HTTP_PROXY or HTTPS_PROXY:
        if httplib2 and ProxyTypes is None:
             logger.warning("httplib2.ProxyTypes not available. Cannot configure proxy via client_options. Relying on OS environment variables.")
        elif not httplib2:
             logger.warning("httplib2 not available. Cannot attempt explicit proxy configuration via client_options. Relying on OS environment variables.")

    try:
        # The genai library primarily relies on standard environment variables (HTTPS_PROXY)
        # for proxy settings. Explicit client_options for proxy are not standard.
        # We will still pass client_options if populated, but it might not be used for proxy.
        genai.configure(api_key=active_key, client_options=client_options if client_options else None)
        logger.debug(f"Gemini client configured with active key: '...{active_key[-4:]}'.")
        if not client_options and (HTTP_PROXY or HTTPS_PROXY):
            logger.info("Gemini client configured. Proxy settings (if any) will be picked from OS environment variables (e.g., HTTPS_PROXY).")
        elif client_options:
             logger.info("Gemini client configured with client_options. Proxy behavior depends on SDK's use of these options.")

    except Exception as e:
        logger.critical(f"FATAL: Failed to configure Gemini with key '...{active_key[-4:]}': {e}", exc_info=True)
        raise


def fix_and_validate_page_data(page_data: dict, page_num_str: str, filename: str) -> dict:
    """Validates page data and DETERMINISTICALLY calculates flags based on complexity."""
    
    # 1. Basic Structure Check
    if not isinstance(page_data, dict):
        logger.warning(f"Page {page_num_str} data for '{filename}' is not a dictionary. Creating default error entry.")
        return {
            "page_content": f"[Processing Error: Invalid data structure]",
            "page_summary": f"[Error: Invalid data]",
            "visual_complexity": "unknown",
            "requires_advanced_processing": False,
            "requires_experimental_processing": False
        }

    # 2. Content & Summary Validation
    if "page_content" not in page_data or not isinstance(page_data["page_content"], str):
        page_data["page_content"] = f"[Processing Error: Page content missing]"
    
    if "page_summary" not in page_data or not isinstance(page_data["page_summary"], str):
        page_data["page_summary"] = f"[Processing Error: Page summary missing]"

    # 3. Truncate Content
    words = page_data["page_content"].split()
    if len(words) > MAX_PAGE_WORDS:
        page_data["page_content"] = " ".join(words[:MAX_PAGE_WORDS]) + f"... [truncated]"

    # 4. DETERMINISTIC FLAG CALCULATION (The Fix)
    # We trust the AI's 'visual_complexity' label, but we define the rules for flags in Python.
    vis_comp = page_data.get("visual_complexity", "unknown")
    valid_complexities = ["text_dominant", "moderately_complex", "highly_complex"]

    # Normalize unknown values
    if vis_comp not in valid_complexities:
        vis_comp = "unknown"
        page_data["visual_complexity"] = "unknown"

    # Logic: 
    # Moderate OR High -> Advanced
    # High -> Experimental
    page_data["requires_advanced_processing"] = (vis_comp in ["moderately_complex", "highly_complex"])
    page_data["requires_experimental_processing"] = (vis_comp == "highly_complex")

    return page_data


def get_processed_filenames_from_db(conn) -> set:
    """
    Queries the database and returns a set of distinct filenames that have been fully processed.
    A file is considered fully processed if it has non-blank page_summary entries.
    """
    processed_files = set()
    try:
        with conn.cursor() as cursor:
            # Check for files that have non-empty page_summary
            sql = """
                SELECT DISTINCT filename 
                FROM dashboard_summary_documents 
                WHERE page_summary IS NOT NULL 
                AND page_summary != '' 
                AND TRIM(page_summary) != ''
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                if row and 'filename' in row and row['filename']:
                    processed_files.add(row['filename'])
        logger.info(f"Found {len(processed_files)} fully processed filenames (with page_summary) in the database.")
    except pymysql.MySQLError as e:
        logger.error(f"Error querying processed filenames from DB: {e}", exc_info=True)
    return processed_files


def generate_prompt_suggestions(document_summary: str, page_analysis_data: dict, filename: str) -> list[str]:
    """
    Generates 1-3 contextual prompt suggestions using Gemini,
    based on document summary and page content.
    """
    # Define fallback suggestions that will be used if data is insufficient or an error occurs.
    fallback_suggestions = [
        "Jelaskan tujuan utama dokumen ini.",
        "Sebutkan poin-poin penting yang dibahas dalam dokumen ini.",
        "Apakah ada kesimpulan atau rekomendasi utama dalam dokumen ini?"
    ]

    # Check if there's enough data to generate meaningful suggestions.
    if not document_summary and not page_analysis_data:
        logger.warning(
            f"Insufficient data (no document summary and no page analysis) for generating prompt suggestions for '{filename}'. "
            "Using fallback suggestions."
        )
        return fallback_suggestions

    try:
        num_pages = len(page_analysis_data) if page_analysis_data else 0
        # Determine the number of suggestions: at least 1, up to 3.
        # Roughly, 1 suggestion per 5 pages, but capped.
        max_suggestions = min(3, max(1, (num_pages // 5) + 1 if num_pages > 0 else 1))

        formatted_pages_preview_list = []
        if page_analysis_data:
            page_keys = list(page_analysis_data.keys())
            try:
                # Attempt to sort page keys numerically if they are digits
                page_keys.sort(key=int)
            except ValueError:
                # Fallback to string sort if keys are not purely numeric (e.g., "A1", "1", "B2")
                page_keys.sort()

            # Create a preview from the content of the first few pages (up to 15).
            for page_num_key in page_keys[:15]:
                page_data = page_analysis_data.get(page_num_key, {})
                summary_preview = page_data.get('page_summary', '')[:150] # Use page_summary instead of page_content
                visual_complexity_preview = page_data.get('visual_complexity', 'unknown')
                formatted_pages_preview_list.append(
                    f" - Halaman {page_num_key} (Visual: {visual_complexity_preview}): {summary_preview}..."
                )

        # Construct the context for the prompt suggestion generation.
        context_for_prompt_generation = ""
        if document_summary:
            context_for_prompt_generation += f"Ringkasan Dokumen Keseluruhan:\n{document_summary}\n\n"
        else:
            context_for_prompt_generation += "Ringkasan Dokumen Keseluruhan: Tidak tersedia.\n\n"

        if formatted_pages_preview_list:
            context_for_prompt_generation += \
                f"Ringkasan Halaman Awal (cuplikan):\n{os.linesep.join(formatted_pages_preview_list)}"
        else:
            context_for_prompt_generation += "Ringkasan Halaman Awal: Tidak ada pratinjau halaman yang tersedia."

        # Define the prompt that will be sent to Gemini to generate suggestions.
        prompt_to_generate_suggestions = f"""
Berdasarkan konteks berikut dari dokumen '{filename}', buatkan {max_suggestions} saran pertanyaan (dalam Bahasa Indonesia) yang relevan dan mendalam. Pertanyaan-pertanyaan ini harus dapat membantu pengguna untuk menjelajahi isi dokumen lebih lanjut dan mendapatkan pemahaman yang lebih baik.

{context_for_prompt_generation}

Format respons Anda **hanya** sebagai daftar JSON string yang valid.
Contoh: ["Pertanyaan spesifik 1?", "Pertanyaan spesifik 2?", "Pertanyaan spesifik 3?"]
Jangan sertakan teks atau markdown lain sebelum atau sesudah daftar JSON.
"""
        # Ensure Gemini is configured (assuming helper function exists)
        configure_gemini()
        # Assuming GEMINI_MODEL, GenerationConfig, SAFETY_SETTINGS are defined globally
        model = genai.GenerativeModel(GEMINI_MODEL)
        
        suggestion_generation_config = GenerationConfig(
            temperature=0.5, # Slightly more creative for suggestions
            response_mime_type="application/json" # Request JSON output directly
        )

        response = model.generate_content(
            prompt_to_generate_suggestions,
            generation_config=suggestion_generation_config,
            safety_settings=SAFETY_SETTINGS,
            request_options={'timeout': 60} # Shorter timeout for this auxiliary call
        )

        raw_text_from_response = ""
        if hasattr(response, 'text') and response.text: # Prioritize direct .text attribute
            raw_text_from_response = response.text.strip()
        elif response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
            # Fallback for models that structure output in parts
            raw_text_from_response = "".join(
                p.text for p in response.candidates[0].content.parts if hasattr(p, 'text')
            ).strip()

        if not raw_text_from_response:
            logger.warning(f"Received empty response from Gemini when generating prompt suggestions for '{filename}'.")
            raise ValueError("Empty response from Gemini for suggestions.")

        # Attempt to parse the response as JSON.
        try:
            suggestions_list = json.loads(raw_text_from_response)
            if isinstance(suggestions_list, list) and all(isinstance(s, str) for s in suggestions_list):
                # Ensure the list has exactly 3 suggestions, padding with empty strings if necessary.
                while len(suggestions_list) < 3:
                    suggestions_list.append("")
                logger.info(f"Successfully generated {len(suggestions_list)} prompt suggestions for '{filename}'.")
                return suggestions_list[:3] # Return the first 3 suggestions.
            else:
                logger.warning(
                    f"Parsed suggestions response for '{filename}' is not a list of strings: {raw_text_from_response}. "
                    "Using fallback suggestions."
                )
                return fallback_suggestions
        except json.JSONDecodeError:
            logger.warning(
                f"Failed to decode JSON from Gemini's response for prompt suggestions for '{filename}'. "
                f"Response: {raw_text_from_response}. Attempting regex fallback."
            )
            # Fallback to regex if direct JSON parsing fails (e.g., if there's extra text around the JSON array)
            # This regex tries to find a JSON array of strings.
            json_array_match = re.search(r'\[\s*(?:\"(?:[^\"\\]|\\.)*\"\s*,\s*)*\"(?:[^\"\\]|\\.)*\"\s*\]', raw_text_from_response, re.DOTALL)
            if json_array_match:
                try:
                    suggestions_list_regex = json.loads(json_array_match.group(0))
                    if isinstance(suggestions_list_regex, list) and all(isinstance(s, str) for s in suggestions_list_regex):
                        while len(suggestions_list_regex) < 3:
                            suggestions_list_regex.append("")
                        logger.info(f"Generated {len(suggestions_list_regex)} prompt suggestions (via regex) for '{filename}'.")
                        return suggestions_list_regex[:3]
                except json.JSONDecodeError as json_err_regex:
                    logger.warning(
                        f"Failed to decode JSON suggestions even with regex for '{filename}'. "
                        f"Regex matched: {json_array_match.group(0)}. Error: {json_err_regex}. Using fallback."
                    )
                    return fallback_suggestions
            else:
                logger.warning(
                    f"Could not find or parse a JSON array structure in prompt suggestion response for '{filename}' "
                    f"even with regex. Response: {raw_text_from_response}. Using fallback."
                )
                return fallback_suggestions

    except Exception as e:
        logger.error(f"An unexpected error occurred while generating prompt suggestions for '{filename}': {e}", exc_info=True)
        logger.warning(f"Using fallback prompt suggestions for '{filename}' due to an unexpected error.")
        return fallback_suggestions


# --- Core Processing Function (Modified) ---
def process_document_and_generate_data(doc_full_path: str) -> list[dict] | None:
    """
    Processes a single document: uploads to Gemini, gets page content and analysis,
    formats data for DB insertion. Includes document_summary and page_summary.
    """
    filepath_obj = Path(doc_full_path)
    filename = filepath_obj.name
    source_path = str(filepath_obj.parent.resolve()) # Get directory path
    timestamp_utc = datetime.now(timezone.utc)
    logger.info(f"--- Starting full processing for: {filename} (with Document and Page Summary) ---")
    try:
        ensure_pipeline_ready()
    except RuntimeError as e:
        logger.error(f"Document processing unavailable: {e}")
        return None

    # 1. Read File Content
    try:
        with open(doc_full_path, 'rb') as f:
            file_content_bytes = f.read()
        if not file_content_bytes:
            logger.error(f"File is empty: {filename}")
            return None
        file_size_mb = len(file_content_bytes) / (1024 * 1024)
        logger.info(f"Read {len(file_content_bytes)} bytes ({file_size_mb:.2f} MB) for {filename}.")
    except MemoryError:
        logger.error(f"Memory Error reading file {filename}.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Failed to read file {doc_full_path}: {e}", exc_info=True)
        return None

    # 2. Determine MIME Type
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type: # Basic fallback
        extension = Path(filename).suffix.lower()
        if extension == ".pdf":
            mime_type = "application/pdf"
        elif extension == ".pptx":
            mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        else:
            mime_type = 'application/octet-stream' # Generic binary
    logger.debug(f"Determined MIME type as '{mime_type}' for {filename}.")

    # 3. Upload to Gemini File API and Wait
    upload_response = None
    file_api_file = None # This will store the File object from genai.get_file or upload_file
    try:
        configure_gemini() # Ensure correct API key is set and proxy configuration is attempted
        logger.info(f"Uploading {filename} ({mime_type}) to Gemini File API...")
        # Use BytesIO for uploading content directly
        with io.BytesIO(file_content_bytes) as file_data_io:
            upload_response = genai.upload_file(
                path=file_data_io, # Corrected: path, not file
                display_name=filename,
                mime_type=mime_type
            )
        del file_content_bytes # Free memory
        logger.info(f"File {filename} uploaded. URI: {upload_response.uri}, Name: {upload_response.name}")
        file_api_file = upload_response # Initial assignment

        processing_start_time = time.time()
        logger.info(f"Waiting for file {file_api_file.name} to become ACTIVE...")
        while True:
            current_wait_time = time.time() - processing_start_time
            if current_wait_time > REQUEST_TIMEOUT_SECONDS: # Global const
                raise TimeoutError(f"File processing timed out for {file_api_file.name} after {current_wait_time:.0f}s")

            try:
                # It's crucial to get the updated file object to check its state
                file_api_file = genai.get_file(name=file_api_file.name)
                state = file_api_file.state.name
                logger.debug(f"File {file_api_file.name} state: {state}. Elapsed: {current_wait_time:.1f}s")
                if state == "ACTIVE":
                    break
                if state == "FAILED":
                    error_message = "File processing failed via API"
                    if file_api_file.error and hasattr(file_api_file.error, 'message'):
                        error_message += f": {file_api_file.error.message}"
                    raise RuntimeError(error_message)
                # Dynamic sleep, longer for longer waits, capped
                time.sleep(min(15, int(current_wait_time * 0.1) + 5))
            except google_exceptions.ResourceExhausted:
                logger.warning(f"Rate limit hit while checking file status for {file_api_file.name}. Attempting key switch...")
                current_key_before_switch = get_active_api_key() # Get key that was used
                if handle_rate_limit(current_key_before_switch):
                    configure_gemini() # Reconfigure with new key if switched
                    logger.info("Successfully switched API key for status check.")
                else:
                    logger.warning("Could not switch API key for status check, continuing wait with longer delay...")
                    time.sleep(30) # Longer delay if key switch fails
            except google_exceptions.NotFound:
                logger.error(f"File {file_api_file.name} not found during status check. This should not happen if upload was successful.")
                raise # This is a critical error
            except Exception as status_err:
                logger.warning(f"Error checking file status for {file_api_file.name}: {status_err}. Retrying check.")
                time.sleep(10) # General retry delay for other status check errors
        logger.info(f"File {filename} ({file_api_file.name}) is ACTIVE.")
    except Exception as upload_err:
        logger.error(f"Failed during Gemini File API upload/wait for {filename}: {upload_err}", exc_info=True)
        if upload_response and hasattr(upload_response, 'name'): # Check if upload_response and its name exist
            try:
                logger.info(f"Attempting cleanup delete for {upload_response.name} after error...")
                genai.delete_file(upload_response.name)
                logger.info(f"Cleanup delete successful for {upload_response.name}.")
            except Exception as del_err:
                logger.warning(f"Failed cleanup delete for {upload_response.name}: {del_err}")
        return None


    # 4. Call Gemini Generate Content with Retry
    attempt = 0
    gemini_result_json = None
    rate_limit_backoff = 5 # Initial backoff in seconds for rate limits

    while attempt < MAX_RETRIES: # Global const
        attempt += 1
        try:
            configure_gemini() # Ensure correct API key and proxy
            active_key_for_gen = get_active_api_key()
            logger.info(f"Calling generate_content for {filename} (Attempt {attempt}/{MAX_RETRIES}) using key '...{active_key_for_gen[-4:]}'...")

            model = genai.GenerativeModel(GEMINI_MODEL)
            prompt_formatted = GEMINI_DOCUMENT_PROMPT.format(filename=filename)

            if not file_api_file or not hasattr(file_api_file, 'state') or file_api_file.state.name != "ACTIVE":
                logger.error(f"File {file_api_file.name if file_api_file else 'Unknown file'} is not ACTIVE before generation. Aborting.")
                gemini_result_json = {"error": "File state became non-ACTIVE before generation."}
                break

            # Generate content using streaming
            response_stream = model.generate_content(
                [prompt_formatted, file_api_file],
                generation_config=GENERATION_CONFIG,
                safety_settings=SAFETY_SETTINGS,
                request_options={'timeout': REQUEST_TIMEOUT_SECONDS},
                stream=True
            )

            # Assemble the full response from the stream
            full_raw_text = ""
            for chunk in response_stream:
                # It's good practice to check if the chunk has text
                if hasattr(chunk, 'text') and chunk.text:
                    full_raw_text += chunk.text
            
            # NOTE: With streaming, token usage metadata is not available on the main response object.
            # It can sometimes be found on the final chunk, but for simplicity, we often omit this log for streams.

            raw_text = full_raw_text.strip()
            rate_limit_backoff = 5 # Reset backoff on successful call

            # NEW check: If after streaming, we have no text, there's a problem.
            if not raw_text:
                logger.error(f"Empty text content received from Gemini stream for {filename}.")
                gemini_result_json = {"error": "Empty text content from Gemini stream"}
                # Allow retry for transient issues
                time.sleep(API_CALL_DELAY_SECONDS * attempt)
                continue

            # Now, proceed directly to parsing the assembled raw_text
            try:
                # 1. Remove Markdown
                clean_text = re.sub(r'^```json\s*', '', raw_text, flags=re.MULTILINE)
                clean_text = re.sub(r'^```\s*', '', clean_text, flags=re.MULTILINE)
                clean_text = re.sub(r'\s*```$', '', clean_text, flags=re.MULTILINE)

                # 2. Fix Backslashes
                clean_text = re.sub(r'\\(?![/u"bfnrt\\])', r'\\\\', clean_text)
                
                # 3. Attempt to Fix "Lazy" Commas (Common LLM error)
                # Looks for: "value" "next_key": and adds a comma -> "value", "next_key":
                clean_text = re.sub(r'"\s+"\w+":', lambda m: m.group(0).replace('" "', '", "'), clean_text)

                # 4. Parse with strict=False
                parsed_json = json.loads(clean_text, strict=False)

                # ... (Keep the rest of your validation logic: if isinstance, etc.) ...
                if isinstance(parsed_json, dict):
                     # ... [Your existing success logic] ...
                     gemini_result_json = parsed_json
                     break
                else:
                     raise json.JSONDecodeError("Not a dict", clean_text, 0)

            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to decode JSON response for {filename} (Attempt {attempt}/{MAX_RETRIES}). Error: {json_err}")
                # Log the specific area of the error if possible
                if hasattr(json_err, 'pos'):
                    start = max(0, json_err.pos - 50)
                    end = min(len(clean_text), json_err.pos + 50)
                    logger.error(f"Error context: ...{clean_text[start:end]}...")
                
                # This part is perfectly implemented!
                try:
                    debug_dir = Path("debug_logs")
                    debug_dir.mkdir(exist_ok=True)
                    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                    debug_filename = f"{timestamp_str}_{filename}_attempt_{attempt}.json.txt"
                    debug_filepath = debug_dir / debug_filename
                    with open(debug_filepath, "w", encoding="utf-8") as f:
                        f.write(raw_text)
                    logger.warning(f"Saved the full malformed response to: {debug_filepath}")
                except Exception as log_save_err:
                    logger.error(f"Could not save malformed response to a file: {log_save_err}")

                logger.debug(f"Invalid JSON string received: {raw_text[:500]}...")
                gemini_result_json = {"error": f"Invalid JSON response: {json_err}", "raw_response": raw_text}

        except google_exceptions.ResourceExhausted as rate_limit_err:
            logger.warning(f"Rate limit hit for {filename} (Attempt {attempt}/{MAX_RETRIES}). Error: {rate_limit_err}")
            current_key_rl = get_active_api_key() # Get the key that was just used
            switched_rl = handle_rate_limit(current_key_rl) # This applies cooldown and switches if possible
            if attempt < MAX_RETRIES:
                wait_time_rl = rate_limit_backoff
                logger.info(f"Waiting {wait_time_rl:.1f}s before retry due to rate limit...")
                time.sleep(wait_time_rl)
                rate_limit_backoff = min(rate_limit_backoff * 1.8, 60) # Increase backoff, cap at 60s
                if switched_rl:
                    configure_gemini() # Reconfigure if key switched
                continue # Retry
            else:
                logger.error(f"Failed {filename} after {MAX_RETRIES} attempts due to persistent rate limits.")
                gemini_result_json = {"error": f"Rate limit exceeded after {MAX_RETRIES} attempts."}
                break
        except (google_exceptions.ServiceUnavailable, google_exceptions.InternalServerError, google_exceptions.DeadlineExceeded) as transient_err:
            error_type = type(transient_err).__name__
            logger.warning(f"{error_type} encountered for {filename} (Attempt {attempt}/{MAX_RETRIES}). Error: {transient_err}")
            if attempt < MAX_RETRIES:
                wait_time_transient = API_CALL_DELAY_SECONDS * (2 ** (attempt -1)) # Exponential backoff
                logger.info(f"Waiting {wait_time_transient:.1f}s before retry...")
                time.sleep(wait_time_transient)
                continue # Retry
            else:
                logger.error(f"Failed {filename} after {MAX_RETRIES} attempts due to {error_type}.")
                gemini_result_json = {"error": f"Failed after {MAX_RETRIES} retries ({error_type})."}
                break
        except Exception as gen_err:
            logger.critical(f"Unexpected error during generate_content for {filename} (Attempt {attempt}): {gen_err}", exc_info=True)
            gemini_result_json = {"error": f"Unexpected error: {gen_err}"}
            break # Break on unexpected critical errors

        # If loop continues (e.g. after JSON decode error and retries left), add a small delay
        # Check if we are about to retry and not because of a successful break
        if attempt < MAX_RETRIES and not (gemini_result_json and "pages" in gemini_result_json and "document_summary" in gemini_result_json and "error" not in gemini_result_json) :
            time.sleep(API_CALL_DELAY_SECONDS)


    # 5. Cleanup File API resource
    if file_api_file and hasattr(file_api_file, 'name') and file_api_file.name: # Check if file_api_file and its name attribute exist
        try:
            logger.info(f"Deleting {file_api_file.name} from File API...")
            genai.delete_file(file_api_file.name)
            logger.info(f"Deleted {file_api_file.name}.")
        except Exception as delete_err:
            logger.warning(f"Could not delete {file_api_file.name} from File API: {delete_err}")
    elif upload_response and hasattr(upload_response, 'name') and upload_response.name: # Fallback if file_api_file object was lost
        try:
            logger.info(f"Deleting {upload_response.name} (from upload_response) from File API as fallback...")
            genai.delete_file(upload_response.name)
            logger.info(f"Deleted {upload_response.name} (fallback).")
        except Exception as delete_err:
            logger.warning(f"Could not delete {upload_response.name} (fallback) from File API: {delete_err}")


    # 6. Process final result
    if not gemini_result_json or "error" in gemini_result_json or \
       "pages" not in gemini_result_json or "document_summary" not in gemini_result_json:
        err_msg = gemini_result_json.get('error', 'Unknown processing error or missing essential keys') if gemini_result_json else 'No JSON result from Gemini'
        raw_resp_snippet = gemini_result_json.get('raw_response', '')[:200] if gemini_result_json else ''
        logger.error(f"Processing failed for {filename}. Final Error: {err_msg}. Raw response (snippet): '{raw_resp_snippet}...'.")
        return None

    db_rows = []
    try:
        document_summary = gemini_result_json.get('document_summary', "[Document summary processing failed or not provided by API]")
        page_analysis_data = gemini_result_json.get('pages', {}) # Should exist due to checks above

        if not isinstance(page_analysis_data, dict) or not page_analysis_data: # Ensure it's a non-empty dict
            logger.error(f"No 'pages' data (or invalid type) in Gemini response for {filename}, even after passing primary checks.")
            return None

        validated_pages = {}
        page_keys_from_json = list(page_analysis_data.keys())
        try:
            # Sort page keys numerically if they are digits, otherwise sort as strings
            sorted_page_keys = sorted(page_keys_from_json, key=lambda k: int(k) if k.isdigit() else k)
        except ValueError: # Fallback to simple string sort if int conversion fails for mixed types
            sorted_page_keys = sorted(page_keys_from_json)

        for page_num_str in sorted_page_keys:
            page_data_from_json = page_analysis_data.get(page_num_str, {}) # Should always get a dict
            validated_pages[page_num_str] = fix_and_validate_page_data(page_data_from_json, page_num_str, filename)
        
        doc_has_complex_visuals_flag = any(p.get("requires_advanced_processing", False) for p in validated_pages.values())
        doc_has_highly_complex_visuals_flag = any(p.get("requires_experimental_processing", False) for p in validated_pages.values())
        
        adv_pages_list = sorted(
            [str(k) for k, v in validated_pages.items() if v.get("requires_advanced_processing", False)],
            key=lambda x: int(x) if x.isdigit() else float('inf') # Sort numerically where possible
        )
        exp_pages_list = sorted(
            [str(k) for k, v in validated_pages.items() if v.get("requires_experimental_processing", False)],
            key=lambda x: int(x) if x.isdigit() else float('inf') # Sort numerically where possible
        )

        suggestions = generate_prompt_suggestions(document_summary, validated_pages, filename) # Pass actual document_summary
        timestamp_str = timestamp_utc.strftime('%Y-%m-%d %H:%M:%S')
        current_model_used = GEMINI_MODEL # Global const

        if not validated_pages: # If after all processing, there are no pages to add
            logger.warning(f"No validated pages to create database rows for {filename}. This might be due to empty 'pages' in response or all pages failing validation.")
            return None # Or an empty list if the calling code expects it

        for page_num_str in sorted_page_keys: # Iterate using sorted keys to ensure consistent row order
            if page_num_str not in validated_pages: # Should not happen if logic is correct
                logger.warning(f"Page key {page_num_str} missing in validated_pages for {filename}. Skipping.")
                continue
            page_data = validated_pages[page_num_str]
            
            db_row_entry = {
                "filename": filename,
                "source_path": source_path,
                "timestamp_utc": timestamp_str,
                "model_used": current_model_used,
                "document_summary": document_summary,
                "page_number": page_num_str,
                "page_content": page_data.get("page_content"),
                "page_summary": page_data.get("page_summary"),
                "page_visual_complexity": page_data.get("visual_complexity"),
                "page_requires_advanced_processing": 1 if page_data.get("requires_advanced_processing", False) else 0,
                "page_requires_experimental_processing": 1 if page_data.get("requires_experimental_processing", False) else 0,
                "doc_has_complex_visuals": 1 if doc_has_complex_visuals_flag else 0,
                "doc_has_highly_complex_visuals": 1 if doc_has_highly_complex_visuals_flag else 0,
                "doc_pages_req_advanced": ",".join(adv_pages_list),
                "doc_pages_req_experimental": ",".join(exp_pages_list),
                "prompt_suggestion_1": suggestions[0] if len(suggestions) > 0 else "",
                "prompt_suggestion_2": suggestions[1] if len(suggestions) > 1 else "",
                "prompt_suggestion_3": suggestions[2] if len(suggestions) > 2 else ""
                # No comma after the last item in a dict
            }
            db_rows.append(db_row_entry)
            
        if not db_rows: # Final check
            logger.error(f"No DB rows were generated for {filename} despite appearing to process pages. Check validation logic.")
            return None

        # --- EMBEDDING GENERATION (BUFFER ONLY) ---
        lancedb_data = {"summary": [], "pages": []}
        try:
            logger.info(f"Generating embeddings for {filename}...")
            
            # 1. Embed Document Summary
            doc_sum_vec = genai.embed_content(
                model=EMBEDDING_MODEL,
                content=document_summary,
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=768
            )['embedding']
            
            lancedb_data["summary"].append({
                "vector": doc_sum_vec,
                "filename": filename,
                "document_summary": document_summary,
                "timestamp": timestamp_str
            })
            
            # 2. Embed Pages
            pages_payload = []
            page_meta = []
            for row in db_rows:
                text_content = f"Page {row['page_number']} Summary: {row['page_summary']}. Content: {row['page_content'][:1000]}"
                pages_payload.append(text_content)
                page_meta.append({
                    "filename": filename,
                    "page_number": int(row['page_number']),
                    "text_content": text_content
                })
            
            if pages_payload:
                page_vecs = genai.embed_content(
                    model=EMBEDDING_MODEL,
                    content=pages_payload,
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=768
                )['embedding']
                
                for i, vec in enumerate(page_vecs):
                    entry = page_meta[i]
                    entry["vector"] = vec
                    lancedb_data["pages"].append(entry)
                
            logger.info(f"Generated embeddings for {filename} (Buffered).")

        except Exception as embed_err:
            logger.error(f"Failed to generate embeddings for {filename}: {embed_err}")
            return None # Fail if embeddings fail to ensure consistency

        logger.info(f"Successfully generated data for '{filename}'.")
        
        # Return both datasets
        return {
            "mysql_rows": db_rows,
            "lancedb_rows": lancedb_data
        }
        
    except Exception as final_transform_err:
         logger.critical(f"Unexpected error during final data transformation for {filename}: {final_transform_err}", exc_info=True)
         return None


# --- Database Helper Functions (Modified and New) ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        logger.debug("Successfully connected to the database.")
        return conn
    except sqlite3.Error as err:
        logger.error(f"Error connecting to database: {err}", exc_info=True)
        raise RuntimeError(f"Database connection failed: {err}") from err

def delete_all_document_data(conn) -> bool:
    """Deletes all records from the dashboard_summary_documents table."""
    try:
        cursor = conn.cursor()
        sql = "DELETE FROM dashboard_summary_documents"
        deleted_rows = cursor.execute(sql)
        conn.commit()
        logger.info(f"Successfully deleted {deleted_rows} records from dashboard_summary_documents.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error deleting all document data: {e}", exc_info=True)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Error during rollback after delete failure: {rb_err}")
        return False


def get_processed_filenames_from_db(conn) -> set:
    """Queries the database and returns a set of distinct filenames already processed."""
    processed_files = set()
    try:
        cursor = conn.cursor()
        sql = "SELECT DISTINCT filename FROM dashboard_summary_documents"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            filename = row["filename"] if isinstance(row, sqlite3.Row) else row[0]
            if filename:
                processed_files.add(filename)
        logger.info(f"Found {len(processed_files)} previously processed filenames in the database.")
    except sqlite3.Error as e:
        logger.error(f"Error querying processed filenames from DB: {e}", exc_info=True)
    return processed_files


def insert_document_data_to_db(conn, db_rows: list[dict]) -> bool:
    """
    Inserts the generated document data rows into the database.
    Includes `document_summary`, `page_summary` and uses `page_content`.
    """
    if not db_rows:
        logger.warning("No rows provided to insert_document_data_to_db.")
        return False

    filename_for_log = db_rows[0].get('filename', 'Unknown Filename')
    # Updated columns list to include page_summary
    columns = [
        "filename", "source_path", "timestamp_utc", "model_used", "document_summary",
        "page_number", "page_content", "page_summary", "page_visual_complexity",
        "page_requires_advanced_processing", "page_requires_experimental_processing",
        "doc_has_complex_visuals", "doc_has_highly_complex_visuals",
        "doc_pages_req_advanced", "doc_pages_req_experimental",
        "prompt_suggestion_1", "prompt_suggestion_2", "prompt_suggestion_3"
    ]

    # Validate that the first row contains all expected columns to prevent later errors
    if not all(col in db_rows[0] for col in columns):
         missing_cols = [col for col in columns if col not in db_rows[0]]
         logger.error(f"Internal Error: Missing expected columns {missing_cols} in the first data row for {filename_for_log}. Available keys: {list(db_rows[0].keys())}")
         return False

    placeholders = ", ".join(["?"] * len(columns))
    # It's good practice to explicitly name the table in your environment config
    # For this example, using the name from your previous tracebacks.
    table_name = "dashboard_summary_documents"
    sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({placeholders})
    """
    
    data_to_insert = []
    for i, row_dict in enumerate(db_rows):
        # Ensure each row_dict also has the keys, or provide default if some are optional
        # (though the construction in process_document_and_generate_data should ensure all keys are present)
        current_row_tuple = []
        valid_row = True
        for col in columns:
            if col in row_dict:
                current_row_tuple.append(row_dict[col])
            else:
                # This case should ideally be prevented by the db_row_entry construction
                logger.error(f"Row {i} for {filename_for_log} is missing expected column '{col}'. Skipping this row.")
                valid_row = False
                break
        if valid_row:
            data_to_insert.append(tuple(current_row_tuple))

    if not data_to_insert:
        logger.error(f"Could not prepare any valid data tuples for insertion for {filename_for_log} after validation.")
        return False

    inserted_count = 0
    try:
        cursor = conn.cursor()
        for i, row_tuple_to_insert in enumerate(data_to_insert):
            try:
                cursor.execute(sql, row_tuple_to_insert)
                inserted_count += 1
            except Exception as insert_err:
                page_num_index = -1
                try:
                    page_num_index = columns.index('page_number')
                except ValueError:
                    pass

                page_info = ""
                if page_num_index != -1 and page_num_index < len(row_tuple_to_insert):
                    page_info = f", Page: {row_tuple_to_insert[page_num_index]}"
                
                logger.error(f"Failed to insert row {i} for {filename_for_log}{page_info}. Error: {insert_err}. Data sample (first 150 chars): {str(row_tuple_to_insert)[:150]}...")
        conn.commit()
        logger.info(f"Successfully inserted {inserted_count}/{len(data_to_insert)} rows for document: {filename_for_log}")
        # Return True only if all intended valid rows were inserted
        return inserted_count > 0 and inserted_count == len(data_to_insert)
    except Exception as e: # Catching general Exception, but pymysql.MySQLError is more specific for DB errors
        logger.error(f"Database error during insertion transaction for {filename_for_log}: {e}", exc_info=True)
        if conn and conn.open: # Check if connection is still open before rollback
            try:
                conn.rollback()
                logger.warning(f"Transaction rolled back for {filename_for_log}.")
            except Exception as rb_err:
                logger.error(f"Error during rollback for {filename_for_log}: {rb_err}")
                
def insert_data_to_lancedb(lancedb_data: dict) -> bool:
    """
    Actually inserts the buffered data into LanceDB.
    """
    try:
        if not tbl_doc_summaries or not tbl_doc_pages:
            logger.error("LanceDB tables are not initialized.")
            return False
        if lancedb_data.get("summary"):
            tbl_doc_summaries.add(lancedb_data["summary"])
        
        if lancedb_data.get("pages"):
            tbl_doc_pages.add(lancedb_data["pages"])
            
        return True
    except Exception as e:
        logger.error(f"Failed to insert into LanceDB: {e}", exc_info=True)
        return False



# --- Main Execution Block (Modified) ---
if __name__ == "__main__":
    # Logger is already set up globally now
    logger.info(f"Starting pipeline. REPLACE_OLD_CONTENT: {REPLACE_OLD_CONTENT}")
    start_time = time.time(); processed_count = 0; failed_count = 0; skipped_count = 0
    try: initialize_api_keys()
    except ValueError as e: logger.critical(f"API Key init failed: {e}"); sys.exit(1)

    db_conn = None
    try:
        db_conn = get_db_connection()
        processed_filenames = set()
        if REPLACE_OLD_CONTENT:
            if not delete_all_document_data(db_conn): logger.critical("Failed to delete old data. Aborting."); sys.exit(1)
            logger.info("All old data deleted. Processing all documents.")
        else:
            processed_filenames = get_processed_filenames_from_db(db_conn)

        files_to_process = []
        if not POLICY_DIR or not os.path.isdir(POLICY_DIR): # Added check for POLICY_DIR existence
            logger.critical(f"POLICY_DIR '{POLICY_DIR}' is invalid or not specified.")
            sys.exit(1)
            
        for item in os.listdir(POLICY_DIR):
            item_path = os.path.join(POLICY_DIR, item)
            if os.path.isfile(item_path) and Path(item).suffix.lower() in {".pdf", ".pptx"}:
                if REPLACE_OLD_CONTENT or item not in processed_filenames:
                    if os.access(item_path, os.R_OK): files_to_process.append(item_path)
                    else: logger.warning(f"Skipping '{item}' (read permission error)."); skipped_count += 1
                else: logger.debug(f"Skipping processed file: {item}"); skipped_count += 1
        
        logger.info(f"Scan complete. {len(files_to_process)} documents to process. Skipped {skipped_count}.")
        if not files_to_process: logger.info("No new documents to process.")
        else:
            for i, doc_path in enumerate(files_to_process):
                doc_name = Path(doc_path).name
                logger.info(f"--- Processing file {i+1}/{len(files_to_process)}: {doc_name} ---")
                try:
                    data_rows = process_document_and_generate_data(doc_path)
                    if data_rows and insert_document_data_to_db(db_conn, data_rows):
                        processed_count += 1; logger.info(f"Successfully processed and stored: {doc_name}")
                    else: 
                        failed_count += 1 
                        logger.error(f"Processing or DB insertion failed for: {doc_name}") # Added more specific log for this case
                except KeyboardInterrupt: 
                    logger.warning("KeyboardInterrupt. Stopping pipeline.")
                    break
                except Exception as e: 
                    failed_count += 1
                    logger.error(f"Unhandled exception for {doc_name}: {e}", exc_info=True)
    except RuntimeError as e: # Catch specific DB connection errors from get_db_connection
        logger.critical(f"Database connection runtime error: {e}", exc_info=True)
        # Not necessarily all files failed, but pipeline cannot proceed
        failed_count = len(files_to_process) - processed_count - skipped_count 
    except Exception as e: 
        logger.critical(f"Main pipeline error: {e}", exc_info=True)
        # Estimate failed count if it's an early error
        failed_count = len(files_to_process) - processed_count - skipped_count if 'files_to_process' in locals() else 0
    finally:
        if db_conn: 
            try:
                db_conn.close()
                logger.info("DB connection closed.")
            except Exception as db_close_err:
                logger.error(f"Error closing DB connection: {db_close_err}")
        logger.info(f"--- Summary --- Time: {time.time() - start_time:.2f}s, Processed: {processed_count}, Failed: {failed_count}, Skipped: {skipped_count}")
        logger.info("Pipeline finished.")
