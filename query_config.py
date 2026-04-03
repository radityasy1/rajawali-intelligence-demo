# query_config.py
"""
Production configuration with real provider and city data.
Auto-generates variations for better accuracy with minimal latency impact.
"""

# ============================================================================
# REAL PROVIDER DATA (from your database)
# ============================================================================

# All providers from both tables (normalized to lowercase for consistency)
PROVIDERS = [
    # From matpro table
    'ailink', 'aksatel', 'aldom', 'alfanet', 'aman isp', 'antas home',
    'anugerah media data nusantara', 'arka net', 'azam', 'biznet', 'bnetfit',
    'bytehome', 'cbn', 'citranet', 'comet', 'cornet', 'csnet', 'delapanbit',
    'evenet', 'firstmedia', 'fisnet', 'fm media', 'gigahome', 'giganet',
    'gnet wifi', 'gnetwork.id', 'griyanet', 'iconnet', 'idplay', 'if media',
    'indihome', 'indosat', 'internetcepat', 'ion network', 'isknet', 'jujung',
    'kafnet', 'kapten naratel', 'kharisma network', 'komdigi', 'kopas access',
    'm-net', 'mamura', 'maxnet', 'mecadata isp', 'myrepublic', 'nethome',
    'neucentrix', 'palapanet', 'patas', 'perkasa networks', 'pt litna pandia',
    'putri id', 'rosbumi.net', 'sabira', 'starlite', 'titan home',
    'tsm akses indonesia', 'viberlink', 'vision', 'xl satu', 'yamnet',
    
    # From proddetail table
    'globalxtreme'
]

# Smart variation generation for major providers
PROVIDER_VARIATIONS = {
    # Major providers with common variations
    'indihome': [
        'indihome', 'indi home', 'indi', 'telkom', 'speedy', 'telkom indonesia',
        'indyHome', 'indihom', 'telkom indihome', 'indiehome', 'indie home'
    ],
    'firstmedia': [
        'firstmedia', 'first media', 'first', 'fastnet', 'fm', 'fm media',
        'firstmedia fastnet', '1st media', 'Fm Media', 'FM Media', 'Fm media'
    ],
    'biznet': [
        'biznet', 'biznet home', 'biznet metro', 'biz', 'biznet fiber',
        'biznet wifi', 'biznetwork'
    ],
    'myrepublic': [
        'myrepublic', 'my republic', 'myrep', 'republic', 'mr',
        'my rep', 'myrepublik'
    ],
    'iconnet': [
        'iconnet', 'icon net', 'icon+', 'iconplus', 'icon', 'icon fiber',
        'iconnet fiber', 'ikon net'
    ],
    'xl satu': [
        'xl satu', 'xl', 'xl home', 'xl fiber', 'xl internet', 'xlsatu',
        'excel satu', 'xl axiata', 'x.satu', 'X.Satu', 'X satu'
    ],
    'cbn': [
        'cbn', 'cbn fiber', 'cyber', 'cybernet', 'cbn net', 'cbn wifi'
    ],
    'globalxtreme': [
        'globalxtreme', 'global xtreme', 'global extreme', 'gx', 'globalx',
        'global x', 'xtreme'
    ],
    'oxygen': [
        'oxygen', 'oxygen.id', 'oxy', 'oxygen fiber', 'oxygen wifi', 'oxigen'
    ],
    'starlite': [
        'starlite', 'starlit'
    ],
    'starlink': [
        'starlink', 'starlin'
    ],
    
    # Auto-generate for other providers (simple variations)
    'ailink': ['ailink', 'ai link', 'ailinx'],
    'aksatel': ['aksatel', 'aksa tel', 'aksatel telecom'],
    'biznet': ['biznet', 'biz net', 'biznet home'],
    'gigahome': ['gigahome', 'giga home', 'giga'],
    'giganet': ['giganet', 'giga net'],
    'neucentrix': ['neucentrix', 'neu centrix', 'neucentric'],
    'internetcepat': ['internetcepat', 'internet cepat', 'inet cepat'],
    'maxnet': ['maxnet', 'max net', 'maxnetwork'],
    'nethome': ['nethome', 'net home', 'net-home'],
    'viberlink': ['viberlink', 'viber link', 'viber'],
    'titan home': ['titan home', 'titan', 'titanhome'],
    'bytehome': ['bytehome', 'byte home', 'byte'],
    'palapanet': ['palapanet', 'palapa net', 'palapa']
}

# 1. Define the keywords that trigger the "local provider" search mode.
# We'll use this in a new intent pattern.
LOCAL_PROVIDER_KEYWORDS = [
    r'provider lokal',
    r'isp lokal',
    r'penyedia lokal',
    r'lokal provider',
    r'rtrw',
    r'lco',
    r'local provider',
    r'local cable operator',
    r'lokal isp',
    r'reseller',
    r'reseler'
]

# 2. Define the list of "National" providers to be excluded.
# Use the CANONICAL names (lowercase) that you use throughout your system.
# This list is now the single source of truth for who is considered a national provider.
NATIONAL_PROVIDER_EXCLUSIONS = {
    'indihome',
    'biznet',
    'xlhome', 
    'xlsatu',
    'xl satu',
    'x.satu',
    'xl axiata',
    'iconnet',
    'myrepublic',
    'indosat',
    'firstmedia',
    'fm media',
    'cbn',
    'starlink'
    # Add any other providers you consider national here.
}

# ============================================================================
# REAL CITY DATA (from your database) 
# ============================================================================

# All cities from your database (normalized and cleaned)
KNOWN_CITIES = {
    # Major cities and their regency counterparts are now distinct
    'yogyakarta', 'kota yogyakarta',
    'semarang', 'kota semarang',
    'surabaya', 'kota surabaya',
    'malang', 'kota malang',
    'denpasar', 'kota denpasar',
    'blitar', 'kota blitar',
    'batu', 'kota batu',
    'surakarta', 'kota surakarta',
    'kediri', 'kota kediri',
    'magelang', 'kota magelang',
    'tegal', 'kota tegal',
    'mataram', 'kota mataram',
    'madiun', 'kota madiun',
    'kupang', 'kota kupang',
    'mojokerto', 'kota mojokerto',
    'probolinggo', 'kota probolinggo',
    'pekalongan', 'kota pekalongan',
    'pasuruan', 'kota pasuruan',
    'salatiga', 'kota salatiga',
    'bima', 'kota bima',
    
    # Regencies and areas
    'jember', 'sumenep', 'banyumas', 'sumbawa barat', 'karang asem', 'bangkalan',
    'situbondo', 'belu', 'rote ndao', 'jepara', 'lamongan', 'gresik',
    'purbalingga', 'banyuwangi', 'tuban', 'batang', 'malaka',
    'bojonegoro', 'banjarnegara', 'ngada', 'brebes', 'sukoharjo', 'bangli',
    'blora', 'kendal', 'bondowoso', 'wonogiri', 'bantul', 'tabanan',
    'boyolali', 'tulungagung', 'cilacap', 'pemalang', 'mojokerto',
    'klaten', 'demak', 'dompu', 'trenggalek', 'sumbawa', 'ende', 'grobogan',
    'sidoarjo', 'lombok barat', 'magetan', 'gianyar', 'jembrana', 'probolingga',
    'sleman', 'kebumen', 'nganjuk', 'ponorogo', 'ngawi',
    'jombang', 'lumajang', 'pati', 'karanganyar', 'timor tengah utara',
    'alor', 'sampang', 'purworejo', 'kudus', 'manggarai barat',
    'pacitan', 'flores timur', 'rembang', 'buleleng', 'lembata', 'lombok timur',
    'sikka', 'timor tengah selatan', 'pamekasan', 'temanggung',
    'lombok tengah', 'manggarai', 'manggarai timur', 'sabu raijua',
    'lombok utara', 'klungkung', 'sragen', 'sumba timur', 'sumba barat',
    'gunung kidul', 'wonosobo', 'kulon progo',
    
    # Common abbreviations and variations 
    'jogja', 'yogya', 'sby', 'mlg', 'jkt', 'bdg', 'mdn', 'solo', 'dps'
}

# Smart city aliases with space variations and common patterns
LOCATION_ALIASES = {
    # Major city variations
    'jogja': 'yogyakarta',
    'yogya': 'yogyakarta', 
    'sby': 'surabaya',
    'mlg': 'malang',
    'solo': 'surakarta',
    'dps': 'denpasar',
    'smg': 'semarang',
    'jkt': 'jakarta',  # Not in your data but common
    'bdg': 'bandung',  # Not in your data but common
    'mdn': 'medan',    # Not in your data but common
    
    # Regional variations with spaces
    'sumbawa bar': 'sumbawa barat',
    'lombok bar': 'lombok barat',
    'lombok tim': 'lombok timur',
    'lombok ten': 'lombok tengah',
    'lombok ut': 'lombok utara',
    'manggarai bar': 'manggarai barat',
    'manggarai tim': 'manggarai timur',
    'timor tengah ut': 'timor tengah utara',
    'timor tengah sel': 'timor tengah selatan',
    'gunung kid': 'gunung kidul',
    'kulon prog': 'kulon progo',
    
    # === SPACE VARIATION MAPPINGS ===
    # Handle cities/regencies written without spaces
    'kulonprogo': 'kulon progo',                    # kulon progo → kulonprogo
    'gunungkidul': 'gunung kidul',                  # gunung kidul → gunungkidul
    'sumbawabarat': 'sumbawa barat',                # sumbawa barat → sumbawabarat
    'lombardbarat': 'lombok barat',                 # lombok barat → lombardbarat
    'lombardtimur': 'lombok timur',                 # lombok timur → lombardtimur
    'lombardtengah': 'lombok tengah',               # lombok tengah → lombardtengah
    'lombardutara': 'lombok utara',                 # lombok utara → lombardutara
    'manggaraibarat': 'manggarai barat',           # manggarai barat → manggaraibarat
    'manggaraitimur': 'manggarai timur',           # manggarai timur → manggaraitimur
    'timortengahutara': 'timor tengah utara',      # timor tengah utara → timortengahutara
    'timortengahselatan': 'timor tengah selatan',  # timor tengah selatan → timortengahselatan
    'karangasem': 'karang asem',                    # karang asem → karangasem
    'rotendao': 'rote ndao',                        # rote ndao → rotendao
    'saburaijua': 'sabu raijua',                    # sabu raijua → saburaijua
    'florestabltimur': 'flores timur',              # flores timur → florestabltimur
    'sumbatimur': 'sumba timur',                    # sumba timur → sumbatimur
    'sumbabarat': 'sumba barat',                    # sumba barat → sumbabarat
    
    # Common alternative spellings
    'kulonprogo': 'kulon progo',                    # Alternative: kulon progo
    'gunungkid': 'gunung kidul',                    # Alternative: gunung kidul  
    'lombardbar': 'lombok barat',                   # Alternative: lombok barat
    'lombardtim': 'lombok timur',                   # Alternative: lombok timur
    'manggaraibar': 'manggarai barat',             # Alternative: manggarai barat
    'manggaraitim': 'manggarai timur',             # Alternative: manggarai timur
    
    # Social media / informal variations
    'kulprog': 'kulon progo',                       # kulprog (social media style)
    'gunkid': 'gunung kidul',                       # gunkid (informal)
    'lombokbar': 'lombok barat',                    # lombokbar (informal)
    'lomboktim': 'lombok timur',                    # lomboktim (informal)
    'mangbar': 'manggarai barat',                   # mangbar (informal)
    'mangtim': 'manggarai timur'                    # mangtim (informal)
}

# ============================================================================
# ENHANCED PATTERN CONFIGURATIONS
# ============================================================================

# Intent detection patterns (optimized for Indonesian queries)
INTENT_PATTERNS = {
    'location_inquiry': [
        r'\b(?:ada\s+di|tersedia\s+di|di\s+mana|kota\s+mana|wilayah\s+mana|area\s+mana)\b',
        r'\bmana\s+saja\b',
        r'\b(?:daftar|list)\s+(?:kota|wilayah|area)\b',
        r'\btersedia\s+(?:dimana|di\s+mana)\b',
        r'\bavailable\s+(?:in|where)\b',
        r'\bcoverage\s+area\b',
        r'\bjangkauan\s+(?:dimana|di\s+mana)\b',
        r'\bmelayani\s+(?:dimana|daerah\s+mana)\b'
    ],
    
    'price_inquiry': [
        r'\b(?:harga|tarif|biaya|berapa)\b',
        r'\brp\.?\s*\d+',
        r'\b(?:murah|mahal|terjangkau|expensive|cheap)\b',
        r'\bprice\s+(?:list|range)\b',
        r'\bcost\b',
        r'\bbudget\b',
        r'\bbayar\s+berapa\b',
        r'\bbiaya\s+bulanan\b'
    ],
    
    'speed_inquiry': [
        r'\b(?:kecepatan|speed)\s+(?:berapa|mana\s+yang)\b',
        r'\b(?:lebih\s+cepat|faster|slower|lambat)\b',
        r'\bcompare\s+speed\b',
        r'\bbandwidth\b',
        r'\bthroughput\b',
        r'\bseberapa\s+cepat\b',
        r'\bkecepatan\s+maksimal\b'
    ],
    
    'comparison': [
        r'\b(?:bandingkan|compare|vs|versus)\b',
        r'\bmana\s+yang\s+(?:lebih\s+)?(?:baik|bagus|recommended)\b',
        r'\bpilih\s+mana\b',
        r'\bbetter\b',
        r'\bwhich\s+(?:is|one)\b',
        r'\blebih\s+(?:baik|bagus|murah|cepat)\b',
        r'\bbeda\s+(?:apa|nya)\b'
    ],
    
    'availability_check': [
        r'\b(?:ada|tersedia|available)\s+(?:tidak|nggak|ga|ndak)?\b',
        r'\bapakah\s+(?:ada|tersedia)\b',
        r'\bcan\s+(?:get|have)\b',
        r'\bbisa\s+(?:dapat|dapetin|langganan)\b',
        r'\bmelayani\s+(?:tidak|nggak|ga)?\b'
    ],
    
    'recommendation': [
        r'\b(?:rekomen|recommend|saran|suggest)\b',
        r'\byang\s+bagus\b',
        r'\bterbaik\b',
        r'\bbest\b',
        r'\btop\s+(?:choice|pick)\b',
        r'\bpaling\s+(?:bagus|baik|cepat|murah)\b',
        r'\bcocok\s+(?:untuk|buat)\b'
    ],

    "all_providers_inquiry": [
        r'seluruh provider',
        r'semua provider',
        r'semua isp',
        r'seluruh isp',
        r'all providers'
    ],

    "local_provider_inquiry": LOCAL_PROVIDER_KEYWORDS
}

# Enhanced speed patterns for Indonesian context
SPEED_PATTERNS = [
    r'(?:kecepatan\s*)?(\d+)\s*(?:mbps|mb/s|mega)',
    r'(\d+)\s*(?:mbps|mb/s)',
    r'speed\s*(\d+)',
    r'(\d+)\s*mega(?:bit)?',
    r'(\d+)\s*(?:mb|mega)\s*(?:per\s+)?(?:second|detik)',
    r'kecepatan\s+(\d+)',
    r'hingga\s+(\d+)\s*(?:mbps|mb)',
    r'sampai\s+(\d+)\s*(?:mbps|mb)'
]

# Dynamic provider pattern (built from PROVIDERS list)
PROVIDER_PATTERN = r'\b(' + '|'.join(PROVIDERS + [var for variations in PROVIDER_VARIATIONS.values() for var in variations]) + r')\b'

# Enhanced location patterns
LOCATION_PATTERNS = [
    r'\b(?:di|kota|area|wilayah|daerah|lokasi)\s+([a-zA-Z\s\-]{3,25})(?:\s|$|\.|\?|,)',
    r'\bdi\s+([a-zA-Z\s\-]{3,20})(?:\s|$|\.|\?)',
    r'\bkota\s+([a-zA-Z\s\-]{3,20})(?:\s|$|\.|\?)',
    r'\barea\s+([a-zA-Z\s\-]{3,20})(?:\s|$|\.|\?)',
    r'\bwilayah\s+([a-zA-Z\s\-]{3,20})(?:\s|$|\.|\?)',
    r'\bdaerah\s+([a-zA-Z\s\-]{3,20})(?:\s|$|\.|\?)'
]

# ============================================================================
# ENHANCED KEYWORD CONFIGURATIONS
# ============================================================================

# Comprehensive stop words (Indonesian + English)
STOP_WORDS = {
    # Indonesian common words
    'di', 'kota', 'area', 'wilayah', 'daerah', 'provider', 'dan', 'atau', 'untuk', 'dari',
    'bagaimana', 'dengan', 'ada', 'mana', 'saja', 'yang', 'adalah', 'ini', 'itu',
    'berapa', 'harga', 'paket', 'promo', 'kecepatan', 'mbps', 'speed', 'gimana',
    'kayak', 'apa', 'apakah', 'dimana', 'kapan', 'kenapa', 'seperti', 'juga',
    'bisa', 'tidak', 'nggak', 'ndak', 'enggak', 'gak', 'ga', 'kan', 'dong',
    'mau', 'mau', 'nya', 'lagi', 'deh', 'sih', 'tuh', 'tapi', 'kalo', 'kalau',
    
    # English common words  
    'the', 'a', 'an', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
    'by', 'about', 'how', 'what', 'where', 'when', 'why', 'which', 'who', 'is',
    'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did',
    'will', 'would', 'could', 'should', 'can', 'may', 'might', 'must', 'shall'
}

# Enhanced feature keywords
FEATURE_KEYWORDS = {
    # Connection types
    'unlimited', 'fiber', 'fibernet', 'dedicated', 'shared', 'wireless', 'wifi',
    'cable', 'adsl', 'vdsl', 'broadband', 'internet',
    
    # User types
    'residential', 'business', 'corporate', 'home', 'office', 'personal',
    'enterprise', 'commercial', 'soho', 'family',
    
    # Usage types
    'gaming', 'streaming', 'download', 'upload', 'video', 'conference',
    'zoom', 'netflix', 'youtube', 'online', 'work', 'study',
    
    # Quality descriptors
    'stabil', 'stable', 'cepat', 'fast', 'murah', 'cheap', 'premium',
    'basic', 'advanced', 'professional', 'reliable', 'konsisten',
    'lancar', 'smooth', 'jernih', 'clear', 'bagus', 'good'
}

# ============================================================================
# PROVIDER EXTRACTION EXCLUSIONS
# ============================================================================

# Words to exclude from provider extraction (prevents false positives)
PROVIDER_EXTRACTION_EXCLUSIONS = [
    # Generic terms that shouldn't be treated as provider names
    'provider', 'internet', 'wifi', 'isp', 'network', 'broadband',
    
    # Indonesian question words
    'yang', 'apa', 'mana', 'siapa', 'dimana', 'bagaimana', 'kenapa',
    
    # Common words that might appear near provider names
    'jual', 'jualan', 'service', 'layanan', 'paket', 'plan', 'bundle',
    'fiber', 'cable', 'wireless', 'home', 'business', 'unlimited',
    
    # Question indicators
    'ada', 'tersedia', 'available', 'berapa', 'harga', 'price', 'cost',
    
    # Location indicators  
    'kota', 'area', 'wilayah', 'daerah', 'region', 'coverage'
]

# ============================================================================
# PERFORMANCE OPTIMIZATIONS
# ============================================================================

# Performance limits (slightly adjusted for better accuracy)
PERFORMANCE_LIMITS = {
    'max_keywords': 4,           # Increased for better matching
    'max_providers': 6,          # Handle multiple provider queries
    'max_speeds': 4,             # Multiple speed comparisons
    'max_locations': 4,          # Multiple location queries
    'min_keyword_length': 3,     # Minimum meaningful keyword
    'max_keyword_length': 25,    # Handle long provider names
    'speed_range_min': 1,        # Minimum valid speed (mbps)
    'speed_range_max': 10000,    # Maximum valid speed (mbps)
    'fuzzy_match_threshold': 0.8 # For advanced fuzzy matching
}

# Search mode configurations
SEARCH_MODES = {
    'location_discovery': {
        'description': 'User asking which cities/areas are available',
        'exclude_location_filter': True,
        'search_type': 'inclusive',
        'max_keywords': 3,
        'boost_provider_match': True
    },
    'provider_discovery': {
        'description': 'User asking which providers are available',
        'exclude_provider_filter': True,        # Don't filter by provider!
        'exclude_location_filter': False,       # Keep location filter
        'search_type': 'discovery',
        'max_keywords': 2,
        'boost_location_match': True
    },
    'comparison': {
        'description': 'Comparing providers or plans', 
        'exclude_location_filter': False,
        'search_type': 'broad',
        'max_keywords': 4,
        'allow_speed_range': True
    },
    'price_focused': {
        'description': 'Focus on pricing information',
        'exclude_location_filter': False, 
        'search_type': 'standard',
        'max_keywords': 3,
        'prioritize_price_fields': True
    },
    'standard': {
        'description': 'Regular search query',
        'exclude_location_filter': False,
        'search_type': 'standard', 
        'max_keywords': 4,
        'balanced_matching': True
    }
}

# ============================================================================
# DYNAMIC LOOKUP BUILDING
# ============================================================================

def build_provider_lookup():
    """Build comprehensive provider lookup with fuzzy matching."""
    lookup = {}
    
    # Add exact variations
    for canonical, variations in PROVIDER_VARIATIONS.items():
        for variation in variations:
            lookup[variation.lower()] = canonical
    
    # Add base providers
    for provider in PROVIDERS:
        lookup[provider.lower()] = provider.lower()
    
    # Auto-generate common variations for providers not in PROVIDER_VARIATIONS
    for provider in PROVIDERS:
        if provider not in PROVIDER_VARIATIONS:
            # Generate basic variations
            variations = [provider]
            if ' ' in provider:
                # "First Media" -> "firstmedia", "first"
                variations.append(provider.replace(' ', ''))
                variations.append(provider.split()[0])
            if len(provider) > 6:
                # Generate abbreviation for long names
                words = provider.split()
                if len(words) > 1:
                    abbrev = ''.join([w[0] for w in words])
                    variations.append(abbrev)
            
            for var in variations:
                lookup[var.lower()] = provider.lower()
    
    return lookup

def build_location_lookup():
    """Build comprehensive location lookup with smart matching."""
    lookup = LOCATION_ALIASES.copy()
    
    # Add all known cities
    for city in KNOWN_CITIES:
        lookup[city.lower()] = city.lower()
    
    # Generate abbreviations for multi-word cities
    for city in KNOWN_CITIES:
        if ' ' in city and len(city.split()) == 2:
            words = city.split()
            # "lombok barat" -> "lombok bar", "lombar"
            if len(words[1]) >= 4:
                abbrev = f"{words[0]} {words[1][:3]}"
                lookup[abbrev] = city
    
    return lookup

# Pre-built lookups for runtime performance
PROVIDER_LOOKUP = build_provider_lookup()
LOCATION_LOOKUP = build_location_lookup()

# ============================================================================
# VERSION AND METADATA
# ============================================================================

CONFIG_VERSION = "2.1"
CONFIG_LAST_UPDATED = "2025-07-15"

# Track changes for maintenance
CHANGE_LOG = [
    "2025-07-14: Initial configuration with real provider/city data",
    "2025-07-14: Added smart variation generation for providers and cities",
    "2025-07-14: Enhanced fuzzy matching capabilities",
    "2025-07-15: Refined location handling to distinguish City/Regency and fixed REGEXP compatibility."
]