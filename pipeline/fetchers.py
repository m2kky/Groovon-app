"""
pipeline.fetchers — All API fetchers, verification, profile builder, deep scraper, validators.

Functions extracted from process_david_excel.py lines 41–1191.
Zero logic changes — pure extraction.
"""
import json, urllib.request, urllib.parse, time, logging, re, base64, threading, socket, html
from difflib import SequenceMatcher
from pipeline.config import (
    SPOTIFY_ID, SPOTIFY_SECRET, LASTFM_API_KEY, YOUTUBE_API_KEY,
    DISCOGS_TOKEN, GENIUS_TOKEN, GOOGLE_KG_KEY, SERPER_KEY,
    SETLISTFM_KEY, SCRAPINGBEE_KEY,
    BANDSINTOWN_APP_ID, SEATGEEK_CLIENT_ID, TICKETMASTER_KEY,
    SOUNDCLOUD_CLIENT_ID,
    SSL_CTX, GENRES, validate_genre, best_genre,
    classify_url, MB_LINK_MAP, normalize,
)

log = logging.getLogger(__name__)

# ── Per-Domain Rate Limiter (v8) ───────────────────────────────────────────────
# Thread-safe: one lock per domain, enforces minimum interval between calls.
_DOMAIN_RATE_LIMITS: dict[str, float] = {
    "api.spotify.com":       0.15,   # ~6.5 req/s (Spotify allows ~10/s)
    "musicbrainz.org":       1.1,    # MB enforces 1 req/s strictly
    "ws.audioscrobbler.com": 0.25,   # Last.fm
    "itunes.apple.com":      0.20,
    "api.discogs.com":       1.05,   # Discogs: 60/min (auth) → 1/s safe
    "www.googleapis.com":    0.12,   # YouTube Data + KG
    "api.genius.com":        0.20,
    "api.setlist.fm":        0.55,   # Setlist.fm: 2 req/s
    "rest.bandsintown.com":  0.35,
    "api.deezer.com":        0.22,   # Deezer: 50 req/5s → 10/s
    "api.soundcloud.com":    0.30,
    "google.serper.dev":     0.25,
}
_DEFAULT_RATE_LIMIT = 0.10           # 10 req/s for unknown domains

_domain_last_call: dict[str, float] = {}
_domain_locks: dict[str, threading.Lock] = {}
_domain_meta_lock = threading.Lock()

def _rate_limit(domain: str) -> None:
    """Sleep if needed to respect per-domain rate limit. Thread-safe."""
    with _domain_meta_lock:
        if domain not in _domain_locks:
            _domain_locks[domain] = threading.Lock()
    lock = _domain_locks[domain]
    interval = _DOMAIN_RATE_LIMITS.get(domain, _DEFAULT_RATE_LIMIT)
    with lock:
        now = time.monotonic()
        last = _domain_last_call.get(domain, 0.0)
        wait = interval - (now - last)
        if wait > 0:
            time.sleep(wait)
        _domain_last_call[domain] = time.monotonic()

def _extract_domain(url: str) -> str:
    """Extract hostname from URL for rate-limit lookup."""
    try:
        return urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return ""

# ── Artist Cache (v7) ──────────────────────────────────────────────────────────
_artist_cache: dict = {}

# ── API Retry Wrapper (v8) ─────────────────────────────────────────────────────
def _api_call(fn, retries=3, base_delay=1.0, url: str | None = None):
    """Run fn() with exponential backoff on transient network errors.
    Returns fn() result or None after retries exhausted.
    Does NOT retry on 4xx HTTP errors (bad request / not found).
    If `url` is given, enforces per-domain rate limiting before each attempt."""
    domain = _extract_domain(url) if url else ""
    for attempt in range(retries):
        try:
            if domain:
                _rate_limit(domain)
            return fn()
        except Exception as e:
            err = str(e).lower()
            # Don't retry client errors
            if any(code in err for code in ("http error 4", "404", "403", "401", "400")):
                return None
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt)  # 1s → 2s → 4s
                log.debug(f"API retry {attempt+1}/{retries} in {delay:.1f}s: {e}")
                time.sleep(delay)
            else:
                log.warning(f"API failed after {retries} attempts: {e}")
    return None

# ── Fuzzy Artist Picker (v7) ───────────────────────────────────────────────────
_STRIP_PREFIXES = ("dj ", "mc ", "the ", "dj. ", "dj/ ")


def _clean_name_for_match(text: str) -> str:
    """Normalize and strip common artist prefixes for safer matching."""
    value = normalize(text or "")
    for pfx in _STRIP_PREFIXES:
        value = value.removeprefix(pfx)
    return value.strip()


def _token_set(text: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", _clean_name_for_match(text)) if t}


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _clean_name_for_match(a), _clean_name_for_match(b)).ratio()


def _token_overlap(a: str, b: str) -> float:
    ta = _token_set(a)
    tb = _token_set(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))

def _fuzzy_pick(candidates: list, query: str) -> dict:
    """Pick the best artist from a list of API candidates.

    Match priority (highest wins):
      1. Exact case-insensitive match
      2. Unicode-normalized exact match  (Tiësto → tiesto)
      3. Prefix-stripped match           (DJ Tiësto → tiësto → tiesto)
      4. Substring match                 (query in name or name in query)
      5. Similarity guard (reject ambiguous fallback)

    Args:
        candidates: list of dicts with at least {"name": str}
        query: the original artist name we searched for
    Returns:
        best matching dict from candidates
    """
    if not candidates:
        return {}

    q_lower = query.lower().strip()
    q_norm  = normalize(query)           # unicode → ascii lowercase (from config)
    q_clean = q_norm
    for pfx in _STRIP_PREFIXES:
        q_clean = q_clean.removeprefix(pfx)

    # Pass 1 — exact lowercase match
    for a in candidates:
        if a.get("name", "").lower().strip() == q_lower:
            return a

    # Pass 2 — unicode normalized exact
    for a in candidates:
        if normalize(a.get("name", "")) == q_norm:
            return a

    # Pass 3 — prefix-stripped match
    for a in candidates:
        a_clean = normalize(a.get("name", ""))
        for pfx in _STRIP_PREFIXES:
            a_clean = a_clean.removeprefix(pfx)
        if a_clean == q_clean and a_clean:  # guard against empty strings
            return a

    # Pass 4 — substring (query contains name or name contains query)
    for a in candidates:
        a_norm = normalize(a.get("name", ""))
        if q_norm and a_norm and (q_norm in a_norm or a_norm in q_norm):
            return a

    # Pass 5 — similarity guard (prefer precision over recall)
    best = None
    best_ratio = 0.0
    best_overlap = 0.0
    for cand in candidates:
        cand_name = cand.get("name", "")
        ratio = _name_similarity(query, cand_name)
        overlap = _token_overlap(query, cand_name)
        if ratio > best_ratio:
            best_ratio = ratio
            best_overlap = overlap
            best = cand

    # Accept only if it's a strong enough match.
    # This reduces false positives for ambiguous/common artist names.
    if best and (best_ratio >= 0.86 or (best_ratio >= 0.75 and best_overlap >= 0.7)):
        return best

    return {}

# ── Spotify Auth ──
_spotify_token = None
_spotify_token_exp = 0

def _spotify_auth():
    global _spotify_token, _spotify_token_exp
    if _spotify_token and time.time() < _spotify_token_exp - 60:
        return _spotify_token
    if not SPOTIFY_ID or not SPOTIFY_SECRET:
        return None
    try:
        creds = base64.b64encode(f"{SPOTIFY_ID}:{SPOTIFY_SECRET}".encode()).decode()
        req = urllib.request.Request(
            "https://accounts.spotify.com/api/token",
            data=b"grant_type=client_credentials",
            headers={"Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            d = json.loads(r.read())
        _spotify_token = d["access_token"]
        _spotify_token_exp = time.time() + d.get("expires_in", 3600)
        return _spotify_token
    except Exception as e:
        log.warning(f"Spotify auth failed: {e}")
        return None

# ── Spotify Search ──

def spotify_search(name):
    """Search Spotify for an artist.
    Returns: {name, spotify_url, spotify_genres, spotify_image,
               spotify_followers, spotify_popularity} or None.
    v7: uses _api_call() retry + _fuzzy_pick() matching."""
    tok = _spotify_auth()
    if not tok:
        return None

    def _fetch():
        q = urllib.parse.quote(name)
        url = f"https://api.spotify.com/v1/search?q={q}&type=artist&limit=5"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            return json.loads(r.read()).get("artists", {}).get("items", [])

    items = _api_call(_fetch) or []
    if not items:
        return None

    best = _fuzzy_pick(items, name)
    if not best:
        return None

    return {
        "name":               best["name"],
        "spotify_url":        best["external_urls"].get("spotify"),
        "spotify_genres":     best.get("genres", []),
        "spotify_image":      best["images"][0]["url"] if best.get("images") else None,
        "spotify_followers":  best.get("followers", {}).get("total"),
        "spotify_popularity": best.get("popularity"),
    }

# ── Deezer Search ──

def deezer_search(name):
    """Deezer: artist url, fans count.
    v7: uses _api_call() retry + _fuzzy_pick() matching."""

    def _fetch():
        q = urllib.parse.quote(name)
        url = f"https://api.deezer.com/search/artist?q={q}&limit=5"
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read()).get("data", [])

    items = _api_call(_fetch) or []
    if not items:
        return None

    best = _fuzzy_pick(items, name)
    if not best:
        return None

    return {
        "name":         best["name"],
        "deezer_url":   best.get("link"),
        "deezer_fans":  best.get("nb_fan"),
        "deezer_image": best.get("picture_xl") or best.get("picture_big"),
    }

# ── MusicBrainz Search ──

def musicbrainz_search(name):
    """MusicBrainz: MBID, tags, area, country, type (for verification).
    Rate-limited to 1 req/sec."""
    try:
        q = urllib.parse.quote(name)
        url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{q}&limit=5&fmt=json"
        req = urllib.request.Request(url, headers={
            "User-Agent": "GroovonScraper/1.0 (groovon.com)",
            "Accept": "application/json"
        })
        time.sleep(1.1)  # MusicBrainz rate limit
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            data = json.loads(r.read())
        artists = data.get("artists", [])
        if not artists:
            return None
        # Prefer exact match
        best = artists[0]
        for a in artists:
            if a.get("name", "").lower().strip() == name.lower().strip():
                best = a
                break
            # Also check sort-name
            if a.get("sort-name", "").lower().strip() == name.lower().strip():
                best = a
                break
        return {
            "name": best.get("name"),
            "mb_id": best.get("id"),
            "mb_score": best.get("score"),
            "mb_type": best.get("type"),  # Person, Group, etc.
            "mb_area": best.get("area", {}).get("name"),
            "mb_country": best.get("country"),
            "mb_begin_area": best.get("begin-area", {}).get("name"),
            "mb_tags": [t["name"] for t in best.get("tags", [])[:5]],
            "mb_isnis": best.get("isnis", []),
            "mb_disambiguation": best.get("disambiguation"),
        }
    except:
        return None

# ── Multi-Platform Verification ──

def verify_multi_platform(name):
    """Verify artist across Spotify + Deezer + MusicBrainz.
    Returns merged verification dict or None.
    v7: results cached by normalized name to avoid duplicate API calls."""
    cache_key = normalize(name)
    if cache_key in _artist_cache:
        log.debug(f"Cache hit: {name!r}")
        return _artist_cache[cache_key]

    sp = spotify_search(name)
    dz = deezer_search(name)
    mb = musicbrainz_search(name)

    if not sp and not dz and not mb:
        _artist_cache[cache_key] = None
        return None

    vf = {
        "name": name,
        "verified": False,
        "platforms_found": 0,
        "confidence": 0,
        "sources": [],
    }

    platforms = 0
    if sp:
        vf.update(sp)
        vf["sources"].append("spotify")
        platforms += 1
    if dz:
        vf["deezer_url"]   = dz.get("deezer_url")
        vf["deezer_fans"]  = dz.get("deezer_fans")
        if not vf.get("name") or vf["name"] == name:
            vf["name"] = dz.get("name", name)
        vf["sources"].append("deezer")
        platforms += 1
    if mb:
        vf["mb_id"]             = mb.get("mb_id")
        vf["mb_score"]          = mb.get("mb_score")
        vf["mb_type"]           = mb.get("mb_type")
        vf["mb_area"]           = mb.get("mb_area")
        vf["mb_country"]        = mb.get("mb_country")
        vf["mb_tags"]           = mb.get("mb_tags", [])
        vf["mb_disambiguation"] = mb.get("mb_disambiguation")
        vf["sources"].append("musicbrainz")
        platforms += 1

    vf["platforms_found"] = platforms
    vf["verified"]        = platforms >= 2
    vf["confidence"]      = _calc_confidence(vf, name)

    _artist_cache[cache_key] = vf
    return vf


def _calc_confidence(vf, original_name):
    """Calculate verification confidence score (0-100)."""
    score = 0
    
    # Name match quality
    verified_name = vf.get("name", "").lower().strip()
    original = original_name.lower().strip()
    if verified_name == original:
        score += 30
    elif normalize(verified_name) == normalize(original):
        score += 25
    elif original in verified_name or verified_name in original:
        score += 15
    
    # Platform presence
    if vf.get("spotify_url"):
        score += 15
    if vf.get("deezer_url"):
        score += 10
    if vf.get("mb_id"):
        score += 10
    
    # Engagement signals
    followers = vf.get("spotify_followers", 0) or 0
    if followers > 100000:
        score += 15
    elif followers > 10000:
        score += 10
    elif followers > 1000:
        score += 5
    
    popularity = vf.get("spotify_popularity", 0) or 0
    if popularity > 50:
        score += 10
    elif popularity > 20:
        score += 5
    
    # Genre data available
    if vf.get("spotify_genres"):
        score += 5
    if vf.get("mb_tags"):
        score += 5
    
    return min(score, 100)


# ── URL Verification ──

def verify_url(url, timeout=6):
    """Quick HEAD/GET check to verify a URL is alive. Returns final URL or None."""
    try:
        req = urllib.request.Request(url, method="HEAD", headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
            return r.url  # follows redirects
    except:
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
                return r.url
        except:
            return None

# ── Email Validation ──

def validate_email(email, timeout=5):
    """Basic email validation: format + MX record check."""
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return False
    domain = email.split('@')[1]
    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(domain, 25)
        return True
    except:
        return False

# ── MusicBrainz Links ──

_mb_lock = threading.Lock()
_mb_last_call = 0.0

def get_mb_links(mbid):
    """Get URL relationships from MusicBrainz for an artist MBID.
    Rate-limited via lock + sleep. Returns {platform: url} dict."""
    global _mb_last_call
    with _mb_lock:
        elapsed = time.time() - _mb_last_call
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        _mb_last_call = time.time()
    
    try:
        url = f"https://musicbrainz.org/ws/2/artist/{mbid}?inc=url-rels&fmt=json"
        req = urllib.request.Request(url, headers={
            "User-Agent": "GroovonScraper/1.0 (groovon.com)",
            "Accept": "application/json"
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=8) as r:
            data = json.loads(r.read())
        
        links = {}
        relations = data.get("relations", [])
        for rel in relations:
            if rel.get("type") == "url" or rel.get("target-type") == "url":
                link_url = rel.get("url", {}).get("resource", "")
                rel_type = rel.get("type", "").lower()
                
                if not link_url:
                    continue
                
                # Try to classify by relation type first, then by URL pattern
                category = MB_LINK_MAP.get(rel_type)
                if not category:
                    category = classify_url(link_url)
                
                # Only keep first of each category (best match)
                if category not in links:
                    links[category] = link_url
        
        return links if links else None
    except:
        return None

# ── Official Website Scraper ──

# Patterns for extracting links from HTML
_SOCIAL_PATTERNS = {
    "instagram": re.compile(r'https?://(?:www\.)?instagram\.com/[a-zA-Z0-9_.]+/?', re.I),
    "facebook": re.compile(r'https?://(?:www\.)?facebook\.com/[a-zA-Z0-9_.]+/?', re.I),
    "twitter": re.compile(r'https?://(?:www\.)?(?:twitter|x)\.com/[a-zA-Z0-9_]+/?', re.I),
    "tiktok": re.compile(r'https?://(?:www\.)?tiktok\.com/@[a-zA-Z0-9_.]+/?', re.I),
    "youtube": re.compile(r'https?://(?:www\.)?youtube\.com/(?:c/|channel/|@)[a-zA-Z0-9_-]+/?', re.I),
    "spotify": re.compile(r'https?://open\.spotify\.com/artist/[a-zA-Z0-9]+', re.I),
    "apple_music": re.compile(r'https?://music\.apple\.com/[a-z]{2}/artist/[a-zA-Z0-9-]+/\d+', re.I),
    "soundcloud": re.compile(r'https?://(?:www\.)?soundcloud\.com/[a-zA-Z0-9_-]+/?', re.I),
    "bandcamp": re.compile(r'https?://[a-zA-Z0-9-]+\.bandcamp\.com/?', re.I),
}
_EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
_META_DESC = re.compile(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', re.I)
_TITLE_TAG = re.compile(r'<title>(.*?)</title>', re.I | re.S)

# Keywords for discovering internal pages worth scraping
_INTERNAL_PAGE_KEYWORDS = re.compile(
    r'(?:^|/)(?:about|bio|contact|booking|press|epk|electronic.?press|'
    r'events|gigs|tour|tours|shows|live|dates|'
    r'links|music|listen)/?$', re.I
)
_EVENTS_PAGE_KEYWORDS = re.compile(r'(?:^|/)(?:events|gigs|tour|tours|shows|live|dates)/?$', re.I)

# Patterns for email context classification
_EMAIL_CONTEXT_PATTERNS = {
    "booking": re.compile(r'(?:book(?:ing)?|gig|live|perform)', re.I),
    "management": re.compile(r'(?:manag|mgmt|represent)', re.I),
    "press": re.compile(r'(?:press|media|pr\b|publicity|journalist)', re.I),
}

# Patterns for management/agent extraction
_MGMT_PATTERN = re.compile(
    r'(?:managed?\s+by|management[:\s]+|booking\s+(?:agent|agency)[:\s]+|'
    r'represented\s+by|booked?\s+(?:through|via|by))\s*[:\-]?\s*([A-Z][A-Za-z\s&.\'-]{2,50})',
    re.I
)

# Simple HTML tag stripper
_HTML_TAGS = re.compile(r'<[^>]+>')
_MULTI_SPACE = re.compile(r'\s{2,}')


def _fetch_page(url, timeout=8):
    """Fetch a single page and return HTML string, or None on failure."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
            ct = r.headers.get("Content-Type", "")
            if "text/html" not in ct and "xhtml" not in ct:
                return None
            return r.read().decode("utf-8", errors="ignore")
    except:
        return None


def _strip_html(html_text):
    """Strip HTML tags and collapse whitespace → plain text."""
    text = _HTML_TAGS.sub(' ', html_text)
    text = html.unescape(text) if hasattr(html, 'unescape') else text
    text = _MULTI_SPACE.sub(' ', text).strip()
    return text


def _discover_internal_links(homepage_html, base_url):
    """Find internal links that match our target page keywords."""
    from urllib.parse import urljoin, urlparse
    base_domain = urlparse(base_url).netloc.lower()
    
    # Find all href links
    link_pattern = re.compile(r'href=["\']([^"\'#]+)["\']', re.I)
    found = {}
    for href in link_pattern.findall(homepage_html):
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        # Must be same domain
        if parsed.netloc.lower() != base_domain:
            continue
        path = parsed.path.rstrip("/")
        if not path or path == "/":
            continue
        # Check if path matches our keywords
        if _INTERNAL_PAGE_KEYWORDS.search(path):
            # Deduplicate by path
            if path not in found:
                found[path] = full_url
    
    return list(found.values())[:6]  # Max 6 internal pages


def _classify_email_context(html_text, email):
    """Look at text surrounding an email to classify it as booking/management/press/general."""
    idx = html_text.lower().find(email.lower())
    if idx < 0:
        return "general"
    # Get ~150 chars before the email for context
    context = html_text[max(0, idx - 150):idx + len(email) + 50].lower()
    for label, pattern in _EMAIL_CONTEXT_PATTERNS.items():
        if pattern.search(context):
            return label
    return "general"


def _extract_bio_from_content(html_text, page_url=""):
    """Extract bio text from page content (not just meta description).
    Looks for about/bio sections, or falls back to main content."""
    # Try to find bio-specific sections
    bio_sections = re.findall(
        r'<(?:div|section|article|p)[^>]*(?:class|id)=["\'][^"\']*(?:about|bio|description|intro|content)[^"\']*["\'][^>]*>(.*?)</(?:div|section|article|p)>',
        html_text, re.I | re.S
    )
    
    best_bio = None
    for section in bio_sections:
        text = _strip_html(section)
        if len(text) > 50 and len(text) < 3000:
            if not best_bio or len(text) > len(best_bio):
                best_bio = text
    
    # Fallback: paragraphs on the page
    if not best_bio:
        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html_text, re.I | re.S)
        long_paras = [_strip_html(p) for p in paragraphs if len(_strip_html(p)) > 80]
        if long_paras:
            best_bio = " ".join(long_paras[:3])  # First 3 substantial paragraphs
    
    if best_bio and len(best_bio) > 50:
        return best_bio[:800]
    return None


def _extract_upcoming_events(html_text):
    """Extract upcoming events/gigs from an events page.
    Returns list of dicts: [{venue, city, date_text}, ...]"""
    events = []
    
    # Common date patterns (2025-03-15, Mar 15, 15th March, etc.)
    date_pattern = re.compile(
        r'(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}|'
        r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s*\d{4})?|'
        r'\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*(?:,?\s*\d{4})?)',
        re.I
    )
    
    # Known venue/location markers
    city_pattern = re.compile(
        r'(?:London|Manchester|Birmingham|Leeds|Bristol|Glasgow|Edinburgh|Liverpool|'
        r'Sheffield|Newcastle|Nottingham|Brighton|Cardiff|Oxford|Cambridge|'
        r'Dublin|Belfast|Cork|New York|Los Angeles|Chicago|Berlin|Paris|Amsterdam|'
        r'Toronto|Sydney|Melbourne|Tokyo)',
        re.I
    )
    
    # Try structured event items (list items, divs with event class)
    event_blocks = re.findall(
        r'<(?:li|div|article|tr)[^>]*(?:class|id)=["\'][^"\']*(?:event|gig|show|date|tour)[^"\']*["\'][^>]*>(.*?)</(?:li|div|article|tr)>',
        html_text, re.I | re.S
    )
    
    # Fallback: split page into text blocks and look for date+venue patterns
    if not event_blocks:
        # Use <li> items or <p> blocks
        event_blocks = re.findall(r'<(?:li|p|div)[^>]*>(.*?)</(?:li|p|div)>', html_text, re.I | re.S)
    
    for block in event_blocks[:30]:  # Max 30 blocks to check
        text = _strip_html(block)
        if len(text) < 10 or len(text) > 500:
            continue
        
        dates = date_pattern.findall(text)
        cities = city_pattern.findall(text)
        
        if dates or cities:
            event = {"raw_text": text[:200]}
            if dates:
                event["date_text"] = dates[0]
            if cities:
                event["city"] = cities[0]
            
            # Try to extract venue name (often bold or in a heading within the block)
            venue_match = re.search(r'(?:at|@|venue[:\s]*)\s*([A-Z][A-Za-z\s&\'\-]{2,40})', text, re.I)
            if venue_match:
                event["venue"] = venue_match.group(1).strip()
            
            events.append(event)
    
    return events[:15]  # Max 15 upcoming events


def deep_scrape_site(url):
    """Deep-scrape an artist's official website: homepage + internal pages.
    Follows /about, /contact, /press, /events, /gigs pages (up to 6).
    Returns dict with classified emails, real bio, social links, upcoming events, mgmt info."""
    try:
        # 1. Fetch homepage
        homepage_html = _fetch_page(url)
        if not homepage_html:
            return None
        
        result = {"source": url}
        all_html_pages = [(url, homepage_html)]  # (url, html) pairs
        
        # 2. Discover and fetch internal pages
        internal_links = _discover_internal_links(homepage_html, url)
        for i, page_url in enumerate(internal_links):
            time.sleep(0.4)  # Be respectful
            page_html = _fetch_page(page_url, timeout=6)
            if page_html:
                all_html_pages.append((page_url, page_html))
        
        # 3. Combine all HTML for link/email extraction
        combined_html = "\n".join(h for _, h in all_html_pages)
        
        # 4. Extract social/streaming links from ALL pages
        for platform, pattern in _SOCIAL_PATTERNS.items():
            matches = pattern.findall(combined_html)
            if matches:
                best = matches[0].rstrip("/")
                if "/login" not in best and "/share" not in best and "/intent" not in best:
                    result[platform] = best
        
        # 5. Extract and CLASSIFY emails from ALL pages
        skip_emails = {"info@squarespace.com", "noreply@", "support@", "admin@", "webmaster@",
                       "help@", "privacy@", "contact@wix.com", "no-reply@"}
        classified_emails = {}  # {email: label}
        for page_url, page_html in all_html_pages:
            for em in _EMAIL_PATTERN.findall(page_html):
                em_lower = em.lower()
                if any(skip in em_lower for skip in skip_emails):
                    continue
                if any(em_lower.endswith(ext) for ext in [".png", ".jpg", ".gif", ".svg", ".webp"]):
                    continue
                if em not in classified_emails:
                    label = _classify_email_context(page_html, em)
                    classified_emails[em] = label
        
        if classified_emails:
            # Sort: booking > management > press > general
            priority = {"booking": 0, "management": 1, "press": 2, "general": 3}
            sorted_emails = sorted(classified_emails.items(), key=lambda x: priority.get(x[1], 3))
            result["emails"] = [e for e, _ in sorted_emails[:5]]
            result["email_labels"] = {e: l for e, l in sorted_emails[:5]}
        
        # 6. Extract bio — prefer /about page content over meta description
        bio = None
        for page_url, page_html in all_html_pages:
            path = urllib.parse.urlparse(page_url).path.lower()
            if any(kw in path for kw in ["about", "bio", "epk", "press"]):
                bio = _extract_bio_from_content(page_html, page_url)
                if bio:
                    break
        # Fallback: meta description from homepage
        if not bio:
            desc_match = _META_DESC.search(homepage_html)
            if desc_match:
                desc = desc_match.group(1).strip()
                if len(desc) > 20:
                    bio = desc[:500]
        # Fallback: bio from homepage content
        if not bio:
            bio = _extract_bio_from_content(homepage_html, url)
        if bio:
            result["bio"] = bio
        
        # 7. Extract upcoming events from /events, /gigs, /tours pages
        for page_url, page_html in all_html_pages:
            path = urllib.parse.urlparse(page_url).path.lower()
            if any(kw in path for kw in ["event", "gig", "tour", "show", "live", "date"]):
                events = _extract_upcoming_events(page_html)
                if events:
                    result["upcoming_events"] = events
                    result["is_active"] = True
                    # Extract cities from events for locale
                    event_cities = [e["city"] for e in events if e.get("city")]
                    if event_cities:
                        result["active_cities"] = list(set(event_cities))
                    break
        
        # 8. Extract management/agent names from contact/booking pages
        for page_url, page_html in all_html_pages:
            path = urllib.parse.urlparse(page_url).path.lower()
            if any(kw in path for kw in ["contact", "booking", "press", "epk"]):
                text = _strip_html(page_html)
                mgmt_match = _MGMT_PATTERN.search(text)
                if mgmt_match:
                    result["management"] = mgmt_match.group(1).strip()
                    break
        
        pages_scraped = len(all_html_pages)
        if pages_scraped > 1:
            result["pages_scraped"] = pages_scraped
        
        return result if len(result) > 1 else None
    except Exception as e:
        log.debug(f"Deep scrape failed for {url}: {e}")
        return None


# Legacy aliases (orchestrator uses these names)
scrape_official_site = deep_scrape_site
verify_artist_multiplatform = verify_multi_platform
mb_get_links = get_mb_links

# ── Profile Builder ──

def build_profile(name, vf, enrich, mb_links=None, scraped=None):
    """Merge all data sources into a complete artist profile.
    Returns dict with categorized, deduplicated links and validated data."""
    profile = {
        "name": vf["name"] if vf else name,
        "platforms": {},  # {platform: url}
        "urls": [],       # flat list of all unique URLs
        "emails": [],     # validated emails
        "email_labels": {},  # {email: booking|management|press|general}
        "bio": None,
        "locale": {},
        "upcoming_events": [],   # [{venue, city, date_text}, ...]
        "is_active": False,      # True if upcoming events found
        "active_cities": [],     # cities from upcoming events
        "management": None,      # management/booking agent name
    }
    
    # 1. Platform URLs from MusicBrainz links
    if mb_links:
        for category, url in mb_links.items():
            if category not in profile["platforms"]:
                profile["platforms"][category] = url
    
    # 2. Platform URLs + deep scrape data from website
    if scraped:
        for key, val in scraped.items():
            if key in _SOCIAL_PATTERNS and key not in profile["platforms"]:
                profile["platforms"][key] = val
            elif key == "emails":
                for em in val:
                    if em not in profile["emails"]:
                        profile["emails"].append(em)
            elif key == "email_labels":
                profile["email_labels"].update(val)
            elif key == "bio" and not profile["bio"]:
                profile["bio"] = val
            elif key == "upcoming_events":
                profile["upcoming_events"] = val
            elif key == "is_active":
                profile["is_active"] = val
            elif key == "active_cities":
                profile["active_cities"] = val
            elif key == "management":
                profile["management"] = val
    
    # 3. URLs from verification cache
    if vf:
        if vf.get("deezer_url") and "deezer" not in profile["platforms"]:
            profile["platforms"]["deezer"] = vf["deezer_url"]
        if vf.get("spotify_url") and "spotify" not in profile["platforms"]:
            profile["platforms"]["spotify"] = vf["spotify_url"]
    
    # 4. (Enrichment block removed — enrich param kept for compat but ignored.
    #     All URLs, emails, locale now come from real API tools only.)

    # 5. Locale from MusicBrainz
    if vf:
        if not profile["locale"].get("city") and vf.get("mb_area"):
            profile["locale"]["city"] = vf["mb_area"]
        if not profile["locale"].get("country") and vf.get("mb_country"):
            profile["locale"]["country"] = vf["mb_country"]
    
    # 6. Locale supplement from active_cities (gig locations)
    if not profile["locale"].get("city") and profile["active_cities"]:
        # If we have no city, use most common city from upcoming gigs as hint
        from collections import Counter
        city_counts = Counter(profile["active_cities"])
        most_common = city_counts.most_common(1)[0][0]
        profile["locale"]["active_city"] = most_common
    
    # Build flat URL list (deduped)
    for plat, url in profile["platforms"].items():
        if url and url not in profile["urls"]:
            profile["urls"].append(url)
    
    return profile

# ── Additional Data Sources ──

def lastfm_search(name):
    """Last.fm: bio, tags, listener stats."""
    if not LASTFM_API_KEY: return None
    try:
        url = f"https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={urllib.parse.quote(name)}&api_key={LASTFM_API_KEY}&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "GroovonScraper/1.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            a = json.loads(r.read()).get("artist", {})
            if not a or not a.get("name"): return None
            bio = a.get("bio", {}).get("summary", "")
            bio = bio.split("<a href")[0].strip() if bio else None
            return {
                "tags": [t["name"] for t in a.get("tags", {}).get("tag", [])],
                "bio": bio,
                "lastfm_url": a.get("url"),
                "listeners": a.get("stats", {}).get("listeners"),
                "name": a.get("name"),
            }
    except:
        return None

def wikipedia_search(name):
    """Wikipedia: bio extract + thumbnail + URL."""
    try:
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(name.replace(' ', '_'))}"
        req = urllib.request.Request(url, headers={"User-Agent": "GroovonScraper/1.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            d = json.loads(r.read())
            if d.get("type") == "disambiguation": return None
            return {
                "bio": d.get("extract"),
                "wiki_url": d.get("content_urls", {}).get("desktop", {}).get("page"),
                "thumbnail": d.get("thumbnail", {}).get("source"),
            }
    except:
        return None

def wikidata_search(name):
    """Wikidata: born, birthplace, years_active, instruments, labels, genres, websites."""
    try:
        # Step 1: get QID from Wikipedia
        search_url = f"https://en.wikipedia.org/w/api.php?action=query&titles={urllib.parse.quote(name.replace(' ','_'))}&prop=pageprops&ppprop=wikibase_item&format=json"
        req = urllib.request.Request(search_url, headers={"User-Agent": "GroovonScraper/1.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            pages = json.loads(r.read()).get("query", {}).get("pages", {})
            qid = next(iter(pages.values()), {}).get("pageprops", {}).get("wikibase_item")
        if not qid: return None
        
        # Step 2: fetch entity
        wd_url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
        req2 = urllib.request.Request(wd_url, headers={"User-Agent": "GroovonScraper/1.0"})
        with urllib.request.urlopen(req2, context=SSL_CTX, timeout=10) as r:
            entity = json.loads(r.read()).get("entities", {}).get(qid, {})
        
        claims = entity.get("claims", {})
        
        def _wd_label(qid_val):
            try:
                lr = urllib.request.Request(f"https://www.wikidata.org/wiki/Special:EntityData/{qid_val}.json", headers={"User-Agent": "GroovonScraper/1.0"})
                with urllib.request.urlopen(lr, context=SSL_CTX, timeout=8) as r2:
                    ent = json.loads(r2.read()).get("entities", {}).get(qid_val, {})
                    return ent.get("labels", {}).get("en", {}).get("value")
            except: return None
        
        def _get_qids(prop):
            vals = claims.get(prop, [])
            return [v["mainsnak"]["datavalue"]["value"]["id"] for v in vals
                    if v.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")] if vals else []
        
        def _get_str(prop):
            vals = claims.get(prop, [])
            return [v["mainsnak"]["datavalue"]["value"] for v in vals
                    if v.get("mainsnak", {}).get("datavalue")] if vals else []
        
        def _get_time(prop):
            vals = claims.get(prop, [])
            if vals:
                v = vals[0].get("mainsnak", {}).get("datavalue", {}).get("value", {})
                t = v.get("time", "").lstrip("+")
                if not t: return None
                parts = t.split("T")[0].split("-")
                try: return parts[0]
                except: return None
            return None
        
        # Resolve key fields (limit requests to avoid rate limiting)
        genre_ids = _get_qids("P136")[:3]
        occ_ids = _get_qids("P106")[:3]
        instr_ids = _get_qids("P1303")[:3]
        label_ids = _get_qids("P264")[:2]
        birthplace_ids = _get_qids("P19")[:1]
        
        genres = [l for l in (_wd_label(q) for q in genre_ids) if l]
        occupations = [l for l in (_wd_label(q) for q in occ_ids) if l]
        instruments = [l for l in (_wd_label(q) for q in instr_ids) if l]
        rec_labels = [l for l in (_wd_label(q) for q in label_ids) if l]
        birthplace = _wd_label(birthplace_ids[0]) if birthplace_ids else None
        
        # Dates
        born_date = _get_time("P569")
        active_start = _get_time("P2031")
        active_end = _get_time("P2032")
        years_active = None
        if active_start:
            years_active = active_start[:4] + ("–" + active_end[:4] if active_end else "–present")
        
        # Websites
        websites = _get_str("P856")[:2]
        
        return {
            "born": born_date,
            "birthplace": birthplace,
            "years_active": years_active,
            "genres": genres,
            "occupations": occupations,
            "instruments": instruments,
            "labels": rec_labels,
            "websites": websites,
        }
    except:
        return None

def itunes_search(name):
    """iTunes/Apple Music: genre + iTunes URL (free, no API key)."""
    try:
        url = f"https://itunes.apple.com/search?term={urllib.parse.quote(name)}&entity=musicArtist&limit=1"
        with urllib.request.urlopen(url, timeout=10) as r:
            results = json.loads(r.read()).get("results", [])
            if results:
                a = results[0]
                return {"genre": a.get("primaryGenreName"), "itunes_url": a.get("artistLinkUrl")}
    except:
        pass
    return None

def discogs_search(name):
    """Discogs: real name, profile, official URLs."""
    if not DISCOGS_TOKEN: return None
    try:
        url = f"https://api.discogs.com/database/search?q={urllib.parse.quote(name)}&type=artist&per_page=1"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Discogs token={DISCOGS_TOKEN}",
            "User-Agent": "GroovonScraper/1.0"
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            results = json.loads(r.read()).get("results", [])
            if not results: return None
            artist_id = results[0]["id"]
        
        req2 = urllib.request.Request(f"https://api.discogs.com/artists/{artist_id}",
            headers={"Authorization": f"Discogs token={DISCOGS_TOKEN}", "User-Agent": "GroovonScraper/1.0"})
        with urllib.request.urlopen(req2, timeout=10) as r:
            a = json.loads(r.read())
            return {
                "real_name": a.get("realname"),
                "discogs_url": a.get("uri"),
                "profile": (a.get("profile", "").split("\n")[0][:300]) if a.get("profile") else None,
                "urls": a.get("urls", [])[:5],
            }
    except:
        return None

def youtube_search(name):
    """YouTube Data API: official channel URL."""
    if not YOUTUBE_API_KEY: return None
    try:
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={urllib.parse.quote(name + ' official')}&type=channel&maxResults=1&key={YOUTUBE_API_KEY}"
        with urllib.request.urlopen(url, timeout=10) as r:
            items = json.loads(r.read()).get("items", [])
            if items:
                ch = items[0]
                channel_id = ch["snippet"]["channelId"]
                return {
                    "channel_id": channel_id,
                    "channel_url": f"https://www.youtube.com/channel/{channel_id}",
                    "channel_title": ch["snippet"]["title"]
                }
    except:
        pass
    return None

def genius_search(name):
    """Genius: top songs + genius URL."""
    if not GENIUS_TOKEN: return None
    try:
        url = f"https://api.genius.com/search?q={urllib.parse.quote(name)}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {GENIUS_TOKEN}"})
        with urllib.request.urlopen(req, timeout=10) as r:
            hits = json.loads(r.read()).get("response", {}).get("hits", [])
            artist_hits = [h["result"] for h in hits if h.get("type") == "song"
                          and h["result"].get("primary_artist", {}).get("name", "").lower() == name.lower()]
            if not artist_hits:
                artist_hits = [h["result"] for h in hits[:5] if h.get("type") == "song"]
            if not artist_hits: return None
            
            artist_info = artist_hits[0].get("primary_artist", {})
            top_songs = [
                {"title": h.get("title"), "url": h.get("url")}
                for h in artist_hits[:5]
            ]
            return {
                "genius_url": artist_info.get("url"),
                "genius_image": artist_info.get("image_url"),
                "top_songs": top_songs,
            }
    except:
        return None

def ddg_search(query, max_results=5):
    """DuckDuckGo HTML search → list of result URLs."""
    _SKIP_DOMAINS = {"google.com", "bing.com", "duckduckgo.com", "wikipedia.org",
                     "wikidata.org", "musicbrainz.org", "genius.com", "allmusic.com",
                     "amazon.com", "apple.com", "shazam.com"}
    try:
        url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html", "Accept-Encoding": "identity"
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=12) as r:
            html_content = r.read().decode("utf-8", errors="ignore")
        urls = re.findall(r'uddg=(https?[^&"<>\s]+)', html_content)
        urls = [urllib.parse.unquote(u) for u in urls]
        def _domain(u):
            try: return u.split("/")[2].replace("www.", "")
            except: return ""
        urls = [u for u in urls if not any(s in _domain(u) for s in _SKIP_DOMAINS)]
        return urls[:max_results]
    except:
        return []

def scrape_linktree(url):
    """Scrape a Linktree page for all social/streaming links."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html"
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            html_content = r.read().decode("utf-8", errors="ignore")
        
        result = {}
        for platform, pattern in _SOCIAL_PATTERNS.items():
            matches = pattern.findall(html_content)
            if matches:
                result[platform] = matches[0].rstrip("/")
        
        # Extract emails
        emails = [e for e in _EMAIL_PATTERN.findall(html_content) 
                  if not any(s in e.lower() for s in ["noreply", "support@", "admin@"])]
        if emails:
            result["emails"] = list(set(emails))[:3]
        
        return result if result else None
    except:
        return None

def bandsintown_search(artist_name):
    """Search Bandsintown for an artist's upcoming events to cross-check."""
    try:
        encoded = urllib.parse.quote(artist_name)
        url = f"https://rest.bandsintown.com/artists/{encoded}?app_id={BANDSINTOWN_APP_ID}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            data = json.loads(r.read())
        if data and data.get("name"):
            return {"name": data["name"], "url": data.get("url")}
        return None
    except:
        return None

# ── v5 Data Sources ──

def google_kg_search(name):
    """Google Knowledge Graph: structured entity data (type, description, website, image)."""
    if not GOOGLE_KG_KEY:
        return None
    try:
        q = urllib.parse.quote(name)
        url = f"https://kgsearch.googleapis.com/v1/entities:search?query={q}&types=MusicGroup&types=Person&key={GOOGLE_KG_KEY}&limit=3&languages=en"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            data = json.loads(r.read())
        
        for item in data.get("itemListElement", []):
            entity = item.get("result", {})
            entity_name = entity.get("name", "")
            # Strict match
            if entity_name.lower().strip() != name.lower().strip():
                continue
            
            result = {"name": entity_name}
            types = entity.get("@type", [])
            if isinstance(types, list):
                result["types"] = types
                result["is_musician"] = any(t in types for t in ["MusicGroup", "MusicRecording", "MusicAlbum"])
            
            desc = entity.get("description", "")
            if desc:
                result["description"] = desc
            
            detailed = entity.get("detailedDescription", {})
            if detailed:
                result["bio"] = detailed.get("articleBody", "")
                result["wiki_url"] = detailed.get("url", "")
            
            img = entity.get("image", {})
            if img and img.get("contentUrl"):
                result["image"] = img["contentUrl"]
            
            if entity.get("url"):
                result["website"] = entity["url"]
            
            return result
        return None
    except:
        return None

def serper_search(query, max_results=5):
    """Serper.dev: Google Search API. Returns list of {title, link, snippet}."""
    if not SERPER_KEY:
        return []
    try:
        payload = json.dumps({"q": query, "num": max_results}).encode("utf-8")
        req = urllib.request.Request(
            "https://google.serper.dev/search",
            data=payload,
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            data = json.loads(r.read())
        
        results = []
        for item in data.get("organic", [])[:max_results]:
            results.append({
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", "")
            })
        
        # Also check Knowledge Graph answer if present
        kg = data.get("knowledgeGraph")
        if kg:
            results.append({
                "title": kg.get("title", ""),
                "link": kg.get("website", "") or kg.get("descriptionLink", ""),
                "snippet": kg.get("description", ""),
                "kg_type": kg.get("type", ""),
                "kg_website": kg.get("website"),
                "kg_image": kg.get("imageUrl")
            })
        return results
    except:
        return []

def setlistfm_search(artist_name):
    """Setlist.fm: search for artist setlists → confirms real, active performer."""
    if not SETLISTFM_KEY:
        return None
    try:
        q = urllib.parse.quote(artist_name)
        url = f"https://api.setlist.fm/rest/1.0/search/artists?artistName={q}&sort=relevance"
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "x-api-key": SETLISTFM_KEY
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            data = json.loads(r.read())
        
        for artist in data.get("artist", []):
            if artist.get("name", "").lower().strip() == artist_name.lower().strip():
                mbid = artist.get("mbid", "")
                disambiguation = artist.get("disambiguation", "")
                setlist_url = f"https://www.setlist.fm/setlists/{artist.get('url', '')}"
                
                # Fetch recent setlists count
                total_setlists = 0
                if mbid:
                    try:
                        sl_url = f"https://api.setlist.fm/rest/1.0/artist/{mbid}/setlists?p=1"
                        req2 = urllib.request.Request(sl_url, headers={
                            "Accept": "application/json",
                            "x-api-key": SETLISTFM_KEY
                        })
                        with urllib.request.urlopen(req2, context=SSL_CTX, timeout=10) as r2:
                            sl_data = json.loads(r2.read())
                        total_setlists = sl_data.get("total", 0)
                    except:
                        pass
                
                return {
                    "name": artist["name"],
                    "mbid": mbid,
                    "disambiguation": disambiguation,
                    "total_setlists": total_setlists,
                    "url": setlist_url,
                    "is_active": total_setlists > 0
                }
        return None
    except:
        return None


# ── Bandsintown Search (Events/Shows) ──

def bandsintown_search(artist_name):
    """Bandsintown: upcoming/past events, social links, tracker count.
    Uses free API with app_id (no OAuth required)."""
    if not BANDSINTOWN_APP_ID:
        return None
    try:
        q = urllib.parse.quote(artist_name)
        app_id = urllib.parse.quote(BANDSINTOWN_APP_ID)

        # Artist info
        def _fetch_artist():
            url = f"https://rest.bandsintown.com/artists/{q}?app_id={app_id}"
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": "GroovonScraper/1.0"
            })
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
                return json.loads(r.read())

        artist = _api_call(_fetch_artist)
        if not artist or isinstance(artist, list):
            return None

        # Check name similarity
        api_name = artist.get("name", "")
        if normalize(api_name) != normalize(artist_name):
            # Loose match: check if one contains the other
            if normalize(artist_name) not in normalize(api_name) and normalize(api_name) not in normalize(artist_name):
                return None

        result = {
            "name": api_name,
            "bandsintown_url": artist.get("url", ""),
            "tracker_count": artist.get("tracker_count", 0),
            "upcoming_events": artist.get("upcoming_event_count", 0),
            "image_url": artist.get("image_url", ""),
            "facebook_url": artist.get("facebook_page_url", ""),
        }

        # Fetch upcoming events (top 5)
        def _fetch_events():
            url = f"https://rest.bandsintown.com/artists/{q}/events?app_id={app_id}&date=upcoming"
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": "GroovonScraper/1.0"
            })
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
                return json.loads(r.read())

        events = _api_call(_fetch_events) or []
        if events and isinstance(events, list):
            result["events"] = []
            for ev in events[:5]:
                venue = ev.get("venue", {})
                result["events"].append({
                    "date": ev.get("datetime", ""),
                    "venue": venue.get("name", ""),
                    "city": venue.get("city", ""),
                    "country": venue.get("country", ""),
                    "url": ev.get("url", ""),
                })
            result["is_touring"] = len(events) > 0

        return result
    except Exception as e:
        log.debug(f"Bandsintown search failed for {artist_name}: {e}")
        return None


# ── SoundCloud Search (public widget API) ──

def soundcloud_search(artist_name):
    """SoundCloud: user profile URL, followers, track count.
    Requires SOUNDCLOUD_CLIENT_ID from environment."""
    if not SOUNDCLOUD_CLIENT_ID:
        return None

    try:
        q = urllib.parse.quote(artist_name)
        client_id = urllib.parse.quote(SOUNDCLOUD_CLIENT_ID)

        def _fetch():
            url = f"https://api-v2.soundcloud.com/search/users?q={q}&limit=5&client_id={client_id}"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
                return json.loads(r.read()).get("collection", [])

        items = _api_call(_fetch) or []
        if not items:
            return None

        # Find best match
        best = None
        for item in items:
            name_lower = (item.get("username") or item.get("full_name") or "").lower().strip()
            if normalize(name_lower) == normalize(artist_name):
                best = item
                break
        if not best:
            # Try partial match
            for item in items:
                name_lower = normalize(item.get("username") or item.get("full_name") or "")
                if normalize(artist_name) in name_lower or name_lower in normalize(artist_name):
                    best = item
                    break
        if not best and items:
            best = items[0]

        if not best:
            return None

        return {
            "name": best.get("full_name") or best.get("username", ""),
            "soundcloud_url": best.get("permalink_url", ""),
            "followers": best.get("followers_count", 0),
            "track_count": best.get("track_count", 0),
            "description": (best.get("description") or "")[:500],
            "avatar_url": best.get("avatar_url", ""),
            "city": best.get("city", ""),
            "country": best.get("country_code", ""),
            "verified": best.get("verified", False),
        }
    except Exception as e:
        log.debug(f"SoundCloud search failed for {artist_name}: {e}")
        return None


# ── Bandcamp Search (scrape) ──

def bandcamp_search(artist_name):
    """Bandcamp: artist page URL, location, genre tags.
    Scrapes the public search page (no API key needed)."""
    try:
        q = urllib.parse.quote(artist_name)

        def _fetch():
            url = f"https://bandcamp.com/search?q={q}&item_type=b"  # b = bands/artists
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html",
            })
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=12) as r:
                return r.read().decode("utf-8", errors="replace")

        html_text = _api_call(_fetch)
        if not html_text:
            return None

        # Parse search results from HTML
        # Bandcamp search results are in <li class="searchresult band"> blocks
        results = []
        # Extract result blocks
        result_pattern = re.compile(
            r'<li\s+class="searchresult\s+band".*?</li>', re.S
        )
        name_pattern = re.compile(r'<div\s+class="heading">\s*<a[^>]*>(.*?)</a>', re.S)
        url_pattern = re.compile(r'<div\s+class="heading">\s*<a\s+href="([^"]+)"', re.S)
        location_pattern = re.compile(r'<div\s+class="subhead">\s*(.*?)\s*</div>', re.S)
        genre_pattern = re.compile(r'<div\s+class="genre">\s*(.*?)\s*</div>', re.S)
        art_pattern = re.compile(r'<div\s+class="art">\s*<img[^>]+src="([^"]+)"', re.S)

        for block in result_pattern.findall(html_text):
            name_match = name_pattern.search(block)
            url_match = url_pattern.search(block)
            if name_match and url_match:
                results.append({
                    "name": html.unescape(name_match.group(1).strip()),
                    "url": url_match.group(1).strip(),
                    "location": html.unescape(location_pattern.search(block).group(1).strip()) if location_pattern.search(block) else "",
                    "genre": html.unescape(genre_pattern.search(block).group(1).strip()) if genre_pattern.search(block) else "",
                    "image": art_pattern.search(block).group(1) if art_pattern.search(block) else "",
                })

        if not results:
            return None

        # Find best match
        best = None
        for r in results:
            if normalize(r["name"]) == normalize(artist_name):
                best = r
                break
        if not best:
            for r in results:
                if normalize(artist_name) in normalize(r["name"]) or normalize(r["name"]) in normalize(artist_name):
                    best = r
                    break
        if not best:
            best = results[0]

        return {
            "name": best["name"],
            "bandcamp_url": best["url"],
            "location": best.get("location", ""),
            "genre": best.get("genre", ""),
            "image": best.get("image", ""),
        }
    except Exception as e:
        log.debug(f"Bandcamp search failed for {artist_name}: {e}")
        return None


# ── Deezer Extended Search (albums, top tracks) ──

def deezer_extended_search(deezer_url_or_name):
    """Deezer: fetch extended info (albums, top tracks, related artists).
    Takes a Deezer artist URL or name. Adds to the basic deezer_search data."""
    try:
        # Extract artist ID from URL if provided
        artist_id = None
        if isinstance(deezer_url_or_name, str) and "deezer.com/artist/" in deezer_url_or_name:
            match = re.search(r'deezer\.com/(?:\w+/)?artist/(\d+)', deezer_url_or_name)
            if match:
                artist_id = match.group(1)

        if not artist_id:
            # Search by name to get ID
            basic = deezer_search(deezer_url_or_name)
            if not basic or not basic.get("deezer_url"):
                return None
            match = re.search(r'/artist/(\d+)', basic["deezer_url"])
            if match:
                artist_id = match.group(1)
            else:
                return None

        result = {}

        # Top tracks
        def _fetch_top():
            url = f"https://api.deezer.com/artist/{artist_id}/top?limit=5"
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.loads(r.read()).get("data", [])

        tracks = _api_call(_fetch_top) or []
        if tracks:
            result["top_tracks"] = [
                {"title": t.get("title", ""), "duration": t.get("duration", 0),
                 "rank": t.get("rank", 0), "preview": t.get("preview", "")}
                for t in tracks[:5]
            ]

        # Albums
        def _fetch_albums():
            url = f"https://api.deezer.com/artist/{artist_id}/albums?limit=10"
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.loads(r.read()).get("data", [])

        albums = _api_call(_fetch_albums) or []
        if albums:
            result["albums"] = [
                {"title": a.get("title", ""), "release_date": a.get("release_date", ""),
                 "type": a.get("record_type", ""), "cover": a.get("cover_medium", "")}
                for a in albums[:10]
            ]
            result["album_count"] = len(albums)

        # Related artists
        def _fetch_related():
            url = f"https://api.deezer.com/artist/{artist_id}/related?limit=5"
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.loads(r.read()).get("data", [])

        related = _api_call(_fetch_related) or []
        if related:
            result["related_artists"] = [r.get("name", "") for r in related[:5]]

        return result if result else None
    except Exception as e:
        log.debug(f"Deezer extended search failed: {e}")
        return None


# ── Extract Extra Links from MusicBrainz ──

_EXTRA_MB_PLATFORMS = {
    "allmusic.com": "allmusic",
    "residentadvisor.net": "resident_advisor",
    "ra.co": "resident_advisor",
    "songkick.com": "songkick",
    "rateyourmusic.com": "rateyourmusic",
    "musicbrainz.org": "musicbrainz",
    "imdb.com": "imdb",
    "whosampled.com": "whosampled",
    "secondhandsongs.com": "secondhandsongs",
    "setlist.fm": "setlistfm",
}

def extract_mb_extra_links(mb_links):
    """Extract additional platform URLs from MusicBrainz link relationships.
    Takes the dict returned by get_mb_links() and extracts RA, AllMusic,
    Songkick, RYM, etc. Returns dict of {platform: url}."""
    if not mb_links:
        return {}

    extras = {}
    for _category, url in mb_links.items():
        if not url:
            continue
        url_lower = url.lower()
        for domain, platform in _EXTRA_MB_PLATFORMS.items():
            if domain in url_lower and platform not in extras:
                extras[platform] = url
                break

    return extras


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Parallel Fetch  (Item 8)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def parallel_fetch(tasks, max_workers=4):
    """Run multiple fetcher calls concurrently and collect results.

    Parameters
    ----------
    tasks : list[tuple[callable, tuple, dict]]
        Each entry is (func, args_tuple, kwargs_dict).
        Example: [(lastfm_search, ("Artist",), {}), ...]
    max_workers : int
        Thread pool size. Keep ≤ 5 to respect rate limits.

    Returns
    -------
    list  – results in the same order as *tasks*. Failed calls return None.

    The existing per-domain rate limiter (`_rate_limit`) is thread-safe, so
    concurrent calls to different domains won't exceed API limits.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = [None] * len(tasks)

    def _run(idx, fn, args, kwargs):
        try:
            return idx, fn(*args, **kwargs)
        except Exception as exc:
            log.debug(f"   parallel_fetch task {idx} ({fn.__name__}) failed: {exc}")
            return idx, None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for i, (fn, args, kwargs) in enumerate(tasks):
            futures.append(pool.submit(_run, i, fn, args, kwargs or {}))
        for fut in as_completed(futures):
            idx, result = fut.result()
            results[idx] = result

    return results
