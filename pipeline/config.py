"""
pipeline.config — API keys, constants, genre/event-type maps, shared helpers.

All values previously scattered in the top ~470 lines of process_david_excel.py.
Zero logic changes — pure extraction.
"""
import os, ssl, unicodedata, warnings

# ── Load .env ──────────────────────────────────────────────────────────────────
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
if os.path.exists(env_path):
    for _line in open(env_path):
        _line = _line.strip()
        if _line and not _line.startswith('#') and '=' in _line:
            k, v = _line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── API Keys ───────────────────────────────────────────────────────────────────
SUPABASE_URL    = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY    = os.environ.get('SUPABASE_KEY', '')
OPENROUTER_KEY  = os.environ.get('OPENROUTER_API_KEY', '')
GOOGLE_API_KEY  = os.environ.get('GOOGLE_API_KEY', '')
SPOTIFY_ID      = os.environ.get('SPOTIFY_CLIENT_ID', '')
SPOTIFY_SECRET  = os.environ.get('SPOTIFY_CLIENT_SECRET', '')
LASTFM_API_KEY  = os.environ.get('LASTFM_API_KEY', '')
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY', '')
DISCOGS_TOKEN   = os.environ.get('DISCOGS_TOKEN', '')
GENIUS_TOKEN    = os.environ.get('GENIUS_ACCESS_TOKEN', '')
GOOGLE_KG_KEY   = os.environ.get('GOOGLE_KG_API_KEY', '')
SERPER_KEY      = os.environ.get('SERPER_API_KEY', '')
SETLISTFM_KEY   = os.environ.get('SETLISTFM_API_KEY', '')
SCRAPINGBEE_KEY = os.environ.get('SCRAPINGBEE_API_KEY', '')
BANDSINTOWN_APP_ID = os.environ.get('BANDSINTOWN_APP_ID', '')
SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID', '')
TICKETMASTER_KEY   = os.environ.get('TICKETMASTER_API_KEY', '')
SOUNDCLOUD_CLIENT_ID = os.environ.get('SOUNDCLOUD_CLIENT_ID', '')
EMAIL_VERIFIER_PROVIDER = os.environ.get('EMAIL_VERIFIER_PROVIDER', '').strip().lower()
ZEROBOUNCE_API_KEY = os.environ.get('ZEROBOUNCE_API_KEY', '')
NEVERBOUNCE_API_KEY = os.environ.get('NEVERBOUNCE_API_KEY', '')
ABSTRACT_API_KEY = os.environ.get('ABSTRACT_API_KEY', '')

# ── Model IDs ──────────────────────────────────────────────────────────────────
MODEL        = "google/gemini-3.1-flash-lite-preview"
GOOGLE_MODEL = "gemini-2.0-flash"  # Google AI Studio model

# ── SSL (secure by default) ────────────────────────────────────────────────────
def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


GROOVON_INSECURE_SSL = _env_bool("GROOVON_INSECURE_SSL", default=False)
SSL_CTX = ssl.create_default_context()
if GROOVON_INSECURE_SSL:
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE
    warnings.warn(
        "GROOVON_INSECURE_SSL=true disables TLS certificate verification. "
        "Use only for local debugging.",
        RuntimeWarning,
    )

# ── Genre System ───────────────────────────────────────────────────────────────
GENRES = [
    "Jazz","Blues","Folk / Acoustic","Punk","Country","House / Techno","Metal",
    "Rock","Alternative","Arabic / MENA Pop","K-Pop","Pop","Indian Pop / Bollywood",
    "Electronic / EDM","Hip-Hop / Rap","R&B / Soul","Reggae","Gospel / Christian",
    "Latin","Indie","World Music","Afrobeat / Afropop","Singer-Songwriter",
    "Classical","Funk / Disco","Karaoke","Don't Box Me!"
]

_GENRE_NORM = {g.lower().replace(' ','').replace('/','').replace('&',''): g for g in GENRES}
_GENRE_KEYWORDS = {
    "hardcore": "Punk", "grunge": "Alternative", "emo": "Alternative",
    "drum and bass": "Electronic / EDM", "dnb": "Electronic / EDM", "d&b": "Electronic / EDM",
    "dubstep": "Electronic / EDM", "trance": "Electronic / EDM", "ambient": "Electronic / EDM",
    "trap": "Hip-Hop / Rap", "grime": "Hip-Hop / Rap",
    "shoegaze": "Alternative", "post-punk": "Punk", "post-rock": "Rock",
    "prog": "Rock", "psychedelic": "Rock", "garage": "Rock",
    "ska": "Reggae", "dancehall": "Reggae",
    "neo-soul": "R&B / Soul", "rnb": "R&B / Soul",
    "techno": "House / Techno", "house": "House / Techno", "minimal": "House / Techno",
    "noise": "Alternative", "experimental": "Alternative",
    "bossa nova": "Latin", "salsa": "Latin", "cumbia": "Latin",
    "death metal": "Metal", "black metal": "Metal", "thrash": "Metal",
    "bluegrass": "Country", "americana": "Country",
    "singer songwriter": "Singer-Songwriter",
    "afro": "Afrobeat / Afropop", "highlife": "Afrobeat / Afropop",
}

def validate_genre(genre):
    """Map any genre string to nearest valid genre from the fixed list."""
    if not genre:
        return "Don't Box Me!"
    # Exact match
    if genre in GENRES:
        return genre
    # Normalized match
    norm = genre.lower().replace(' ','').replace('/','').replace('&','')
    if norm in _GENRE_NORM:
        return _GENRE_NORM[norm]
    # Keyword match
    gl = genre.lower()
    for kw, mapped in _GENRE_KEYWORDS.items():
        if kw in gl:
            return mapped
    # Partial match
    for g in GENRES:
        if g.lower() in gl or gl in g.lower():
            return g
    return "Don't Box Me!"

def best_genre(ai_genre, vf, enrich_bio=""):
    """Pick best genre: Spotify > AI classification > MusicBrainz tags."""
    # 1. Try Spotify genres (most reliable)
    if vf and vf.get("spotify_genres"):
        for sg in vf["spotify_genres"]:
            mapped = validate_genre(sg)
            if mapped != "Don't Box Me!":
                return mapped
    # 2. Use AI classification genre (context-aware)
    ai_mapped = validate_genre(ai_genre)
    if ai_mapped != "Don't Box Me!":
        return ai_mapped
    # 3. Fallback to MusicBrainz tags (least reliable alone)
    if vf and vf.get("mb_tags"):
        for tag in vf["mb_tags"]:
            mapped = validate_genre(tag)
            if mapped != "Don't Box Me!":
                return mapped
    return ai_mapped

# ── Event Type System ──────────────────────────────────────────────────────────
VALID_TYPES = [
    "Live Music Performance", "Music Festival", "Dance Party",
    "Jam / Open Mike", "Other",
]
DELETE_TYPES = [
    "Comedy", "Podcast", "Dance performance", "Theater production",
    "Stage play", "Drag performance", "Spoken word", "Sport",
    "Culinairy", "Arts & Crafts", "Exhibition",
]

def sanitize_event_type(et):
    """Catch garbage AI outputs and map to valid event type."""
    if not et:
        return "Live Music Performance"
    et_stripped = et.strip()
    # Exact match
    if et_stripped in VALID_TYPES:
        return et_stripped
    # Case-insensitive match
    et_lower = et_stripped.lower()
    for vt in VALID_TYPES:
        if vt.lower() == et_lower:
            return vt
    # If AI copied instruction text or produced gibberish (>40 chars = not a type)
    if len(et_stripped) > 40 or "should be" in et_lower or "event type" in et_lower or "from the list" in et_lower:
        return "Other"
    # Partial match
    for vt in VALID_TYPES:
        if vt.lower() in et_lower or et_lower in vt.lower():
            return vt
    return "Other"

# ── URL Classification ─────────────────────────────────────────────────────────
def classify_url(url):
    """Classify a URL into a known platform category."""
    u = url.lower()
    if "spotify.com" in u: return "spotify"
    if "music.apple.com" in u or "itunes.apple.com" in u: return "apple_music"
    if "soundcloud.com" in u: return "soundcloud"
    if "youtube.com" in u or "youtu.be" in u: return "youtube"
    if "bandcamp.com" in u: return "bandcamp"
    if "deezer.com" in u: return "deezer"
    if "instagram.com" in u: return "instagram"
    if "twitter.com" in u or "x.com" in u: return "twitter"
    if "facebook.com" in u or "fb.com" in u: return "facebook"
    if "tiktok.com" in u: return "tiktok"
    if "discogs.com" in u: return "discogs"
    if "allmusic.com" in u: return "allmusic"
    if "songkick.com" in u: return "songkick"
    if "last.fm" in u or "lastfm" in u: return "lastfm"
    if "wikidata.org" in u: return "wikidata"
    if "wikipedia.org" in u: return "wikipedia"
    return "website"

# ── MusicBrainz Link Type Map ─────────────────────────────────────────────────
MB_LINK_MAP = {
    "spotify": "spotify",
    "apple music": "apple_music",
    "soundcloud": "soundcloud",
    "youtube": "youtube",
    "bandcamp": "bandcamp",
    "deezer": "deezer",
    "instagram": "instagram",
    "twitter": "twitter",
    "facebook": "facebook",
    "tiktok": "tiktok",
    "official homepage": "website",
    "official site": "website",
    "wikidata": "wikidata",
    "discogs": "discogs",
    "allmusic": "allmusic",
}

# ── Shared Helpers ─────────────────────────────────────────────────────────────
def normalize(text):
    """Unicode-normalize to ASCII lowercase for fuzzy matching."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower().strip()
