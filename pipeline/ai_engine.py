"""
pipeline/ai_engine.py
AI wrappers (Google AI Studio → OpenRouter fallback), batch classify, batch enrich.
"""
import json, logging, time, urllib.request, urllib.error

from pipeline.config import (
    GOOGLE_API_KEY, GOOGLE_MODEL, OPENROUTER_KEY, MODEL,
    SSL_CTX, GENRES,
)

log = logging.getLogger(__name__)

# ── Rate-limiter state for Google free tier (15 req/min) ──
_last_google_call = 0
GOOGLE_DELAY = 4.5  # seconds between Google AI calls


# ── Core AI call ──

def ai_call(prompt, retries=3):
    """Call AI API. Priority: Google AI Studio (free) → OpenRouter (paid).
    Includes rate limiting for Google free tier."""
    global _last_google_call

    # Try Google AI Studio first (free)
    if GOOGLE_API_KEY:
        for attempt in range(retries + 1):
            # Rate limit: wait between calls
            elapsed = time.time() - _last_google_call
            if elapsed < GOOGLE_DELAY:
                time.sleep(GOOGLE_DELAY - elapsed)

            try:
                data = json.dumps({
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"responseMimeType": "application/json"}
                }).encode()
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{GOOGLE_MODEL}:generateContent?key={GOOGLE_API_KEY}"
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                _last_google_call = time.time()
                with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as r:
                    resp = json.loads(r.read())
                    content = resp["candidates"][0]["content"]["parts"][0]["text"]
                    return json.loads(content)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = min(15 * (attempt + 1), 60)  # 15s, 30s, 45s, 60s
                    log.warning(f"  Google AI rate limited (429), waiting {wait}s... (attempt {attempt+1}/{retries+1})")
                    time.sleep(wait)
                elif attempt < retries:
                    time.sleep(2)
                else:
                    log.warning(f"  Google AI failed: {e}")
            except Exception as e:
                if attempt < retries:
                    time.sleep(2)
                else:
                    log.warning(f"  Google AI failed: {e}, trying OpenRouter...")

    # Fallback to OpenRouter
    if OPENROUTER_KEY:
        for attempt in range(retries + 1):
            try:
                data = json.dumps({
                    "model": MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": {"type": "json_object"}
                }).encode()
                req = urllib.request.Request(
                    "https://openrouter.ai/api/v1/chat/completions",
                    data=data,
                    headers={"Authorization": f"Bearer {OPENROUTER_KEY}", "Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=60) as r:
                    content = json.loads(r.read())["choices"][0]["message"]["content"]
                    return json.loads(content)
            except Exception as e:
                if attempt < retries:
                    time.sleep(2)
                else:
                    log.warning(f"  OpenRouter failed: {e}")
                    return None

    log.error("  No API key available! Set GOOGLE_API_KEY or OPENROUTER_API_KEY")
    return None


# ── Result parsing ──

def parse_ai_result(result):
    """Parse AI result that could be dict with results key or direct list."""
    if not result:
        return []
    if isinstance(result, dict):
        for key in ["results", "data", "titles"]:
            if key in result and isinstance(result[key], list):
                return result[key]
        vals = list(result.values())
        if vals and isinstance(vals[0], list):
            return vals[0]
    return result if isinstance(result, list) else []


# ── Batch classify ──

def classify_batch(events_batch):
    """Classify events using David's methodology: artist isolation + verification."""
    items = "\n".join([
        f'{i+1}. City: {e["city"]} | Venue: {e["venue"]} | Event: "{e["title"]}"'
        for i, e in enumerate(events_batch)
    ])

    genres_list = ", ".join(GENRES)

    prompt = f"""You are an internet researcher specialized in live music. Analyze each event listing and extract performing artist info.

## ARTIST ISOLATION RULES:
- Identify the PERFORMING artist(s) from the event string
- "Artist at Venue" → artist is "Artist"
- "Artist: Tour Name" → artist is "Artist"
- "An Evening With X" → artist is "X" 
- "X presents Y" → Y is the performer, NOT X
- If MULTIPLE artists perform (e.g. "X, Y & Z" or "X w/ Y feat. Z"), list ALL of them in the artists array
  - Include ensemble leaders as individual artists AND keep ensemble name
  - Example: "John Doe Quartet feat. Jane Roe" → ["John Doe Quartet", "Jane Roe"]

## HOST vs PERFORMER (CRITICAL):
- "Late Late Show hosted by X" → X is the PERFORMER (they host AND perform)
- "X presents Y" → Y is the performer, X is the presenter/host → only list Y
- "X - The Y Show" → if X is a TV PRESENTER or non-musician celebrity, set artists = [] and is_event = true
- TV presenters, actors, comedians HOSTING music events are NOT artists
  - Examples: Ore Oduba, Graham Norton, James Corden → these are HOSTS, not musicians
  - If title = "ORE ODUBA - The Vintage Boys Rock 'n' Roll Show" → artists = ["The Vintage Boys"], NOT Ore Oduba
- If someone is HOSTING/PRESENTING a show, it's the PERFORMERS in the show that are the artists

## "+MORE" / "& GUESTS" DETECTION:
- If title contains "+ more", "& more", "and more", "+ guests", "& friends", "+ special guests", "+ support", "+ tba"
  → set has_more = true (we will search for full lineup separately)
  → still list ALL artists that ARE named in the title

## CRITICAL: COVER BAND / TRIBUTE DETECTION:
- "live album Guns and Roses at club X" → this is a COVER BAND, NOT Guns N Roses. Artist = null
- "Jimmy J band plays Bruce Springsteen" → artist is "Jimmy J band", NOT Bruce Springsteen  
- If a DECEASED artist name appears for a small venue → it's a tribute act. Artist = null
- If a MEGA-STAR (e.g. Michael Jackson, Nirvana, Beatles) appears at a small local venue → likely a tribute. Artist = null
- Words like "tribute", "covers", "plays songs of", "best of", "legacy" suggest tribute act
- "The Year Grunge Broke" → this is an EVENT NAME about grunge genre, NOT an artist

## EVENT TYPE DETECTION:
- Pick ONE event type:
  KEEP: Live Music Performance, Music Festival, Dance Party, Jam / Open Mike, Other
  DELETE: Comedy, Podcast, Dance performance, Theater production, Stage play, Drag performance, Spoken word, Sport, Culinairy, Arts & Crafts, Exhibition
- NOTE: Use "Live Music Performance" for concerts with specific performing artists

## GENRE:
- Pick ONE best-fitting genre from: [{genres_list}]
- MUST come from this exact list. Use "Don't Box Me!" only if truly unclassifiable.

## EVENT BIO (for is_event=true ONLY):
- Write a 1-sentence description of what the event is about
- Example: "A monthly open mic night featuring local acoustic artists."
- Example: "Multi-day metal and doom festival with international headliners."
- If you cannot determine what the event is about, use null

## WHAT TO DO WHEN NO ARTIST:
- Festivals (DESERTFEST, Camden Rocks, etc.) → artists = [], is_event = true
- Generic events (open mic, jam session, dance party by genre name) → artists = [], is_event = true  
- Non-music events → artists = [], delete = true
- Radio show anniversaries, club nights without specific performers → artists = [], is_event = true

Events:
{items}

Return JSON: {{"results": [
  {{"i": 1, "artists": ["Name1", "Name2"], "genre": "Genre", "event_type": "Type", "delete": false, "is_event": false, "has_more": false, "event_bio": null}},
  ...
]}}
For events with no artist, set artists to empty array [] and is_event to true."""

    return parse_ai_result(ai_call(prompt))


# ── Batch enrich ──

def enrich_batch(artists_batch):
    """Enrich artists using David's methodology: deep research + 3-pass email."""
    items = "\n".join([
        f'{i+1}. Artist: "{a["name"]}" (performing in: {a["city"]})'
        for i, a in enumerate(artists_batch)
    ])

    prompt = f"""You are doing deep research on music artists. For each artist do TWO rounds of searches.

## BIO (≤ 2 sentences, STRICT FORMAT):
- Format: "[Artist Name] is a [genre] artist from [city/country], known for [notable style/works/achievements]."
- MUST be specifically about THIS artist — NO generic filler text
- MUST mention genre/style and origin if known
- If you cannot find verified info about the artist, write: "Emerging [genre] artist." — do NOT fabricate
- REJECTED examples (DO NOT generate bios like these):
  - "This domain is available for sale" (scraped junk)
  - "A lens is a transmissive optical device" (irrelevant)
  - "Music is a universal language" (generic filler)
  - Any text not specifically about the artist
- GOOD examples:
  - "Mark Mayura is a London-based jazz trumpeter known for his mellow flows and live compositions."
  - "Solar Bears are an Irish electronic duo from Dublin, blending ambient textures with retro synth melodies."

## LOCALE:
- Where the artist is FROM or resides (NOT where they are performing)
- locale_city, locale_state, locale_country
- If unknown after research, use null

## URLs (up to 3, verified):
- Prioritize: official artist website, Bandcamp, SoundCloud, MusicBrainz, Spotify, Apple Music BEFORE social pages
- All URLs must be cross-checked: artist name must match page content
- REJECT Spotify URLs with placeholder/repeating character IDs
- YouTube only as last resort with 100% match certainty
- If not verified, use null

## EMAILS (3-pass search):
- Pass 1: Look for "mailto:" links in official websites, social media bios, Linktrees
- Pass 2: Search web for press releases, booking agencies, music blogs mentioning email
- Pass 3: Use locale context to find regional directories
- ONLY provide emails you are 100% sure belong to the artist or their official team
- You may ONLY guess "info@domain.com" if artist has a verified personal homepage AND no email is listed
- If no verified email found, use null. Do NOT invent generic emails like artist@gmail.com
- Booking agency contacts are acceptable (e.g., Wasserman, Red Light Management)
- REJECTED email patterns: noreply@, info@gmail.com, contact@yahoo.com, booking@hotmail.com

Artists:
{items}

Return JSON: {{"results": [
  {{"i": 1, "bio": "...", "locale_city": "...", "locale_state": "...", "locale_country": "...", "url1": "...", "url2": null, "url3": null, "email1": null, "email2": null, "email3": null}},
  ...
]}}"""

    return parse_ai_result(ai_call(prompt))


# ── Bio synthesis from verified data ──

def synthesize_bio_from_data(artist_summaries: list[dict]) -> list[dict]:
    """Write bios from verified profile data only. No web search, no fabrication.

    Each item in *artist_summaries* should have:
        name, genre, locale_city, locale_country, platforms (list of names),
        years_active, notable_facts
    Returns list of {"i": 1, "bio": "..."} dicts.
    """
    items = "\n".join([
        (
            f'{i+1}. "{a["name"]}"'
            f' | genre: {a.get("genre", "unknown")}'
            f' | from: {a.get("locale_city") or "?"}, {a.get("locale_country") or "?"}'
            f' | platforms: {", ".join(a.get("platforms", [])) or "none known"}'
            f' | active: {a.get("years_active", "?")}'
            f' | facts: {a.get("notable_facts", "none")}'
        )
        for i, a in enumerate(artist_summaries)
    ])

    prompt = f"""You are writing short artist bios from VERIFIED DATA ONLY.
You must NOT add any information not present in the data below.
If the data is too sparse for a meaningful bio, return "Emerging artist." — do NOT fabricate.

## Rules:
- 1-2 sentences maximum
- Format: "[Name] is a [genre] artist from [city/country], known for [style/facts]."
- Use ONLY the data provided — no external knowledge, no guessing
- If genre is "unknown" and no facts exist, return "Emerging artist."
- Do NOT mention platform names in the bio (Spotify, SoundCloud, etc.)

Artists:
{items}

Return JSON: {{"results": [
  {{"i": 1, "bio": "..."}},
  ...
]}}"""

    return parse_ai_result(ai_call(prompt))


# ── Event lineup search ──

def search_event_lineup(event_title, venue, city, known_artists):
    """Use AI to find additional artists for events with '+ more' etc."""
    prompt = f"""An event listing says: "{event_title}" at venue "{venue}" in {city}.

We already identified these artists: {', '.join(known_artists)}

But the title contains "+ more" or similar, meaning there are ADDITIONAL performers not listed in the title.

Using your knowledge of this venue, city, and the known artists' genre/scene, identify any additional artists that are likely performing at this event.

Rules:
- Only list artists you are reasonably confident about
- These should be real, active music artists
- Consider the genre of known artists to narrow down possibilities
- If you cannot determine additional artists with confidence, return empty array

Return JSON: {{"additional_artists": ["Artist1", "Artist2"]}}"""

    result = ai_call(prompt)
    if result and isinstance(result, dict):
        return result.get("additional_artists", [])
    return []
