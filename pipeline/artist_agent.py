"""
artist_agent.py — Bio-only synthesis from real API data.

The pipeline (Phase 3.5) calls 14+ real APIs and merges results in engine.py.
This module takes the *already-collected* profile dict and asks the LLM
to write a short, factual bio from the evidence — nothing else.

No URLs, no emails, no locale are produced by the LLM.
Those come exclusively from the deterministic pipeline.
"""

import json
import logging
from pipeline.ai_engine import ai_call

log = logging.getLogger(__name__)


# ── public API ────────────────────────────────────────────────────────


def synthesize_bio_batch(profiles: dict[str, dict], batch_size: int = 15) -> dict[str, str]:
    """Generate bios for a batch of profiles using evidence from real APIs.

    Args:
        profiles: {normalized_name: profile_dict}  (profile already has
                  merged data from Last.fm, Wikipedia, Wikidata, Discogs,
                  Genius, etc.)
        batch_size: how many artists per LLM call (default 15).

    Returns:
        {normalized_name: bio_string}
    """
    results: dict[str, str] = {}
    items = list(profiles.items())

    for start in range(0, len(items), batch_size):
        chunk = items[start : start + batch_size]
        bios = _synthesize_chunk(chunk)
        results.update(bios)

    log.info(f"   ✅ AI bios synthesized: {len(results)}/{len(profiles)}")
    return results


# ── internal ──────────────────────────────────────────────────────────


def _evidence_summary(profile: dict) -> str:
    """Compress a profile into a concise evidence block for the LLM."""
    parts: list[str] = []

    # Name
    parts.append(f"Name: {profile.get('name', '?')}")

    # Genre
    genre = profile.get("genre")
    if genre and genre != "Don't Box Me!":
        parts.append(f"Genre: {genre}")
    all_genres = profile.get("all_genres")
    if all_genres:
        parts.append(f"All genres: {', '.join(all_genres[:5])}")

    # Origin / locale
    locale = profile.get("locale", {})
    birthplace = profile.get("birthplace")
    if birthplace:
        parts.append(f"From: {birthplace}")
    elif locale.get("city") or locale.get("country"):
        origin = ", ".join(filter(None, [locale.get("city"), locale.get("state"), locale.get("country")]))
        parts.append(f"From: {origin}")

    # Bio snippets already collected from APIs
    existing_bio = profile.get("bio")
    if existing_bio:
        parts.append(f"Existing bio excerpt: {existing_bio[:300]}")

    # Structured facts
    if profile.get("born"):
        parts.append(f"Born: {profile['born']}")
    if profile.get("years_active"):
        parts.append(f"Years active: {profile['years_active']}")
    if profile.get("instruments"):
        parts.append(f"Instruments: {', '.join(profile['instruments'][:5])}")
    if profile.get("record_labels"):
        parts.append(f"Labels: {', '.join(profile['record_labels'][:4])}")
    if profile.get("occupations"):
        parts.append(f"Occupations: {', '.join(profile['occupations'][:4])}")
    if profile.get("kg_description"):
        parts.append(f"Knowledge Graph: {profile['kg_description']}")

    # Popularity signals
    if profile.get("listeners"):
        parts.append(f"Last.fm listeners: {profile['listeners']}")
    if profile.get("soundcloud_followers"):
        parts.append(f"SoundCloud followers: {profile['soundcloud_followers']}")
    if profile.get("bandsintown_trackers"):
        parts.append(f"Bandsintown trackers: {profile['bandsintown_trackers']}")

    # Songs / albums
    if profile.get("top_songs"):
        songs = profile["top_songs"][:5]
        song_names = [
            (s.get("name") or s.get("title") or str(s)) if isinstance(s, dict) else str(s)
            for s in songs
        ]
        parts.append(f"Top songs: {', '.join(song_names)}")
    if profile.get("album_count"):
        parts.append(f"Albums: {profile['album_count']}")
    if profile.get("discography"):
        titles = [a.get("title", "") for a in profile["discography"][:4]]
        parts.append(f"Discography: {', '.join(titles)}")

    # Tags
    if profile.get("lastfm_tags"):
        parts.append(f"Last.fm tags: {', '.join(profile['lastfm_tags'][:5])}")

    # Touring
    if profile.get("is_touring"):
        parts.append("Currently touring: yes")
    if profile.get("total_setlists"):
        parts.append(f"Total setlists on Setlist.fm: {profile['total_setlists']}")

    return "\n".join(parts)


def _synthesize_chunk(chunk: list[tuple[str, dict]]) -> dict[str, str]:
    """Ask the LLM to write bios for a chunk of artists using evidence."""
    numbered: list[str] = []
    keys: list[str] = []

    for idx, (norm, profile) in enumerate(chunk, 1):
        evidence = _evidence_summary(profile)
        numbered.append(f"--- Artist {idx} ---\n{evidence}")
        keys.append(norm)

    evidence_block = "\n\n".join(numbered)

    prompt = f"""You are writing factual artist bios for a music database.
Below is VERIFIED data collected from MusicBrainz, Wikipedia, Last.fm,
Wikidata, Discogs, Genius, Spotify, SoundCloud, etc.

RULES:
- Write exactly 1-2 sentences per artist.
- Use ONLY the evidence provided — do NOT add facts you are not given.
- Format: "[Artist Name] is a [genre] [artist/band/DJ/producer] from
  [origin], known for [notable style/works/achievements]."
- If evidence is too thin, write: "Emerging [genre] artist." — do NOT
  fabricate.
- Do NOT include URLs, emails, or social links in the bio text.

{evidence_block}

Return JSON: {{"bios": ["bio for artist 1", "bio for artist 2", ...]}}"""

    raw = ai_call(prompt)
    bios: dict[str, str] = {}

    if raw and isinstance(raw, dict) and "bios" in raw:
        bio_list = raw["bios"]
        for i, norm in enumerate(keys):
            if i < len(bio_list) and bio_list[i]:
                bios[norm] = str(bio_list[i]).strip()
    elif raw and isinstance(raw, list):
        # Fallback: ai_call returned a list directly
        for i, norm in enumerate(keys):
            if i < len(raw):
                entry = raw[i]
                if isinstance(entry, dict) and "bio" in entry:
                    bios[norm] = str(entry["bio"]).strip()
                elif isinstance(entry, str):
                    bios[norm] = entry.strip()

    return bios
