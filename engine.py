"""
engine.py — GroovonEngine: pluggable source → pipeline → sink orchestrator.

    engine = GroovonEngine(
        source=ExcelSource(config={"path": "input.xlsx"}),
        sinks=[
            ExcelSink(config={"output_path": "output.xlsx"}),
            JsonSink(config={"output_path": "profiles.json"}),
        ],
    )
    engine.run(batch_size=15, start=0, limit=9999)

The engine owns the 5-phase pipeline:
  1. AI Classification  (classify_batch)
  2. Spotify/MusicBrainz Verification  (verify_artist_multiplatform)
  3. AI Enrichment  (enrich_batch — David's 3-pass email search)
  3.5. Profile Building  (13 sources: MB links, Last.fm, Wikipedia, Wikidata,
       iTunes, Discogs, YouTube, Genius, Google KG, Setlist.fm, Bandsintown,
       SoundCloud, Bandcamp + website scraping + genre resolution)
  4. URL Verification + Email Validation
  5. Output via sinks

Internally it delegates to the same functions that process_david_excel.py
calls — zero logic duplication.
"""

from __future__ import annotations

import logging
import os
import time
import urllib.parse
from typing import Any

from sources.base import EventSource
from sinks.base import OutputSink
from pipeline.checkpoint import Checkpoint
from pipeline.config import (
    normalize, GENRES, validate_genre, classify_url,
    SERPER_KEY,
)
from pipeline.event_model import canonical_event_id, canonical_artist_id
from pipeline.artist_agent import synthesize_bio_batch

log = logging.getLogger(__name__)


class GroovonEngine:
    """Orchestrates: source.fetch() → pipeline phases → sink.write()."""

    def __init__(
        self,
        source: EventSource,
        sinks: list[OutputSink] | None = None,
        *,
        dry_run: bool = False,
    ):
        self.source = source
        self.sinks: list[OutputSink] = sinks or []
        self.dry_run = dry_run

    # ── main entry point ────────────────────────────────────────────
    def run(
        self,
        *,
        batch_size: int = 15,
        start: int = 0,
        limit: int = 9999,
    ) -> dict[str, Any]:
        """Execute the full pipeline and return a stats dict."""

        # 0. Validate source + sinks
        if not self.source.validate():
            raise RuntimeError(f"Source validation failed: {self.source}")
        for sink in self.sinks:
            if not sink.validate():
                log.warning(f"Sink validation failed: {sink} — will skip")

        # 1. Fetch events
        events = self.source.fetch()
        original_events_count = len(events)
        events = self._dedupe_events(events)
        deduped = original_events_count - len(events)
        if deduped > 0:
            log.info(f"   🔁 Deduped events: removed {deduped} duplicates")
        events = events[start : start + limit]
        log.info(f"   Processing {len(events)} events (start={start}, limit={limit})\n")

        # 2. Checkpoint
        batch_id = f"engine_{self.source.name}"
        cp = Checkpoint(batch_id)
        cp_state = cp.load()
        cp_phase = cp_state["phase"] if cp_state else 0
        cp_data = cp_state["data"] if cp_state else {}
        if cp_phase:
            log.info(f"   ⏩ Resuming — skipping phases already completed (up to phase {cp_phase})\n")

        # ── Lazy imports (avoid circular / heavy load at module level) ──
        from pipeline.ai_engine import classify_batch
        from pipeline.artist_agent import synthesize_bio_batch
        from pipeline.fetchers import (
            verify_artist_multiplatform,
            mb_get_links, lastfm_search, wikipedia_search, wikidata_search,
            itunes_search, discogs_search, youtube_search, genius_search,
            google_kg_search, setlistfm_search,
            bandsintown_search, soundcloud_search, bandcamp_search,
            deezer_extended_search, extract_mb_extra_links,
            build_profile, scrape_official_site, scrape_linktree,
            serper_search, ddg_search,
            verify_url,
            parallel_fetch,
        )
        from pipeline.validator import (
            rank_emails,
            email_label,
            is_trusted,
            validate_email_detailed,
            compute_profile_quality,
        )
        from pipeline.excel_io import sanitize_event_type

        # ────────────────────────────────────────────────────────────
        # Phase 1: AI Classification (skipped for artist_list sources)
        # ────────────────────────────────────────────────────────────
        _is_artist_list = getattr(self.source, "source_type", None) == "artist_list"

        if _is_artist_list:
            # Events are already pre-classified by ArtistListSource
            classified = list(events)
            log.info(f"🤖 Phase 1: ⏩ Skipped (artist_list source) — {len(classified)} pre-classified artists\n")
        elif cp_phase >= 1:
            classified = cp_data["classified"]
            log.info(f"🤖 Phase 1: ⏩ Restored {len(classified)} classified events from checkpoint")
        else:
            log.info("🤖 Phase 1: AI Classification …")
            classified = []
            total_batches = (len(events) + batch_size - 1) // batch_size

            for batch_start_idx in range(0, len(events), batch_size):
                batch = events[batch_start_idx : batch_start_idx + batch_size]
                batch_num = batch_start_idx // batch_size + 1
                results = classify_batch(batch)
                processed_indices: set[int] = set()

                for res in results:
                    idx = res.get("i", 1) - 1
                    if 0 <= idx < len(batch):
                        processed_indices.add(idx)
                        event = dict(batch[idx])
                        event["artists"] = res.get("artists") or []
                        event["genre"] = res.get("genre", "Don't Box Me!")
                        event["event_type"] = res.get("event_type", "Other")
                        event["delete"] = res.get("delete", False)
                        event["is_event"] = res.get("is_event", False)
                        event["has_more"] = res.get("has_more", False)
                        event["event_bio"] = res.get("event_bio")
                        classified.append(event)

                for idx, e in enumerate(batch):
                    if idx not in processed_indices:
                        e2 = dict(e)
                        e2["artists"] = []
                        e2["genre"] = "Don't Box Me!"
                        e2["event_type"] = "Other"
                        e2["delete"] = False
                        e2["is_event"] = True
                        classified.append(e2)

                log.info(f"  [{batch_num}/{total_batches}] ✅ batch classified")

            cp.save(phase=1, classified=classified)
            log.info(f"   Classification done: {len(classified)} events\n")

        # ── Build unique_artists (needed by all later phases) ──
        unique_artists: dict[str, dict] = {}
        for e in classified:
            for a in e.get("artists", []):
                norm = normalize(a)
                if norm and norm not in unique_artists:
                    entry: dict[str, Any] = {
                        "name": a,
                        "city": e["city"],
                        "genre": e.get("genre", ""),
                        "artist_id": canonical_artist_id(a, city_hint=e.get("city")),
                        "city_hints": [e.get("city", "")],
                    }
                    # Carry forward seed data from ArtistListSource
                    seed = e.get("_seed")
                    if seed:
                        entry["_seed"] = seed
                    unique_artists[norm] = entry
                elif norm:
                    city_hint = e.get("city", "")
                    if city_hint and city_hint not in unique_artists[norm].get("city_hints", []):
                        unique_artists[norm].setdefault("city_hints", []).append(city_hint)

        # ────────────────────────────────────────────────────────────
        # Phase 2: Multi-Platform Verification (Spotify + Deezer + MB)
        # ────────────────────────────────────────────────────────────
        if cp_phase >= 2:
            verify_cache = cp_data["verify_cache"]
            log.info(f"🔍 Phase 2: ⏩ Restored {len(verify_cache)} verified artists from checkpoint")
        else:
            log.info("🔍 Phase 2: Multi-Platform Verification (Spotify + Deezer + MusicBrainz)...")
            log.info(f"   {len(unique_artists)} unique artists to verify")
            verify_cache: dict = {}
            verified = 0

            for i, (norm, info) in enumerate(unique_artists.items()):
                result = verify_artist_multiplatform(info["name"])
                if result:
                    verify_cache[norm] = result
                    verified += 1
                if (i + 1) % 20 == 0:
                    sources_count: dict[str, int] = {}
                    for v in verify_cache.values():
                        for s in v.get("sources", []):
                            sources_count[s] = sources_count.get(s, 0) + 1
                    src_str = ", ".join(f"{k}={v}" for k, v in sources_count.items())
                    log.info(f"  [{i + 1}/{len(unique_artists)}] ✅ {verified} verified ({src_str})")
                time.sleep(0.05)

            sources_count = {}
            for v in verify_cache.values():
                for s in v.get("sources", []):
                    sources_count[s] = sources_count.get(s, 0) + 1
            src_str = ", ".join(f"{k}={v}" for k, v in sources_count.items())
            log.info(f"   Verified: {verified}/{len(unique_artists)} ({src_str})\n")
            cp.save(phase=2, classified=classified, verify_cache=verify_cache)

        # ────────────────────────────────────────────────────────────
        # Phase 3: (Enrichment removed — was hallucinating data)
        #   Bio synthesis now happens AFTER profile building in Phase 3.5
        # ────────────────────────────────────────────────────────────
        enrichment_cache: dict = {}  # kept for downstream compat
        log.info("📝 Phase 3: ⏩ Skipped (hallucinating enrich_batch replaced by bio synthesis in Phase 3.5)")

        # ────────────────────────────────────────────────────────────
        # Phase 3.5: Profile Building (13 sources + scraping + genre)
        # ────────────────────────────────────────────────────────────
        if cp_phase >= 35:
            profile_cache = cp_data.get("profile_cache", {})
            log.info(f"🌐 Phase 3.5: ⏩ Restored {len(profile_cache)} artist profiles from checkpoint")
        else:
            profile_cache = self._run_profile_building(
                unique_artists=unique_artists,
                verify_cache=verify_cache,
                enrichment_cache=enrichment_cache,
                # fetcher functions (passed to avoid re-importing)
                mb_get_links=mb_get_links,
                lastfm_search=lastfm_search,
                wikipedia_search=wikipedia_search,
                wikidata_search=wikidata_search,
                itunes_search=itunes_search,
                discogs_search=discogs_search,
                youtube_search=youtube_search,
                genius_search=genius_search,
                google_kg_search=google_kg_search,
                setlistfm_search=setlistfm_search,
                bandsintown_search=bandsintown_search,
                soundcloud_search=soundcloud_search,
                bandcamp_search=bandcamp_search,
                deezer_extended_search=deezer_extended_search,
                extract_mb_extra_links=extract_mb_extra_links,
                build_profile=build_profile,
                scrape_official_site=scrape_official_site,
                scrape_linktree=scrape_linktree,
                serper_search=serper_search,
                ddg_search=ddg_search,
                parallel_fetch=parallel_fetch,
            )
            cp.save(
                phase=35,
                classified=classified,
                verify_cache=verify_cache,
                enrichment_cache=enrichment_cache,
                profile_cache=profile_cache,
            )

        # ────────────────────────────────────────────────────────────
        # Phase 4: URL Verification + Email Validation
        # ────────────────────────────────────────────────────────────
        log.info("🔗 Phase 4: URL Verification + Email Validation...")

        urls_checked = urls_valid = urls_removed = 0
        emails_checked = emails_valid = emails_removed = 0

        for _norm, profile in profile_cache.items():
            provenance = profile.setdefault("provenance", {})
            if not provenance:
                if profile.get("bio"):
                    provenance["bio"] = {"source": "unknown"}
                if profile.get("genre"):
                    provenance["genre"] = {"source": profile.get("genre_source") or "unknown"}
                locale = profile.get("locale", {}) or {}
                if locale.get("city"):
                    provenance["locale.city"] = {"source": "unknown"}
                if locale.get("state"):
                    provenance["locale.state"] = {"source": "unknown"}
                if locale.get("country"):
                    provenance["locale.country"] = {"source": "unknown"}
                for p_name, p_url in (profile.get("platforms") or {}).items():
                    provenance[f"platforms.{p_name}"] = {"source": "unknown", "evidence_url": p_url}

            valid_platforms: dict[str, str] = {}
            for platform, url in profile.get("platforms", {}).items():
                if url:
                    urls_checked += 1
                    if is_trusted(url) or verify_url(url):
                        urls_valid += 1
                        valid_platforms[platform] = url
                        prov_key = f"platforms.{platform}"
                        if prov_key in provenance:
                            provenance[prov_key]["validated"] = True
                        else:
                            provenance[prov_key] = {
                                "source": "unknown",
                                "evidence_url": url,
                                "validated": True,
                            }
                    else:
                        urls_removed += 1
            profile["platforms"] = valid_platforms
            profile["urls"] = list(valid_platforms.values())

            valid_emails: list[str] = []
            email_checks: dict[str, dict] = {}
            for em in profile.get("emails", []):
                emails_checked += 1
                details = validate_email_detailed(em)
                email_checks[em] = details
                if details.get("valid"):
                    emails_valid += 1
                    valid_emails.append(em)
                    provenance[f"emails.{em}"] = {
                        "source": "email_validator",
                        "method": details.get("method"),
                        "provider": details.get("provider"),
                        "status": details.get("status"),
                        "validated": True,
                    }
                else:
                    emails_removed += 1
            profile["emails"] = rank_emails(valid_emails)
            profile["email_verification"] = email_checks
            labels = profile.setdefault("email_labels", {})
            for em in profile["emails"]:
                labels[em] = email_label(em)

            quality = compute_profile_quality(profile)
            profile.update(quality)

        log.info(f"   URLs: {urls_valid}/{urls_checked} valid ({urls_removed} removed)")
        log.info(f"   Emails: {emails_valid}/{emails_checked} valid ({emails_removed} removed)\n")

        # ────────────────────────────────────────────────────────────
        # Phase 5: Build output rows + write to sinks
        # ────────────────────────────────────────────────────────────
        output_rows = self._build_output_rows(classified)

        if self.dry_run:
            log.info("🔍 DRY-RUN mode: skipping sink writes")
        else:
            for sink in self.sinks:
                try:
                    result = sink.write(
                        classified=classified,
                        verify_cache=verify_cache,
                        enrichment_cache=enrichment_cache,
                        profile_cache=profile_cache,
                        output_rows=output_rows,
                    )
                    log.info(f"   ✅ {sink.name}: {result}")
                except Exception as exc:
                    log.error(f"   ❌ {sink.name}: {exc}")

        # Clean up checkpoint
        cp.delete()

        stats = {
            "events_in": len(events),
            "total_events": len(events),  # backward-compatible alias
            "events_deduped": deduped,
            "classified": len(classified),
            "unique_artists": len(unique_artists),
            "verified": len(verify_cache),
            "enriched": len(enrichment_cache),
            "profiles": len(profile_cache),
            "output_rows": len(output_rows),
            "sinks": [s.name for s in self.sinks],
        }
        log.info(f"\n🎉 Done! {stats}")
        return stats

    # ── Phase 3.5 — extracted for readability ────────────────────────
    def _run_profile_building(
        self,
        *,
        unique_artists: dict,
        verify_cache: dict,
        enrichment_cache: dict,
        # All fetcher functions passed explicitly to avoid re-importing
        **fetchers,
    ) -> dict:
        """Phase 3.5: query 13 sources, scrape websites, resolve genres."""
        log.info(
            "🌐 Phase 3.5: Profile Building (13 sources: MusicBrainz, Last.fm, "
            "Wikipedia, Wikidata, iTunes, Discogs, YouTube, Genius, Google KG, "
            "Setlist.fm, Bandsintown, SoundCloud, Bandcamp + scraping)..."
        )

        # Unpack fetcher functions
        mb_get_links = fetchers["mb_get_links"]
        lastfm_search = fetchers["lastfm_search"]
        wikipedia_search = fetchers["wikipedia_search"]
        wikidata_search = fetchers["wikidata_search"]
        itunes_search = fetchers["itunes_search"]
        discogs_search = fetchers["discogs_search"]
        youtube_search = fetchers["youtube_search"]
        genius_search = fetchers["genius_search"]
        google_kg_search = fetchers["google_kg_search"]
        setlistfm_search = fetchers["setlistfm_search"]
        bandsintown_search = fetchers["bandsintown_search"]
        soundcloud_search = fetchers["soundcloud_search"]
        bandcamp_search = fetchers["bandcamp_search"]
        deezer_extended_search = fetchers["deezer_extended_search"]
        extract_mb_extra_links = fetchers["extract_mb_extra_links"]
        build_profile = fetchers["build_profile"]
        scrape_official_site = fetchers["scrape_official_site"]
        scrape_linktree = fetchers["scrape_linktree"]
        serper_search = fetchers["serper_search"]
        ddg_search = fetchers["ddg_search"]
        parallel_fetch = fetchers["parallel_fetch"]

        # Sub-caches
        mb_links_cache: dict = {}
        lastfm_cache: dict = {}
        wiki_cache: dict = {}
        wikidata_cache: dict = {}
        itunes_cache: dict = {}
        discogs_cache: dict = {}
        youtube_cache: dict = {}
        genius_cache: dict = {}
        kg_cache: dict = {}
        setlistfm_cache: dict = {}
        bandsintown_cache: dict = {}
        soundcloud_cache: dict = {}
        bandcamp_cache: dict = {}
        scraped_cache: dict = {}
        deezer_ext_cache: dict = {}
        mb_extras_cache: dict = {}
        profile_cache: dict = {}

        mb_links_found = 0
        spotify_via_mb = 0
        sites_scraped = 0
        stats: dict[str, int] = {
            "lastfm": 0, "wiki": 0, "wikidata": 0, "itunes": 0,
            "discogs": 0, "youtube": 0, "genius": 0, "ddg": 0,
            "linktree": 0, "kg": 0, "serper": 0, "setlistfm": 0,
            "bandsintown": 0, "soundcloud": 0, "bandcamp": 0,
            "deezer_ext": 0, "mb_extras": 0,
        }

        def _set_prov(prov: dict, key: str, source: str, evidence_url: str | None = None) -> None:
            entry = {"source": source}
            if evidence_url:
                entry["evidence_url"] = evidence_url
            prov[key] = entry

        def _platform_source(
            *,
            url: str,
            platform: str,
            vf: dict | None,
            enrich: dict,
            mb_links: dict | None,
            mb_ext: dict | None,
            scraped: dict | None,
            lfm: dict | None,
            wiki: dict | None,
            it: dict | None,
            dc: dict | None,
            yt: dict | None,
            gn: dict | None,
            kg: dict | None,
            sl: dict | None,
            bit: dict | None,
            sc: dict | None,
            bc: dict | None,
        ) -> str:
            if mb_links and url in mb_links.values():
                return "musicbrainz_links"
            if mb_ext and url in mb_ext.values():
                return "musicbrainz_links_extra"
            if scraped and (
                (platform in scraped and scraped.get(platform) == url)
                or (platform == "website" and scraped.get("website") == url)
            ):
                return "website_scrape"
            if vf and url in (vf.get("spotify_url"), vf.get("deezer_url")):
                return "platform_verify"
            if lfm and url == lfm.get("lastfm_url"):
                return "lastfm"
            if wiki and url == wiki.get("wiki_url"):
                return "wikipedia"
            if it and url == it.get("itunes_url"):
                return "itunes"
            if dc and (url == dc.get("discogs_url") or url in (dc.get("urls") or [])):
                return "discogs"
            if yt and url == yt.get("channel_url"):
                return "youtube"
            if gn and url == gn.get("genius_url"):
                return "genius"
            if kg and url in (kg.get("website"), kg.get("wiki_url")):
                return "google_kg"
            if sl and url == sl.get("url"):
                return "setlistfm"
            if bit and url in (bit.get("bandsintown_url"), bit.get("facebook_url")):
                return "bandsintown"
            if sc and url == sc.get("soundcloud_url"):
                return "soundcloud"
            if bc and url == bc.get("bandcamp_url"):
                return "bandcamp"
            return "unknown"

        def _build_profile_provenance(
            *,
            profile: dict,
            vf: dict | None,
            enrich: dict,
            mb_links: dict | None,
            mb_ext: dict | None,
            scraped: dict | None,
            lfm: dict | None,
            wiki: dict | None,
            wd: dict | None,
            it: dict | None,
            dc: dict | None,
            yt: dict | None,
            gn: dict | None,
            kg: dict | None,
            sl: dict | None,
            bit: dict | None,
            sc: dict | None,
            bc: dict | None,
        ) -> dict:
            prov = dict(profile.get("provenance", {}))

            _set_prov(prov, "name", "verify_cache" if vf else "classification")

            bio = profile.get("bio")
            if bio:
                if lfm and bio == lfm.get("bio"):
                    _set_prov(prov, "bio", "lastfm", lfm.get("lastfm_url"))
                elif wiki and bio == wiki.get("bio"):
                    _set_prov(prov, "bio", "wikipedia", wiki.get("wiki_url"))
                elif kg and bio == kg.get("bio"):
                    _set_prov(prov, "bio", "google_kg", kg.get("website") or kg.get("wiki_url"))
                elif sc and bio == sc.get("description"):
                    _set_prov(prov, "bio", "soundcloud", sc.get("soundcloud_url"))
                else:
                    _set_prov(prov, "bio", "unknown")

            genre_source = profile.get("genre_source")
            if genre_source:
                _set_prov(prov, "genre", genre_source)
            elif profile.get("genre"):
                _set_prov(prov, "genre", "unknown")

            locale = profile.get("locale", {}) or {}
            if locale.get("city"):
                if vf and locale["city"] == vf.get("mb_area"):
                    _set_prov(prov, "locale.city", "musicbrainz")
                elif sc and locale["city"] == sc.get("city"):
                    _set_prov(prov, "locale.city", "soundcloud")
                else:
                    _set_prov(prov, "locale.city", "unknown")
            if locale.get("state"):
                _set_prov(prov, "locale.state", "unknown")
            if locale.get("country"):
                if vf and locale["country"] == vf.get("mb_country"):
                    _set_prov(prov, "locale.country", "musicbrainz")
                elif sc and locale["country"] == sc.get("country"):
                    _set_prov(prov, "locale.country", "soundcloud")
                else:
                    _set_prov(prov, "locale.country", "unknown")

            if wd and profile.get("wikidata_genres"):
                _set_prov(prov, "wikidata_genres", "wikidata")
            if wd and profile.get("birthplace"):
                _set_prov(prov, "birthplace", "wikidata")
            if wd and profile.get("years_active"):
                _set_prov(prov, "years_active", "wikidata")

            for platform, url in (profile.get("platforms") or {}).items():
                src = _platform_source(
                    url=url,
                    platform=platform,
                    vf=vf,
                    enrich=enrich,
                    mb_links=mb_links,
                    mb_ext=mb_ext,
                    scraped=scraped,
                    lfm=lfm,
                    wiki=wiki,
                    it=it,
                    dc=dc,
                    yt=yt,
                    gn=gn,
                    kg=kg,
                    sl=sl,
                    bit=bit,
                    sc=sc,
                    bc=bc,
                )
                _set_prov(prov, f"platforms.{platform}", src, url)

            scraped_emails = set((scraped or {}).get("emails", []))
            for em in profile.get("emails", []):
                if em in scraped_emails:
                    _set_prov(prov, f"emails.{em}", "website_scrape")
                else:
                    _set_prov(prov, f"emails.{em}", "unknown")

            return prov

        # ── Sub-step A: MusicBrainz links ──
        for i, (norm, info) in enumerate(unique_artists.items()):
            vf = verify_cache.get(norm)
            if vf and vf.get("mb_id"):
                mb_id = vf["mb_id"]
                links = mb_get_links(mb_id)
                if links:
                    mb_links_cache[norm] = links
                    mb_links_found += 1
                    if links.get("spotify") and vf and not vf.get("spotify_url"):
                        vf["spotify_url"] = links["spotify"]
                        spotify_via_mb += 1
                # MB Extra Links (RA, AllMusic, Songkick, RYM, etc.)
                extras = extract_mb_extra_links(links or {})
                if extras:
                    mb_extras_cache[norm] = extras
                    stats["mb_extras"] += 1
            if (i + 1) % 50 == 0:
                log.info(f"  [{i + 1}/{len(unique_artists)}] MB links: {mb_links_found}")

        log.info(f"   ✅ MB links: {mb_links_found}, Spotify via MB: {spotify_via_mb}, MB extras: {stats['mb_extras']}")

        # ── Sub-step B: Parallel API fetches ──
        for norm, info in unique_artists.items():
            name = info["name"]
            vf = verify_cache.get(norm)

            # Last.fm
            try:
                lfm = lastfm_search(name)
                if lfm:
                    lastfm_cache[norm] = lfm
                    stats["lastfm"] += 1
            except Exception:
                pass

            # Wikipedia
            try:
                wiki = wikipedia_search(name)
                if wiki:
                    wiki_cache[norm] = wiki
                    stats["wiki"] += 1
            except Exception:
                pass

            # Wikidata
            try:
                wd = wikidata_search(name)
                if wd:
                    wikidata_cache[norm] = wd
                    stats["wikidata"] += 1
            except Exception:
                pass

            # iTunes
            try:
                it = itunes_search(name)
                if it:
                    itunes_cache[norm] = it
                    stats["itunes"] += 1
            except Exception:
                pass

            # Discogs
            try:
                dc = discogs_search(name)
                if dc:
                    discogs_cache[norm] = dc
                    stats["discogs"] += 1
            except Exception:
                pass

            # YouTube
            try:
                yt = youtube_search(name)
                if yt:
                    youtube_cache[norm] = yt
                    stats["youtube"] += 1
            except Exception:
                pass

            # Genius
            try:
                gn = genius_search(name)
                if gn:
                    genius_cache[norm] = gn
                    stats["genius"] += 1
            except Exception:
                pass

            # Google Knowledge Graph
            try:
                kg = google_kg_search(name)
                if kg:
                    kg_cache[norm] = kg
                    stats["kg"] += 1
            except Exception:
                pass

            # Setlist.fm
            try:
                sl = setlistfm_search(name)
                if sl:
                    setlistfm_cache[norm] = sl
                    stats["setlistfm"] += 1
            except Exception:
                pass

            # Bandsintown
            try:
                bit = bandsintown_search(name)
                if bit:
                    bandsintown_cache[norm] = bit
                    stats["bandsintown"] += 1
            except Exception:
                pass

            # SoundCloud
            try:
                sc = soundcloud_search(name)
                if sc:
                    soundcloud_cache[norm] = sc
                    stats["soundcloud"] += 1
            except Exception:
                pass

            # Bandcamp
            try:
                bc = bandcamp_search(name)
                if bc:
                    bandcamp_cache[norm] = bc
                    stats["bandcamp"] += 1
            except Exception:
                pass

        log.info(
            f"   ✅ APIs done: Last.fm={stats['lastfm']}, Wiki={stats['wiki']}, "
            f"Wikidata={stats['wikidata']}, iTunes={stats['itunes']}, "
            f"Discogs={stats['discogs']}, YouTube={stats['youtube']}, "
            f"Genius={stats['genius']}, KG={stats['kg']}, Setlist.fm={stats['setlistfm']}"
        )
        log.info(
            f"   ✅ NEW APIs: Bandsintown={stats['bandsintown']}, "
            f"SoundCloud={stats['soundcloud']}, Bandcamp={stats['bandcamp']}"
        )

        # ── Sub-step C: Scrape official websites ──
        websites_to_scrape: list[tuple[str, str]] = []
        for norm, links in mb_links_cache.items():
            if links.get("website"):
                websites_to_scrape.append((norm, links["website"]))
        for norm, wd in wikidata_cache.items():
            if norm not in [w[0] for w in websites_to_scrape] and wd.get("websites"):
                websites_to_scrape.append((norm, wd["websites"][0]))
        for norm, dc in discogs_cache.items():
            if norm not in [w[0] for w in websites_to_scrape] and dc.get("urls"):
                for durl in dc["urls"]:
                    if durl and "discogs.com" not in durl and "facebook.com" not in durl:
                        if classify_url(durl) == "website":
                            websites_to_scrape.append((norm, durl))
                            break
        # (Enrichment URL scraping removed — URLs come from real APIs only)

        log.info(f"   {len(websites_to_scrape)} official websites to scrape")
        for i, (norm, site_url) in enumerate(websites_to_scrape):
            scraped = scrape_official_site(site_url)
            if scraped:
                scraped_cache[norm] = scraped
                sites_scraped += 1
                for _platform, pu in scraped.items():
                    if isinstance(pu, str) and "linktr.ee" in pu:
                        lt_data = scrape_linktree(pu)
                        if lt_data:
                            for k2, v2 in lt_data.items():
                                if k2 not in scraped:
                                    scraped[k2] = v2
                            stats["linktree"] += 1
            if (i + 1) % 10 == 0:
                log.info(f"  [{i + 1}/{len(websites_to_scrape)}] Scraped: {sites_scraped} sites")

        # Also scrape KG-discovered websites
        for norm, kg in kg_cache.items():
            if kg.get("website") and norm not in [w[0] for w in websites_to_scrape]:
                scraped = scrape_official_site(kg["website"])
                if scraped:
                    scraped_cache[norm] = scraped
                    sites_scraped += 1

        # Search for artists without a website via Serper/DDG
        scraped_norms = set(scraped_cache.keys()) | {w[0] for w in websites_to_scrape}
        artists_without_site = [n for n in unique_artists if n not in scraped_norms]
        if artists_without_site:
            search_fn = serper_search if SERPER_KEY else ddg_search
            search_name = "Serper (Google)" if SERPER_KEY else "DuckDuckGo"
            log.info(f"   {search_name}: searching for {len(artists_without_site)} artists without website...")
            serper_found = 0
            for norm in artists_without_site[:50]:
                name = unique_artists[norm]["name"]
                if SERPER_KEY:
                    results = serper_search(f"{name} music artist official website")
                    found_urls = [r["link"] for r in results if r.get("link")]
                else:
                    found_urls = ddg_search(f"{name} music artist official website")
                for durl in found_urls[:5]:
                    if not durl:
                        continue
                    cat = classify_url(durl)
                    if cat == "website":
                        scraped = scrape_official_site(durl)
                        if scraped:
                            scraped_cache[norm] = scraped
                            sites_scraped += 1
                            serper_found += 1
                        break
                    elif "linktr.ee" in durl:
                        lt_data = scrape_linktree(durl)
                        if lt_data:
                            scraped_cache[norm] = lt_data
                            stats["linktree"] += 1
                            serper_found += 1
                        break
            stats["serper" if SERPER_KEY else "ddg"] = serper_found
            log.info(f"   ✅ {search_name} found: {serper_found} new sites/linktrees")

        log.info(f"   ✅ Websites scraped: {sites_scraped}, Linktrees: {stats['linktree']}")

        # ── Sub-step D: Build merged profiles + genre resolution ──
        for norm in unique_artists:
            vf = verify_cache.get(norm)
            enrich = {}  # enrichment eliminated — all data from real APIs
            mb_links = mb_links_cache.get(norm)
            scraped = scraped_cache.get(norm)

            profile = build_profile(unique_artists[norm]["name"], vf, enrich, mb_links, scraped)
            profile["canonical_artist_id"] = unique_artists[norm].get("artist_id")

            # Merge Last.fm
            lfm = lastfm_cache.get(norm)
            if lfm:
                if lfm.get("lastfm_url") and "lastfm" not in profile["platforms"]:
                    profile["platforms"]["lastfm"] = lfm["lastfm_url"]
                    if lfm["lastfm_url"] not in profile["urls"]:
                        profile["urls"].append(lfm["lastfm_url"])
                if lfm.get("bio") and (not profile.get("bio") or len(profile["bio"]) < len(lfm["bio"])):
                    profile["bio"] = lfm["bio"]
                if lfm.get("listeners"):
                    profile["listeners"] = lfm["listeners"]
                if lfm.get("tags"):
                    profile["lastfm_tags"] = lfm["tags"]

            # Merge Wikipedia
            wiki = wiki_cache.get(norm)
            if wiki:
                if wiki.get("wiki_url") and "wikipedia" not in profile["platforms"]:
                    profile["platforms"]["wikipedia"] = wiki["wiki_url"]
                    if wiki["wiki_url"] not in profile["urls"]:
                        profile["urls"].append(wiki["wiki_url"])
                if wiki.get("bio") and (not profile.get("bio") or len(profile["bio"]) < len(wiki["bio"])):
                    profile["bio"] = wiki["bio"]
                if wiki.get("thumbnail"):
                    profile["photo"] = wiki["thumbnail"]

            # Merge Wikidata (structured fields)
            wd = wikidata_cache.get(norm)
            if wd:
                if wd.get("born"):
                    profile["born"] = wd["born"]
                if wd.get("birthplace"):
                    profile["birthplace"] = wd["birthplace"]
                if wd.get("years_active"):
                    profile["years_active"] = wd["years_active"]
                if wd.get("instruments"):
                    profile["instruments"] = wd["instruments"]
                if wd.get("labels"):
                    profile["record_labels"] = wd["labels"]
                if wd.get("occupations"):
                    profile["occupations"] = wd["occupations"]
                if wd.get("genres"):
                    profile["wikidata_genres"] = wd["genres"]
                if wd.get("websites"):
                    for wsite in wd["websites"]:
                        if "website" not in profile["platforms"]:
                            profile["platforms"]["website"] = wsite
                            if wsite not in profile["urls"]:
                                profile["urls"].append(wsite)

            # Merge iTunes
            it = itunes_cache.get(norm)
            if it:
                if it.get("itunes_url") and "apple_music" not in profile["platforms"]:
                    profile["platforms"]["apple_music"] = it["itunes_url"]
                    if it["itunes_url"] not in profile["urls"]:
                        profile["urls"].append(it["itunes_url"])
                if it.get("genre"):
                    profile["itunes_genre"] = it["genre"]

            # Merge Discogs
            dc = discogs_cache.get(norm)
            if dc:
                if dc.get("discogs_url") and "discogs" not in profile["platforms"]:
                    profile["platforms"]["discogs"] = dc["discogs_url"]
                    if dc["discogs_url"] not in profile["urls"]:
                        profile["urls"].append(dc["discogs_url"])
                if dc.get("real_name"):
                    profile["real_name"] = dc["real_name"]
                if dc.get("urls"):
                    for durl in dc["urls"]:
                        if durl and durl not in profile["urls"]:
                            cat = classify_url(durl)
                            if cat not in profile["platforms"]:
                                profile["platforms"][cat] = durl
                                profile["urls"].append(durl)

            # Merge YouTube
            yt = youtube_cache.get(norm)
            if yt:
                if yt.get("channel_url") and "youtube" not in profile["platforms"]:
                    profile["platforms"]["youtube"] = yt["channel_url"]
                    if yt["channel_url"] not in profile["urls"]:
                        profile["urls"].append(yt["channel_url"])

            # Merge Genius
            gn = genius_cache.get(norm)
            if gn:
                if gn.get("genius_url") and "genius" not in profile["platforms"]:
                    profile["platforms"]["genius"] = gn["genius_url"]
                    if gn["genius_url"] not in profile["urls"]:
                        profile["urls"].append(gn["genius_url"])
                if gn.get("top_songs"):
                    profile["top_songs"] = gn["top_songs"]
                if gn.get("genius_image") and not profile.get("photo"):
                    profile["photo"] = gn["genius_image"]

            # Add photo from verification (Deezer)
            if vf and vf.get("deezer_picture") and not profile.get("photo"):
                profile["photo"] = vf["deezer_picture"]

            # Merge Google Knowledge Graph
            kg = kg_cache.get(norm)
            if kg:
                if kg.get("description"):
                    profile["kg_description"] = kg["description"]
                if kg.get("types"):
                    profile["kg_types"] = kg["types"]
                if kg.get("is_musician"):
                    profile["kg_confirmed_musician"] = True
                if kg.get("bio") and (not profile.get("bio") or len(profile["bio"]) < len(kg["bio"])):
                    profile["bio"] = kg["bio"]
                if kg.get("image") and not profile.get("photo"):
                    profile["photo"] = kg["image"]
                if kg.get("website") and "website" not in profile["platforms"]:
                    profile["platforms"]["website"] = kg["website"]
                    if kg["website"] not in profile["urls"]:
                        profile["urls"].append(kg["website"])

            # Merge Setlist.fm
            sl = setlistfm_cache.get(norm)
            if sl:
                profile["setlistfm_url"] = sl.get("url", "")
                profile["total_setlists"] = sl.get("total_setlists", 0)
                profile["is_active_performer"] = sl.get("is_active", False)
                if sl.get("disambiguation"):
                    profile["disambiguation"] = sl["disambiguation"]
                if sl.get("url") and sl["url"] not in profile["urls"]:
                    profile["urls"].append(sl["url"])

            # Merge Bandsintown (events/touring)
            bit = bandsintown_cache.get(norm)
            if bit:
                if bit.get("bandsintown_url") and "bandsintown" not in profile["platforms"]:
                    profile["platforms"]["bandsintown"] = bit["bandsintown_url"]
                    if bit["bandsintown_url"] not in profile["urls"]:
                        profile["urls"].append(bit["bandsintown_url"])
                if bit.get("tracker_count"):
                    profile["bandsintown_trackers"] = bit["tracker_count"]
                if bit.get("upcoming_events"):
                    profile["upcoming_events"] = bit["upcoming_events"]
                if bit.get("is_touring"):
                    profile["is_touring"] = True
                if bit.get("events"):
                    profile["upcoming_shows"] = bit["events"]
                if bit.get("facebook_url") and "facebook" not in profile["platforms"]:
                    profile["platforms"]["facebook"] = bit["facebook_url"]

            # Merge SoundCloud
            sc = soundcloud_cache.get(norm)
            if sc:
                if sc.get("soundcloud_url") and "soundcloud" not in profile["platforms"]:
                    profile["platforms"]["soundcloud"] = sc["soundcloud_url"]
                    if sc["soundcloud_url"] not in profile["urls"]:
                        profile["urls"].append(sc["soundcloud_url"])
                if sc.get("followers"):
                    profile["soundcloud_followers"] = sc["followers"]
                if sc.get("track_count"):
                    profile["soundcloud_tracks"] = sc["track_count"]
                if sc.get("description") and (not profile.get("bio") or len(profile["bio"]) < 50):
                    profile["bio"] = sc["description"]
                if sc.get("verified"):
                    profile["soundcloud_verified"] = True

            # Merge Bandcamp
            bc = bandcamp_cache.get(norm)
            if bc:
                if bc.get("bandcamp_url") and "bandcamp" not in profile["platforms"]:
                    profile["platforms"]["bandcamp"] = bc["bandcamp_url"]
                    if bc["bandcamp_url"] not in profile["urls"]:
                        profile["urls"].append(bc["bandcamp_url"])
                if bc.get("location") and not profile.get("birthplace"):
                    profile["location"] = bc["location"]
                if bc.get("genre"):
                    profile["bandcamp_genre"] = bc["genre"]

            # Merge Deezer Extended (albums, top tracks, related artists)
            vf_deezer = vf.get("deezer_url", "") if vf else ""
            if vf_deezer and norm not in deezer_ext_cache:
                try:
                    dext = deezer_extended_search(vf_deezer)
                    if dext:
                        deezer_ext_cache[norm] = dext
                        stats["deezer_ext"] += 1
                except Exception:
                    pass
            dext = deezer_ext_cache.get(norm)
            if dext:
                if dext.get("top_tracks") and not profile.get("top_songs"):
                    profile["top_songs"] = [t["title"] for t in dext["top_tracks"]]
                if dext.get("albums"):
                    profile["discography"] = dext["albums"]
                    profile["album_count"] = dext.get("album_count", len(dext["albums"]))
                if dext.get("related_artists"):
                    profile["related_artists"] = dext["related_artists"]

            # Merge MB Extra Links (RA, AllMusic, Songkick, RYM, etc.)
            mb_ext = mb_extras_cache.get(norm)
            if mb_ext:
                for platform, url in mb_ext.items():
                    if platform not in profile["platforms"]:
                        profile["platforms"][platform] = url
                        if url not in profile["urls"]:
                            profile["urls"].append(url)

            # ── Genre Resolution ──
            genre_candidates: list[tuple[str, str, int]] = []

            # Priority 1: Spotify genres
            if vf and vf.get("spotify_genres"):
                for sg in vf["spotify_genres"]:
                    mapped = validate_genre(sg)
                    if mapped != "Don't Box Me!":
                        genre_candidates.append((mapped, "spotify", 1))
                        break

            # Priority 2: iTunes genre
            if profile.get("itunes_genre"):
                mapped = validate_genre(profile["itunes_genre"])
                if mapped != "Don't Box Me!":
                    genre_candidates.append((mapped, "itunes", 2))

            # Priority 3: AI classification
            ai_genre = unique_artists[norm].get("genre", "")
            if ai_genre:
                mapped = validate_genre(ai_genre)
                if mapped != "Don't Box Me!":
                    genre_candidates.append((mapped, "ai_classify", 3))

            # Priority 4: Last.fm tags
            if profile.get("lastfm_tags"):
                for tag in profile["lastfm_tags"]:
                    mapped = validate_genre(tag)
                    if mapped != "Don't Box Me!":
                        genre_candidates.append((mapped, "lastfm", 4))
                        break

            # Priority 5: MusicBrainz tags
            if vf and vf.get("mb_tags"):
                for tag in vf["mb_tags"]:
                    mapped = validate_genre(tag)
                    if mapped != "Don't Box Me!":
                        genre_candidates.append((mapped, "musicbrainz", 5))
                        break

            # Priority 6: Wikidata genres
            if profile.get("wikidata_genres"):
                for wg in profile["wikidata_genres"]:
                    mapped = validate_genre(wg)
                    if mapped != "Don't Box Me!":
                        genre_candidates.append((mapped, "wikidata", 6))
                        break

            # Priority 7: Bandcamp genre
            if profile.get("bandcamp_genre"):
                mapped = validate_genre(profile["bandcamp_genre"])
                if mapped != "Don't Box Me!":
                    genre_candidates.append((mapped, "bandcamp", 7))

            if genre_candidates:
                genre_candidates.sort(key=lambda x: x[2])
                profile["genre"] = genre_candidates[0][0]
                profile["genre_source"] = genre_candidates[0][1]
                seen: set[str] = set()
                profile["all_genres"] = []
                for g, _src, _ in genre_candidates:
                    if g not in seen:
                        seen.add(g)
                        profile["all_genres"].append(g)
            else:
                profile["genre"] = "Don't Box Me!"
                profile["genre_source"] = None
                profile["all_genres"] = []

            profile["provenance"] = _build_profile_provenance(
                profile=profile,
                vf=vf,
                enrich=enrich,
                mb_links=mb_links,
                mb_ext=mb_ext,
                scraped=scraped,
                lfm=lfm,
                wiki=wiki,
                wd=wd,
                it=it,
                dc=dc,
                yt=yt,
                gn=gn,
                kg=kg,
                sl=sl,
                bit=bit,
                sc=sc,
                bc=bc,
            )

            profile_cache[norm] = profile

        log.info(f"   ✅ Profiles built: {len(profile_cache)}")
        log.info(f"   📊 Summary: MB_links={mb_links_found}, Spotify(MB)={spotify_via_mb}, Scraped={sites_scraped}")
        log.info(
            f"   📊 APIs: Last.fm={stats['lastfm']}, Wiki={stats['wiki']}, "
            f"Wikidata={stats['wikidata']}, iTunes={stats['itunes']}"
        )
        log.info(
            f"   📊 APIs: Discogs={stats['discogs']}, YouTube={stats['youtube']}, "
            f"Genius={stats['genius']}, KG={stats['kg']}, Setlist.fm={stats['setlistfm']}"
        )
        log.info(
            f"   📊 NEW:  Bandsintown={stats['bandsintown']}, SoundCloud={stats['soundcloud']}, "
            f"Bandcamp={stats['bandcamp']}, Deezer_ext={stats['deezer_ext']}, "
            f"MB_extras={stats['mb_extras']}"
        )
        log.info(
            f"   📊 Discovery: Serper/DDG={stats.get('serper', 0) + stats.get('ddg', 0)}, "
            f"Linktree={stats['linktree']}\n"
        )

        # ── Sub-step E: AI Bio Synthesis (from real gathered data only) ──
        profiles_needing_bio = {
            norm: p for norm, p in profile_cache.items() if not p.get("bio")
        }
        if profiles_needing_bio:
            log.info(f"   ✍️  AI Bio: synthesising bios for {len(profiles_needing_bio)} artists...")
            bios = synthesize_bio_batch(profiles_needing_bio)
            applied = 0
            for norm, bio_text in bios.items():
                if bio_text and norm in profile_cache:
                    profile_cache[norm]["bio"] = bio_text
                    # Mark provenance
                    prov = profile_cache[norm].get("provenance", {})
                    prov["bio"] = {"source": "ai_synthesis", "note": "Written from verified API data only"}
                    profile_cache[norm]["provenance"] = prov
                    applied += 1
            log.info(f"   ✅ AI Bio: {applied} bios synthesised")

        return profile_cache

    @staticmethod
    def _dedupe_events(events: list[dict]) -> list[dict]:
        """Deduplicate events using canonical id, with fallback fingerprint."""
        deduped: list[dict] = []
        seen: set[str] = set()
        for ev in events:
            cid = ev.get("canonical_event_id")
            if not cid:
                cid = canonical_event_id(
                    source=ev.get("source", ""),
                    source_id=ev.get("source_id"),
                    city=ev.get("city", ""),
                    venue=ev.get("venue", ""),
                    title=ev.get("title", ""),
                    date=ev.get("date"),
                    time=ev.get("time"),
                )
                ev["canonical_event_id"] = cid
            if cid in seen:
                continue
            seen.add(cid)
            deduped.append(ev)
        return deduped

    # ── helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _build_output_rows(classified: list[dict]) -> list[dict]:
        """Convert classified events into flat output rows (split multi-artist)."""
        from pipeline.excel_io import sanitize_event_type

        rows: list[dict] = []
        for e in classified:
            artists = e.get("artists", [])
            if e.get("delete"):
                rows.append({
                    "id": "DELETE", "event_id": e.get("canonical_event_id"), "city": e["city"], "venue": e["venue"],
                    "title": e["title"], "artist": None,
                    "genre": e.get("genre"), "event_type": sanitize_event_type(e.get("event_type")),
                    "delete": True, "is_event": True, "event_bio": e.get("event_bio"),
                })
            elif not artists or e.get("is_event"):
                rows.append({
                    "id": e.get("canonical_event_id"), "event_id": e.get("canonical_event_id"), "city": e["city"], "venue": e["venue"],
                    "title": e["title"], "artist": "Event",
                    "genre": e.get("genre"), "event_type": sanitize_event_type(e.get("event_type")),
                    "delete": False, "is_event": True, "event_bio": e.get("event_bio"),
                })
            elif len(artists) == 1:
                rows.append({
                    "id": e.get("canonical_event_id"), "event_id": e.get("canonical_event_id"), "city": e["city"], "venue": e["venue"],
                    "title": e["title"], "artist": artists[0],
                    "genre": e.get("genre"), "event_type": sanitize_event_type(e.get("event_type")),
                    "delete": False, "is_event": False,
                })
            else:
                for artist_name in artists:
                    rows.append({
                        "id": e.get("canonical_event_id"), "event_id": e.get("canonical_event_id"), "city": e["city"], "venue": e["venue"],
                        "title": e["title"], "artist": artist_name,
                        "genre": e.get("genre"), "event_type": sanitize_event_type(e.get("event_type")),
                        "delete": False, "is_event": False,
                    })
        return rows
