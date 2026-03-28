"""
pipeline/validator.py — URL verification, email validation, ranking, quality scoring.

Adds:
- Provider-backed email validation (ZeroBounce / NeverBounce / Abstract)
- Detailed validation payload per email
- Unified profile quality score (score + tier + confidence + breakdown)
"""

import json
import logging
import re
import socket
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline.config import (
    SSL_CTX,
    EMAIL_VERIFIER_PROVIDER,
    ZEROBOUNCE_API_KEY,
    NEVERBOUNCE_API_KEY,
    ABSTRACT_API_KEY,
)

log = logging.getLogger(__name__)

# ── Trusted domains (skip HTTP reachability check) ──────────────────────────
TRUSTED_DOMAINS = frozenset({
    "deezer.com", "spotify.com", "musicbrainz.org", "apple.com",
    "music.apple.com", "soundcloud.com", "bandcamp.com", "youtube.com",
    "youtu.be", "instagram.com", "facebook.com", "twitter.com", "x.com",
    "tiktok.com", "discogs.com", "allmusic.com", "songkick.com", "last.fm",
    "wikidata.org", "wikipedia.org", "genius.com", "setlist.fm",
    "audiomack.com", "bandsintown.com",
})

# ── Email ranking tiers ─────────────────────────────────────────────────────
# Lower number = higher priority
_EMAIL_TIERS = {
    "booking": 1,
    "agent": 2,
    "management": 3,
    "manager": 3,
    "press": 4,
    "media": 4,
    "pr": 4,
    "promo": 5,
    "contact": 6,
    "info": 7,
    "hello": 8,
    "enquiries": 8,
    "general": 9,
}

_EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _email_tier(email: str) -> int:
    """Return a tier number for email sorting (lower = better)."""
    local = email.split("@")[0].lower()
    for prefix, tier in _EMAIL_TIERS.items():
        if prefix in local:
            return tier
    return 10


def rank_emails(emails: list[str]) -> list[str]:
    """Sort emails so that booking@ / management@ float to the top."""
    return sorted(set(emails), key=_email_tier)


def email_label(email: str) -> str:
    """Return a human-friendly label for an email address."""
    local = email.split("@")[0].lower()
    for prefix in _EMAIL_TIERS:
        if prefix in local:
            return prefix
    return "general"


# ── URL verification ────────────────────────────────────────────────────────

def is_trusted(url: str) -> bool:
    """Check if URL belongs to a known-good domain."""
    try:
        host = urllib.parse.urlparse(url).hostname or ""
        return any(host.endswith(d) for d in TRUSTED_DOMAINS)
    except Exception:
        return False


def verify_url(url: str, timeout: int = 6) -> str | None:
    """Quick HEAD/GET check. Returns the final (redirect-resolved) URL or None."""
    try:
        req = urllib.request.Request(
            url,
            method="HEAD",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
            return r.url
    except Exception:
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
                return r.url
        except Exception:
            return None


# ── Email validation ────────────────────────────────────────────────────────

def _selected_provider() -> str:
    """Pick explicit provider, or infer from available keys."""
    if EMAIL_VERIFIER_PROVIDER:
        return EMAIL_VERIFIER_PROVIDER
    if ZEROBOUNCE_API_KEY:
        return "zerobounce"
    if NEVERBOUNCE_API_KEY:
        return "neverbounce"
    if ABSTRACT_API_KEY:
        return "abstract"
    return ""


def _provider_check_zerobounce(email: str, timeout: int) -> dict | None:
    if not ZEROBOUNCE_API_KEY:
        return None
    url = (
        "https://api.zerobounce.net/v2/validate"
        f"?api_key={urllib.parse.quote(ZEROBOUNCE_API_KEY)}"
        f"&email={urllib.parse.quote(email)}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "GroovonValidator/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
        data = json.loads(r.read())

    status = str(data.get("status", "")).lower()
    sub_status = str(data.get("sub_status", "")).lower()
    valid = status in {"valid"}
    reason = sub_status or status or "unknown"
    return {"provider": "zerobounce", "valid": valid, "status": status, "reason": reason, "raw": data}


def _provider_check_neverbounce(email: str, timeout: int) -> dict | None:
    if not NEVERBOUNCE_API_KEY:
        return None
    url = (
        "https://api.neverbounce.com/v4/single/check"
        f"?key={urllib.parse.quote(NEVERBOUNCE_API_KEY)}"
        f"&email={urllib.parse.quote(email)}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "GroovonValidator/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
        data = json.loads(r.read())

    status = str(data.get("result", "")).lower()
    valid = status == "valid"
    reason = status or "unknown"
    return {"provider": "neverbounce", "valid": valid, "status": status, "reason": reason, "raw": data}


def _provider_check_abstract(email: str, timeout: int) -> dict | None:
    if not ABSTRACT_API_KEY:
        return None
    url = (
        "https://emailvalidation.abstractapi.com/v1/"
        f"?api_key={urllib.parse.quote(ABSTRACT_API_KEY)}"
        f"&email={urllib.parse.quote(email)}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "GroovonValidator/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
        data = json.loads(r.read())

    deliverability = str(data.get("deliverability", "")).upper()
    valid = deliverability == "DELIVERABLE"
    reason = deliverability.lower() if deliverability else "unknown"
    return {"provider": "abstract", "valid": valid, "status": reason, "reason": reason, "raw": data}


def _provider_check(email: str, timeout: int = 8) -> dict | None:
    provider = _selected_provider()
    if not provider:
        return None
    try:
        if provider == "zerobounce":
            return _provider_check_zerobounce(email, timeout)
        if provider == "neverbounce":
            return _provider_check_neverbounce(email, timeout)
        if provider == "abstract":
            return _provider_check_abstract(email, timeout)
        log.warning("Unknown EMAIL_VERIFIER_PROVIDER=%r, falling back to DNS checks", provider)
    except Exception as exc:
        log.debug("Email provider check failed for %s via %s: %s", email, provider, exc)
    return None


def validate_email_detailed(email: str, timeout: int = 5) -> dict:
    """Validate one email with reason + method details."""
    if not _EMAIL_REGEX.match(email):
        return {
            "email": email,
            "valid": False,
            "method": "format",
            "provider": None,
            "status": "invalid_format",
            "reason": "format_failed",
        }

    provider_result = _provider_check(email, timeout=max(8, timeout))
    if provider_result is not None:
        return {
            "email": email,
            "valid": bool(provider_result.get("valid")),
            "method": "provider",
            "provider": provider_result.get("provider"),
            "status": provider_result.get("status"),
            "reason": provider_result.get("reason"),
        }

    domain = email.split("@")[1]
    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(domain, 25)
        return {
            "email": email,
            "valid": True,
            "method": "dns",
            "provider": None,
            "status": "reachable",
            "reason": "dns_passed",
        }
    except Exception:
        return {
            "email": email,
            "valid": False,
            "method": "dns",
            "provider": None,
            "status": "unreachable",
            "reason": "dns_failed",
        }


def validate_email(email: str, timeout: int = 5) -> bool:
    """Backward-compatible boolean email validator."""
    return bool(validate_email_detailed(email=email, timeout=timeout).get("valid"))


def validate_email_batch(emails: list[str], workers: int = 4) -> list[str]:
    """Validate a list of emails in parallel. Returns only valid ones."""
    valid = []
    with ThreadPoolExecutor(max_workers=workers) as tp:
        futs = {tp.submit(validate_email, em): em for em in emails}
        for fut in as_completed(futs):
            if fut.result():
                valid.append(futs[fut])
    return rank_emails(valid)


# ── Batch URL verification ──────────────────────────────────────────────────

def verify_urls_batch(urls: list[str], workers: int = 6) -> dict[str, str | None]:
    """Verify a batch of URLs in parallel. Returns {original_url: final_url_or_None}."""
    results = {}
    with ThreadPoolExecutor(max_workers=workers) as tp:
        futs = {}
        for u in urls:
            if is_trusted(u):
                results[u] = u
            else:
                futs[tp.submit(verify_url, u)] = u
        for fut in as_completed(futs):
            results[futs[fut]] = fut.result()
    return results


# ── Unified profile quality score ───────────────────────────────────────────

def compute_profile_quality(profile: dict) -> dict:
    """Compute unified quality score + tier + confidence from merged profile."""
    score = 0
    breakdown: dict[str, int] = {}

    platforms = profile.get("platforms", {}) or {}
    platform_points = min(len(platforms) * 4, 32)
    score += platform_points
    breakdown["platforms"] = platform_points

    bio = (profile.get("bio") or "").strip()
    if len(bio) >= 120:
        score += 12
        breakdown["bio"] = 12
    elif len(bio) >= 40:
        score += 7
        breakdown["bio"] = 7
    else:
        breakdown["bio"] = 0

    locale = profile.get("locale", {}) or {}
    locale_points = 0
    if locale.get("city"):
        locale_points += 4
    if locale.get("country"):
        locale_points += 4
    score += locale_points
    breakdown["locale"] = locale_points

    genre = profile.get("genre", "Don't Box Me!")
    genre_points = 5 if genre and genre != "Don't Box Me!" else 0
    score += genre_points
    breakdown["genre"] = genre_points

    stream_points = 0
    if "spotify" in platforms:
        stream_points += 6
    if "deezer" in platforms:
        stream_points += 4
    score += stream_points
    breakdown["streaming"] = stream_points

    activity_points = 0
    if profile.get("is_touring") or profile.get("is_active"):
        activity_points += 5
    if profile.get("upcoming_events") or profile.get("upcoming_shows"):
        activity_points += 5
    score += activity_points
    breakdown["activity"] = activity_points

    # Email points are stricter when provider validation is enabled.
    email_points = 0
    email_checks = profile.get("email_verification", {}) or {}
    if email_checks:
        valid_provider = sum(1 for d in email_checks.values() if d.get("valid") and d.get("method") == "provider")
        valid_dns = sum(1 for d in email_checks.values() if d.get("valid") and d.get("method") == "dns")
        email_points = min(valid_provider * 6 + valid_dns * 3, 12)
    elif profile.get("emails"):
        email_points = 3
    score += email_points
    breakdown["emails"] = email_points

    # Provenance coverage bonus
    provenance = profile.get("provenance", {}) or {}
    coverage_points = min(len(provenance), 10)
    score += coverage_points
    breakdown["provenance"] = coverage_points

    score = min(score, 100)
    tier = "A" if score >= 75 else ("B" if score >= 45 else "C")
    confidence = "HIGH" if score >= 80 else ("MEDIUM" if score >= 55 else "LOW")

    # Must-pass gate for HIGH confidence:
    # needs sufficient identity evidence + rich profile core fields.
    has_core_identity = len(platforms) >= 2
    has_rich_bio = len(bio) >= 80
    has_locale = bool((profile.get("locale", {}) or {}).get("city") or (profile.get("locale", {}) or {}).get("country"))
    email_checks = profile.get("email_verification", {}) or {}
    has_valid_email = any(v.get("valid") for v in email_checks.values()) or bool(profile.get("emails"))

    must_pass_high = has_core_identity and has_rich_bio and has_locale and has_valid_email
    if confidence == "HIGH" and not must_pass_high:
        confidence = "MEDIUM"

    return {
        "profile_score": score,
        "profile_tier": tier,
        "confidence": confidence,
        "quality_breakdown": breakdown,
        "quality_flags": {
            "must_pass_high": must_pass_high,
            "has_core_identity": has_core_identity,
            "has_rich_bio": has_rich_bio,
            "has_locale": has_locale,
            "has_valid_email": has_valid_email,
        },
    }


# ── Output Validation Report (Item 9) ──────────────────────────────────────

def validate_output_report(profiles: list[dict]) -> dict:
    """Scan rich profiles and return a quality summary."""
    from collections import Counter

    tiers = Counter()
    conf = Counter()
    no_email = no_url = no_bio = 0
    issues: list[str] = []

    for p in profiles:
        tiers[p.get("profile_tier", "C")] += 1
        conf[p.get("confidence", "LOW")] += 1

        if not p.get("emails"):
            no_email += 1
        if not p.get("platforms"):
            no_url += 1
        if not p.get("bio"):
            no_bio += 1

    total = len(profiles) or 1
    if no_email > total * 0.5:
        issues.append(f"{no_email}/{len(profiles)} profiles have no email")
    if no_url > total * 0.3:
        issues.append(f"{no_url}/{len(profiles)} profiles have no platform URLs")
    if no_bio > total * 0.6:
        issues.append(f"{no_bio}/{len(profiles)} profiles have no bio")
    if conf.get("LOW", 0) > total * 0.5:
        issues.append(f"{conf['LOW']}/{len(profiles)} profiles have LOW confidence")
    if tiers.get("C", 0) > total * 0.5:
        issues.append(f"{tiers['C']}/{len(profiles)} profiles are Tier C (score < 45)")

    report = {
        "tiers": dict(tiers),
        "confidence": dict(conf),
        "no_email": no_email,
        "no_url": no_url,
        "no_bio": no_bio,
        "issues": issues,
    }

    log.info("   📊 Output Validation Report:")
    log.info("      Tiers: A=%s, B=%s, C=%s", tiers.get("A", 0), tiers.get("B", 0), tiers.get("C", 0))
    log.info(
        "      Confidence: HIGH=%s, MEDIUM=%s, LOW=%s",
        conf.get("HIGH", 0), conf.get("MEDIUM", 0), conf.get("LOW", 0),
    )
    log.info("      Missing: %s no-email, %s no-URLs, %s no-bio", no_email, no_url, no_bio)
    if issues:
        for iss in issues:
            log.warning("      ⚠️  %s", iss)
    else:
        log.info("      ✅ No major quality issues detected")

    return report
