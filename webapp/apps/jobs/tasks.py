"""
Celery task that wraps the existing GroovonEngine for background execution.
"""
import logging
import traceback
from celery import shared_task
from django.utils import timezone

log = logging.getLogger(__name__)


def _profile_to_artist_defaults(profile: dict, job):
    """Map a pipeline profile payload into Artist model fields."""
    locale = profile.get("locale", {}) if isinstance(profile.get("locale"), dict) else {}
    platforms = profile.get("platforms", {}) if isinstance(profile.get("platforms"), dict) else {}
    emails = profile.get("emails", []) if isinstance(profile.get("emails"), list) else []
    first_email = emails[0] if emails else profile.get("email_1", "")

    profile_score = profile.get("profile_score")
    if profile_score is None and isinstance(profile.get("profile_quality"), dict):
        profile_score = profile.get("profile_quality", {}).get("pct", 0)
    if profile_score is None:
        profile_score = 0

    return {
        "name": (profile.get("name") or "").strip(),
        "genre": profile.get("genre") or "",
        "city": (locale.get("city") or locale.get("active_city") or profile.get("city") or ""),
        "country": (locale.get("country") or profile.get("country") or ""),
        "bio": profile.get("bio") or "",
        "spotify_url": platforms.get("spotify") or profile.get("spotify_url") or "",
        "youtube_url": platforms.get("youtube") or profile.get("youtube_url") or "",
        "instagram_url": platforms.get("instagram") or profile.get("instagram_url") or "",
        "website_url": platforms.get("website") or profile.get("website_url") or "",
        "email": first_email or "",
        "profile_score": int(profile_score or 0),
        "profile_data": profile,
        "source_job": job,
    }


@shared_task(bind=True)
def run_pipeline_task(self, job_id: str):
    """
    Execute the Groovon pipeline as a background Celery task.
    Updates the Job model with progress and results.
    """
    from apps.jobs.models import Job

    try:
        job = Job.objects.get(id=job_id)
    except Job.DoesNotExist:
        log.error(f"Job {job_id} not found")
        return {"error": "Job not found"}

    # Mark as running
    job.status = Job.Status.RUNNING
    job.celery_task_id = self.request.id or ""
    job.current_phase = "Initializing..."
    job.save(update_fields=["status", "celery_task_id", "current_phase"])

    try:
        from engine import GroovonEngine
        from sources.excel_source import ExcelSource
        from sources.api_source import APISource
        from sources.artist_list_source import ArtistListSource
        from sinks.json_sink import JsonSink

        config = job.config or {}
        sinks = []

        # Build source based on type
        if job.source_type == "excel" and job.input_file:
            source = ExcelSource(config={"path": job.input_file.path})
        elif job.source_type == "api":
            source = APISource(config=config)
        elif job.source_type == "artist_list":
            source = ArtistListSource(config=config)
        else:
            raise ValueError(f"Unsupported source type: {job.source_type}")

        # Always output JSON
        import tempfile, os
        json_out = os.path.join(tempfile.gettempdir(), f"groovon_{job_id}.json")
        sinks.append(JsonSink(config={"output_path": json_out}))

        # Update progress callback — patches into engine phases
        def update_progress(phase: str, pct: int):
            Job.objects.filter(id=job_id).update(
                progress=min(pct, 100),
                current_phase=phase,
            )

        engine = GroovonEngine(source=source, sinks=sinks)

        # Monkey-patch log.info to capture progress from engine phases
        phase_map = {
            "Phase 1": ("🤖 AI Classification", 10),
            "Phase 2": ("🔍 Verification", 30),
            "Phase 3": ("✨ AI Enrichment", 50),
            "Phase 3.5": ("📊 Profile Building", 70),
            "Phase 4": ("✅ URL Validation", 85),
            "Phase 5": ("💾 Output", 95),
        }

        engine_logger = logging.getLogger("engine")
        _original_engine_info = engine_logger.info

        def _patched_info(msg, *a, **kw):
            _original_engine_info(msg, *a, **kw)
            msg_str = str(msg)
            for key, (label, pct) in phase_map.items():
                if key in msg_str:
                    update_progress(label, pct)
                    break

        engine_logger.info = _patched_info

        # Run the pipeline
        stats = engine.run(
            batch_size=config.get("batch_size", 15),
            start=config.get("start", 0),
            limit=config.get("limit", 9999),
        )

        # Restore logger
        engine_logger.info = _original_engine_info

        # Read results
        import json
        results = []
        if os.path.exists(json_out):
            with open(json_out, "r", encoding="utf-8") as f:
                results = json.load(f)

        # Update job with results
        job.status = Job.Status.COMPLETED
        job.progress = 100
        job.current_phase = "✅ Done"
        if isinstance(stats, dict):
            job.total_events = stats.get("events_in", stats.get("total_events", 0))
        else:
            job.total_events = 0
        job.total_artists = len(results)
        job.output_json = results[:500]  # Limit storage
        job.completed_at = timezone.now()
        job.save()

        # Save results to Artist model
        _save_artists(results, job)

        return {"status": "completed", "artists": len(results)}

    except Exception as exc:
        job.status = Job.Status.FAILED
        job.error_message = f"{exc}\n\n{traceback.format_exc()}"
        job.current_phase = "❌ Failed"
        job.save(update_fields=["status", "error_message", "current_phase"])
        log.exception(f"Pipeline job {job_id} failed")
        return {"error": str(exc)}


def _save_artists(results: list, job):
    """Save pipeline results as Artist records."""
    from apps.artists.models import Artist

    for profile in results:
        if not isinstance(profile, dict):
            continue
        name = profile.get("name", "").strip()
        if not name:
            continue

        Artist.objects.update_or_create(
            name__iexact=name,
            defaults=_profile_to_artist_defaults(profile, job),
        )
