import uuid
from django.conf import settings
from django.db import models


class Job(models.Model):
    """A pipeline run — wraps GroovonEngine.run()."""

    class SourceType(models.TextChoices):
        EXCEL = "excel", "Excel Upload"
        API = "api", "API Search"
        ARTIST_LIST = "artist_list", "Artist List"

    class Status(models.TextChoices):
        PENDING = "pending", "⏳ Pending"
        RUNNING = "running", "🔄 Running"
        COMPLETED = "completed", "✅ Completed"
        FAILED = "failed", "❌ Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, blank=True)
    source_type = models.CharField(max_length=20, choices=SourceType.choices)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    progress = models.IntegerField(default=0)  # 0-100
    current_phase = models.CharField(max_length=100, blank=True, default="")

    # Source config (JSON)
    config = models.JSONField(default=dict, blank=True)

    # File upload (for Excel source)
    input_file = models.FileField(upload_to="uploads/", blank=True, null=True)

    # Results
    total_events = models.IntegerField(default=0)
    total_artists = models.IntegerField(default=0)
    output_file = models.FileField(upload_to="outputs/", blank=True, null=True)
    output_json = models.JSONField(default=list, blank=True)
    error_message = models.TextField(blank=True, default="")
    log = models.TextField(blank=True, default="")

    # Celery task ID
    celery_task_id = models.CharField(max_length=255, blank=True, default="")

    # Meta
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        db_table = "groovon_job"

    def __str__(self):
        return f"{self.name or self.source_type} — {self.status}"

    @property
    def duration(self):
        if self.completed_at and self.created_at:
            delta = self.completed_at - self.created_at
            mins = int(delta.total_seconds() // 60)
            secs = int(delta.total_seconds() % 60)
            return f"{mins}m {secs}s"
        return "—"

    @property
    def is_active(self):
        return self.status in (self.Status.PENDING, self.Status.RUNNING)
