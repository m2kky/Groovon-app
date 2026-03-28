import uuid
from django.db import models


class Artist(models.Model):
    """An artist discovered and enriched by the pipeline."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=500)
    genre = models.CharField(max_length=200, blank=True, default="")
    city = models.CharField(max_length=200, blank=True, default="")
    country = models.CharField(max_length=200, blank=True, default="")
    bio = models.TextField(blank=True, default="")

    # Links
    spotify_url = models.URLField(max_length=500, blank=True, default="")
    youtube_url = models.URLField(max_length=500, blank=True, default="")
    instagram_url = models.URLField(max_length=500, blank=True, default="")
    website_url = models.URLField(max_length=500, blank=True, default="")

    # Contact
    email = models.EmailField(blank=True, default="")

    # Quality
    profile_score = models.IntegerField(default=0)  # 0-100

    # Full profile JSON from pipeline
    profile_data = models.JSONField(default=dict, blank=True)

    # Source
    source_job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="artists",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-profile_score", "name"]
        db_table = "groovon_artist"

    def __str__(self):
        return f"{self.name} ({self.genre})"

    @property
    def score_color(self):
        if self.profile_score >= 75:
            return "#10b981"  # emerald
        if self.profile_score >= 50:
            return "#f59e0b"  # amber
        return "#ef4444"  # red

    @property
    def platform_count(self):
        count = 0
        for url in [self.spotify_url, self.youtube_url, self.instagram_url, self.website_url]:
            if url:
                count += 1
        return count
