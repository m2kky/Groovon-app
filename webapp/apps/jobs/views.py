import logging

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect
from django.utils import timezone
from django.views import View
from django.views.generic import CreateView, DetailView, ListView, TemplateView

from .models import Job
from apps.artists.models import Artist

logger = logging.getLogger(__name__)


class DashboardView(LoginRequiredMixin, TemplateView):
    template_name = "dashboard.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["recent_jobs"] = Job.objects.all()[:10]
        ctx["active_jobs"] = Job.objects.filter(status__in=["pending", "running"])
        ctx["total_jobs"] = Job.objects.count()
        ctx["completed_jobs"] = Job.objects.filter(status="completed").count()
        ctx["total_artists"] = Artist.objects.count()
        ctx["failed_jobs"] = Job.objects.filter(status="failed").count()
        return ctx


class JobCreateView(LoginRequiredMixin, View):
    def get(self, request):
        from django.shortcuts import render
        return render(request, "jobs/create.html")

    def post(self, request):
        source_type = request.POST.get("source_type", "excel")
        name = request.POST.get("name", "").strip()
        config = {}

        if source_type == "api":
            config["city"] = request.POST.get("city", "")
            config["scraper_id"] = request.POST.get("scraper_id", "")

        if source_type == "artist_list":
            artist_names = request.POST.get("artist_names", "")
            config["artists"] = [
                a.strip() for a in artist_names.split("\n") if a.strip()
            ]

        batch_size = request.POST.get("batch_size", "15")
        try:
            config["batch_size"] = int(batch_size)
        except ValueError:
            config["batch_size"] = 15

        limit = request.POST.get("limit", "")
        if limit:
            try:
                config["limit"] = int(limit)
            except ValueError:
                pass

        job = Job.objects.create(
            name=name or f"{source_type.title()} Job",
            source_type=source_type,
            config=config,
            created_by=request.user,
        )

        # Handle file upload
        if source_type == "excel" and request.FILES.get("file"):
            job.input_file = request.FILES["file"]
            job.save(update_fields=["input_file"])

        # Launch Celery task
        from .tasks import run_pipeline_task
        try:
            run_pipeline_task.delay(str(job.id))
        except Exception as exc:
            job.status = Job.Status.FAILED
            job.error_message = f"Failed to enqueue job: {exc}"
            job.save(update_fields=["status", "error_message"])
            logger.exception("Failed to enqueue job %s", job.id)
            messages.error(request, "Failed to start job. Check worker/Redis configuration.")
            return redirect("jobs:detail", pk=job.id)

        return redirect("jobs:detail", pk=job.id)


class JobDetailView(LoginRequiredMixin, DetailView):
    model = Job
    template_name = "jobs/detail.html"
    context_object_name = "job"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        job = self.object
        ctx["artists"] = Artist.objects.filter(source_job=job)[:50]
        return ctx


class JobProgressView(LoginRequiredMixin, View):
    """HTMX endpoint — returns a progress bar fragment."""

    def get(self, request, pk):
        job = get_object_or_404(Job, pk=pk)
        from django.shortcuts import render
        return render(request, "jobs/_progress.html", {"job": job})


class JobStatusJsonView(LoginRequiredMixin, View):
    """JSON endpoint for polling job status."""

    def get(self, request, pk):
        job = get_object_or_404(Job, pk=pk)
        return JsonResponse({
            "status": job.status,
            "progress": job.progress,
            "current_phase": job.current_phase,
            "total_artists": job.total_artists,
            "total_events": job.total_events,
            "is_active": job.is_active,
        })


class JobListView(LoginRequiredMixin, ListView):
    model = Job
    template_name = "jobs/list.html"
    context_object_name = "jobs"
    paginate_by = 20


class DownloadTemplateView(LoginRequiredMixin, View):
    """Serve downloadable template files for each source type."""

    def get(self, request, template_type):
        from django.http import HttpResponse
        from .templates_generator import (
            generate_events_template,
            generate_artist_list_template,
            generate_scraper_template,
        )

        if template_type == "events":
            buf = generate_events_template()
            response = HttpResponse(
                buf.getvalue(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = 'attachment; filename="groovon_events_template.xlsx"'
            return response

        elif template_type == "artist_list":
            buf = generate_artist_list_template()
            response = HttpResponse(
                buf.getvalue(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = 'attachment; filename="groovon_artist_list_template.xlsx"'
            return response

        elif template_type == "scraper":
            data = generate_scraper_template()
            response = HttpResponse(data, content_type="application/json")
            response["Content-Disposition"] = 'attachment; filename="groovon_scraper_template.json"'
            return response

        else:
            from django.http import Http404
            raise Http404("Unknown template type")
