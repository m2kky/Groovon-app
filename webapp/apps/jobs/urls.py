from django.urls import path
from . import views

app_name = "jobs"

urlpatterns = [
    path("", views.JobListView.as_view(), name="list"),
    path("create/", views.JobCreateView.as_view(), name="create"),
    path("template/<str:template_type>/", views.DownloadTemplateView.as_view(), name="download_template"),
    path("<uuid:pk>/", views.JobDetailView.as_view(), name="detail"),
    path("<uuid:pk>/progress/", views.JobProgressView.as_view(), name="progress"),
    path("<uuid:pk>/status/", views.JobStatusJsonView.as_view(), name="status"),
]
