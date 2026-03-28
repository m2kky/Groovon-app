from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import ListView, DetailView
from .models import Artist


class ArtistListView(LoginRequiredMixin, ListView):
    model = Artist
    template_name = "artists/list.html"
    context_object_name = "artists"
    paginate_by = 24

    def get_queryset(self):
        qs = Artist.objects.all()
        q = self.request.GET.get("q")
        genre = self.request.GET.get("genre")
        if q:
            qs = qs.filter(name__icontains=q)
        if genre:
            qs = qs.filter(genre__icontains=genre)
        return qs

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["total_artists"] = Artist.objects.count()
        ctx["genres"] = (
            Artist.objects.exclude(genre="")
            .values_list("genre", flat=True)
            .distinct()[:20]
        )
        return ctx


class ArtistDetailView(LoginRequiredMixin, DetailView):
    model = Artist
    template_name = "artists/detail.html"
    context_object_name = "artist"
