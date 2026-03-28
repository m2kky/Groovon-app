from django.contrib.auth import login
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import views as auth_views
from django.shortcuts import redirect, render
from django.views import View


class LoginView(auth_views.LoginView):
    template_name = "accounts/login.html"


class RegisterView(View):
    def get(self, request):
        form = UserCreationForm()
        return render(request, "accounts/register.html", {"form": form})

    def post(self, request):
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect("/")
        return render(request, "accounts/register.html", {"form": form})
