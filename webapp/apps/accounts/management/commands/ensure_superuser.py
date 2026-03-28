"""Ensure a superuser exists from environment variables.

Environment variables:
  DJANGO_SUPERUSER_USERNAME
  DJANGO_SUPERUSER_EMAIL
  DJANGO_SUPERUSER_PASSWORD
  DJANGO_SUPERUSER_UPDATE_PASSWORD=true|false (optional, default: true)
"""

from __future__ import annotations

import os

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Command(BaseCommand):
    help = "Create or update a superuser from environment variables (idempotent)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would happen without writing to the database.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options.get("dry_run"))

        username = (os.getenv("DJANGO_SUPERUSER_USERNAME") or "").strip()
        email = (os.getenv("DJANGO_SUPERUSER_EMAIL") or "").strip()
        password = os.getenv("DJANGO_SUPERUSER_PASSWORD")
        update_password = _env_bool("DJANGO_SUPERUSER_UPDATE_PASSWORD", default=True)

        if not username:
            raise CommandError("DJANGO_SUPERUSER_USERNAME is required.")
        if not email:
            raise CommandError("DJANGO_SUPERUSER_EMAIL is required.")

        User = get_user_model()
        user = User.objects.filter(username__iexact=username).first()

        if user:
            changed = []

            if user.email != email:
                changed.append("email")
                if not dry_run:
                    user.email = email

            if not user.is_staff:
                changed.append("is_staff")
                if not dry_run:
                    user.is_staff = True

            if not user.is_superuser:
                changed.append("is_superuser")
                if not dry_run:
                    user.is_superuser = True

            if not user.is_active:
                changed.append("is_active")
                if not dry_run:
                    user.is_active = True

            if password and update_password:
                changed.append("password")
                if not dry_run:
                    user.set_password(password)

            if changed and not dry_run:
                user.save()

            if changed:
                action = "Would update" if dry_run else "Updated"
                self.stdout.write(self.style.SUCCESS(f"{action} superuser '{username}': {', '.join(changed)}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"Superuser '{username}' already up to date."))
            return

        if not password:
            raise CommandError("DJANGO_SUPERUSER_PASSWORD is required to create a new superuser.")

        if dry_run:
            self.stdout.write(self.style.SUCCESS(f"Would create superuser '{username}' ({email})."))
            return

        User.objects.create_superuser(username=username, email=email, password=password)
        self.stdout.write(self.style.SUCCESS(f"Created superuser '{username}'."))
