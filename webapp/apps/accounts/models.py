from django.contrib.auth.models import AbstractUser


class User(AbstractUser):
    """Custom user model for Groovon."""

    class Meta:
        db_table = "groovon_user"

    def __str__(self):
        return self.username
