import os


SECRET_KEY = "test-secret-key"
USE_TZ = True
ROOT_URLCONF = "tests.urls"
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DATABASE_NAME", "django_object_streams"),
        "USER": os.environ.get("DATABASE_USER", "postgres"),
        "PASSWORD": os.environ.get("DATABASE_PASSWORD", ""),
        "HOST": os.environ.get("DATABASE_HOST", ""),
        "PORT": os.environ.get("DATABASE_PORT", ""),
    },
}

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "pgtrigger",
    "object_streams",
    "tests.testapp",
]

# tests.testapp has no migrations, so triggers declared in its model state are
# installed after migrate rather than by a migration.
PGTRIGGER_INSTALL_ON_MIGRATE = True

MIDDLEWARE = []
