"""Django application configuration for django-object-streams."""

from django.apps import AppConfig


__all__ = ("ObjectStreamsConfig",)


class ObjectStreamsConfig(AppConfig):
    name = "object_streams"
    verbose_name = "Django Object Streams"
    # The outbox cursor must stay a bigserial regardless of the host project's
    # DEFAULT_AUTO_FIELD.
    default_auto_field = "django.db.models.BigAutoField"
