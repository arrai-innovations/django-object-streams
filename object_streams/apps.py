"""Django application configuration for django-object-streams."""

from django.apps import AppConfig


__all__ = ("ObjectStreamsConfig",)


class ObjectStreamsConfig(AppConfig):
    name = "object_streams"
    verbose_name = "Django Object Streams"
