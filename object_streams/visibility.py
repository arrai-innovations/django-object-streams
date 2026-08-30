"""Visibility policies define the permission-scoped queryset for a user."""

from __future__ import annotations

from typing import Any
from typing import Protocol

from django.db import models


__all__ = (
    "AllowAllVisibilityPolicy",
    "DenyAllVisibilityPolicy",
    "VisibilityPolicy",
)


class VisibilityPolicy(Protocol):
    def get_queryset(self, user: Any, model: type[models.Model], action: str = "read") -> models.QuerySet:
        """Return the queryset this user can see for the model and action."""


class AllowAllVisibilityPolicy:
    """Default visibility policy that exposes the model's default queryset."""

    def get_queryset(self, user: Any, model: type[models.Model], action: str = "read") -> models.QuerySet:
        return model._default_manager.all()


class DenyAllVisibilityPolicy:
    """Visibility policy that exposes no rows."""

    def get_queryset(self, user: Any, model: type[models.Model], action: str = "read") -> models.QuerySet:
        return model._default_manager.none()
