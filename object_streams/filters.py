"""FilterSet helpers for subscription membership evaluation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Protocol

from django.db import models

from object_streams.exceptions import FilterValidationError


__all__ = ("FilterSetLike", "apply_filterset")


class FilterSetLike(Protocol):
    qs: models.QuerySet
    errors: Any

    def is_valid(self) -> bool:
        """Return whether the supplied filter data is valid."""


def apply_filterset(
    filterset_class: type[FilterSetLike] | None,
    queryset: models.QuerySet,
    data: Mapping[str, Any] | None = None,
    *,
    request: Any = None,
) -> models.QuerySet:
    """Apply a django-filter compatible FilterSet class to a queryset."""

    if filterset_class is None:
        if data:
            raise FilterValidationError({"filter": "Filters are not supported for this model."})
        return queryset

    filterset = filterset_class(data=dict(data or {}) or None, queryset=queryset, request=request)
    if not filterset.is_valid():
        raise FilterValidationError(filterset.errors)
    return filterset.qs
