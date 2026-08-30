"""Model registration for object stream subscriptions."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from django.apps import apps
from django.db import models

from object_streams.exceptions import AlreadyRegistered
from object_streams.exceptions import NotRegistered
from object_streams.filters import FilterSetLike
from object_streams.filters import apply_filterset
from object_streams.visibility import AllowAllVisibilityPolicy
from object_streams.visibility import VisibilityPolicy


__all__ = (
    "ObjectStreamRegistration",
    "ObjectStreamRegistry",
    "register",
    "registry",
)


def _normalize_model_label(model: type[models.Model] | str) -> str:
    if isinstance(model, str):
        return model
    return model._meta.label


def _resolve_model(model: type[models.Model] | str) -> type[models.Model]:
    if not isinstance(model, str):
        return model

    try:
        app_label, model_name = model.split(".", 1)
    except ValueError as exc:
        msg = f"Model labels must use 'app_label.ModelName' format: {model!r}"
        raise LookupError(msg) from exc

    model_class = apps.get_model(app_label, model_name)
    if model_class is None:
        msg = f"No installed model matches {model!r}."
        raise LookupError(msg)
    return model_class


@dataclass(frozen=True, slots=True)
class ObjectStreamRegistration:
    """Configuration used to subscribe to one Django model."""

    model: type[models.Model]
    filterset: type[FilterSetLike] | None = None
    visibility: VisibilityPolicy = field(default_factory=AllowAllVisibilityPolicy)
    sources: Iterable[Any] = field(default_factory=tuple)
    facets: Iterable[str] = field(default_factory=lambda: ("object",))

    def __post_init__(self):
        sources = tuple(self.sources)
        facets = {str(facet) for facet in self.facets}
        for source in sources:
            facet = getattr(source, "facet", None)
            if facet is not None:
                facets.add(str(facet))

        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "facets", frozenset(facets or {"object"}))

    @property
    def model_label(self) -> str:
        return self.model._meta.label

    def get_queryset(
        self,
        user: Any,
        filters: Mapping[str, Any] | None = None,
        *,
        action: str = "read",
        request: Any = None,
    ) -> models.QuerySet:
        queryset = self.visibility.get_queryset(user, self.model, action=action)
        return apply_filterset(self.filterset, queryset, filters, request=request)


class ObjectStreamRegistry:
    """In-memory registry for object stream model configuration."""

    def __init__(self):
        self._registrations: dict[str, ObjectStreamRegistration] = {}

    def register(
        self,
        model: type[models.Model] | str | None = None,
        *,
        filterset: type[FilterSetLike] | None = None,
        visibility: VisibilityPolicy | None = None,
        sources: Iterable[Any] = (),
        facets: Iterable[str] | None = None,
    ):
        def do_register(model_class: type[models.Model]) -> ObjectStreamRegistration:
            registration = ObjectStreamRegistration(
                model=model_class,
                filterset=filterset,
                visibility=visibility or AllowAllVisibilityPolicy(),
                sources=tuple(sources),
                facets=tuple(facets or ("object",)),
            )
            label = registration.model_label
            if label in self._registrations:
                msg = f"{label} is already registered for object streams."
                raise AlreadyRegistered(msg)
            self._registrations[label] = registration
            return registration

        if model is None:
            return do_register
        return do_register(_resolve_model(model))

    def unregister(self, model: type[models.Model] | str) -> None:
        label = _normalize_model_label(model)
        try:
            del self._registrations[label]
        except KeyError as exc:
            msg = f"{label} is not registered for object streams."
            raise NotRegistered(msg) from exc

    def get(self, model: type[models.Model] | str) -> ObjectStreamRegistration:
        label = _normalize_model_label(model)
        try:
            return self._registrations[label]
        except KeyError as exc:
            msg = f"{label} is not registered for object streams."
            raise NotRegistered(msg) from exc

    def clear(self) -> None:
        self._registrations.clear()

    def __contains__(self, model: type[models.Model] | str) -> bool:
        return _normalize_model_label(model) in self._registrations

    def __iter__(self) -> Iterator[ObjectStreamRegistration]:
        return iter(self._registrations.values())


registry = ObjectStreamRegistry()
register = registry.register
