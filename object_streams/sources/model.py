"""Source adapter for direct model changes."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from django.db import models

from object_streams.events import ObjectRef


__all__ = ("ModelSource", "Source")


class Source(Protocol):
    facet: str

    def subjects_for_source(self, instance: models.Model) -> Iterable[ObjectRef]:
        """Return stream subjects affected by a changed source instance."""

    def changed_fields(self, instance: models.Model) -> Sequence[str]:
        """Return changed subject fields when the source can report them."""


@dataclass(frozen=True, slots=True)
class ModelSource:
    """Source adapter for events where the source row and subject row match."""

    model: type[models.Model]
    facet: str = "object"

    def matches(self, instance: models.Model) -> bool:
        return isinstance(instance, self.model)

    def subjects_for_source(self, instance: models.Model) -> Iterable[ObjectRef]:
        if not self.matches(instance):
            return ()
        return (ObjectRef.from_instance(instance),)

    def changed_fields(self, instance: models.Model) -> Sequence[str]:
        return ()
