"""Serializable event primitives used by subscriptions and the outbox."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Any

from django.db import models


__all__ = (
    "EventOperation",
    "ListAction",
    "ObjectRef",
    "SourceRef",
    "StreamEvent",
)


class EventOperation(StrEnum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


class ListAction(StrEnum):
    ADDED = "added"
    CHANGED = "changed"
    REMOVED = "removed"
    DELETED = "deleted"
    RESYNC_REQUIRED = "resync_required"


def _model_label(model: type[models.Model] | str) -> str:
    if isinstance(model, str):
        return model
    return model._meta.label


def _coerce_pk(pk: Any) -> str:
    if pk is None:
        msg = "Object stream references require a primary key."
        raise ValueError(msg)
    return str(pk)


@dataclass(frozen=True, slots=True)
class ObjectRef:
    """A serializable reference to a Django model object."""

    model: str
    pk: str

    def __post_init__(self):
        object.__setattr__(self, "model", _model_label(self.model))
        object.__setattr__(self, "pk", _coerce_pk(self.pk))

    @classmethod
    def for_model(cls, model: type[models.Model] | str, pk: Any) -> ObjectRef:
        return cls(model=_model_label(model), pk=_coerce_pk(pk))

    @classmethod
    def from_instance(cls, instance: models.Model) -> ObjectRef:
        return cls.for_model(instance.__class__, instance.pk)

    def as_dict(self) -> dict[str, str]:
        return {
            "model": self.model,
            "pk": self.pk,
        }


@dataclass(frozen=True, slots=True)
class SourceRef:
    """A serializable reference to the source that produced a subject event."""

    model: str | None = None
    pk: str | None = None
    history_model: str | None = None
    history_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.model is not None:
            object.__setattr__(self, "model", _model_label(self.model))
        if self.pk is not None:
            object.__setattr__(self, "pk", str(self.pk))
        if self.history_model is not None:
            object.__setattr__(self, "history_model", _model_label(self.history_model))
        if self.history_id is not None:
            object.__setattr__(self, "history_id", str(self.history_id))
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def from_instance(cls, instance: models.Model, *, metadata: Mapping[str, Any] | None = None) -> SourceRef:
        return cls(model=instance.__class__, pk=instance.pk, metadata=metadata or {})

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {}
        if self.model:
            value["model"] = self.model
        if self.pk:
            value["pk"] = self.pk
        if self.history_model:
            value["history_model"] = self.history_model
        if self.history_id:
            value["history_id"] = self.history_id
        if self.metadata:
            value["metadata"] = dict(self.metadata)
        return value


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """A subscription-relative event suitable for WebSocket or outbox delivery."""

    subject: ObjectRef
    facet: str = "object"
    op: EventOperation | str = EventOperation.UPDATED
    cursor: int | None = None
    subscription_id: str | None = None
    list_action: ListAction | str | None = None
    changed_fields: Sequence[str] = field(default_factory=tuple)
    source: ObjectRef | SourceRef | None = None
    fetch: bool = True
    before: Mapping[str, Any] | None = None
    after: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "op", str(self.op))
        if self.list_action is not None:
            object.__setattr__(self, "list_action", str(self.list_action))
        object.__setattr__(self, "changed_fields", tuple(self.changed_fields))
        object.__setattr__(self, "metadata", dict(self.metadata))
        if self.before is not None:
            object.__setattr__(self, "before", dict(self.before))
        if self.after is not None:
            object.__setattr__(self, "after", dict(self.after))

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "type": "event",
            "subject": self.subject.as_dict(),
            "facet": self.facet,
            "op": self.op,
            "changed_fields": list(self.changed_fields),
            "fetch": self.fetch,
        }
        if self.subscription_id is not None:
            value["subscription_id"] = self.subscription_id
        if self.cursor is not None:
            value["cursor"] = self.cursor
        if self.list_action is not None:
            value["list_action"] = self.list_action
        if self.source is not None:
            value["source"] = self.source.as_dict()
        if self.before is not None:
            value["before"] = dict(self.before)
        if self.after is not None:
            value["after"] = dict(self.after)
        if self.metadata:
            value["metadata"] = dict(self.metadata)
        return value
