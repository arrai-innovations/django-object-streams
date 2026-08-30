"""Subscription request parsing and normalization."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Any


__all__ = ("ResyncRequired", "SubscriptionKind", "SubscriptionRequest")


class SubscriptionKind(StrEnum):
    OBJECT = "object"
    FILTER = "filter"
    MODEL = "model"


@dataclass(frozen=True, slots=True)
class ResyncRequired:
    """Subscription-level message for cases where precise replay is unavailable."""

    subscription_id: str
    cursor: int
    reason: str = "cursor_replay_unavailable"

    def as_dict(self) -> dict[str, Any]:
        return {
            "type": "resync_required",
            "subscription_id": self.subscription_id,
            "cursor": self.cursor,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class SubscriptionRequest:
    """Normalized client subscription request."""

    kind: SubscriptionKind | str
    model: str
    pk: str | None = None
    filters: Mapping[str, Any] = field(default_factory=dict)
    search: str | None = None
    ordering: Sequence[str] = field(default_factory=tuple)
    shape: Mapping[str, Any] = field(default_factory=dict)
    cursor: int | None = None
    subscription_id: str | None = None

    def __post_init__(self):
        kind = SubscriptionKind(str(self.kind))
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "filters", dict(self.filters))
        object.__setattr__(self, "ordering", tuple(self.ordering))
        object.__setattr__(self, "shape", dict(self.shape))
        if self.pk is not None:
            object.__setattr__(self, "pk", str(self.pk))
        if self.cursor is not None:
            cursor = int(self.cursor)
            if cursor < 0:
                msg = "Subscription cursors must be non-negative."
                raise ValueError(msg)
            object.__setattr__(self, "cursor", cursor)
        if kind == SubscriptionKind.OBJECT and self.pk is None:
            msg = "Object subscriptions require a primary key."
            raise ValueError(msg)

    @classmethod
    def from_message(cls, message: Mapping[str, Any]) -> SubscriptionRequest:
        if message.get("op") != "subscribe":
            msg = "SubscriptionRequest only accepts subscribe messages."
            raise ValueError(msg)
        if not message.get("model"):
            msg = "Subscribe messages require a model label."
            raise ValueError(msg)

        return cls(
            kind=message.get("kind", SubscriptionKind.OBJECT),
            model=str(message["model"]),
            pk=message.get("pk"),
            filters=message.get("filter") or message.get("filters") or {},
            search=message.get("search"),
            ordering=message.get("ordering") or (),
            shape=message.get("shape") or {},
            cursor=message.get("cursor"),
            subscription_id=message.get("subscription_id"),
        )

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "op": "subscribe",
            "kind": str(self.kind),
            "model": self.model,
            "filter": dict(self.filters),
        }
        if self.pk is not None:
            value["pk"] = self.pk
        if self.search is not None:
            value["search"] = self.search
        if self.ordering:
            value["ordering"] = list(self.ordering)
        if self.shape:
            value["shape"] = dict(self.shape)
        if self.cursor is not None:
            value["cursor"] = self.cursor
        if self.subscription_id is not None:
            value["subscription_id"] = self.subscription_id
        return value
