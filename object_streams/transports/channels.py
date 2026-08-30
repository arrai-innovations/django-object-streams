"""Channels transport helpers."""

from __future__ import annotations

import hashlib

from object_streams.events import ObjectRef


__all__ = ("model_group_name", "object_group_name")


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def object_group_name(ref: ObjectRef) -> str:
    """Return a stable Channels group name for an object subject."""

    return f"object_streams.object.{_digest(f'{ref.model}:{ref.pk}')}"


def model_group_name(model_label: str) -> str:
    """Return a stable Channels group name for model-level fanout."""

    return f"object_streams.model.{_digest(model_label)}"
