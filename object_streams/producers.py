"""Producer helpers that turn source changes into outbox events."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from django.db import models

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.events import StreamEvent
from object_streams.models import ObjectStreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.outbox import enqueue_outbox_event
from object_streams.registry import ObjectStreamRegistry
from object_streams.registry import registry as default_registry


__all__ = (
    "build_source_events",
    "create_source_events",
    "enqueue_source_events",
)


def build_source_events(
    instance: models.Model,
    *,
    op: EventOperation | str = EventOperation.UPDATED,
    changed_fields: Sequence[str] = (),
    before: Mapping[str, Any] | None = None,
    after: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    registry: ObjectStreamRegistry = default_registry,
) -> tuple[StreamEvent, ...]:
    """Build stream events for a changed source instance."""

    events = []
    for registration in registry:
        for source in registration.sources:
            if not _source_matches(source, instance):
                continue
            source_changed_fields = _source_changed_fields(source, instance) or changed_fields
            source_ref = _source_ref(source, instance)
            for subject in _subjects_for_source(source, instance):
                if subject.model != registration.model_label:
                    continue
                events.append(
                    StreamEvent(
                        subject=subject,
                        facet=str(getattr(source, "facet", "object")),
                        op=op,
                        changed_fields=tuple(source_changed_fields),
                        source=source_ref,
                        before=before,
                        after=after,
                        metadata=metadata or {},
                    )
                )
    return tuple(events)


def create_source_events(
    instance: models.Model,
    *,
    op: EventOperation | str = EventOperation.UPDATED,
    changed_fields: Sequence[str] = (),
    before: Mapping[str, Any] | None = None,
    after: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    notify: bool = True,
    registry: ObjectStreamRegistry = default_registry,
    using: str | None = None,
) -> tuple[ObjectStreamEvent, ...]:
    """Create outbox rows for a changed source instance immediately."""

    return tuple(
        create_outbox_event(event, notify=notify, using=using)
        for event in build_source_events(
            instance,
            op=op,
            changed_fields=changed_fields,
            before=before,
            after=after,
            metadata=metadata,
            registry=registry,
        )
    )


def enqueue_source_events(
    instance: models.Model,
    *,
    op: EventOperation | str = EventOperation.UPDATED,
    changed_fields: Sequence[str] = (),
    before: Mapping[str, Any] | None = None,
    after: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    notify: bool = True,
    registry: ObjectStreamRegistry = default_registry,
    using: str | None = None,
) -> tuple[StreamEvent, ...]:
    """Schedule outbox rows for a changed source instance after commit."""

    events = build_source_events(
        instance,
        op=op,
        changed_fields=changed_fields,
        before=before,
        after=after,
        metadata=metadata,
        registry=registry,
    )
    for event in events:
        enqueue_outbox_event(event, using=using, notify=notify)
    return events


def _source_matches(source: Any, instance: models.Model) -> bool:
    matches = getattr(source, "matches", None)
    if matches is not None:
        return bool(matches(instance))

    source_model = getattr(source, "source_model", None)
    if source_model is None:
        return True
    if isinstance(source_model, str):
        return source_model == instance._meta.label
    return isinstance(instance, source_model)


def _source_changed_fields(source: Any, instance: models.Model) -> Sequence[str]:
    changed_fields = getattr(source, "changed_fields", None)
    if changed_fields is None:
        return ()
    return tuple(changed_fields(instance))


def _source_ref(source: Any, instance: models.Model) -> ObjectRef | SourceRef:
    source_ref = getattr(source, "source_ref", None)
    if source_ref is not None:
        return source_ref(instance)
    return SourceRef.from_instance(instance)


def _subjects_for_source(source: Any, instance: models.Model) -> Iterable[ObjectRef]:
    return tuple(source.subjects_for_source(instance))
