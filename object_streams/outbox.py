"""Helpers for writing stream events to the replayable outbox."""

from __future__ import annotations

from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.db import transaction

from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.events import StreamEvent
from object_streams.models import ObjectStreamEvent
from object_streams.models import ObjectStreamOutboxState
from object_streams.postgres import notify_outbox_event


__all__ = (
    "assign_outbox_cursors",
    "broadcasted_through_cursor",
    "create_outbox_event",
    "enqueue_outbox_event",
    "latest_outbox_cursor",
    "outbox_events_after",
    "outbox_events_pending_broadcast",
    "pruned_through_cursor",
    "record_broadcasted_through",
    "record_pruned_through",
    "replay_is_complete",
)


def _model_label(model: type[models.Model] | str) -> str:
    if isinstance(model, str):
        return model
    return model._meta.label


def _content_type_for_model_label(model_label: str, *, using: str | None = None) -> ContentType:
    app_label, model_name = model_label.split(".", 1)
    model_class = apps.get_model(app_label, model_name)
    if model_class is None:
        msg = f"No installed model matches {model_label!r}."
        raise LookupError(msg)
    manager = ContentType.objects
    if using is not None:
        manager = manager.db_manager(using)
    return manager.get_for_model(model_class)


def _source_content_type(source: ObjectRef | SourceRef | None, *, using: str | None = None) -> ContentType | None:
    if source is None:
        return None
    if isinstance(source, ObjectRef):
        return _content_type_for_model_label(source.model, using=using)
    if source.model:
        return _content_type_for_model_label(source.model, using=using)
    return None


def _source_object_id(source: ObjectRef | SourceRef | None) -> str:
    if source is None:
        return ""
    return source.pk or ""


def create_outbox_event(event: StreamEvent, *, notify: bool = True, using: str | None = None) -> ObjectStreamEvent:
    """Persist a stream event immediately and return the created outbox row."""

    source_history_content_type = None
    source_history_id = ""
    if isinstance(event.source, SourceRef) and event.source.history_model:
        source_history_content_type = _content_type_for_model_label(event.source.history_model, using=using)
        source_history_id = event.source.history_id or ""

    manager = ObjectStreamEvent.objects
    if using is not None:
        manager = manager.db_manager(using)

    row = manager.create(
        subject_content_type=_content_type_for_model_label(event.subject.model, using=using),
        subject_object_id=event.subject.pk,
        source_content_type=_source_content_type(event.source, using=using),
        source_object_id=_source_object_id(event.source),
        source_history_content_type=source_history_content_type,
        source_history_id=source_history_id,
        facet=event.facet,
        op=event.op,
        changed_fields=list(event.changed_fields),
        before=event.before,
        after=event.after,
        metadata=dict(event.metadata),
    )
    connection = transaction.get_connection(using=using)
    if not connection.in_atomic_block:
        assign_outbox_cursors(using=using)
        row.refresh_from_db(fields=["cursor"], using=using)
    if notify:
        notify_outbox_event(row.pk, using=using)
    return row


def enqueue_outbox_event(event: StreamEvent, *, using: str | None = None, notify: bool = True) -> None:
    """Write an event after the current database transaction commits."""

    transaction.on_commit(lambda: create_outbox_event(event, notify=notify, using=using), using=using)


def _state_manager(using: str | None = None):
    manager = ObjectStreamOutboxState.objects
    if using is not None:
        manager = manager.db_manager(using)
    return manager


def assign_outbox_cursors(*, using: str | None = None, limit: int | None = None) -> tuple[ObjectStreamEvent, ...]:
    """Assign commit-visible delivery cursors to captured outbox rows."""

    manager = ObjectStreamEvent.objects
    if using is not None:
        manager = manager.db_manager(using)

    with transaction.atomic(using=using):
        state, _ = _state_manager(using).select_for_update().get_or_create(pk=1)
        queryset = manager.select_for_update().filter(cursor__isnull=True).order_by("id")
        if limit is not None:
            queryset = queryset[:limit]
        rows = list(queryset)
        if not rows:
            return ()

        next_cursor = state.next_cursor
        for row in rows:
            row.cursor = next_cursor
            next_cursor += 1
        manager.bulk_update(rows, ["cursor"])
        state.next_cursor = next_cursor
        state.save(update_fields=["next_cursor"])
    return tuple(rows)


def latest_outbox_cursor(*, using: str | None = None) -> int:
    """Return the latest global outbox cursor, or 0 when the outbox is empty."""

    manager = ObjectStreamEvent.objects
    if using is not None:
        manager = manager.db_manager(using)
    return manager.filter(cursor__isnull=False).order_by("-cursor").values_list("cursor", flat=True).first() or 0


def pruned_through_cursor(*, using: str | None = None) -> int:
    """Return the highest outbox cursor removed by retention pruning."""

    return _state_manager(using).values_list("pruned_through", flat=True).first() or 0


def broadcasted_through_cursor(*, using: str | None = None) -> int:
    """Return the highest delivery cursor successfully fanned out by the listener."""

    return _state_manager(using).values_list("broadcasted_through", flat=True).first() or 0


def record_pruned_through(cursor: int, *, using: str | None = None) -> None:
    """Raise the pruning watermark to a cursor that has been deleted."""

    state, created = _state_manager(using).get_or_create(pk=1, defaults={"pruned_through": cursor})
    if not created and cursor > state.pruned_through:
        state.pruned_through = cursor
        state.save(update_fields=["pruned_through"])


def record_broadcasted_through(cursor: int, *, using: str | None = None) -> None:
    """Advance the durable listener watermark after successful fanout."""

    with transaction.atomic(using=using):
        state, _ = _state_manager(using).select_for_update().get_or_create(pk=1)
        if cursor > state.broadcasted_through:
            state.broadcasted_through = cursor
            state.save(update_fields=["broadcasted_through"])


def replay_is_complete(cursor: int) -> bool:
    """Return whether every outbox row after a cursor is still retained."""

    return cursor >= pruned_through_cursor()


def outbox_events_after(
    cursor: int,
    *,
    model: type[models.Model] | str | None = None,
    subject: ObjectRef | None = None,
    through_cursor: int | None = None,
    limit: int | None = None,
) -> models.QuerySet:
    """Return outbox rows after a cursor, optionally scoped to a model or subject."""

    queryset = ObjectStreamEvent.objects.filter(cursor__gt=cursor).order_by("cursor")
    if through_cursor is not None:
        queryset = queryset.filter(cursor__lte=through_cursor)
    if subject is not None:
        queryset = queryset.filter(
            subject_content_type=_content_type_for_model_label(subject.model),
            subject_object_id=subject.pk,
        )
    elif model is not None:
        queryset = queryset.filter(subject_content_type=_content_type_for_model_label(_model_label(model)))
    if limit is not None:
        return queryset[:limit]
    return queryset


def outbox_events_pending_broadcast(
    *,
    using: str | None = None,
    limit: int | None = None,
) -> models.QuerySet:
    """Return cursor-assigned rows after the listener's durable watermark."""

    manager = ObjectStreamEvent.objects
    if using is not None:
        manager = manager.db_manager(using)
    queryset = (
        manager.select_related(
            "subject_content_type",
            "source_content_type",
            "source_history_content_type",
        )
        .filter(cursor__gt=broadcasted_through_cursor(using=using))
        .order_by("cursor")
    )
    if limit is not None:
        return queryset[:limit]
    return queryset
