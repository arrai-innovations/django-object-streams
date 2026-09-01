"""Retention settings and pruning for the replayable outbox."""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from object_streams.models import ObjectStreamEvent
from object_streams.outbox import broadcasted_through_cursor
from object_streams.outbox import record_pruned_through


__all__ = (
    "get_retention_days",
    "get_retention_max_rows",
    "prune_outbox",
    "retention_cutoff",
)


def _positive_int_setting(name: str) -> int | None:
    value = getattr(settings, name, None)
    if value is None:
        return None
    value = int(value)
    if value < 1:
        msg = f"{name} must be a positive integer or None."
        raise ValueError(msg)
    return value


def get_retention_days() -> int | None:
    """Return the configured outbox age limit in days, or None to keep every row."""

    return _positive_int_setting("OBJECT_STREAMS_RETENTION_DAYS")


def get_retention_max_rows() -> int | None:
    """Return the configured outbox row limit, or None to keep every row."""

    return _positive_int_setting("OBJECT_STREAMS_RETENTION_MAX_ROWS")


def retention_cutoff(days: int) -> datetime:
    """Return the timestamp before which rows are older than the age limit."""

    if days < 1:
        msg = "Retention days must be a positive integer."
        raise ValueError(msg)
    return timezone.now() - timedelta(days=days)


def _manager(using: str | None):
    manager = ObjectStreamEvent.objects
    if using is not None:
        manager = manager.db_manager(using)
    return manager


def _age_prune_through(manager, before: datetime) -> int | None:
    return manager.filter(created_at__lt=before).order_by("-cursor").values_list("cursor", flat=True).first()


def _row_limit_prune_through(manager, max_rows: int) -> int | None:
    if max_rows < 1:
        msg = "Retention row limits must be a positive integer."
        raise ValueError(msg)
    oldest_kept = list(manager.order_by("-cursor").values_list("cursor", flat=True)[max_rows - 1 : max_rows])
    if not oldest_kept:
        return None
    return oldest_kept[0] - 1


def prune_outbox(
    *,
    before: datetime | None = None,
    max_rows: int | None = None,
    using: str | None = None,
    dry_run: bool = False,
) -> int:
    """Delete outbox rows past the retention limits and return how many were removed.

    Pruning never deletes the newest retained row, so the global cursor never
    moves backwards. Deleted ranges are recorded as a watermark so replay can
    answer with a resync instead of an empty catch-up.
    """

    if before is None and max_rows is None:
        return 0
    if max_rows is not None and max_rows < 1:
        msg = "Retention row limits must be a positive integer."
        raise ValueError(msg)

    manager = _manager(using).filter(cursor__isnull=False)
    newest = manager.order_by("-cursor").values_list("cursor", flat=True).first()
    if newest is None:
        return 0

    candidates = []
    if before is not None:
        candidates.append(_age_prune_through(manager, before))
    if max_rows is not None:
        candidates.append(_row_limit_prune_through(manager, max_rows))

    retained = [candidate for candidate in candidates if candidate is not None]
    if not retained:
        return 0

    prune_through = min(max(retained), newest - 1, broadcasted_through_cursor(using=using))
    if prune_through < 1:
        return 0

    queryset = manager.filter(cursor__lte=prune_through)
    if dry_run:
        return queryset.count()

    with transaction.atomic(using=using):
        deleted, _ = queryset.delete()
        if deleted:
            record_pruned_through(prune_through, using=using)
    return deleted
