from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.utils import timezone

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.models import ObjectStreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.outbox import latest_outbox_cursor
from object_streams.outbox import pruned_through_cursor
from object_streams.outbox import replay_is_complete
from object_streams.retention import get_retention_days
from object_streams.retention import get_retention_max_rows
from object_streams.retention import prune_outbox
from object_streams.retention import retention_cutoff
from tests.testapp.models import Note


RETENTION_DAYS = 30
RETENTION_MAX_ROWS = 100_000


def make_rows(count):
    note = Note.objects.create(title="Open")
    return [
        create_outbox_event(
            StreamEvent(subject=ObjectRef.from_instance(note), op=EventOperation.UPDATED),
            notify=False,
        )
        for _ in range(count)
    ]


def retained_ids():
    return list(ObjectStreamEvent.objects.order_by("id").values_list("id", flat=True))


def age_rows(rows, *, days):
    ObjectStreamEvent.objects.filter(pk__in=[row.pk for row in rows]).update(
        created_at=timezone.now() - timedelta(days=days)
    )


@pytest.mark.django_db
def test_prune_outbox_without_limits_keeps_every_row():
    rows = make_rows(3)

    assert prune_outbox() == 0
    assert retained_ids() == [row.pk for row in rows]


@pytest.mark.django_db
def test_prune_outbox_on_empty_outbox_reports_nothing_deleted():
    assert prune_outbox(max_rows=1) == 0
    assert pruned_through_cursor() == 0


@pytest.mark.django_db
def test_prune_outbox_deletes_rows_older_than_the_cutoff():
    rows = make_rows(4)
    age_rows(rows[:2], days=30)

    deleted = prune_outbox(before=retention_cutoff(7))

    assert deleted == len(rows[:2])
    assert retained_ids() == [rows[2].pk, rows[3].pk]


@pytest.mark.django_db
def test_prune_outbox_keeps_the_newest_rows_under_a_row_limit():
    rows = make_rows(5)

    deleted = prune_outbox(max_rows=2)

    assert deleted == len(rows[:3])
    assert retained_ids() == [rows[3].pk, rows[4].pk]


@pytest.mark.django_db
def test_prune_outbox_keeps_the_newest_row_when_every_row_is_expired():
    rows = make_rows(3)
    age_rows(rows, days=30)

    deleted = prune_outbox(before=retention_cutoff(7))

    assert deleted == len(rows[:2])
    assert retained_ids() == [rows[2].pk]
    assert latest_outbox_cursor() == rows[2].pk


@pytest.mark.django_db
def test_prune_outbox_applies_the_stricter_of_both_limits():
    rows = make_rows(5)
    age_rows(rows[:1], days=30)

    deleted = prune_outbox(before=retention_cutoff(7), max_rows=2)

    assert deleted == len(rows[:3])
    assert retained_ids() == [rows[3].pk, rows[4].pk]


@pytest.mark.django_db
def test_prune_outbox_ignores_a_row_limit_larger_than_the_outbox():
    rows = make_rows(2)

    assert prune_outbox(max_rows=10) == 0
    assert retained_ids() == [row.pk for row in rows]


@pytest.mark.django_db
def test_prune_outbox_records_the_pruning_watermark():
    rows = make_rows(4)

    prune_outbox(max_rows=2)

    assert pruned_through_cursor() == rows[1].pk
    assert replay_is_complete(rows[1].pk) is True
    assert replay_is_complete(rows[0].pk) is False


@pytest.mark.django_db
def test_prune_outbox_watermark_only_moves_forward():
    rows = make_rows(6)

    prune_outbox(max_rows=2)
    prune_outbox(max_rows=5)

    assert pruned_through_cursor() == rows[3].pk


@pytest.mark.django_db
def test_prune_outbox_dry_run_counts_without_deleting():
    rows = make_rows(4)

    assert prune_outbox(max_rows=1, dry_run=True) == len(rows[:3])
    assert retained_ids() == [row.pk for row in rows]
    assert pruned_through_cursor() == 0


@pytest.mark.django_db
def test_prune_outbox_rejects_a_non_positive_row_limit():
    make_rows(2)

    with pytest.raises(ValueError, match="positive integer"):
        prune_outbox(max_rows=0)


def test_retention_cutoff_rejects_a_non_positive_day_limit():
    with pytest.raises(ValueError, match="positive integer"):
        retention_cutoff(0)


def test_retention_settings_default_to_keeping_every_row():
    assert get_retention_days() is None
    assert get_retention_max_rows() is None


def test_retention_settings_read_configured_limits(settings):
    settings.OBJECT_STREAMS_RETENTION_DAYS = RETENTION_DAYS
    settings.OBJECT_STREAMS_RETENTION_MAX_ROWS = RETENTION_MAX_ROWS

    assert get_retention_days() == RETENTION_DAYS
    assert get_retention_max_rows() == RETENTION_MAX_ROWS


def test_retention_settings_reject_non_positive_limits(settings):
    settings.OBJECT_STREAMS_RETENTION_DAYS = 0

    with pytest.raises(ValueError, match="OBJECT_STREAMS_RETENTION_DAYS"):
        get_retention_days()


@pytest.mark.django_db
def test_prune_command_uses_configured_retention_settings(settings):
    settings.OBJECT_STREAMS_RETENTION_MAX_ROWS = 2
    rows = make_rows(5)
    stdout = StringIO()

    call_command("object_streams_prune", stdout=stdout)

    assert "Deleted 3 object stream outbox rows." in stdout.getvalue()
    assert retained_ids() == [rows[3].pk, rows[4].pk]


@pytest.mark.django_db
def test_prune_command_options_override_settings(settings):
    settings.OBJECT_STREAMS_RETENTION_MAX_ROWS = 4
    rows = make_rows(5)

    call_command("object_streams_prune", "--max-rows", "1", verbosity=0)

    assert retained_ids() == [rows[4].pk]


@pytest.mark.django_db
def test_prune_command_supports_an_age_limit():
    rows = make_rows(3)
    age_rows(rows[:2], days=30)

    call_command("object_streams_prune", "--days", "7", verbosity=0)

    assert retained_ids() == [rows[2].pk]


@pytest.mark.django_db
def test_prune_command_dry_run_reports_without_deleting():
    rows = make_rows(3)
    stdout = StringIO()

    call_command("object_streams_prune", "--max-rows", "1", "--dry-run", stdout=stdout)

    assert "Would delete 2 object stream outbox rows." in stdout.getvalue()
    assert retained_ids() == [row.pk for row in rows]


@pytest.mark.django_db
def test_prune_command_requires_a_retention_limit():
    with pytest.raises(CommandError, match="No retention limit is configured"):
        call_command("object_streams_prune", verbosity=0)


@pytest.mark.django_db
def test_prune_command_rejects_an_invalid_setting(settings):
    settings.OBJECT_STREAMS_RETENTION_MAX_ROWS = -1

    with pytest.raises(CommandError, match="OBJECT_STREAMS_RETENTION_MAX_ROWS"):
        call_command("object_streams_prune", verbosity=0)


@pytest.mark.django_db
def test_prune_command_rejects_an_invalid_option():
    with pytest.raises(CommandError, match="positive integer"):
        call_command("object_streams_prune", "--days", "0", verbosity=0)
