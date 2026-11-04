import pytest
from django.db import connection
from django.db import transaction
from django.test.utils import CaptureQueriesContext

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.events import StreamEvent
from object_streams.models import ObjectStreamEvent
from object_streams.outbox import assign_outbox_cursors
from object_streams.outbox import create_outbox_event
from object_streams.outbox import current_transaction_id
from object_streams.outbox import enqueue_outbox_events
from object_streams.producers import build_source_events
from object_streams.producers import create_source_events
from object_streams.producers import enqueue_source_events
from object_streams.registry import ObjectStreamRegistry
from object_streams.sources import ModelSource
from tests.testapp.models import Note


class RelatedNoteSource:
    facet = "related"
    source_model = Note

    def __init__(self, subject):
        self.subject = subject

    def subjects_for_source(self, instance):
        return (ObjectRef.from_instance(self.subject),)

    def changed_fields(self, instance):
        return ("related_title",)

    def source_ref(self, instance):
        return SourceRef(
            model=Note,
            pk=instance.pk,
            history_model=Note,
            history_id=f"history-{instance.pk}",
        )


class NeverSource:
    facet = "object"

    def matches(self, instance):
        return False

    def subjects_for_source(self, instance):
        raise AssertionError("unmatched sources must not be evaluated")

    def changed_fields(self, instance):
        return ()


@pytest.mark.django_db
def test_build_source_events_for_model_source():
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[ModelSource(Note)])
    note = Note.objects.create(title="Draft")

    events = build_source_events(
        note,
        op=EventOperation.UPDATED,
        changed_fields=("title",),
        after={"title": "Draft"},
        metadata={"reason": "test"},
        registry=registry,
    )

    assert len(events) == 1
    assert events[0].subject == ObjectRef.from_instance(note)
    assert events[0].source == SourceRef.from_instance(note)
    assert events[0].facet == "object"
    assert events[0].op == "updated"
    assert events[0].changed_fields == ("title",)
    assert events[0].after == {"title": "Draft"}
    assert events[0].metadata == {"reason": "test"}


@pytest.mark.django_db
def test_create_source_events_writes_outbox_rows():
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[ModelSource(Note)])
    note = Note.objects.create(title="Draft")

    rows = create_source_events(
        note,
        op=EventOperation.CREATED,
        changed_fields=("title",),
        registry=registry,
    )

    assert len(rows) == 1
    assert ObjectStreamEvent.objects.count() == 1
    assign_outbox_cursors()
    rows[0].refresh_from_db(fields=["cursor"])
    event = rows[0].to_stream_event()
    assert event.cursor == rows[0].cursor
    assert event.subject == ObjectRef.from_instance(note)
    assert event.source == SourceRef.from_instance(note)
    assert event.op == "created"
    assert event.changed_fields == ("title",)


@pytest.mark.django_db
def test_custom_source_maps_source_instance_to_different_subject():
    subject = Note.objects.create(title="Subject")
    source_instance = Note.objects.create(title="Source")
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[RelatedNoteSource(subject)])

    rows = create_source_events(
        source_instance,
        op=EventOperation.UPDATED,
        registry=registry,
    )

    assert len(rows) == 1
    event = rows[0].to_stream_event()
    assert event.subject == ObjectRef.from_instance(subject)
    assert event.source == SourceRef(
        model=Note,
        pk=source_instance.pk,
        history_model=Note,
        history_id=f"history-{source_instance.pk}",
    )
    assert event.facet == "related"
    assert event.changed_fields == ("related_title",)


@pytest.mark.django_db
def test_unmatched_sources_produce_no_events():
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[NeverSource()])
    note = Note.objects.create(title="Draft")

    assert build_source_events(note, registry=registry) == ()
    assert create_source_events(note, registry=registry) == ()
    assert ObjectStreamEvent.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test_enqueue_source_events_writes_outbox_rows_after_commit():
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[ModelSource(Note)])
    note = Note.objects.create(title="Queued")

    with transaction.atomic():
        events = enqueue_source_events(
            note,
            op=EventOperation.UPDATED,
            changed_fields=("title",),
            registry=registry,
        )
        assert len(events) == 1
        assert ObjectStreamEvent.objects.count() == 0

    assert ObjectStreamEvent.objects.count() == 1
    row = ObjectStreamEvent.objects.get()
    assert row.to_stream_event().subject == ObjectRef.from_instance(note)


SUBJECTS_PER_NOTE = 2


def two_subject_registry():
    """Register one source that writes a row for itself and one for another note."""
    registry = ObjectStreamRegistry()
    other = Note.objects.create(title="Other")
    registry.register(Note, sources=[ModelSource(Note), RelatedNoteSource(other)])
    return registry


@pytest.mark.django_db(transaction=True)
def test_enqueue_source_events_records_the_callers_transaction_with_one_query():
    """The rows are written after commit, in a new transaction, so the id is read at enqueue time."""
    registry = two_subject_registry()
    with transaction.atomic():
        note = Note.objects.create(title="Draft")
        caller_transaction_id = current_transaction_id()
        with CaptureQueriesContext(connection) as queries:
            enqueue_source_events(note, registry=registry, notify=False)
        assert len(queries) == 1

    rows = list(ObjectStreamEvent.objects.order_by("id"))
    assert len(rows) == SUBJECTS_PER_NOTE
    assert {row.metadata["transaction_id"] for row in rows} == {caller_transaction_id}


@pytest.mark.django_db(transaction=True)
def test_enqueue_source_events_outside_a_transaction_writes_one_transaction():
    registry = two_subject_registry()
    note = Note.objects.create(title="Draft")

    enqueue_source_events(note, registry=registry, notify=False)

    rows = list(ObjectStreamEvent.objects.order_by("id"))
    assert len(rows) == SUBJECTS_PER_NOTE
    assert len({row.metadata["transaction_id"] for row in rows}) == 1
    assert all(row.cursor is not None for row in rows)


@pytest.mark.django_db(transaction=True)
def test_create_source_events_outside_a_transaction_writes_one_transaction():
    registry = two_subject_registry()
    note = Note.objects.create(title="Draft")

    with CaptureQueriesContext(connection) as queries:
        rows = create_source_events(note, registry=registry, notify=False)

    assert len(rows) == SUBJECTS_PER_NOTE
    assert len({row.metadata["transaction_id"] for row in rows}) == 1
    assert all(row.cursor is not None for row in rows)
    assert sum("pg_current_xact_id" in query["sql"] for query in queries) == 1


@pytest.mark.django_db
def test_create_source_events_records_the_transaction_that_writes_the_rows():
    registry = two_subject_registry()
    note = Note.objects.create(title="Draft")

    rows = create_source_events(note, registry=registry, notify=False)

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT xmin::text FROM object_streams_objectstreamevent WHERE id = %s",
            [rows[0].pk],
        )
        [row_xid] = cursor.fetchone()
    transaction_ids = {row.metadata["transaction_id"] for row in rows}
    assert transaction_ids == {current_transaction_id()}
    # xmin is the 32-bit form of the id that wrote the row.
    assert int(transaction_ids.pop()) % 2**32 == int(row_xid)


@pytest.mark.django_db(transaction=True)
def test_create_outbox_event_outside_a_transaction_records_one_id_per_row():
    note = Note.objects.create(title="Draft")
    event = StreamEvent(subject=ObjectRef.from_instance(note), metadata={"transaction_id": "supplied"})

    first = create_outbox_event(event, notify=False)
    second = create_outbox_event(event, notify=False)

    assert first.metadata["transaction_id"] != "supplied"
    assert first.metadata["transaction_id"] != second.metadata["transaction_id"]
    assert first.cursor is not None
    assert first.to_stream_event().transaction_id == first.metadata["transaction_id"]


@pytest.mark.django_db
def test_enqueue_outbox_events_with_no_events_does_not_query():
    with CaptureQueriesContext(connection) as queries:
        enqueue_outbox_events(())

    assert len(queries) == 0
