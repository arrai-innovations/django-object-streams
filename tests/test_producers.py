import pytest
from django.db import transaction

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.models import ObjectStreamEvent
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
    event = rows[0].to_stream_event()
    assert event.cursor == rows[0].pk
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
