import pytest

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.events import StreamEvent
from object_streams.outbox import create_outbox_event
from tests.testapp.models import Note


@pytest.mark.django_db
def test_create_outbox_event_round_trips_to_stream_event():
    event = StreamEvent(
        subject=ObjectRef.for_model(Note, 1),
        facet="object",
        op=EventOperation.UPDATED,
        changed_fields=("title",),
        source=SourceRef(model=Note, pk=1),
        after={"title": "Revised"},
        metadata={"reason": "test"},
    )

    row = create_outbox_event(event)
    round_tripped = row.to_stream_event()

    assert round_tripped.cursor == row.pk
    assert round_tripped.subject == ObjectRef.for_model(Note, 1)
    assert round_tripped.source == SourceRef(model=Note, pk=1)
    assert round_tripped.changed_fields == ("title",)
    assert round_tripped.after == {"title": "Revised"}
    assert round_tripped.metadata == {"reason": "test"}
