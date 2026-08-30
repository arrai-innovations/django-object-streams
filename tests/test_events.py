from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from tests.testapp.models import Note


def test_stream_event_as_dict_is_subscription_relative():
    event = StreamEvent(
        subscription_id="sub_7",
        cursor=120044,
        subject=ObjectRef.for_model(Note, 123),
        facet="workflow_state",
        op=EventOperation.UPDATED,
        list_action=ListAction.CHANGED,
        changed_fields=("workflow_state",),
    )

    assert event.as_dict() == {
        "type": "event",
        "subscription_id": "sub_7",
        "cursor": 120044,
        "subject": {"model": "testapp.Note", "pk": "123"},
        "facet": "workflow_state",
        "op": "updated",
        "list_action": "changed",
        "changed_fields": ["workflow_state"],
        "fetch": True,
    }
