import django_filters
import pytest
from asgiref.sync import async_to_sync

from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.registry import ObjectStreamRegistry
from object_streams.retention import prune_outbox
from object_streams.sessions import AsyncSubscriptionSession
from tests.testapp.models import Note


class RecordingTransport:
    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []
        self.events = []
        self.resyncs = []
        self.errors = []

    async def send_subscribed(self, subscription):
        self.subscribed.append(subscription)

    async def send_unsubscribed(self, subscription_id):
        self.unsubscribed.append(subscription_id)

    async def send_event(self, event):
        self.events.append(event)

    async def send_resync(self, resync):
        self.resyncs.append(resync)

    async def send_error(self, code, message, *, details=None):
        self.errors.append(
            {
                "code": code,
                "message": message,
                "details": details,
            }
        )


class NoteFilter(django_filters.FilterSet):
    class Meta:
        model = Note
        fields = ["title"]


def make_session(registry):
    return AsyncSubscriptionSession(
        user=None,
        transport=RecordingTransport(),
        registry=registry,
    )


@pytest.mark.django_db(transaction=True)
def test_async_session_subscribes_and_unsubscribes_filter():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = make_session(registry)

    subscription = async_to_sync(session.handle_message)(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
        }
    )

    assert subscription is not None
    assert subscription.subscription_id == "sub_1"
    assert subscription.cursor == 0
    assert session.transport.subscribed == [subscription]
    assert async_to_sync(session.handle_message)({"op": "unsubscribe", "subscription_id": "sub_1"}) is True
    assert session.transport.unsubscribed == ["sub_1"]
    assert session.subscriptions == ()


@pytest.mark.django_db(transaction=True)
def test_async_session_publishes_subscription_relative_events():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    included = Note.objects.create(title="Open")
    excluded = Note.objects.create(title="Closed")
    session = make_session(registry)
    async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
        }
    )

    excluded.title = "Open"
    excluded.save(update_fields=["title"])
    added = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(excluded),
            op=EventOperation.UPDATED,
            changed_fields=("title",),
        )
    )
    async_to_sync(session.publish)(added)

    included_pk = included.pk
    included.delete()
    deleted = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.for_model(Note, included_pk),
            op=EventOperation.DELETED,
            changed_fields=("title",),
        )
    )
    async_to_sync(session.publish)(deleted)

    assert [event.list_action for event in session.transport.events] == [
        str(ListAction.ADDED),
        str(ListAction.DELETED),
    ]
    assert [event.subject.pk for event in session.transport.events] == [
        str(excluded.pk),
        str(included_pk),
    ]


@pytest.mark.django_db(transaction=True)
def test_async_session_filter_replay_sends_resync_without_subject():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(note),
            op=EventOperation.UPDATED,
        )
    )
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
            "cursor": 0,
        }
    )

    assert subscription.cursor == row.pk
    assert session.transport.events == []
    assert session.transport.resyncs[0].as_dict() == {
        "type": "resync_required",
        "subscription_id": "sub_1",
        "cursor": row.pk,
        "reason": "cursor_replay_unavailable",
    }


@pytest.mark.django_db(transaction=True)
def test_async_session_object_replay_sends_subject_events_after_cursor():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(note),
            op=EventOperation.UPDATED,
            changed_fields=("title",),
        )
    )
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
            "cursor": 0,
        }
    )

    assert subscription.cursor == row.pk
    assert [event.as_dict() for event in session.transport.events] == [
        {
            "type": "event",
            "subscription_id": "sub_1",
            "cursor": row.pk,
            "subject": {"model": "testapp.Note", "pk": str(note.pk)},
            "facet": "object",
            "op": "updated",
            "list_action": "changed",
            "changed_fields": ["title"],
            "fetch": True,
        }
    ]


@pytest.mark.django_db(transaction=True)
def test_async_object_replay_resyncs_when_the_cursor_was_pruned():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    rows = [
        create_outbox_event(
            StreamEvent(subject=ObjectRef.from_instance(note), op=EventOperation.UPDATED),
            notify=False,
        )
        for _ in range(3)
    ]
    prune_outbox(max_rows=1)
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
            "cursor": rows[0].pk,
        }
    )

    assert subscription.cursor == rows[2].pk
    assert session.transport.events == []
    assert session.transport.resyncs[0].as_dict() == {
        "type": "resync_required",
        "subscription_id": "sub_1",
        "cursor": rows[2].pk,
        "reason": "cursor_pruned",
    }
