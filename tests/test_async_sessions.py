import django_filters
import pytest
from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async

from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import record_broadcasted_through
from object_streams.registry import ObjectStreamRegistry
from object_streams.retention import prune_outbox
from object_streams.sessions import AsyncSubscriptionSession
from tests.helpers import create_deliverable_outbox_event as create_outbox_event
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


class OnSubscribeTransport(RecordingTransport):
    def __init__(self, callback):
        super().__init__()
        self.callback = callback

    async def prepare_subscription(self, subscription):
        await database_sync_to_async(self.callback)()


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
def test_async_object_subscription_replays_an_event_captured_while_the_transport_joins():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    rows = []

    def capture_during_subscribe():
        note.title = "Revised"
        note.save(update_fields=["title"])
        rows.append(
            create_outbox_event(
                StreamEvent(
                    subject=ObjectRef.from_instance(note),
                    op=EventOperation.UPDATED,
                    changed_fields=("title",),
                )
            )
        )

    transport = OnSubscribeTransport(capture_during_subscribe)
    session = AsyncSubscriptionSession(user=None, transport=transport, registry=registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
        }
    )

    assert subscription.cursor == 0
    assert [event.cursor for event in transport.events] == [rows[0].cursor]
    assert async_to_sync(session.publish)(rows[0]) == []


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

    assert subscription.cursor == row.cursor
    assert session.transport.events == []
    assert session.transport.resyncs[0].as_dict() == {
        "type": "resync_required",
        "subscription_id": "sub_1",
        "cursor": row.cursor,
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

    assert subscription.cursor == row.cursor
    assert [event.as_dict() for event in session.transport.events] == [
        {
            "type": "event",
            "subscription_id": "sub_1",
            "cursor": row.cursor,
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
    record_broadcasted_through(rows[-1].cursor)
    prune_outbox(max_rows=1)
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
            "cursor": rows[0].cursor,
        }
    )

    assert subscription.cursor == rows[2].cursor
    assert session.transport.events == []
    assert session.transport.resyncs[0].as_dict() == {
        "type": "resync_required",
        "subscription_id": "sub_1",
        "cursor": rows[2].cursor,
        "reason": "cursor_pruned",
    }


@pytest.mark.django_db(transaction=True)
def test_async_search_subscriptions_are_rejected_as_unsupported():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "search": "Open",
        }
    )

    assert subscription is None
    assert session.subscriptions == ()
    assert session.transport.subscribed == []
    assert session.transport.errors == [
        {
            "code": "unsupported_search",
            "message": "Search subscriptions are not supported yet.",
            "details": None,
        }
    ]


@pytest.mark.django_db(transaction=True)
def test_async_subscription_cursor_newer_than_the_outbox_is_rejected():
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
            "cursor": row.cursor + 1,
        }
    )

    assert subscription is None
    assert session.subscriptions == ()
    assert session.transport.subscribed == []
    assert session.transport.errors == [
        {
            "code": "invalid_cursor",
            "message": "Subscription cursor is newer than the outbox.",
            "details": None,
        }
    ]


@pytest.mark.django_db(transaction=True)
def test_async_object_subscription_for_a_missing_object_is_not_found():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    missing_pk = note.pk
    note.delete()
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": missing_pk,
        }
    )

    assert subscription is None
    assert session.subscriptions == ()
    assert session.transport.subscribed == []
    assert session.transport.errors == [
        {
            "code": "not_found",
            "message": "Object does not exist or is not visible.",
            "details": None,
        }
    ]


@pytest.mark.django_db(transaction=True)
def test_async_object_replay_resyncs_when_the_replay_limit_is_exceeded():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    rows = [
        create_outbox_event(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            )
        )
        for _ in range(3)
    ]
    session = AsyncSubscriptionSession(
        user=None,
        transport=RecordingTransport(),
        registry=registry,
        replay_limit=2,
    )

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
            "cursor": 0,
        }
    )

    assert subscription.cursor == rows[-1].cursor
    assert session.transport.events == []
    assert session.transport.resyncs[0].as_dict() == {
        "type": "resync_required",
        "subscription_id": "sub_1",
        "cursor": rows[-1].cursor,
        "reason": "object_replay_limit_exceeded",
    }


@pytest.mark.django_db(transaction=True)
def test_async_subscribing_to_an_unregistered_model_is_an_invalid_request():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "model",
            "model": "testapp.Note",
        }
    )

    assert subscription is None
    assert session.subscriptions == ()
    assert session.transport.subscribed == []
    assert session.transport.errors[0]["code"] == "invalid_request"


@pytest.mark.django_db(transaction=True)
def test_async_filter_params_require_registered_filterset():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    session = make_session(registry)

    subscription = async_to_sync(session.subscribe)(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
        }
    )

    assert subscription is None
    assert session.subscriptions == ()
    assert session.transport.subscribed == []
    assert session.transport.errors[0]["code"] == "invalid_filter"


@pytest.mark.django_db(transaction=True)
def test_async_unsubscribe_without_a_subscription_id_is_an_invalid_request():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    assert async_to_sync(session.handle_message)({"op": "unsubscribe"}) is None
    assert session.transport.errors == [
        {
            "code": "invalid_request",
            "message": "Unsubscribe messages require a subscription_id.",
            "details": None,
        }
    ]


@pytest.mark.django_db(transaction=True)
def test_async_unsupported_op_is_an_invalid_request():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    assert async_to_sync(session.handle_message)({"op": "resubscribe"}) is None
    assert session.transport.errors == [
        {
            "code": "invalid_request",
            "message": "Messages require a supported op.",
            "details": None,
        }
    ]


@pytest.mark.django_db(transaction=True)
def test_async_unsubscribing_an_inactive_subscription_reports_not_subscribed():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    assert async_to_sync(session.unsubscribe)("sub_1") is False
    assert session.transport.unsubscribed == []
    assert session.transport.errors == [
        {
            "code": "not_subscribed",
            "message": "Subscription is not active.",
            "details": None,
        }
    ]
