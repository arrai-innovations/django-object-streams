from types import SimpleNamespace

import django_filters
import pytest
from channels.db import database_sync_to_async

from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import record_broadcasted_through
from object_streams.registry import ObjectStreamRegistry
from object_streams.retention import prune_outbox
from object_streams.sessions import SubscriptionSession
from object_streams.subscriptions import SubscriptionKind
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


class RequestAwareNoteFilter(django_filters.FilterSet):
    title = django_filters.CharFilter(method="filter_title")

    class Meta:
        model = Note
        fields = ["title"]

    def filter_title(self, queryset, name, value):
        return queryset.filter(title=f"{self.request.title_prefix}:{value}")


class RejectingNoteFilter(django_filters.FilterSet):
    class Meta:
        model = Note
        fields = ["title"]

    def is_valid(self):
        msg = "FilterSets should not validate non-filter subscriptions."
        raise AssertionError(msg)


TEST_MAX_SUBSCRIPTIONS = 2
TEST_MAX_MEMBER_PKS = 2


class PrefixVisibilityPolicy:
    def get_queryset(self, user, model, action="read"):
        return model._default_manager.filter(title__startswith=user)


def make_session(registry, *, request=None):
    return SubscriptionSession(
        user=None,
        request=request,
        transport=RecordingTransport(),
        registry=registry,
    )


@pytest.mark.django_db
def test_session_subscribes_and_unsubscribes_filter():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = make_session(registry)

    subscription = session.handle_message(
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
    assert session.handle_message({"op": "unsubscribe", "subscription_id": "sub_1"}) is True
    assert session.transport.unsubscribed == ["sub_1"]
    assert session.subscriptions == ()


@pytest.mark.django_db
def test_empty_filter_subscription_matches_visible_queryset():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    open_note = Note.objects.create(title="Open")
    closed_note = Note.objects.create(title="Closed")
    session = make_session(registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {},
        }
    )

    assert subscription is not None
    assert session.transport.errors == []
    assert session.subscriptions[0].member_pks == {str(open_note.pk), str(closed_note.pk)}


@pytest.mark.django_db
def test_session_accepts_filters_alias():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    open_note = Note.objects.create(title="Open")
    Note.objects.create(title="Closed")
    session = make_session(registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filters": {"title": "Open"},
        }
    )

    assert subscription is not None
    assert subscription.filters == {"title": "Open"}
    assert session.subscriptions[0].member_pks == {str(open_note.pk)}


@pytest.mark.django_db
def test_filterset_receives_session_request():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=RequestAwareNoteFilter)
    visible = Note.objects.create(title="alice:Open")
    Note.objects.create(title="bob:Open")
    session = make_session(registry, request=SimpleNamespace(title_prefix="alice"))

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
        }
    )

    assert subscription is not None
    assert session.subscriptions[0].member_pks == {str(visible.pk)}


@pytest.mark.django_db
def test_model_and_object_subscriptions_do_not_apply_filterset():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=RejectingNoteFilter)
    note = Note.objects.create(title="Open")
    session = make_session(registry)

    model_subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "model",
            "model": "testapp.Note",
        }
    )
    object_subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
        }
    )

    assert model_subscription is not None
    assert object_subscription is not None
    assert session.transport.errors == []


@pytest.mark.django_db
def test_session_classifies_filter_membership_changes():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    included = Note.objects.create(title="Open")
    excluded = Note.objects.create(title="Closed")
    session = make_session(registry)
    session.subscribe(
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
    session.publish(added)

    changed = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(excluded),
            op=EventOperation.UPDATED,
            changed_fields=("ignored",),
        )
    )
    session.publish(changed)

    excluded.title = "Closed"
    excluded.save(update_fields=["title"])
    removed = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(excluded),
            op=EventOperation.UPDATED,
            changed_fields=("title",),
        )
    )
    session.publish(removed)

    included_pk = included.pk
    included.delete()
    deleted = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.for_model(Note, included_pk),
            op=EventOperation.DELETED,
            changed_fields=("title",),
        )
    )
    session.publish(deleted)

    assert [event.list_action for event in session.transport.events] == [
        str(ListAction.ADDED),
        str(ListAction.CHANGED),
        str(ListAction.REMOVED),
        str(ListAction.DELETED),
    ]
    assert [event.subject.pk for event in session.transport.events] == [
        str(excluded.pk),
        str(excluded.pk),
        str(excluded.pk),
        str(included_pk),
    ]


@pytest.mark.django_db
def test_session_does_not_deliver_events_outside_visibility():
    registry = ObjectStreamRegistry()
    registry.register(Note, visibility=PrefixVisibilityPolicy())
    visible = Note.objects.create(title="alice visible")
    hidden = Note.objects.create(title="bob hidden")
    session = SubscriptionSession(
        user="alice",
        transport=RecordingTransport(),
        registry=registry,
    )
    session.subscribe(
        {
            "op": "subscribe",
            "kind": "model",
            "model": "testapp.Note",
        }
    )

    hidden.title = "bob changed"
    hidden.save(update_fields=["title"])
    hidden_row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(hidden),
            op=EventOperation.UPDATED,
        )
    )
    assert session.publish(hidden_row) == []

    hidden.title = "alice joined"
    hidden.save(update_fields=["title"])
    added_row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(hidden),
            op=EventOperation.UPDATED,
        )
    )
    session.publish(added_row)

    visible.title = "bob moved"
    visible.save(update_fields=["title"])
    removed_row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(visible),
            op=EventOperation.UPDATED,
        )
    )
    session.publish(removed_row)

    assert [event.list_action for event in session.transport.events] == [
        str(ListAction.ADDED),
        str(ListAction.REMOVED),
    ]
    assert [event.subject.pk for event in session.transport.events] == [
        str(hidden.pk),
        str(visible.pk),
    ]


@pytest.mark.django_db(transaction=True)
def test_filter_subscription_resyncs_an_event_captured_while_the_transport_joins():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    note = Note.objects.create(title="Closed")
    rows = []

    def capture_during_subscribe():
        note.title = "Open"
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
    session = SubscriptionSession(user=None, transport=transport, registry=registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
        }
    )

    assert subscription.cursor == 0
    assert transport.resyncs[0].cursor == rows[0].cursor
    assert session.subscriptions[0].member_pks == {str(note.pk)}
    assert session.publish(rows[0]) == []


@pytest.mark.django_db
def test_filter_params_require_registered_filterset():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    session = make_session(registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
        }
    )

    assert subscription is None
    assert session.transport.subscribed == []
    assert session.transport.errors[0]["code"] == "invalid_filter"


@pytest.mark.django_db
def test_filter_replay_sends_subscription_resync_without_subject():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    cursor = 0
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(note),
            op=EventOperation.UPDATED,
        )
    )
    session = make_session(registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
            "cursor": cursor,
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


@pytest.mark.django_db
def test_object_replay_sends_subject_events_after_cursor():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    cursor = 0
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(note),
            op=EventOperation.UPDATED,
            changed_fields=("title",),
        )
    )
    session = make_session(registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
            "cursor": cursor,
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


@pytest.mark.django_db
def test_object_replay_resyncs_when_the_cursor_was_pruned():
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

    subscription = session.subscribe(
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


@pytest.mark.django_db
def test_object_replay_still_replays_a_retained_cursor_after_pruning():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    rows = [
        create_outbox_event(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            ),
            notify=False,
        )
        for _ in range(3)
    ]
    record_broadcasted_through(rows[-1].cursor)
    prune_outbox(max_rows=2)
    session = make_session(registry)

    subscription = session.subscribe(
        {
            "op": "subscribe",
            "kind": "object",
            "model": "testapp.Note",
            "pk": note.pk,
            "cursor": rows[1].cursor,
        }
    )

    assert subscription.cursor == rows[2].cursor
    assert session.transport.resyncs == []
    assert [event.cursor for event in session.transport.events] == [rows[2].cursor]


@pytest.mark.django_db
def test_filter_replay_resyncs_when_the_cursor_was_pruned():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
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

    session.subscribe(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
            "cursor": rows[0].cursor,
        }
    )

    assert session.transport.resyncs[0].reason == "cursor_pruned"


@pytest.mark.django_db
def test_search_subscriptions_are_rejected_as_unsupported():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = make_session(registry)

    subscription = session.subscribe(
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


@pytest.mark.django_db
def test_subscription_cursor_newer_than_the_outbox_is_rejected():
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

    subscription = session.subscribe(
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


@pytest.mark.django_db
def test_object_subscription_for_a_missing_object_is_not_found():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    missing_pk = note.pk
    note.delete()
    session = make_session(registry)

    subscription = session.subscribe(
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


@pytest.mark.django_db
def test_object_replay_resyncs_when_the_replay_limit_is_exceeded():
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
    session = SubscriptionSession(
        user=None,
        transport=RecordingTransport(),
        registry=registry,
        replay_limit=2,
    )

    subscription = session.subscribe(
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


@pytest.mark.django_db
def test_subscribing_to_an_unregistered_model_is_an_invalid_request():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    subscription = session.subscribe(
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


@pytest.mark.django_db
def test_unsubscribe_without_a_subscription_id_is_an_invalid_request():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    assert session.handle_message({"op": "unsubscribe"}) is None
    assert session.transport.errors == [
        {
            "code": "invalid_request",
            "message": "Unsubscribe messages require a subscription_id.",
            "details": None,
        }
    ]


@pytest.mark.django_db
def test_unsupported_op_is_an_invalid_request():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    assert session.handle_message({"op": "resubscribe"}) is None
    assert session.transport.errors == [
        {
            "code": "invalid_request",
            "message": "Messages require a supported op.",
            "details": None,
        }
    ]


@pytest.mark.django_db
def test_unsubscribing_an_inactive_subscription_reports_not_subscribed():
    registry = ObjectStreamRegistry()
    session = make_session(registry)

    assert session.unsubscribe("sub_1") is False
    assert session.transport.unsubscribed == []
    assert session.transport.errors == [
        {
            "code": "not_subscribed",
            "message": "Subscription is not active.",
            "details": None,
        }
    ]


@pytest.mark.django_db
def test_reusing_an_active_subscription_id_is_rejected():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = make_session(registry)
    first = {
        "op": "subscribe",
        "kind": "model",
        "model": "testapp.Note",
        "subscription_id": "client_1",
    }

    assert session.subscribe(first) is not None
    assert session.subscribe(dict(first, kind="object", pk=1)) is None
    assert [active.subscription_id for active in session.subscriptions] == ["client_1"]
    assert session.subscriptions[0].request.kind == SubscriptionKind.MODEL
    assert session.transport.errors == [
        {
            "code": "duplicate_subscription",
            "message": "Subscription id is already active on this connection.",
            "details": None,
        }
    ]


@pytest.mark.django_db
def test_subscription_id_is_reusable_after_unsubscribing():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = make_session(registry)
    request = {
        "op": "subscribe",
        "kind": "model",
        "model": "testapp.Note",
        "subscription_id": "client_1",
    }

    assert session.subscribe(request) is not None
    assert session.unsubscribe("client_1") is True
    assert session.subscribe(request) is not None
    assert session.transport.errors == []


@pytest.mark.django_db
def test_connection_rejects_subscriptions_past_the_limit():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    session = SubscriptionSession(
        user=None,
        transport=RecordingTransport(),
        registry=registry,
        max_subscriptions=TEST_MAX_SUBSCRIPTIONS,
    )
    request = {"op": "subscribe", "kind": "model", "model": "testapp.Note"}

    assert session.subscribe(request) is not None
    assert session.subscribe(request) is not None
    assert session.subscribe(request) is None
    assert len(session.subscriptions) == TEST_MAX_SUBSCRIPTIONS
    assert session.transport.errors == [
        {
            "code": "subscription_limit_exceeded",
            "message": "Connection has too many active subscriptions.",
            "details": None,
        }
    ]


@pytest.mark.django_db
def test_subscription_matching_too_many_objects_is_rejected():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    for index in range(TEST_MAX_MEMBER_PKS + 1):
        Note.objects.create(title=f"Open {index}")
    session = SubscriptionSession(
        user=None,
        transport=RecordingTransport(),
        registry=registry,
        max_member_pks=TEST_MAX_MEMBER_PKS,
    )

    subscription = session.subscribe({"op": "subscribe", "kind": "model", "model": "testapp.Note"})

    assert subscription is None
    assert session.subscriptions == ()
    assert session.transport.errors == [
        {
            "code": "subscription_too_large",
            "message": "Subscription matches too many objects.",
            "details": None,
        }
    ]


@pytest.mark.django_db
def test_subscription_at_the_member_limit_is_accepted():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    for index in range(TEST_MAX_MEMBER_PKS):
        Note.objects.create(title=f"Open {index}")
    session = SubscriptionSession(
        user=None,
        transport=RecordingTransport(),
        registry=registry,
        max_member_pks=TEST_MAX_MEMBER_PKS,
    )

    subscription = session.subscribe({"op": "subscribe", "kind": "model", "model": "testapp.Note"})

    assert subscription is not None
    assert session.transport.errors == []
    assert len(session.subscriptions[0].member_pks) == TEST_MAX_MEMBER_PKS
