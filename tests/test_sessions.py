from types import SimpleNamespace

import django_filters
import pytest

from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.registry import ObjectStreamRegistry
from object_streams.sessions import SubscriptionSession
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

    assert subscription.cursor == row.pk
    assert session.transport.events == []
    assert session.transport.resyncs[0].as_dict() == {
        "type": "resync_required",
        "subscription_id": "sub_1",
        "cursor": row.pk,
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
