import asyncio
import json
import os
import queue
import threading
import uuid
from io import StringIO

import django_filters
import pytest
from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.testing import WebsocketCommunicator
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import override_settings

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.producers import create_source_events
from object_streams.registry import ObjectStreamRegistry
from object_streams.sources import ModelSource
from object_streams.transports.channels import ObjectStreamConsumer
from object_streams.transports.channels import broadcast_outbox_event
from tests.testapp.models import Note


class NoteFilter(django_filters.FilterSet):
    class Meta:
        model = Note
        fields = ["title"]


class OwnerVisibilityPolicy:
    """Scope rows to the connected user, the way an integrator would."""

    def get_queryset(self, user, model, action="read"):
        if user is None or not user.is_authenticated:
            return model._default_manager.none()
        return model._default_manager.filter(title__startswith=user.username)


def make_user(username):
    return get_user_model()._default_manager.create_user(username=username, password="test-password")


CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}
REDIS_URL = os.environ.get("OBJECT_STREAMS_TEST_REDIS_URL", "redis://localhost:6379/15")
REDIS_LISTENER_CHANNEL = "object_streams_redis_listener_test"
REDIS_LISTENER_TIMEOUT = 3
REDIS_LISTENER_ATTEMPTS = 20
REDIS_POLL_TIMEOUT = 0.25


def make_communicator(registry, *, user=None):
    communicator = WebsocketCommunicator(
        ObjectStreamConsumer.as_asgi(registry=registry),
        "/object-streams/",
    )
    communicator.scope["user"] = user
    return communicator


async def connect(communicator):
    connected, _subprotocol = await communicator.connect()
    assert connected


async def maybe_receive_json(communicator, *, timeout):
    if communicator.future.done():
        communicator.future.result()
    try:
        response = await asyncio.wait_for(communicator.output_queue.get(), timeout=timeout)
    except TimeoutError:
        return None
    assert response["type"] == "websocket.send"
    return json.loads(response["text"])


def redis_channel_layers():
    pytest.importorskip("channels_redis")
    redis = pytest.importorskip("redis")
    client = None
    try:
        client = redis.Redis.from_url(REDIS_URL)
        client.ping()
    except redis.exceptions.RedisError as exc:
        pytest.skip(f"Redis is not available at {REDIS_URL}: {exc}")
    finally:
        if client is not None:
            client.close()

    return {
        "default": {
            "BACKEND": "channels_redis.core.RedisChannelLayer",
            "CONFIG": {
                "hosts": [REDIS_URL],
                "prefix": f"object-streams-test-{uuid.uuid4().hex}",
                "expiry": 5,
                "group_expiry": 5,
            },
        },
    }


def update_note_and_create_source_event(note_id, title, registry):
    note = Note.objects.get(pk=note_id)
    note.title = title
    note.save(update_fields=["title"])
    return create_source_events(
        note,
        op=EventOperation.UPDATED,
        changed_fields=("title",),
        registry=registry,
    )


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_subscribes_and_unsubscribes():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "filter",
                "model": "testapp.Note",
                "filter": {"title": "Open"},
            },
        )

        assert await communicator.receive_json_from() == {
            "type": "subscribed",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
            "cursor": 0,
            "subscription_id": "sub_1",
        }

        await communicator.send_json_to(
            {
                "op": "unsubscribe",
                "subscription_id": "sub_1",
            },
        )

        assert await communicator.receive_json_from() == {
            "type": "unsubscribed",
            "subscription_id": "sub_1",
        }
        await communicator.disconnect()

    async_to_sync(run_test)()


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_delivers_outbox_event_json():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            },
        )
        subscribed = await communicator.receive_json_from()

        row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            )
        )
        await communicator.send_input(
            {
                "type": "object.stream.event",
                "id": row.pk,
            }
        )

        assert await communicator.receive_json_from() == {
            "type": "event",
            "subscription_id": subscribed["subscription_id"],
            "cursor": row.cursor,
            "subject": {"model": "testapp.Note", "pk": str(note.pk)},
            "facet": "object",
            "op": "updated",
            "list_action": "changed",
            "changed_fields": ["title"],
            "fetch": True,
        }
        await communicator.disconnect()

    async_to_sync(run_test)()


@pytest.mark.django_db(transaction=True)
def test_redis_listener_command_delivers_producer_event_to_websocket():
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[ModelSource(Note)])
    note = Note.objects.create(title="Open")
    listener_result = queue.Queue()

    def listen_once():
        try:
            call_command(
                "object_streams_listen",
                "--once",
                "--timeout",
                str(REDIS_LISTENER_TIMEOUT),
                "--channel",
                REDIS_LISTENER_CHANNEL,
                verbosity=0,
                stdout=StringIO(),
                stderr=StringIO(),
            )
        except Exception as exc:
            listener_result.put(exc)
        else:
            listener_result.put(None)

    async def run_test():
        communicator = make_communicator(registry)
        listener_thread = threading.Thread(target=listen_once, daemon=True)
        produced_cursors = set()
        payload = None

        await connect(communicator)
        try:
            await communicator.send_json_to(
                {
                    "op": "subscribe",
                    "kind": "model",
                    "model": "testapp.Note",
                },
            )
            subscribed = await communicator.receive_json_from()
            listener_thread.start()

            for attempt in range(REDIS_LISTENER_ATTEMPTS):
                rows = await database_sync_to_async(update_note_and_create_source_event)(
                    note.pk,
                    f"Open {attempt}",
                    registry,
                )
                produced_cursors.update(row.cursor for row in rows)
                payload = await maybe_receive_json(communicator, timeout=REDIS_POLL_TIMEOUT)
                if payload is not None:
                    break
                if not listener_thread.is_alive() and not listener_result.empty():
                    break

            listener_thread.join(timeout=REDIS_LISTENER_TIMEOUT)
            assert not listener_thread.is_alive()
            result = listener_result.get_nowait()
            if result is not None:
                raise result
        finally:
            if not communicator.future.done():
                await communicator.disconnect()

        assert payload is not None
        assert payload["type"] == "event"
        assert payload["subscription_id"] == subscribed["subscription_id"]
        assert payload["cursor"] in produced_cursors
        assert payload["subject"] == {"model": "testapp.Note", "pk": str(note.pk)}
        assert payload["op"] == "updated"
        assert payload["list_action"] == "changed"
        assert payload["changed_fields"] == ["title"]

    with override_settings(
        CHANNEL_LAYERS=redis_channel_layers(),
        OBJECT_STREAMS_NOTIFY_CHANNEL=REDIS_LISTENER_CHANNEL,
    ):
        async_to_sync(run_test)()


@override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS)
@pytest.mark.django_db(transaction=True)
def test_broadcast_outbox_event_delivers_to_matching_consumers():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    note = Note.objects.create(title="Open")
    filter_communicator = make_communicator(registry)
    object_communicator = make_communicator(registry)

    async def run_test():
        await connect(filter_communicator)
        await connect(object_communicator)
        await filter_communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "filter",
                "model": "testapp.Note",
                "filter": {"title": "Open"},
            },
        )
        filter_subscription = await filter_communicator.receive_json_from()
        await object_communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "object",
                "model": "testapp.Note",
                "pk": note.pk,
            },
        )
        object_subscription = await object_communicator.receive_json_from()

        row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            )
        )
        await broadcast_outbox_event(row.pk)

        assert await filter_communicator.receive_json_from() == {
            "type": "event",
            "subscription_id": filter_subscription["subscription_id"],
            "cursor": row.cursor,
            "subject": {"model": "testapp.Note", "pk": str(note.pk)},
            "facet": "object",
            "op": "updated",
            "list_action": "changed",
            "changed_fields": ["title"],
            "fetch": True,
        }
        assert await object_communicator.receive_json_from() == {
            "type": "event",
            "subscription_id": object_subscription["subscription_id"],
            "cursor": row.cursor,
            "subject": {"model": "testapp.Note", "pk": str(note.pk)},
            "facet": "object",
            "op": "updated",
            "list_action": "changed",
            "changed_fields": ["title"],
            "fetch": True,
        }
        await filter_communicator.disconnect()
        await object_communicator.disconnect()

    async_to_sync(run_test)()


@override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS)
@pytest.mark.django_db(transaction=True)
def test_consumer_deduplicates_events_from_overlapping_groups():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            },
        )
        model_subscription = await communicator.receive_json_from()
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "object",
                "model": "testapp.Note",
                "pk": note.pk,
            },
        )
        object_subscription = await communicator.receive_json_from()

        row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
            )
        )
        await broadcast_outbox_event(row.pk)

        model_event = await communicator.receive_json_from()
        object_event = await communicator.receive_json_from()
        assert model_event["subscription_id"] == model_subscription["subscription_id"]
        assert object_event["subscription_id"] == object_subscription["subscription_id"]
        assert model_event["cursor"] == row.cursor
        assert object_event["cursor"] == row.cursor

        assert await communicator.receive_nothing()
        await communicator.disconnect()

    async_to_sync(run_test)()


@override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS)
@pytest.mark.django_db(transaction=True)
def test_unsubscribing_keeps_shared_group_until_last_subscription_leaves():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    note = Note.objects.create(title="Open")
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            },
        )
        first_subscription = await communicator.receive_json_from()
        assert first_subscription["type"] == "subscribed", first_subscription
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "filter",
                "model": "testapp.Note",
                "filter": {"title": "Open"},
            },
        )
        remaining_subscription = await communicator.receive_json_from()
        assert remaining_subscription["type"] == "subscribed", remaining_subscription

        await communicator.send_json_to(
            {
                "op": "unsubscribe",
                "subscription_id": first_subscription["subscription_id"],
            },
        )
        assert await communicator.receive_json_from() == {
            "type": "unsubscribed",
            "subscription_id": first_subscription["subscription_id"],
        }

        row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
            )
        )
        await broadcast_outbox_event(row.pk)

        event = await communicator.receive_json_from()
        assert event["subscription_id"] == remaining_subscription["subscription_id"]
        assert event["cursor"] == row.cursor
        await communicator.disconnect()

    async_to_sync(run_test)()


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_sends_collection_resync_json():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(note),
            op=EventOperation.UPDATED,
        )
    )
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "filter",
                "model": "testapp.Note",
                "filter": {"title": "Open"},
                "cursor": 0,
            },
        )

        subscribed = await communicator.receive_json_from()
        assert await communicator.receive_json_from() == {
            "type": "resync_required",
            "subscription_id": subscribed["subscription_id"],
            "cursor": row.cursor,
            "reason": "cursor_replay_unavailable",
        }
        await communicator.disconnect()

    async_to_sync(run_test)()


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_sends_error_for_invalid_subscribe():
    registry = ObjectStreamRegistry()
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            },
        )

        payload = await communicator.receive_json_from()
        assert payload["type"] == "error"
        assert payload["code"] == "invalid_request"
        assert "not registered" in payload["message"]
        await communicator.disconnect()

    async_to_sync(run_test)()


@override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS)
@pytest.mark.django_db(transaction=True)
def test_consumer_applies_visibility_policy_to_the_scope_user():
    registry = ObjectStreamRegistry()
    registry.register(Note, visibility=OwnerVisibilityPolicy())
    visible = Note.objects.create(title="alice visible")
    hidden = Note.objects.create(title="bob hidden")
    communicator = make_communicator(registry, user=make_user("alice"))

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            }
        )
        subscribed = await communicator.receive_json_from()

        hidden_row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(hidden),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            )
        )
        await broadcast_outbox_event(hidden_row.pk)
        assert await communicator.receive_nothing()

        visible_row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(visible),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            )
        )
        await broadcast_outbox_event(visible_row.pk)

        payload = await communicator.receive_json_from()
        assert payload["subscription_id"] == subscribed["subscription_id"]
        assert payload["cursor"] == visible_row.cursor
        assert payload["subject"] == {"model": "testapp.Note", "pk": str(visible.pk)}
        await communicator.disconnect()

    async_to_sync(run_test)()


@override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS)
@pytest.mark.django_db(transaction=True)
def test_consumer_without_a_scope_user_sees_nothing_under_a_user_visibility_policy():
    registry = ObjectStreamRegistry()
    registry.register(Note, visibility=OwnerVisibilityPolicy())
    note = Note.objects.create(title="alice visible")
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await communicator.send_json_to(
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            }
        )
        await communicator.receive_json_from()

        row = await database_sync_to_async(create_outbox_event)(
            StreamEvent(
                subject=ObjectRef.from_instance(note),
                op=EventOperation.UPDATED,
                changed_fields=("title",),
            )
        )
        await broadcast_outbox_event(row.pk)

        assert await communicator.receive_nothing()
        await communicator.disconnect()

    async_to_sync(run_test)()


class SmallDedupeConsumer(ObjectStreamConsumer):
    event_dedupe_size = 2


def test_dedupe_window_suppresses_repeats_it_still_remembers():
    consumer = SmallDedupeConsumer()

    assert consumer._remember_outbox_id(1) is True
    assert consumer._remember_outbox_id(2) is True
    assert consumer._remember_outbox_id(1) is False
    assert consumer._remember_outbox_id(2) is False


def test_dedupe_window_evicts_the_oldest_id_once_it_is_full():
    consumer = SmallDedupeConsumer()

    consumer._remember_outbox_id(1)
    consumer._remember_outbox_id(2)
    consumer._remember_outbox_id(3)

    assert consumer._seen_outbox_ids == {2, 3}
    assert list(consumer._seen_outbox_order) == [2, 3]
    assert consumer._remember_outbox_id(1) is True


def test_dedupe_window_does_not_grow_past_its_size():
    consumer = SmallDedupeConsumer()

    for outbox_id in range(50):
        consumer._remember_outbox_id(outbox_id)

    assert len(consumer._seen_outbox_ids) == SmallDedupeConsumer.event_dedupe_size
    assert len(consumer._seen_outbox_order) == SmallDedupeConsumer.event_dedupe_size
