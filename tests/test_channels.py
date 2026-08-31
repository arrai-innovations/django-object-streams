import json

import django_filters
import pytest
from asgiref.sync import async_to_sync
from asgiref.testing import ApplicationCommunicator
from channels.db import database_sync_to_async

from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.registry import ObjectStreamRegistry
from object_streams.transports.channels import ObjectStreamConsumer
from tests.testapp.models import Note


class NoteFilter(django_filters.FilterSet):
    class Meta:
        model = Note
        fields = ["title"]


def make_communicator(registry):
    application = ObjectStreamConsumer.as_asgi(registry=registry)
    return ApplicationCommunicator(
        application,
        {
            "type": "websocket",
            "path": "/object-streams/",
            "headers": [],
            "query_string": b"",
            "subprotocols": [],
        },
    )


async def connect(communicator):
    await communicator.send_input({"type": "websocket.connect"})
    response = await communicator.receive_output(timeout=1)
    assert response["type"] == "websocket.accept"


async def disconnect(communicator):
    await communicator.send_input({"type": "websocket.disconnect", "code": 1000})
    await communicator.wait(timeout=1)


async def send_json(communicator, payload):
    await communicator.send_input(
        {
            "type": "websocket.receive",
            "text": json.dumps(payload),
        }
    )


async def receive_json(communicator):
    response = await communicator.receive_output(timeout=1)
    assert response["type"] == "websocket.send"
    return json.loads(response["text"])


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_subscribes_and_unsubscribes():
    registry = ObjectStreamRegistry()
    registry.register(Note, filterset=NoteFilter)
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await send_json(
            communicator,
            {
                "op": "subscribe",
                "kind": "filter",
                "model": "testapp.Note",
                "filter": {"title": "Open"},
            },
        )

        assert await receive_json(communicator) == {
            "type": "subscribed",
            "kind": "filter",
            "model": "testapp.Note",
            "filter": {"title": "Open"},
            "cursor": 0,
            "subscription_id": "sub_1",
        }

        await send_json(
            communicator,
            {
                "op": "unsubscribe",
                "subscription_id": "sub_1",
            },
        )

        assert await receive_json(communicator) == {
            "type": "unsubscribed",
            "subscription_id": "sub_1",
        }
        await disconnect(communicator)

    async_to_sync(run_test)()


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_delivers_outbox_event_json():
    registry = ObjectStreamRegistry()
    registry.register(Note)
    note = Note.objects.create(title="Open")
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await send_json(
            communicator,
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            },
        )
        subscribed = await receive_json(communicator)

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

        assert await receive_json(communicator) == {
            "type": "event",
            "subscription_id": subscribed["subscription_id"],
            "cursor": row.pk,
            "subject": {"model": "testapp.Note", "pk": str(note.pk)},
            "facet": "object",
            "op": "updated",
            "list_action": "changed",
            "changed_fields": ["title"],
            "fetch": True,
        }
        await disconnect(communicator)

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
        await send_json(
            communicator,
            {
                "op": "subscribe",
                "kind": "filter",
                "model": "testapp.Note",
                "filter": {"title": "Open"},
                "cursor": 0,
            },
        )

        subscribed = await receive_json(communicator)
        assert await receive_json(communicator) == {
            "type": "resync_required",
            "subscription_id": subscribed["subscription_id"],
            "cursor": row.pk,
            "reason": "cursor_replay_unavailable",
        }
        await disconnect(communicator)

    async_to_sync(run_test)()


@pytest.mark.django_db(transaction=True)
def test_object_stream_consumer_sends_error_for_invalid_subscribe():
    registry = ObjectStreamRegistry()
    communicator = make_communicator(registry)

    async def run_test():
        await connect(communicator)
        await send_json(
            communicator,
            {
                "op": "subscribe",
                "kind": "model",
                "model": "testapp.Note",
            },
        )

        payload = await receive_json(communicator)
        assert payload["type"] == "error"
        assert payload["code"] == "invalid_request"
        assert "not registered" in payload["message"]
        await disconnect(communicator)

    async_to_sync(run_test)()
