from types import SimpleNamespace

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection

from object_streams import postgres
from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.outbox import create_outbox_event
from object_streams.postgres import validate_notify_channel
from tests.testapp.models import Note


LISTEN_TIMEOUT = 0.1
LISTEN_STOP_AFTER = 3


def test_validate_notify_channel_rejects_unsafe_names():
    with pytest.raises(ValueError):
        validate_notify_channel("object-streams")

    with pytest.raises(ValueError):
        validate_notify_channel("1_object_streams")


def test_listen_outbox_event_ids_yields_matching_integer_payloads(monkeypatch):
    class FakeCursor:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return None

        def execute(self, sql):
            self.connection.sql.append(sql)

    class FakeRawConnection:
        def notifies(self, *, timeout=None, stop_after=None):
            assert timeout == LISTEN_TIMEOUT
            assert stop_after == LISTEN_STOP_AFTER
            yield SimpleNamespace(channel="object_streams_test", payload="43")
            yield SimpleNamespace(channel="other_channel", payload="44")
            yield SimpleNamespace(channel="object_streams_test", payload="bad")

    class FakeConnection:
        connection = FakeRawConnection()

        def __init__(self):
            self.sql = []
            self.autocommit_values = []

        def ensure_connection(self):
            return None

        def get_autocommit(self):
            return False

        def set_autocommit(self, value):
            self.autocommit_values.append(value)

        def cursor(self):
            return FakeCursor(self)

    fake_connection = FakeConnection()
    monkeypatch.setattr(postgres, "connections", {"default": fake_connection})

    assert list(
        postgres.listen_outbox_event_ids(
            channel="object_streams_test",
            timeout=LISTEN_TIMEOUT,
            stop_after=LISTEN_STOP_AFTER,
        )
    ) == [43]
    assert fake_connection.sql == [
        "LISTEN object_streams_test",
        "UNLISTEN object_streams_test",
    ]
    assert fake_connection.autocommit_values == [True, False]


@pytest.mark.django_db(transaction=True)
def test_create_outbox_event_sends_postgres_notification(settings):
    settings.OBJECT_STREAMS_NOTIFY_CHANNEL = "object_streams_create_test"
    connection.ensure_connection()
    raw_connection = connection.connection
    if not hasattr(raw_connection, "notifies"):
        pytest.skip("The notification listener requires psycopg 3.")

    note = Note.objects.create(title="Open")
    with connection.cursor() as cursor:
        cursor.execute("LISTEN object_streams_create_test")
    row = create_outbox_event(
        StreamEvent(
            subject=ObjectRef.from_instance(note),
            op=EventOperation.UPDATED,
        )
    )

    notifications = list(raw_connection.notifies(timeout=1, stop_after=1))
    with connection.cursor() as cursor:
        cursor.execute("UNLISTEN object_streams_create_test")
    assert [notification.payload for notification in notifications] == [str(row.pk)]


def test_listen_command_broadcasts_received_event_id(monkeypatch):
    calls = []

    def fake_listen_outbox_event_ids(**kwargs):
        assert kwargs == {
            "using": "default",
            "channel": "object_streams_command_test",
            "timeout": None,
            "stop_after": 1,
        }
        yield 42

    def fake_broadcast_outbox_event_sync(event_id):
        calls.append(event_id)

    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.listen_outbox_event_ids",
        fake_listen_outbox_event_ids,
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.broadcast_outbox_event_sync",
        fake_broadcast_outbox_event_sync,
    )

    call_command(
        "object_streams_listen",
        "--channel",
        "object_streams_command_test",
        "--once",
    )

    assert calls == [42]


def test_listen_command_errors_when_once_times_out(monkeypatch):
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.listen_outbox_event_ids",
        lambda **kwargs: iter(()),
    )

    with pytest.raises(CommandError):
        call_command("object_streams_listen", "--once", "--timeout", "0")
