import queue
import threading
from io import StringIO
from types import SimpleNamespace

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DatabaseError
from django.db import close_old_connections
from django.db import connection
from django.db import transaction

from object_streams import postgres
from object_streams.events import EventOperation
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.management.commands.object_streams_listen import Command as ListenCommand
from object_streams.models import ObjectStreamEvent
from object_streams.outbox import assign_outbox_cursors
from object_streams.outbox import broadcasted_through_cursor
from object_streams.outbox import create_outbox_event
from object_streams.outbox import outbox_events_after
from object_streams.postgres import validate_notify_channel
from object_streams.producers import create_source_events
from object_streams.registry import ObjectStreamRegistry
from object_streams.sources import ModelSource
from tests.testapp.models import Note
from tests.testapp.models import TriggeredNote


LISTEN_TIMEOUT = 0.1
LISTEN_STOP_AFTER = 3
LISTENER_CHANNEL = "object_streams_listener_test"
LISTENER_COMMAND_TIMEOUT = 3
LISTENER_JOIN_TIMEOUT = 5
LISTENER_ATTEMPTS = 20
LISTENER_POLL_TIMEOUT = 0.1
RETRY_TEST_ATTEMPTS = 2


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


@pytest.mark.django_db(transaction=True)
def test_listener_command_broadcasts_producer_notification(settings, monkeypatch):
    settings.OBJECT_STREAMS_NOTIFY_CHANNEL = LISTENER_CHANNEL
    registry = ObjectStreamRegistry()
    registry.register(Note, sources=[ModelSource(Note)])
    note = Note.objects.create(title="Open")
    broadcasts = queue.Queue()
    listener_result = queue.Queue()

    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.broadcast_outbox_event_sync",
        broadcasts.put,
    )

    def listen_once():
        try:
            call_command(
                "object_streams_listen",
                "--once",
                "--timeout",
                str(LISTENER_COMMAND_TIMEOUT),
                "--channel",
                LISTENER_CHANNEL,
                verbosity=0,
                stdout=StringIO(),
                stderr=StringIO(),
            )
        except Exception as exc:
            listener_result.put(exc)
        else:
            listener_result.put(None)

    listener_thread = threading.Thread(target=listen_once, daemon=True)
    listener_thread.start()
    produced_ids = set()
    broadcasted_row = None

    try:
        for attempt in range(LISTENER_ATTEMPTS):
            note.title = f"Open {attempt}"
            note.save(update_fields=["title"])
            rows = create_source_events(
                note,
                op=EventOperation.UPDATED,
                changed_fields=("title",),
                registry=registry,
            )
            produced_ids.update(row.pk for row in rows)
            try:
                broadcasted_row = broadcasts.get(timeout=LISTENER_POLL_TIMEOUT)
            except queue.Empty:
                if not listener_thread.is_alive() and not listener_result.empty():
                    break
                continue
            break

        listener_thread.join(timeout=LISTENER_JOIN_TIMEOUT)
        assert not listener_thread.is_alive()
        result = listener_result.get_nowait()
        if result is not None:
            raise result
    finally:
        listener_thread.join(timeout=LISTENER_JOIN_TIMEOUT)

    assert broadcasted_row in produced_ids


@pytest.mark.django_db(transaction=True)
def test_listener_command_drains_rows_captured_before_it_starts(monkeypatch):
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(subject=ObjectRef.from_instance(note), op=EventOperation.UPDATED),
        notify=False,
    )
    broadcasts = []
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.broadcast_outbox_event_sync",
        broadcasts.append,
    )

    call_command(
        "object_streams_listen",
        "--once",
        "--timeout",
        "0",
        verbosity=0,
    )

    assert broadcasts == [row.pk]


@pytest.mark.django_db(transaction=True)
def test_listener_retries_a_row_when_fanout_failed_before_the_watermark(monkeypatch):
    note = Note.objects.create(title="Open")
    row = create_outbox_event(
        StreamEvent(subject=ObjectRef.from_instance(note), op=EventOperation.UPDATED),
        notify=False,
    )
    command = ListenCommand()

    def fail_fanout(event_id):
        raise RuntimeError("channel layer unavailable")

    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.broadcast_outbox_event_sync",
        fail_fanout,
    )
    with pytest.raises(RuntimeError, match="channel layer unavailable"):
        command._drain_pending(database="default", once=True, verbosity=0)

    assert broadcasted_through_cursor() == 0

    broadcasts = []
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.broadcast_outbox_event_sync",
        broadcasts.append,
    )
    assert command._drain_pending(database="default", once=True, verbosity=0) == 1
    assert broadcasts == [row.pk]
    assert broadcasted_through_cursor() == row.cursor


def test_listen_command_drains_after_listening(monkeypatch):
    drains = []
    stdout = StringIO()

    def fake_listen_outbox_event_ids(**kwargs):
        assert kwargs["using"] == "default"
        assert kwargs["channel"] == "object_streams_command_test"
        assert kwargs["timeout"] is None
        assert kwargs["stop_after"] == 1
        assert kwargs["on_listening"]() is True
        return
        yield

    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.listen_outbox_event_ids",
        fake_listen_outbox_event_ids,
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.Command._drain_pending",
        lambda self, **kwargs: drains.append(kwargs) or 1,
    )

    call_command(
        "object_streams_listen",
        "--channel",
        "object_streams_command_test",
        "--once",
        stdout=stdout,
    )

    assert drains == [{"database": "default", "once": True, "verbosity": 1}]
    assert "Listening for object stream events" in stdout.getvalue()


def test_listen_command_errors_when_once_times_out(monkeypatch):
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.listen_outbox_event_ids",
        lambda **kwargs: iter(()),
    )

    with pytest.raises(CommandError):
        call_command("object_streams_listen", "--once", "--timeout", "0")


def test_listen_command_rejects_negative_retry_options():
    with pytest.raises(CommandError, match="Retry delay must be non-negative"):
        call_command("object_streams_listen", "--retry-delay", "-1")

    with pytest.raises(CommandError, match="Max retries must be non-negative"):
        call_command("object_streams_listen", "--max-retries", "-1")


def test_listen_command_retries_database_errors(monkeypatch):
    calls = []
    drains = []
    closes = []
    sleeps = []

    def fake_listen_outbox_event_ids(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise DatabaseError("temporary database error")
        assert kwargs["on_listening"]() is True
        return
        yield

    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.listen_outbox_event_ids",
        fake_listen_outbox_event_ids,
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.Command._drain_pending",
        lambda self, **kwargs: drains.append(kwargs) or 1,
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.connections",
        {"default": SimpleNamespace(close=lambda: closes.append("default"))},
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.sleep",
        sleeps.append,
    )

    call_command(
        "object_streams_listen",
        "--once",
        "--channel",
        "object_streams_retry_test",
        "--max-retries",
        "1",
        "--retry-delay",
        "0",
        stdout=StringIO(),
        stderr=StringIO(),
    )

    assert len(calls) == RETRY_TEST_ATTEMPTS
    assert drains == [{"database": "default", "once": True, "verbosity": 1}]
    assert closes == ["default", "default"]
    assert sleeps == [0.0]


@pytest.mark.django_db(transaction=True)
def test_delivery_cursors_follow_commit_visibility_instead_of_capture_ids():
    first_inserted = threading.Event()
    allow_first_commit = threading.Event()
    results = queue.Queue()

    def create_first():
        close_old_connections()
        try:
            with transaction.atomic():
                note = TriggeredNote.objects.create(title="First")
                results.put(("first", note.pk))
                first_inserted.set()
                assert allow_first_commit.wait(timeout=LISTENER_COMMAND_TIMEOUT)
        finally:
            close_old_connections()

    first_thread = threading.Thread(target=create_first)
    first_thread.start()
    assert first_inserted.wait(timeout=LISTENER_COMMAND_TIMEOUT)

    second = TriggeredNote.objects.create(title="Second")
    assign_outbox_cursors()
    second_row = ObjectStreamEvent.objects.get(subject_object_id=str(second.pk))

    allow_first_commit.set()
    first_thread.join(timeout=LISTENER_COMMAND_TIMEOUT)
    assert not first_thread.is_alive()
    _, first_pk = results.get_nowait()
    assign_outbox_cursors()
    first_row = ObjectStreamEvent.objects.get(subject_object_id=str(first_pk))

    assert first_row.pk < second_row.pk
    assert second_row.cursor < first_row.cursor
    assert list(outbox_events_after(second_row.cursor).values_list("pk", flat=True)) == [first_row.pk]


def test_listen_command_errors_after_max_retries(monkeypatch):
    calls = []
    closes = []

    def fake_listen_outbox_event_ids(**kwargs):
        calls.append(kwargs)
        raise DatabaseError("persistent database error")

    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.listen_outbox_event_ids",
        fake_listen_outbox_event_ids,
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.connections",
        {"default": SimpleNamespace(close=lambda: closes.append("default"))},
    )
    monkeypatch.setattr(
        "object_streams.management.commands.object_streams_listen.sleep",
        lambda delay: None,
    )

    with pytest.raises(CommandError, match="failed after 1 retries"):
        call_command(
            "object_streams_listen",
            "--once",
            "--channel",
            "object_streams_retry_test",
            "--max-retries",
            "1",
            "--retry-delay",
            "0",
            stdout=StringIO(),
            stderr=StringIO(),
        )

    assert len(calls) == RETRY_TEST_ATTEMPTS
    assert closes == ["default", "default", "default"]
