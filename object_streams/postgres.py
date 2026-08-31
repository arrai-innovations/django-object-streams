"""PostgreSQL wakeup helpers for object stream outbox rows."""

from __future__ import annotations

import re
from collections.abc import Iterator

from django.conf import settings
from django.db import DEFAULT_DB_ALIAS
from django.db import connections


__all__ = (
    "DEFAULT_NOTIFY_CHANNEL",
    "get_notify_channel",
    "listen_outbox_event_ids",
    "notify_outbox_event",
    "validate_notify_channel",
)


DEFAULT_NOTIFY_CHANNEL = "object_streams_events"
MAX_NOTIFY_CHANNEL_BYTES = 63
_CHANNEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_notify_channel() -> str:
    """Return the PostgreSQL NOTIFY channel used for outbox wakeups."""

    return str(getattr(settings, "OBJECT_STREAMS_NOTIFY_CHANNEL", DEFAULT_NOTIFY_CHANNEL))


def validate_notify_channel(channel: str) -> str:
    """Validate a PostgreSQL notification channel name."""

    if not _CHANNEL_RE.fullmatch(channel):
        msg = "PostgreSQL notification channels must be unquoted identifier names."
        raise ValueError(msg)
    if len(channel.encode("utf-8")) > MAX_NOTIFY_CHANNEL_BYTES:
        msg = "PostgreSQL notification channels must be 63 bytes or shorter."
        raise ValueError(msg)
    return channel


def notify_outbox_event(event_id: int, *, using: str | None = None, channel: str | None = None) -> None:
    """Send a PostgreSQL wakeup notification for an outbox event id."""

    channel_name = validate_notify_channel(channel or get_notify_channel())
    connection = connections[using or DEFAULT_DB_ALIAS]
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_notify(%s, %s)", [channel_name, str(event_id)])


def listen_outbox_event_ids(
    *,
    using: str = DEFAULT_DB_ALIAS,
    channel: str | None = None,
    timeout: float | None = None,
    stop_after: int | None = None,
) -> Iterator[int]:
    """Yield outbox event ids from PostgreSQL notifications."""

    channel_name = validate_notify_channel(channel or get_notify_channel())
    connection = connections[using]
    connection.ensure_connection()
    raw_connection = connection.connection
    notifies = getattr(raw_connection, "notifies", None)
    if notifies is None:
        msg = "Object stream listening requires a psycopg 3 PostgreSQL connection."
        raise RuntimeError(msg)

    previous_autocommit = connection.get_autocommit()
    connection.set_autocommit(True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"LISTEN {channel_name}")

        for notification in notifies(timeout=timeout, stop_after=stop_after):
            if notification.channel != channel_name:
                continue
            try:
                yield int(notification.payload)
            except ValueError:
                continue
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"UNLISTEN {channel_name}")
        connection.set_autocommit(previous_autocommit)
