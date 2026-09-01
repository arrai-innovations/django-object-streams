"""Listen for object stream outbox notifications."""

from __future__ import annotations

from time import sleep

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import DEFAULT_DB_ALIAS
from django.db import DatabaseError
from django.db import connections

from object_streams.outbox import assign_outbox_cursors
from object_streams.outbox import outbox_events_pending_broadcast
from object_streams.outbox import record_broadcasted_through
from object_streams.postgres import get_notify_channel
from object_streams.postgres import listen_outbox_event_ids
from object_streams.transports.channels import broadcast_outbox_event_sync


DEFAULT_RETRY_DELAY = 1.0
BROADCAST_BATCH_SIZE = 1000


class Command(BaseCommand):
    help = "Listen for PostgreSQL object stream notifications and fan out through Channels."

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help="Database alias to listen on.",
        )
        parser.add_argument(
            "--channel",
            default=None,
            help="PostgreSQL notification channel. Defaults to OBJECT_STREAMS_NOTIFY_CHANNEL.",
        )
        parser.add_argument(
            "--timeout",
            default=None,
            type=float,
            help="Maximum seconds to wait for notifications before returning.",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Broadcast one notification and exit.",
        )
        parser.add_argument(
            "--retry-delay",
            default=DEFAULT_RETRY_DELAY,
            type=float,
            help="Seconds to wait before retrying after a database error.",
        )
        parser.add_argument(
            "--max-retries",
            default=None,
            type=int,
            help="Maximum database reconnect attempts. Defaults to retrying forever.",
        )

    def handle(self, *args, **options):
        database = options["database"]
        channel = options["channel"] or get_notify_channel()
        timeout = options["timeout"]
        once = options["once"]
        retry_delay = options["retry_delay"]
        max_retries = options["max_retries"]
        verbosity = int(options["verbosity"])
        retries = 0

        if retry_delay < 0:
            msg = "Retry delay must be non-negative."
            raise CommandError(msg)
        if max_retries is not None and max_retries < 0:
            msg = "Max retries must be non-negative."
            raise CommandError(msg)

        try:
            while True:
                try:
                    received = self._listen_once(
                        database=database,
                        channel=channel,
                        timeout=timeout,
                        once=once,
                        verbosity=verbosity,
                    )
                except ValueError as exc:
                    raise CommandError(str(exc)) from exc
                except RuntimeError as exc:
                    raise CommandError(str(exc)) from exc
                except DatabaseError as exc:
                    connections[database].close()
                    if max_retries is not None and retries >= max_retries:
                        msg = f"Object stream listener failed after {retries} retries: {exc}"
                        raise CommandError(msg) from exc
                    retries += 1
                    if verbosity >= 1:
                        self.stderr.write(
                            f"Object stream listener database error: {exc}. Retrying in {retry_delay:g} seconds."
                        )
                    sleep(retry_delay)
                    continue

                if once and not received:
                    msg = "No object stream notifications were received before the timeout."
                    raise CommandError(msg)
                break
        finally:
            connections[database].close()

    def _listen_once(
        self,
        *,
        database: str,
        channel: str,
        timeout: float | None,
        once: bool,
        verbosity: int,
    ) -> bool:
        received = False
        if verbosity >= 1:
            self.stdout.write(f"Listening for object stream events on database {database!r}, channel {channel!r}.")

        def drain_after_listen() -> bool:
            nonlocal received
            count = self._drain_pending_on_worker(database=database, once=once, verbosity=verbosity)
            received = received or count > 0
            return once and received

        for _event_id in listen_outbox_event_ids(
            using=database,
            channel=channel,
            timeout=timeout,
            stop_after=1 if once else None,
            on_listening=drain_after_listen,
        ):
            count = self._drain_pending_on_worker(database=database, once=once, verbosity=verbosity)
            received = received or count > 0
            if once and received:
                break

        return received

    def _drain_pending_on_worker(self, *, database: str, once: bool, verbosity: int) -> int:
        drain = database_sync_to_async(self._drain_pending, thread_sensitive=False)
        return async_to_sync(drain)(database=database, once=once, verbosity=verbosity)

    def _drain_pending(self, *, database: str, once: bool, verbosity: int) -> int:
        broadcasted = 0
        limit = 1 if once else BROADCAST_BATCH_SIZE
        while True:
            assign_outbox_cursors(using=database, limit=limit)
            rows = list(outbox_events_pending_broadcast(using=database, limit=limit))
            if not rows:
                return broadcasted

            for row in rows:
                broadcast_outbox_event_sync(row.pk)
                record_broadcasted_through(row.cursor, using=database)
                broadcasted += 1
                if verbosity >= 1:
                    self.stdout.write(f"Broadcasted object stream event {row.pk} at cursor {row.cursor}.")
                if once:
                    return broadcasted
