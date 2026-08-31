"""Listen for object stream outbox notifications."""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import DEFAULT_DB_ALIAS

from object_streams.postgres import get_notify_channel
from object_streams.postgres import listen_outbox_event_ids
from object_streams.transports.channels import broadcast_outbox_event_sync


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

    def handle(self, *args, **options):
        database = options["database"]
        channel = options["channel"] or get_notify_channel()
        timeout = options["timeout"]
        once = options["once"]
        received = False

        try:
            for event_id in listen_outbox_event_ids(
                using=database,
                channel=channel,
                timeout=timeout,
                stop_after=1 if once else None,
            ):
                received = True
                broadcast_outbox_event_sync(event_id)
                if once:
                    break
        except RuntimeError as exc:
            raise CommandError(str(exc)) from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        if once and not received:
            msg = "No object stream notifications were received before the timeout."
            raise CommandError(msg)
