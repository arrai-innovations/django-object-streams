"""Prune outbox rows past the configured retention limits."""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import DEFAULT_DB_ALIAS

from object_streams.retention import get_retention_days
from object_streams.retention import get_retention_max_rows
from object_streams.retention import prune_outbox
from object_streams.retention import retention_cutoff


class Command(BaseCommand):
    help = "Delete object stream outbox rows past the configured retention limits."

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help="Database alias to prune.",
        )
        parser.add_argument(
            "--days",
            default=None,
            type=int,
            help="Age limit in days. Defaults to OBJECT_STREAMS_RETENTION_DAYS.",
        )
        parser.add_argument(
            "--max-rows",
            default=None,
            type=int,
            help="Row limit for the outbox. Defaults to OBJECT_STREAMS_RETENTION_MAX_ROWS.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many rows would be deleted without deleting them.",
        )

    def handle(self, *args, **options):
        database = options["database"]
        dry_run = options["dry_run"]
        verbosity = int(options["verbosity"])

        try:
            days = options["days"] if options["days"] is not None else get_retention_days()
            max_rows = options["max_rows"] if options["max_rows"] is not None else get_retention_max_rows()
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        if days is None and max_rows is None:
            msg = (
                "No retention limit is configured. "
                "Set OBJECT_STREAMS_RETENTION_DAYS or OBJECT_STREAMS_RETENTION_MAX_ROWS, "
                "or pass --days or --max-rows."
            )
            raise CommandError(msg)

        try:
            before = retention_cutoff(days) if days is not None else None
            deleted = prune_outbox(before=before, max_rows=max_rows, using=database, dry_run=dry_run)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        if verbosity >= 1:
            action = "Would delete" if dry_run else "Deleted"
            self.stdout.write(f"{action} {deleted} object stream outbox rows.")
        return None
