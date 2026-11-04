"""System checks for object stream capture."""

from __future__ import annotations

from typing import Any

from django.core import checks
from django.db import connections
from django.db.migrations.executor import MigrationExecutor


__all__ = ("check_capture_triggers",)


@checks.register(checks.Tags.database)
def check_capture_triggers(app_configs: Any = None, databases: Any = None, **kwargs: Any) -> list[checks.CheckMessage]:
    """Warn about a declared capture trigger that the database does not run.

    A trigger that was never migrated is not installed, and an outdated one runs an
    older body, so capture can silently do nothing or record the wrong thing. The
    check queries each requested database, which is why it is tagged as a database
    check. Django runs it for `migrate` and for `check --database <alias>`. A
    database that has unapplied migrations is skipped, because one of them may
    install the trigger.
    """

    if not databases:
        return []
    try:
        import pgtrigger
        from pgtrigger import core
    except ImportError:
        return []

    from object_streams.triggers import ObjectStreamTrigger

    declared = [
        (model, trigger)
        for model, trigger in pgtrigger.registered()
        if isinstance(trigger, ObjectStreamTrigger) and (app_configs is None or model._meta.app_config in app_configs)
    ]
    if not declared:
        return []

    messages: list[checks.CheckMessage] = []
    for alias in databases:
        connection = connections[alias]
        if connection.vendor != "postgresql" or _has_unapplied_migrations(connection):
            continue
        for model, trigger in declared:
            status, _enabled = trigger.get_installation_status(model, database=alias)
            uri = trigger.get_uri(model)
            if status == core.UNINSTALLED:
                messages.append(
                    checks.Warning(
                        f"Capture trigger {uri} is not installed in database {alias!r}.",
                        hint="Run makemigrations and migrate. Until then, writes to this model are not captured.",
                        obj=model,
                        id="object_streams.W001",
                    )
                )
            elif status == core.OUTDATED:
                messages.append(
                    checks.Warning(
                        f"Capture trigger {uri} in database {alias!r} does not match its declaration.",
                        hint="Run makemigrations and migrate to install the current trigger definition.",
                        obj=model,
                        id="object_streams.W002",
                    )
                )
    return messages


def _has_unapplied_migrations(connection: Any) -> bool:
    """Return whether migrate has work left, which may include installing a trigger."""

    executor = MigrationExecutor(connection)
    return bool(executor.migration_plan(executor.loader.graph.leaf_nodes()))
