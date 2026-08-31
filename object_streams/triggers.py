"""Trigger-based outbox capture.

Producers written in Python run after a write and, for the ``enqueue_*`` variants,
after the transaction commits. A trigger writes the outbox row inside the same
transaction as the write that caused it, so an event cannot be lost between commit
and callback, cannot be forgotten at a new write site, and cannot be faked by
application code sending a signal by hand.

``pg_notify`` is transactional: a notification is delivered when the transaction
commits and discarded when it rolls back. Doing the insert and the notify in one
trigger is therefore both durable and rollback safe.

This module requires ``django-pgtrigger``. Install the ``triggers`` extra.

Declare capture in model state so migrations pick it up, either on a model you own:

```python
class Order(models.Model):
    class Meta:
        triggers = [ObjectStreamTrigger(name="order_stream")]
```

or, for a model you do not own, on a proxy in your own app so the migration lands
in your app rather than in the one that defines the model:

```python
class OrderStream(ThirdPartyOrder):
    class Meta:
        proxy = True
        triggers = [ObjectStreamTrigger(name="order_stream")]
```

Run ``makemigrations`` after declaring one. A declared trigger that was never
migrated is not installed, and capture silently does nothing.
"""

from __future__ import annotations

import pgtrigger

from object_streams.postgres import DEFAULT_NOTIFY_CHANNEL
from object_streams.postgres import validate_notify_channel


__all__ = ("NOTIFY_CHANNEL_SETTING", "ObjectStreamTrigger")


# Read at write time from a database setting rather than baked into the trigger body,
# so changing the channel does not require a migration:
#     ALTER DATABASE mydb SET object_streams.notify_channel = 'my_channel';
NOTIFY_CHANNEL_SETTING = "object_streams.notify_channel"

_FUNC = """
    IF (TG_OP = 'DELETE') THEN
        subject_row := to_jsonb(OLD);
        subject_op := 'deleted';
    ELSIF (TG_OP = 'INSERT') THEN
        subject_row := to_jsonb(NEW);
        subject_op := 'created';
    ELSE
        subject_row := to_jsonb(NEW);
        subject_op := 'updated';
    END IF;

    SELECT id INTO subject_type
    FROM django_content_type
    WHERE app_label = '{meta.app_label}' AND model = '{meta.model_name}';

    IF subject_type IS NULL THEN
        RETURN NULL;
    END IF;

    IF (TG_OP = 'UPDATE') THEN
        SELECT coalesce(jsonb_agg(field_name ORDER BY field_name), '[]'::jsonb)
        INTO changed
        FROM jsonb_object_keys(to_jsonb(NEW)) AS field_name
        WHERE to_jsonb(NEW) -> field_name IS DISTINCT FROM to_jsonb(OLD) -> field_name;
    ELSE
        changed := '[]'::jsonb;
    END IF;

    INSERT INTO object_streams_objectstreamevent (
        subject_content_type_id,
        subject_object_id,
        source_content_type_id,
        source_object_id,
        source_history_content_type_id,
        source_history_id,
        facet,
        op,
        changed_fields,
        before,
        after,
        metadata,
        created_at
    ) VALUES (
        subject_type,
        subject_row ->> '{meta.pk.column}',
        subject_type,
        subject_row ->> '{meta.pk.column}',
        NULL,
        '',
        __FACET__,
        subject_op,
        changed,
        NULL,
        NULL,
        jsonb_build_object('transaction_id', pg_current_xact_id()::text),
        now()
    ) RETURNING id INTO event_id;

    PERFORM pg_notify(
        coalesce(current_setting('__CHANNEL_SETTING__', true), __DEFAULT_CHANNEL__),
        event_id::text
    );

    RETURN NULL;
"""


def _quote(value: str) -> str:
    """Return a single-quoted SQL literal."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class ObjectStreamTrigger(pgtrigger.Trigger):
    """Write an outbox row whenever the model's table changes.

    The row that changed is also the subject, which covers models whose own writes
    are what subscribers care about. A source whose subject is a different object,
    such as a workflow state row, needs its subject mapping expressed in SQL or left
    to a Python producer.

    ``changed_fields`` is computed by comparing the old and new row, so it reports
    database column names. A foreign key appears as ``supplier_id`` rather than
    ``supplier``, and it is populated even when the write did not pass
    ``update_fields``, which the signal-based producers cannot do.

    Every row carries the Postgres transaction id in ``metadata``, so a client can
    tell that several events came from one logical change.
    """

    def __init__(self, *, facet: str = "object", channel: str | None = None, **kwargs):
        self.facet = facet
        self.channel = validate_notify_channel(channel) if channel is not None else None
        kwargs.setdefault("when", pgtrigger.After)
        kwargs.setdefault("operation", pgtrigger.Insert | pgtrigger.Update | pgtrigger.Delete)
        kwargs.setdefault(
            "declare",
            [
                ("subject_row", "JSONB"),
                ("subject_type", "INTEGER"),
                ("subject_op", "TEXT"),
                ("changed", "JSONB"),
                ("event_id", "BIGINT"),
            ],
        )
        kwargs.setdefault("func", pgtrigger.Func(self._sql()))
        super().__init__(**kwargs)

    def _sql(self) -> str:
        """Return the trigger body, leaving pgtrigger's own ``{meta.*}`` intact."""
        return (
            _FUNC.replace("__FACET__", _quote(self.facet))
            .replace("__CHANNEL_SETTING__", NOTIFY_CHANNEL_SETTING)
            .replace("__DEFAULT_CHANNEL__", _quote(self.channel or DEFAULT_NOTIFY_CHANNEL))
        )
