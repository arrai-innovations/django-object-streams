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

When the subscribers care about another object than the changed row, map the
subject. A child row can name its parent through a foreign key:

```python
class OrderLine(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE)

    class Meta:
        triggers = [ObjectStreamTrigger(name="order_line_stream", subject="order")]
```

A generic state row can name a content type and an object id:

```python
class StateRecord(models.Model):
    definition = models.ForeignKey(StateDefinition, on_delete=models.CASCADE)
    object_id = models.CharField(max_length=64)

    class Meta:
        triggers = [
            ObjectStreamTrigger(
                name="state_record_stream",
                facet="workflow_state",
                subject_content_type="definition__content_type",
                subject_pk="object_id",
            )
        ]
```
"""

from __future__ import annotations

from typing import Any

import pgtrigger
from django.core.exceptions import FieldDoesNotExist
from django.db import models

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
    WHERE app_label = '{subject_meta.app_label}' AND model = '{subject_meta.model_name}';

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
        subject_row ->> '{subject_meta.pk.column}',
        subject_type,
        subject_row ->> '{subject_meta.pk.column}',
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


# Body for a trigger whose subject is another object than the changed row. The
# changed row stays the source. The subject is read through OLD and NEW, so an
# update that moves the row to another subject writes a row for each subject.
_MAPPED_FUNC = """
    SELECT id INTO source_type
    FROM django_content_type
    WHERE app_label = '{source_meta.app_label}' AND model = '{source_meta.model_name}';

    IF source_type IS NULL THEN
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

    IF (TG_OP = 'DELETE') THEN
        source_id := OLD.{source_pk}::text;
    ELSE
        source_id := NEW.{source_pk}::text;
    END IF;

    IF (TG_OP <> 'INSERT') THEN
        old_subject_type := {old_subject_type};
        old_subject_id := ({old_subject_pk})::text;
    END IF;

    IF (TG_OP <> 'DELETE') THEN
        new_subject_type := {new_subject_type};
        new_subject_id := ({new_subject_pk})::text;
    END IF;

    FOR target IN
        SELECT mapped_type, mapped_id
        FROM (
            VALUES (1, old_subject_type, old_subject_id), (2, new_subject_type, new_subject_id)
        ) AS mapped (slot, mapped_type, mapped_id)
        WHERE mapped_type IS NOT NULL
            AND mapped_id IS NOT NULL
            AND NOT (
                slot = 2
                AND mapped_type IS NOT DISTINCT FROM old_subject_type
                AND mapped_id IS NOT DISTINCT FROM old_subject_id
            )
        ORDER BY slot
    LOOP
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
            target.mapped_type,
            target.mapped_id,
            source_type,
            source_id,
            NULL,
            '',
            __FACET__,
            'updated',
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
    END LOOP;

    RETURN NULL;
"""

_ROW = "__ROW__"


def _quote_name(name: str) -> str:
    """Return a double-quoted SQL identifier."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _quote_table(name: str) -> str:
    """Quote a table name the way Django and pgtrigger do.

    A name that is already quoted is left alone, so the ``'schema"."table'`` form of
    ``db_table`` compiles to ``"schema"."table"``.
    """
    if name.startswith('"') and name.endswith('"'):
        return name
    return f'"{name}"'


def _content_type_lookup(meta) -> str:
    return (
        f"(SELECT id FROM django_content_type WHERE app_label = {_quote(meta.app_label)} "
        f"AND model = {_quote(meta.model_name)})"
    )


def _stored_field(model, name: str, path: str):
    """Return a concrete field stored in the model's own table."""
    concrete = model._meta.concrete_model
    try:
        field = concrete._meta.get_field(name)
    except FieldDoesNotExist:
        msg = f"ObjectStreamTrigger subject path {path!r}: {concrete._meta.label} has no field {name!r}."
        raise ValueError(msg) from None
    stored = getattr(field, "concrete", False) and field.column is not None
    if not stored or field.model._meta.concrete_model._meta.label_lower != concrete._meta.label_lower:
        msg = f"ObjectStreamTrigger subject path {path!r}: {concrete._meta.label}.{name} is not a column of its table."
        raise ValueError(msg)
    return field


def _foreign_key(model, name: str, path: str):
    field = _stored_field(model, name, path)
    if not field.many_to_one and not field.one_to_one:
        msg = f"ObjectStreamTrigger subject path {path!r}: {field.model._meta.label}.{name} is not a foreign key."
        raise ValueError(msg)
    return field


def _path_value(model, path: str) -> tuple[str, Any]:
    """Compile a ``__`` separated field path into SQL that reads its value from ``__ROW__``.

    Every name except the last must be a foreign key. Each hop is a scalar subquery
    on the related table's indexed target column. Returns the SQL and the last field.
    """
    *hops, last = path.split("__")
    value = None
    key_column = None
    current = model
    for name in hops:
        field = _foreign_key(current, name, path)
        value = _column_value(current, field.column, value, key_column)
        key_column = field.target_field.column
        current = field.related_model
    field = _stored_field(current, last, path)
    return _column_value(current, field.column, value, key_column), field


def _column_value(model, column: str, key_value: str | None, key_column: str | None) -> str:
    if key_value is None:
        return f"{_ROW}.{_quote_name(column)}"
    table = _quote_table(model._meta.concrete_model._meta.db_table)
    return f"(SELECT {_quote_name(column)} FROM {table} WHERE {_quote_name(key_column)} = {key_value})"


def _reject_composite_pk(meta) -> None:
    if isinstance(meta.pk, models.CompositePrimaryKey):
        msg = f"ObjectStreamTrigger does not support composite primary keys ({meta.label})."
        raise ValueError(msg)


def _quote(value: str) -> str:
    """Return a single-quoted SQL literal."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class ObjectStreamTrigger(pgtrigger.Trigger):
    """Write an outbox row whenever the model's table changes.

    By default the row that changed is also the subject, which covers models whose
    own writes are what subscribers care about. Two options map the subject to
    another object. The changed row stays the source in both cases.

    ``subject`` names a foreign key path to the subject, such as ``"order"`` or
    ``"order__customer"``. The subject model is the model that the last foreign key
    points at.

    ``subject_content_type`` and ``subject_pk`` are given together for a generic
    relation. ``subject_content_type`` names a path that ends in a foreign key to
    ``ContentType``, such as ``"content_type"`` or ``"definition__content_type"``.
    ``subject_pk`` names a path to the column that holds the subject's primary key,
    such as ``"object_id"``.

    Every name in a path except the last must be a foreign key, and each hop reads
    the related row with one indexed lookup. A mapped trigger writes ``updated`` for
    the subject on insert, update, and delete of the changed row, because the subject
    itself still exists. A delete is mapped through the old row. An update that moves
    the row to another subject writes one row for the old subject and one for the new
    subject. A subject whose content type or primary key is null is skipped.

    Only the first name in a path is read from the old or new row. Each later hop
    reads the related row as it is when the trigger runs. If that row was already
    deleted in the same transaction, the subject resolves to null and the write is
    skipped. A database-level cascade such as ``DB_CASCADE`` deletes the parent row
    before its children, so with ``subject="order__customer"`` the cascaded delete
    of an order's lines writes nothing for the customer. An ORM ``CASCADE`` deletes
    the children first and is captured.

    ``changed_fields`` is computed by comparing the old and new row, so it reports
    database column names. A foreign key appears as ``supplier_id`` rather than
    ``supplier``, and it is populated even when the write did not pass
    ``update_fields``, which the signal-based producers cannot do.

    Every row carries the Postgres transaction id in ``metadata``, so a client can
    tell that several events came from the same database transaction. It is not an
    application action identifier because one action may span several transactions.
    """

    def __init__(
        self,
        *,
        facet: str = "object",
        channel: str | None = None,
        subject: str | None = None,
        subject_content_type: str | None = None,
        subject_pk: str | None = None,
        **kwargs,
    ):
        if subject is not None and (subject_content_type is not None or subject_pk is not None):
            msg = "ObjectStreamTrigger takes either subject or subject_content_type and subject_pk, not both."
            raise ValueError(msg)
        if (subject_content_type is None) != (subject_pk is None):
            msg = "ObjectStreamTrigger requires subject_content_type and subject_pk together."
            raise ValueError(msg)
        self.facet = facet
        self.channel = validate_notify_channel(channel) if channel is not None else None
        self.subject = subject
        self.subject_content_type = subject_content_type
        self.subject_pk = subject_pk
        kwargs.setdefault("when", pgtrigger.After)
        kwargs.setdefault("operation", pgtrigger.Insert | pgtrigger.Update | pgtrigger.Delete)
        if self.is_mapped:
            kwargs.setdefault(
                "declare",
                [
                    ("source_type", "INTEGER"),
                    ("source_id", "TEXT"),
                    ("old_subject_type", "INTEGER"),
                    ("old_subject_id", "TEXT"),
                    ("new_subject_type", "INTEGER"),
                    ("new_subject_id", "TEXT"),
                    ("changed", "JSONB"),
                    ("target", "RECORD"),
                    ("event_id", "BIGINT"),
                ],
            )
        else:
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

    @property
    def is_mapped(self) -> bool:
        """Return whether the subject is another object than the changed row."""
        return self.subject is not None or self.subject_content_type is not None

    def _sql(self) -> str:
        """Return the trigger body, leaving its model placeholders intact."""
        return (
            (_MAPPED_FUNC if self.is_mapped else _FUNC)
            .replace("__FACET__", _quote(self.facet))
            .replace("__CHANNEL_SETTING__", NOTIFY_CHANNEL_SETTING)
            .replace("__DEFAULT_CHANNEL__", _quote(self.channel or DEFAULT_NOTIFY_CHANNEL))
        )

    def get_func_template_kwargs(self, model):
        """Render outbox references with the concrete model's identity."""
        kwargs = super().get_func_template_kwargs(model)
        concrete_meta = model._meta.concrete_model._meta
        _reject_composite_pk(concrete_meta)
        if not self.is_mapped:
            kwargs["subject_meta"] = concrete_meta
            return kwargs

        subject_type, subject_pk = self._subject_sql(model)
        kwargs["source_meta"] = concrete_meta
        kwargs["source_pk"] = _quote_name(concrete_meta.pk.column)
        for row in ("old", "new"):
            kwargs[f"{row}_subject_type"] = subject_type.replace(_ROW, row.upper())
            kwargs[f"{row}_subject_pk"] = subject_pk.replace(_ROW, row.upper())
        return kwargs

    def _subject_sql(self, model) -> tuple[str, str]:
        """Return SQL for the subject content type id and primary key, reading ``__ROW__``."""
        if self.subject is not None:
            subject_pk, field = _path_value(model, self.subject)
            if not field.many_to_one and not field.one_to_one:
                msg = f"ObjectStreamTrigger subject {self.subject!r} must end in a foreign key."
                raise ValueError(msg)
            subject_meta = field.related_model._meta.concrete_model._meta
            _reject_composite_pk(subject_meta)
            if field.target_field.column != subject_meta.pk.column:
                subject_pk, _field = _path_value(model, f"{self.subject}__{subject_meta.pk.name}")
            return _content_type_lookup(subject_meta), subject_pk

        # Compare by label: makemigrations compiles against historical model classes.
        subject_type, field = _path_value(model, self.subject_content_type)
        is_foreign_key = field.many_to_one or field.one_to_one
        if not is_foreign_key or field.related_model._meta.label_lower != "contenttypes.contenttype":
            msg = f"ObjectStreamTrigger subject_content_type {self.subject_content_type!r} must end in a foreign key to ContentType."
            raise ValueError(msg)
        subject_pk, _field = _path_value(model, self.subject_pk)
        return subject_type, subject_pk
