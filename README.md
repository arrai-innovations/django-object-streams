# Django Object Streams

`django-object-streams` is a Django library for lightweight, permission-aware, filter-aware subscriptions to model objects and querysets.

It is designed to tell clients what changed and whether they should refetch through the canonical REST API. It is not a replacement for REST, a generic Channels wrapper, or an activity feed library.

## Status

Pre-alpha. The current package includes the core registration API, a replayable
outbox table, generic source-to-event producer helpers, and a connection-local
subscription session runtime. A concrete Channels consumer, PostgreSQL
`LISTEN/NOTIFY` wakeups, and history integrations are not implemented yet.

## Database Support

This library targets PostgreSQL. That is a feature choice, not just a reduced
support matrix: replayable cursors, low-latency wakeups, and future producer
adapters are expected to use PostgreSQL behavior directly.

## Install

```console
uv add django-object-streams
```

For local development:

```console
just bootstrap
```

## Django Setup

Add the app to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "object_streams",
]
```

Run migrations after installing:

```console
python manage.py migrate object_streams
```

## Core Concepts

- Subject: the user-visible object subscribers care about.
- Source: the row, event, or history record that caused the stream event.
- Facet: the part of a subject that changed, such as `object`, `workflow_state`, `permissions`, or `related`.
- Cursor: the global outbox id used for reconnect and replay.
- Visibility policy: the permission-scoped queryset for a user and model.
- Subscription: an object, filtered queryset, or model-level watch.

## Usage Sketch

Register models that can be subscribed to:

```python
import django_filters

from object_streams import register
from object_streams.sources import ModelSource
from object_streams.visibility import AllowAllVisibilityPolicy

from store.models import CustomerOrder


class CustomerOrderSubscriptionFilter(django_filters.FilterSet):
    class Meta:
        model = CustomerOrder
        fields = ["status"]


register(
    CustomerOrder,
    filterset=CustomerOrderSubscriptionFilter,
    visibility=AllowAllVisibilityPolicy(),
    sources=[ModelSource(CustomerOrder)],
)
```

Produce outbox events after application writes:

```python
from object_streams.events import EventOperation
from object_streams.producers import enqueue_source_events


order.status = "open"
order.save(update_fields=["status"])
enqueue_source_events(
    order,
    op=EventOperation.UPDATED,
    changed_fields=["status"],
)
```

For scripts, tests, or jobs that need the outbox row immediately, use
`create_source_events(...)` instead.

Handle connection-local subscriptions with a transport object:

```python
from object_streams.sessions import SubscriptionSession


session = SubscriptionSession(
    user=request.user,
    request=request,
    transport=transport,
)

session.handle_message(
    {
        "op": "subscribe",
        "kind": "filter",
        "model": "store.CustomerOrder",
        "filter": {"status": "open"},
        "cursor": 120000,
    }
)
```

`SubscriptionSession` is intentionally imported from `object_streams.sessions`.
It is not exported from the package root because it imports Django models.

Client subscription messages look like:

```json
{"op": "subscribe", "kind": "filter", "model": "store.CustomerOrder", "filter": {"status": "open"}, "cursor": 120000}
```

Subscription-relative event messages look like:

```json
{
  "type": "event",
  "subscription_id": "sub_7",
  "cursor": 120044,
  "subject": {"model": "store.CustomerOrder", "pk": "123"},
  "facet": "workflow_state",
  "op": "updated",
  "list_action": "changed",
  "changed_fields": ["workflow_state"],
  "fetch": true
}
```

When the session cannot safely replay collection changes after a cursor, it
sends a subscription-level resync message instead of a subject-bearing event:

```json
{
  "type": "resync_required",
  "subscription_id": "sub_7",
  "cursor": 120044,
  "reason": "cursor_replay_unavailable"
}
```

## Development

Tests expect a local PostgreSQL server and a role that can create test
databases. By default, `tests.settings` connects through the local PostgreSQL
socket as the `postgres` role and uses a database named
`django_object_streams`.

Create the local database if needed:

```console
createdb -U postgres django_object_streams
```

To override the connection, copy `.env.local.example` to `.env.local` and set
the database variables there.

```console
just check
just test
```

Package code follows standard uv, setuptools, ruff, pytest-django, and Justfile
conventions for a focused Django library.
