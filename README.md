# Django Object Streams

![pytest status][] [![coverage status][]][coverage] ![ruff status][] ![pysentry status][]

`django-object-streams` is a Django library for lightweight, permission-aware, filter-aware subscriptions to model objects and querysets.

It is designed to tell clients what changed and whether they should refetch through the canonical REST API. It is not a replacement for REST, a generic Channels wrapper, or an activity feed library.

## Status

Pre-alpha. The current package includes the core registration API, a replayable
outbox table, generic source-to-event producer helpers, and a connection-local
subscription session runtime. A minimal Channels JSON websocket consumer is
available with channel-layer fanout and PostgreSQL `LISTEN/NOTIFY` wakeups.
A pruning command enforces outbox retention limits. History integrations are
not implemented yet.

## Database Support

This library targets PostgreSQL. That is a feature choice, not just a reduced
support matrix: replayable cursors, low-latency wakeups, and future producer
adapters are expected to use PostgreSQL behavior directly.

## Install

This pre-alpha package is not published to PyPI yet. Install from GitHub while
the API is still settling:

```console
uv add git+https://github.com/arrai-innovations/django-object-streams.git
```

After the package is published:

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

from object_streams import AllowAllVisibilityPolicy
from object_streams import ModelSource
from object_streams import register

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
from object_streams import EventOperation
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

Outbox writes send a PostgreSQL notification with the committed outbox id by
default. Pass `notify=False` to `create_source_events(...)`,
`enqueue_source_events(...)`, `create_outbox_event(...)`, or
`enqueue_outbox_event(...)` when creating rows that should not wake connected
consumers.

## Trigger-Based Capture

Calling the producers from application code works, but it depends on every write
site remembering to do it. Postgres triggers do not:

- The outbox row is written inside the same transaction as the write that caused
  it, so nothing is lost between commit and a callback.
- `DELETE` is captured. `post_save` cannot see deletes, and a `post_delete`
  receiver is a second thing to remember.
- Bulk writes are captured. `QuerySet.update()` and `bulk_create()` bypass
  `save()` and signals entirely.
- `changed_fields` is computed by comparing the old and new row, so it is
  populated whether or not the write passed `update_fields`.

Install the extra:

```console
uv add "django-object-streams[triggers]"
```

Add `pgtrigger` to `INSTALLED_APPS`, then declare capture in model state so
migrations pick it up. On a model you own:

```python
from object_streams.triggers import ObjectStreamTrigger


class Order(models.Model):
    class Meta:
        triggers = [ObjectStreamTrigger(name="order_stream")]
```

On a model you do not own, declare it on a proxy in your own app so the migration
lands in your app rather than in the one that defines the model:

```python
class OrderStream(ThirdPartyOrder):
    class Meta:
        proxy = True
        triggers = [ObjectStreamTrigger(name="order_stream")]
```

Run `makemigrations` and `migrate` afterwards. A declared trigger that was never
migrated is not installed, and capture silently does nothing.

Every captured row records the Postgres transaction id in `metadata`:

```json
{"transaction_id": "3208276"}
```

Events written by one transaction share it, so a client that receives several
events from one logical change can coalesce them into a single refetch instead of
one per row.

The trigger sends its own `pg_notify`, which is transactional: the notification
arrives when the transaction commits and is discarded when it rolls back. It
reads the channel from a database setting so that changing it needs no migration:

```sql
ALTER DATABASE mydb SET object_streams.notify_channel = 'my_channel';
```

Without that setting the trigger uses `object_streams_events`, the same default
as `OBJECT_STREAMS_NOTIFY_CHANNEL`. Set both when you override either.

Two limits are worth knowing:

- The captured row is its own subject, which suits models whose own writes are
  what subscribers care about. A source whose subject is a different object, such
  as a workflow state row pointing at the object it governs, needs its subject
  mapping written as SQL or left to a Python producer.
- `changed_fields` reports database column names, so a foreign key appears as
  `supplier_id` rather than `supplier`.

Producers and triggers can coexist, but not on the same table: a table with a
capture trigger should not also have producers called against it, or each write
lands in the outbox twice.

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

Filter subscriptions use the registered `django-filter` compatible FilterSet.
The FilterSet receives the visibility-scoped queryset and the `request` passed
to `SubscriptionSession`, so request-aware method filters can use the same
context as normal Django view filters. Clients may send either `filter` or
`filters`. An empty filter object is valid and means no FilterSet constraints
beyond visibility. Object and model subscriptions use visibility only.

## Public Import Surface

The package root exports only names that are safe to import before
`django.setup()` runs:

```python
from object_streams import AllowAllVisibilityPolicy
from object_streams import DenyAllVisibilityPolicy
from object_streams import EventOperation
from object_streams import ListAction
from object_streams import ModelSource
from object_streams import ObjectRef
from object_streams import ObjectStreamRegistration
from object_streams import ObjectStreamRegistry
from object_streams import ResyncRequired
from object_streams import Source
from object_streams import SourceRef
from object_streams import StreamEvent
from object_streams import SubscriptionKind
from object_streams import SubscriptionRequest
from object_streams import VisibilityPolicy
from object_streams import register
from object_streams import registry
```

Registration errors (`ObjectStreamsError`, `RegistrationError`,
`AlreadyRegistered`, `NotRegistered`, `FilterValidationError`) are exported from
the root as well.

Import the rest from their own modules, because they load Django models:
`object_streams.models`, `object_streams.outbox`, `object_streams.postgres`,
`object_streams.producers`, `object_streams.retention`,
`object_streams.sessions`, and `object_streams.transports.channels`.

## Protocol Reference

The protocol is JSON messages in both directions. Client messages carry an
`op`. Server messages carry a `type`.

### Client to server

`subscribe` registers one subscription and returns a `subscribed`
acknowledgement.

| Field | Required | Meaning |
|---|---|---|
| `op` | yes | `"subscribe"`. |
| `kind` | no | `"object"`, `"filter"`, or `"model"`. Defaults to `"object"`. |
| `model` | yes | Model label, such as `"store.CustomerOrder"`. |
| `pk` | for `object` | Primary key of the subscribed object. |
| `filter` | no | FilterSet parameters. `filters` is accepted as an alias. |
| `cursor` | no | Last cursor the client processed. Triggers replay. |
| `subscription_id` | no | Client-chosen id. The server assigns `sub_N` when omitted. |
| `search` | no | Rejected with `unsupported_search` for now. |
| `ordering` | no | Accepted and echoed. Does not affect membership. |
| `shape` | no | Accepted and echoed. Transport payload hint. |

```json
{"op": "subscribe", "kind": "filter", "model": "store.CustomerOrder", "filter": {"status": "open"}, "cursor": 120000}
```

`unsubscribe` removes one subscription.

```json
{"op": "unsubscribe", "subscription_id": "sub_7"}
```

### Server to client

`subscribed` acknowledges a subscription. Its `cursor` is the outbox cursor the
subscription starts from, which is the global cursor at subscribe time, not the
cursor the client requested.

```json
{
  "type": "subscribed",
  "subscription_id": "sub_7",
  "kind": "filter",
  "model": "store.CustomerOrder",
  "filter": {"status": "open"},
  "cursor": 120044
}
```

`unsubscribed` acknowledges removal.

```json
{
  "type": "unsubscribed",
  "subscription_id": "sub_7"
}
```

`event` reports one subscription-relative change.

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

`op` is the source operation: `created`, `updated`, or `deleted`.

`list_action` is the effect on this subscription:

| `list_action` | Meaning |
|---|---|
| `added` | The object now belongs in the subscribed set. |
| `changed` | The object was in the set and still is. |
| `removed` | The object left the set but still exists. |
| `deleted` | The object was deleted. |

Object subscriptions can usually ignore `list_action`. Filter and model
subscriptions need it. Events with no effect on a subscription are not sent.

`resync_required` is a subscription-level message for cases where the server
cannot safely compute the delta. It carries no subject. The client should
refetch through REST and resubscribe from the supplied `cursor`.

```json
{
  "type": "resync_required",
  "subscription_id": "sub_7",
  "cursor": 120044,
  "reason": "cursor_replay_unavailable"
}
```

| `reason` | Cause |
|---|---|
| `cursor_replay_unavailable` | A filter or model subscription asked to replay from an older cursor. Collection membership cannot be recomputed from outbox rows alone. |
| `object_replay_limit_exceeded` | An object subscription had more missed events than `replay_limit`, which defaults to 1000. |
| `cursor_pruned` | The requested cursor is older than the retention watermark, so the missed rows are gone. |

`error` reports a rejected message. `details` is present only when the error
carries structured data, such as FilterSet validation errors.

```json
{
  "type": "error",
  "code": "invalid_filter",
  "message": "Subscription filters are invalid.",
  "details": {"status": ["Select a valid choice."]}
}
```

| `code` | Cause |
|---|---|
| `invalid_request` | Unsupported `op`, missing `model`, missing `subscription_id`, or a non-object JSON message. |
| `invalid_cursor` | The requested cursor is newer than the outbox. |
| `invalid_filter` | FilterSet validation failed. `details` carries the errors. |
| `not_found` | An object subscription target does not exist or is not visible. |
| `not_subscribed` | Unsubscribe named an inactive subscription. |
| `unsupported_search` | The request included `search`. |
| `invalid_event` | A fanout message arrived without an outbox id. |
| `event_not_found` | A fanout message named an outbox row that does not exist. |

## Deployment

### Process topology

A production deployment runs three kinds of process against one PostgreSQL
database and one process-shared channel layer:

```text
ASGI workers      run ObjectStreamConsumer, hold connection-local subscriptions
listener process  runs object_streams_listen, turns NOTIFY into channel fanout
application       writes model changes and outbox rows
```

The path from a write to a client is:

```text
application transaction commits
outbox row is created and NOTIFY sends its id
listener receives the id and loads the row
listener fans the row out to Channels groups
each ASGI worker evaluates the row against its own subscriptions
consumer sends subscription-relative messages
```

Subscription state is connection-local. Every worker evaluates visibility and
filter membership for its own connections, so no permission decision is shared
between processes. Only outbox ids cross the channel layer.

### ASGI routing

```python
from channels.routing import ProtocolTypeRouter
from channels.routing import URLRouter
from django.urls import path

from object_streams.transports.channels import ObjectStreamConsumer


application = ProtocolTypeRouter(
    {
        "websocket": URLRouter(
            [
                path("ws/object-streams/", ObjectStreamConsumer.as_asgi()),
            ]
        ),
    }
)
```

The consumer joins deterministic Channels groups for active subscriptions and
publishes outbox rows when it receives an `object.stream.event` ASGI message
containing `id` or `outbox_id`. Object subscriptions join object groups. Filter
and model subscriptions join model groups, then each consumer evaluates the
event against its own connection-local subscriptions and visibility policy.

Your ASGI workers can run under Uvicorn, Daphne, or another ASGI server.

### Channel layer

The listener and the ASGI workers are separate processes, so the in-memory
channel layer will not deliver between them. Production deployments need a
process-shared layer such as Redis:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": ["redis://127.0.0.1:6379/0"],
        },
    },
}
```

Install `channels-redis` alongside this package to use that backend. Every ASGI
worker and the listener process must point at the same layer.

### Listener process

```console
python manage.py object_streams_listen
```

| Option | Meaning |
|---|---|
| `--database` | Database alias to listen on. Defaults to `default`. |
| `--channel` | PostgreSQL notification channel. Defaults to the configured channel. |
| `--timeout` | Maximum seconds to wait for a notification before returning. |
| `--once` | Broadcast one notification and exit. |
| `--retry-delay` | Seconds to wait before retrying after a database error. Defaults to 1. |
| `--max-retries` | Maximum reconnect attempts. Defaults to retrying forever. |

The listener retries database errors by default, so a PostgreSQL restart does
not need a supervisor restart. Use `--once --timeout <seconds>` for smoke tests
or supervisor health checks.

### Settings

| Setting | Default | Meaning |
|---|---|---|
| `OBJECT_STREAMS_NOTIFY_CHANNEL` | `object_streams_events` | PostgreSQL `LISTEN/NOTIFY` channel carrying committed outbox ids. Must be an unquoted identifier of 63 bytes or fewer. |
| `OBJECT_STREAMS_RETENTION_DAYS` | `None` | Age limit for outbox rows, in days. `None` keeps every row. |
| `OBJECT_STREAMS_RETENTION_MAX_ROWS` | `None` | Row limit for the outbox. `None` keeps every row. |

Give each deployment sharing a database its own
`OBJECT_STREAMS_NOTIFY_CHANNEL` so listeners do not wake on ids they cannot
load.

## Outbox Retention

The outbox grows without bound until it is pruned. Configure a limit and run
the pruning command on a schedule:

```python
OBJECT_STREAMS_RETENTION_DAYS = 30
OBJECT_STREAMS_RETENTION_MAX_ROWS = 5_000_000
```

```console
python manage.py object_streams_prune
```

When both limits are set, the stricter one wins. `--days` and `--max-rows`
override the settings, `--dry-run` reports what would be deleted, and
`--database` selects a database alias.

Retention sets the cursor contract. Pick a window longer than the longest
client disconnect you want to replay rather than resync.

Two properties keep pruning safe:

- Pruning never deletes the newest retained row. The global cursor never moves
  backwards, so a returning client never sees `invalid_cursor` for a cursor it
  legitimately holds.
- Pruning records how far it deleted. A client that reconnects with a cursor
  older than that watermark receives `resync_required` with reason
  `cursor_pruned` rather than a silent, empty catch-up.

## Development

Tests expect a local PostgreSQL server and a role that can create test
databases. By default, `tests.settings` connects through the local PostgreSQL
socket as the `postgres` role and uses a database named
`django_object_streams`.

The Redis-backed live listener integration test runs when Redis is available.
It uses `OBJECT_STREAMS_TEST_REDIS_URL`, defaulting to
`redis://localhost:6379/15`, and skips when Redis is not reachable.

Create the local database if needed:

```console
createdb -U postgres django_object_streams
```

To override the connection, copy `.env.local.example` to `.env.local` and set
the database variables there.

```console
just check
just test
just coverage
```

CI enforces a coverage floor of 80 percent.

Package code follows standard uv, setuptools, ruff, pytest-django, and Justfile
conventions for a focused Django library.

## License

BSD 3-Clause. See [LICENSE](LICENSE).

[coverage]: https://docs.arrai.dev/django-object-streams/artifacts/main/htmlcov_pytest/
[coverage status]: https://docs.arrai.dev/django-object-streams/artifacts/main/coverage.svg
[pysentry status]: https://docs.arrai.dev/django-object-streams/artifacts/main/pysentry.svg
[pytest status]: https://docs.arrai.dev/django-object-streams/artifacts/main/pytest.svg
[ruff status]: https://docs.arrai.dev/django-object-streams/artifacts/main/ruff.svg
