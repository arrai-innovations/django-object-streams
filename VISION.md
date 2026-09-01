# django-object-streams: Vision and Shape

Status: ACTIVE brainstorming.
This is the initial shape document for a new Django library. It captures the
current product boundary, the landscape surveyed so far, and the design pressure
from downstream workflow, history, permissions, and existing subscription clients.

Scope: `django-object-streams/**`

Purpose: Give a cold session enough context to continue designing or
scaffolding the library without re-walking the same architectural questions.

## What this is

`django-object-streams` is a Django library for lightweight, permission-aware,
filter-aware subscriptions to Django model objects and querysets.

The central promise:

- A client can subscribe to one object, a filtered queryset, or a whole model.
- Filters use `django-filter` or a compatible FilterSet interface.
- Notifications are evaluated through the same row visibility rules the REST API
  uses.
- Notifications are lightweight. They tell the client what changed and whether
  to refetch, rather than replacing canonical REST reads and writes.
- Change delivery is backed by a replayable global cursor so reconnects have a
  clear recovery story.

Database decision:

- PostgreSQL is the only database target. This is a feature decision, not only a
  support matrix narrowing.

First README-level promise:

```text
django-object-streams is a Django library for lightweight, permission-aware,
filter-aware subscriptions to model objects and querysets.
```

The public mental model is:

```text
subscribe to the objects this user can see, under this filter, and tell me when
an object enters, changes inside, leaves, or disappears from that set.
```

## What this is not

- Not DRF over WebSockets.
- Not a replacement for REST.
- Not a generic Channels wrapper.
- Not a thin Postgres `LISTEN/NOTIFY` helper.
- Not a frontend client package.
- Not application-specific at the core.
- Not wired to any downstream application.
- Not an activity feed library in the `django-activity-stream` sense.
- Not a durable business audit log by itself. It may reference one.

## Landscape survey

### Django Channels

Channels provides the WebSocket edge, ASGI consumers, channel layers, and group
fanout. Its native concepts are consumers, channels, channel layers, groups, and
events.

Channels should be treated as a transport adapter. The library should not make
Channels concepts the domain model.

### Django Channels REST Framework

DCRF provides DRF-like WebSocket consumers, actions, model observers, instance
observers, and signal-backed model subscriptions.

Useful ideas to retain:

- Subscription ids or request ids matter.
- Object subscriptions and collection subscriptions are both useful.
- Group mapping is a real extension point.

Ideas to avoid copying:

- CRUD actions over WebSockets as the default API shape.
- Django signals as the only change source.
- Serializing one global event before user-specific permission filtering.
- Making subscription semantics live inside DRF action handlers.

### DRF

DRF remains the canonical API surface for reads, writes, validation, and
serializers. This library may reuse serializers for optional payload shaping,
but it should not expose DRF viewsets as WebSocket actions.

### django-filter

`django-filter` is the right public model for filtered subscriptions. A filter
subscription should validate input through a FilterSet and evaluate membership
against the permission-scoped queryset for the current user.

### Postgres LISTEN/NOTIFY

Postgres `LISTEN/NOTIFY` is useful as a low-latency wakeup signal. It is not a
complete message bus, replay log, or permission engine.

The likely shape:

```text
database transaction commits
outbox row is created
NOTIFY sends the outbox id
listener assigns a commit-visible delivery cursor
listener fans out through the transport adapter
consumer sends subscription-relative notifications
```

### django-simple-history and django-pghistory

History systems should be source adapters, not the core event protocol.

The core should support a source reference like:

```json
{
  "model": "workflow.ObjectState",
  "pk": "991",
  "history_model": "workflow.HistoricalObjectState",
  "history_id": 44120
}
```

That allows a downstream workflow integration to start with one history backend
and later move to another without changing the public subscription protocol.

### django-activity-stream

`django-activity-stream` is interesting but adjacent. It is about activity feed
items, actors, verbs, targets, and following sources.

This library is about live object and queryset invalidation. The word `streams`
is acceptable, but the docs should be explicit that this is not an activity feed
or social activity stream implementation.

## Core concepts

### Subject

The subject is the user-visible object whose stream subscribers care about.

Example:

```json
{
  "model": "store.CustomerOrder",
  "pk": "123"
}
```

### Source

The source is the row, event, or history record that caused the stream event.
The source may be the same row as the subject, but it does not have to be.

Example:

```json
{
  "model": "workflow.ObjectState",
  "pk": "991",
  "history_id": 44120
}
```

### Facet

A facet names the part of a subject that changed.

Initial facet candidates:

- `object`
- `workflow_state`
- `permissions`
- `related`

Facet examples:

```text
object changed because CustomerOrder changed
workflow_state changed because ObjectState.state_id changed
permissions changed because a state permission or group membership changed
```

### Cursor

The cursor is a global monotonic delivery sequence assigned after the capture
transaction is visible. It is not the pre-commit outbox row id, a timestamp, or
a per-model history id.

The cursor supports:

- reconnect catch-up
- per-subscription acknowledgement
- ordering across models and facets
- deciding when to return `resync_required`

### Visibility policy

Visibility is the permission-scoped queryset for a user and model.

The core should not assume that Django model permissions are enough. Row-level
visibility must be supplied by the application or an integration adapter.

Conceptual API:

```python
class VisibilityPolicy:
    def get_queryset(self, user, model, action="read"):
        ...
```

### Subscription

A subscription binds a connection to a subject set.

Supported subscription kinds:

- object by model and pk
- filter by model and filter params
- model by model

Initial client verbs:

```jsonl
{"op": "subscribe", "kind": "object", "model": "store.CustomerOrder", "pk": "123", "cursor": 120000}
{"op": "subscribe", "kind": "filter", "model": "store.CustomerOrder", "filter": {"status": "open"}, "cursor": 120000}
{"op": "subscribe", "kind": "model", "model": "store.CustomerOrder", "cursor": 120000}
{"op": "unsubscribe", "subscription_id": "sub_7"}
```

### Query shape

Subscription input may reuse familiar REST query concepts, but not every REST
query parameter has the same meaning in a live subscription.

Candidate request shape:

```json
{
  "op": "subscribe",
  "kind": "filter",
  "model": "store.CustomerOrder",
  "filter": {"status": "open", "workflow_state": "submitted"},
  "search": "acme",
  "ordering": ["-updated_at", "id"],
  "shape": {
    "fields": ["id", "number", "status", "workflow_state"],
    "omit": [],
    "expand": ["customer"]
  },
  "page": {
    "limit": 50
  },
  "cursor": 120000
}
```

The core distinction:

- Membership params decide whether an object belongs in the subscription.
- Shape params decide what optional payload can be sent.
- Ordering params decide relative placement when the client maintains a sorted
  collection.
- Window params decide which slice of a larger result set is currently watched.

## REST query parameter semantics

### Filters

Filters are membership-defining.

If a filter changes from false to true for an object, the subscription should
receive `added`. If it changes from true to false, the subscription should
receive `removed`.

### Search

Search should be treated as a membership filter when it is part of the subscribed
query.

Open question: search is often fuzzy, database-specific, or backed by external
indexes. The first version may require search adapters to declare whether they
can evaluate before and after membership. Otherwise, search subscriptions may
need to return `resync_required` more often.

### Ordering

Ordering is not membership, but it can change client placement.

If an object still matches the subscription but an ordering field changed, the
event should still be `changed`. The notification should include enough metadata
for the client to know that list placement may need to be recomputed.

Candidate field:

```json
{
  "list_action": "changed",
  "changed_fields": ["updated_at"],
  "ordering_changed": true,
  "fetch": true
}
```

The conservative client behavior is to refetch the list or affected page when
`ordering_changed` is true.

### Pagination and windows

Pagination is the hardest REST query concept to map onto subscriptions.

Traditional `page` and `page_size` describe a result window at one point in
time. In a live collection, inserts, deletes, removals, and ordering changes can
shift objects across page boundaries without any changed field on those shifted
objects.

Initial policy options:

- Do not support numbered page subscriptions. Subscribe to the filtered set and
  let clients refetch paginated REST results after relevant events.
- Support only cursor or limit windows, where the server can say
  `resync_required` whenever membership or ordering changes before the watched
  window.
- Treat pagination params as shape or fetch hints, not as strict subscription
  membership.

Default recommendation: do not promise exact live numbered pagination in the
core protocol. For paginated UIs, send collection invalidations and let the
client refetch the canonical page through REST.

### Fields, omit, and expand

Dynamic response shape is not membership.

For downstream DRF applications, `fields`, `omit`, and `expand` style params can
be useful for optional payloads and compatibility with existing clients. They
should not define whether an object belongs in a subscription.

The core can model this as a projection or payload shape:

```json
{
  "shape": {
    "fields": ["id", "number", "workflow_state"],
    "omit": [],
    "expand": ["customer"]
  }
}
```

The base notification can remain lightweight. If a transport or integration
sends partial payloads, it should respect the subscription's shape.

Open question: shape may affect `changed_fields` relevance. If only omitted
fields changed, the server could suppress payloads or set `fetch` to false, but
that should be an optimization, not the initial correctness model.

## Notification shape

Notifications should be subscription-relative. The client should not have to
derive filter membership effects from raw model history.

Base event:

```json
{
  "type": "event",
  "subscription_id": "sub_7",
  "cursor": 120044,
  "subject": {
    "model": "store.CustomerOrder",
    "pk": "123"
  },
  "facet": "workflow_state",
  "op": "updated",
  "list_action": "changed",
  "changed_fields": ["workflow_state"],
  "source": {
    "model": "workflow.ObjectState",
    "pk": "991",
    "history_id": 44120
  },
  "fetch": true
}
```

`list_action` describes the event effect for a particular subscription:

- `added`: the object now belongs in the subscribed set.
- `changed`: the object was already in the set and still is.
- `removed`: the object used to be in the set and no longer is.
- `deleted`: the object was deleted.
- `resync_required`: the server cannot cheaply or safely compute the delta.

Object subscriptions can usually ignore `list_action`, but filter and model
subscriptions need it.

## Outbox shape

The outbox should distinguish subject from source.

Candidate table:

```text
object_stream_event
  id bigserial primary key
  subject_content_type_id integer not null
  subject_object_id text not null
  source_content_type_id integer
  source_object_id text
  source_history_content_type_id integer
  source_history_id text
  facet text not null
  op text not null
  changed_fields jsonb not null default '[]'
  before jsonb
  after jsonb
  created_at timestamptz not null
  metadata jsonb not null default '{}'
```

Open table questions:

- Should `subject_object_id` and source ids be `text` to support UUIDs and
  custom primary keys, or should the core use Django's pk serialization?
- Should source history be represented as content type plus id, or as plain
  adapter metadata?
- Should `before` and `after` be full projections, partial projections, or
  source-specific payloads?

## Downstream workflow support

External workflow systems create a strong requirement that source rows and
subject rows can be different.

The core package is upstream of those systems. It should provide generic subject,
source, facet, visibility, history, and subscription primitives. It should not
import downstream application code, ship downstream-specific adapters, or require
knowledge of a particular workflow schema.

Conceptual downstream adapter:

```python
class WorkflowStateSource:
    source_model = ObjectState
    facet = "workflow_state"

    def subjects_for_source(self, object_state):
        model = object_state.workflow.content_type.model_class()
        return [ObjectRef(model=model, pk=object_state.object_id)]

    def changed_fields(self, event):
        return ["workflow_state"]
```

That adapter should live in the downstream package. It can produce a subject
event like:

```json
{
  "cursor": 120044,
  "subject": {
    "model": "store.CustomerOrder",
    "pk": "123"
  },
  "facet": "workflow_state",
  "source": {
    "model": "workflow.ObjectState",
    "pk": "991",
    "history_id": 44120
  }
}
```

## Downstream integration boundary

The core library should own:

- subscription protocol
- subscription ids
- outbox cursor
- subject and source abstractions
- filter validation hooks
- membership evaluation
- replay and resync semantics
- transport adapter interface
- history adapter interface

Downstream integrations should own:

- resource discovery from application configuration
- mapping application permissions into a visibility policy
- workflow state source mapping
- history adapter wiring for simple-history and pghistory
- compatibility with application-specific client expectations

The integration should feel like:

```python
object_streams.register(
    CustomerOrder,
    filterset=CustomerOrderSubscriptionFilter,
    visibility=CustomerOrderVisibilityPolicy(),
    sources=[
        ModelSource(CustomerOrder),
        WorkflowStateSource(),
    ],
)
```

A downstream package can later wrap this with:

```python
workflow_object_streams.register_resource(CustomerOrderResource)
```

## Reuse vs skip

| Reuse or adapt | Skip |
|---|---|
| Channels consumers and groups as a transport implementation. | Making Channels group names the public subscription model. |
| DCRF's idea of subscription ids and model observers. | DCRF's DRF action over WebSocket API shape. |
| DRF serializers for optional payload shaping. | Treating serializers as the core change protocol. |
| django-filter FilterSets for filter validation and queryset filtering. | Hand-written filter protocol that diverges from Django API filters. |
| Postgres NOTIFY as a wakeup for outbox ids. | Treating NOTIFY as durable delivery or a replay log. |
| simple-history and pghistory as source adapters. | Exposing history ids as the whole public notification contract. |
| Downstream workflow integrations as proving consumers. | Importing downstream workflow models in the core package. |

## Established facts

1. DCRF terminology centers on observers, model observers, actions, and
   subscribe or unsubscribe methods. It is not where the library name `streams`
   is coming from.
2. Channels terminology centers on consumers, channels, channel layers, groups,
   and events. It is useful infrastructure but not the product abstraction.
3. The hard problem is not opening a WebSocket. The hard problem is deciding
   whether a source event makes a subject enter, remain in, leave, or disappear
   from a user's subscribed set.
4. Workflow systems prove that source and subject must be separate concepts.
   A workflow state row can change while subscribers care about the
   workflow-managed subject object.
5. Lightweight notifications plus REST refetch should be the default. Diffs can
   be an optional optimization later.
6. A global commit-visible delivery cursor is the cleanest recovery primitive.
   Pre-commit sequence ids, datetimes, and per-model history ids are weaker
   cursors.
7. PostgreSQL is the database target because the outbox, wakeup, and future
   producer features are expected to rely on PostgreSQL behavior directly.

## Decisions to settle

- Public package name: keep `django-object-streams` or switch before publishing.
- Core transport: require Channels initially, or define a transport interface
  and ship Channels as the first adapter.
- Outbox producer: start with Django transaction hooks, database triggers,
  pghistory events, or adapter-supplied producers.
- Subscription storage: keep active subscriptions in memory per consumer, Redis,
  database, or a hybrid.
- Replay policy: replay missed events when possible, or use cursor gaps to force
  `resync_required`.
- Payload policy: invalidation only by default, optional partial payloads, or
  adapter-defined projections.
- Permission policy API: require a visibility queryset, object permission check,
  or both.
- Filter membership evaluation: compare before and after projections, query the
  current database state, or delegate hard cases to adapters. Internally this
  may classify source events that have no subscription effect, but that should
  not be part of the normal client-facing notification protocol.

## NEXT CHECKS

1. Decide the first README-level promise in one sentence. Record the exact
   wording here before writing package code.
2. Sketch the smallest public registration API that supports plain model changes
   and downstream workflow state changes through generic adapters.
3. Keep the first proof of concept in this repo with a tiny Django test app.
   Downstream application wiring should come after the core behavior is proven.
4. Define the exact websocket protocol for subscribe, unsubscribe, subscribed,
   event, error, and resync messages.
5. Choose the initial history adapter target: simple-history, pghistory, or a
   neutral manual outbox producer.
6. Write one end-to-end scenario:
   a user subscribes to open customer orders, an order changes workflow state,
   the object enters or leaves the filtered set, and the client receives one
   subscription-relative event.

## Early implementation sketch

Candidate modules:

```text
object_streams/
  registry.py
  subscriptions.py
  events.py
  outbox.py
  filters.py
  visibility.py
  transports/
    channels.py
  history/
    base.py
    simple_history.py
    pghistory.py
  sources/
    model.py
```

Candidate integration modules outside core:

```text
workflow_object_streams/
  registry.py
  permissions.py
  workflow.py
  history.py
  reactive_helpers.py
```

## Verification

This document has no runtime verification yet. When code exists, add the exact
commands here.

Initial checks should include:

```text
python -m pytest
python -m ruff check .
python -m ruff format --check .
```
