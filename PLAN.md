# Plan

`PLAN.md` tracks durable implementation status and follow-up work for the
library. `README.md` should stay public-facing and describe current supported
behavior. `VISION.md` is temporary design context and should be removed once the
remaining decisions have been implemented, rejected, or moved here.

## Status Legend

- Done: implemented and covered by tests.
- Next: useful before an initial pre-alpha release.
- Later: likely useful, but not required for the minimum core.
- External: belongs outside this package.

## Minimum Core

- [x] Done: model registration with `django-filter` compatible FilterSets.
- [x] Done: visibility policies for permission-scoped querysets.
- [x] Done: source adapters with separate subject and source references.
- [x] Done: facets for classifying the part of a subject that changed.
- [x] Done: durable PostgreSQL outbox rows with global cursors.
- [x] Done: producer helpers for immediate and transaction-on-commit writes.
- [x] Done: PostgreSQL `LISTEN/NOTIFY` wakeups for committed outbox ids.
- [x] Done: connection-local sync and async subscription runtimes.
- [x] Done: object, filter, and model subscription kinds.
- [x] Done: subscription-relative `added`, `changed`, `removed`, and `deleted`
  events.
- [x] Done: object replay after cursor.
- [x] Done: collection replay fallback through `resync_required`.
- [x] Done: Channels JSON WebSocket consumer.
- [x] Done: channel-layer fanout from the listener to WebSocket workers.
- [x] Done: Redis-backed live listener integration coverage.

## Release Cleanup

- [ ] Next: add a `LICENSE` file matching the package metadata.
- [ ] Next: confirm the package publication path and keep install docs accurate.
- [ ] Next: add CI for supported Python and Django versions.
- [ ] Next: choose and enforce a coverage floor.
- [ ] Next: review the public import surface before publishing.
- [ ] Next: write a compact protocol reference for subscribe, unsubscribe,
  subscribed, event, error, and resync messages.
- [ ] Next: document production process topology: ASGI workers, listener
  process, PostgreSQL, and a process-shared channel layer.
- [ ] Next: add outbox retention settings and a pruning command.

## Subscription Semantics

- [x] Done: `django-filter` validation and membership evaluation for filter
  subscriptions.
- [x] Done: request-aware FilterSets receive the session request.
- [x] Done: empty filter objects are valid and mean no constraints beyond
  visibility.
- [x] Done: object and model subscriptions use visibility only.
- [ ] Later: define search subscription support beyond the current
  `unsupported_search` response.
- [ ] Later: add ordering change hints for subscribed collections.
- [ ] Later: decide whether shape parameters remain transport hints or become a
  payload adapter contract.
- [ ] Later: document that exact live numbered pagination is not a core
  guarantee unless a future window adapter proves otherwise.

## Source And History

- [x] Done: direct model changes through `ModelSource`.
- [x] Done: custom source adapters can map one source instance to one or more
  subject refs.
- [x] Done: source references can carry history model and history id metadata.
- [ ] Later: implement a `django-simple-history` source adapter.
- [ ] Later: implement a `django-pghistory` source adapter.
- [ ] Later: document source adapter examples for workflow-like downstream
  systems without importing downstream code.

## Operations

- [x] Done: listener management command.
- [x] Done: listener reconnect behavior for database errors.
- [x] Done: bounded listener mode for smoke tests and health checks.
- [ ] Next: add deployment notes for Redis or another process-shared Channels
  layer.
- [ ] Next: document `OBJECT_STREAMS_NOTIFY_CHANNEL`.
- [ ] Later: add structured logging around listener wakeups, fanout, and
  malformed notifications.
- [ ] Later: add metrics hooks for outbox lag, fanout count, and active
  subscriptions if the core can expose them without choosing a metrics backend.

## Testing

- [x] Done: registry, event, outbox, producer, session, async session, Channels,
  PostgreSQL, and Redis-backed listener coverage.
- [ ] Next: add CI matrix coverage for Python 3.11 to 3.14.
- [ ] Next: add CI matrix coverage for Django 5.2, 6.0, and 6.1.
- [ ] Next: make the Redis integration test conditional in CI through an
  explicit service configuration.
- [ ] Later: add compatibility tests for non-integer primary keys.

## External Boundaries

- [ ] External: VUEDA integration belongs in a downstream adapter, not this
  package.
- [ ] External: application-specific visibility policies belong in the
  application or downstream integration package.
- [ ] External: workflow state mappings belong in a downstream source adapter.

