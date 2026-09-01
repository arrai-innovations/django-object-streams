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
- [x] Done: durable PostgreSQL outbox rows with commit-visible global delivery
  cursors, separate from capture row ids.
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
- [x] Done: outbox retention limits, pruning command, and a pruning watermark
  that turns a pruned cursor into `resync_required`.
- [x] Done: trigger-based capture through `django-pgtrigger`, covering deletes and
  bulk writes, computing changed columns, and stamping the Postgres transaction id.

## Release Cleanup

- [x] Done: `LICENSE` file matching the package metadata.
- [x] Done: CI for supported Python and Django versions.
- [x] Done: coverage floor of 80 percent enforced through `fail_under`.
- [x] Done: public import surface limited to names that are safe to import
  before `django.setup()`, with a regression test.
- [x] Done: protocol reference for subscribe, unsubscribe, subscribed,
  unsubscribed, event, error, and resync messages.
- [x] Done: production process topology, channel layer, and settings reference.
- [x] Done: outbox retention settings and a pruning command.
- [ ] Next: publish the first pre-alpha to PyPI and update the install docs to
  drop the GitHub fallback.

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
- [ ] Next: surface the capture transaction id on `StreamEvent` and document how a
  client coalesces the events of one transaction.
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
- [x] Done: listener startup and reconnect drain for committed rows whose
  channel-layer fanout was not durably recorded.
- [x] Done: bounded listener mode for smoke tests and health checks.
- [x] Done: deployment notes for Redis or another process-shared Channels
  layer.
- [x] Done: settings reference for `OBJECT_STREAMS_NOTIFY_CHANNEL`,
  `OBJECT_STREAMS_RETENTION_DAYS`, and `OBJECT_STREAMS_RETENTION_MAX_ROWS`.
- [ ] Later: report pruning progress and retention lag from the pruning
  command.
- [ ] Next: system check that reports a declared capture trigger which was never
  migrated, so capture cannot silently do nothing.
- [ ] Later: express subject mapping in SQL so a source whose subject is another
  object can be captured by trigger rather than by a Python producer.
- [ ] Later: add structured logging around listener wakeups, fanout, and
  malformed notifications.
- [ ] Later: add metrics hooks for outbox lag, fanout count, and active
  subscriptions if the core can expose them without choosing a metrics backend.

## Testing

- [x] Done: registry, event, outbox, producer, session, async session, Channels,
  PostgreSQL, and Redis-backed listener coverage.
- [x] Done: CI matrix coverage for Python 3.11 to 3.14.
- [x] Done: CI matrix coverage for Django 5.2, 6.0, and 6.1.
- [x] Done: Redis integration test enabled in CI through an explicit service
  configuration.
- [x] Done: retention, pruning, pruned-cursor replay, and public import surface
  coverage.
- [x] Done: trigger capture coverage for inserts, updates, deletes, bulk writes,
  rollback, notification, and transaction grouping.
- [x] Done: concurrent reverse-commit coverage proving that delivery cursor order
  does not depend on pre-commit sequence allocation.
- [x] Done: subscription handshake catch-up coverage for events committed while
  transport routing is being prepared.
- [ ] Later: add compatibility tests for non-integer primary keys.

## External Boundaries

- [ ] External: VUEDA integration belongs in a downstream adapter, not this
  package.
- [ ] External: application-specific visibility policies belong in the
  application or downstream integration package.
- [ ] External: workflow state mappings belong in a downstream source adapter.
