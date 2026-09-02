# Plan

`PLAN.md` tracks durable implementation status and follow-up work for the
library. `README.md` should stay public-facing and describe current supported
behavior. The original `VISION.md` design context has been implemented,
rejected, or moved here, and was removed.

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
- [x] Done: websocket authentication reference covering `scope["user"]`, auth
  middleware, and the default visibility policy.
- [x] Done: install docs assume PyPI and orient a reader to the registration,
  capture, routing, and listener steps a working install needs.
- [x] Done: `CHANGELOG.md` in Keep a Changelog format, with entry conventions
  and the release step documented in `CONTRIBUTING.md`.
- [ ] Next: publish the first pre-alpha to PyPI. The install docs already
  describe the published package, so the README is only correct once the
  release tag has been built and uploaded.

## Subscription Semantics

- [x] Done: `django-filter` validation and membership evaluation for filter
  subscriptions.
- [x] Done: request-aware FilterSets receive the session request.
- [x] Done: empty filter objects are valid and mean no constraints beyond
  visibility.
- [x] Done: object and model subscriptions use visibility only.
- [ ] Later: define search subscription support beyond the current
  `unsupported_search` response. Search is a membership filter, but it is often
  fuzzy, database-specific, or backed by an external index. A search adapter
  would have to declare whether it can evaluate membership before and after a
  change. Where it cannot, search subscriptions fall back to `resync_required`
  more often than filter subscriptions do.
- [ ] Later: add ordering change hints for subscribed collections. Ordering is
  not membership, so an object whose ordering field changed is still `changed`.
  The candidate shape is an `ordering_changed` boolean alongside
  `changed_fields`, so a client can recompute list placement or refetch the
  affected page rather than guess from `changed_fields` alone.
- [ ] Next: surface the capture transaction id on `StreamEvent` and document how a
  client coalesces the events of one transaction.
- [ ] Later: decide whether shape parameters remain transport hints or become a
  payload adapter contract. Subscription parameters divide into four kinds:
  membership (`filter`, `search`) decides what belongs in the set, shape
  (`fields`, `omit`, `expand`) decides optional payload, ordering decides
  placement, and window decides which slice is watched. Only membership may
  affect whether an event is delivered. Open question: when only omitted fields
  changed, a server could set `fetch` to false, but that is an optimization
  rather than the initial correctness model.
- [ ] Later: document that exact live numbered pagination is not a core
  guarantee unless a future window adapter proves otherwise. Numbered pages
  describe a window at one point in time, and inserts, deletes, and ordering
  changes shift objects across page boundaries without any changed field on the
  shifted objects. The recommended policy is collection invalidation plus a REST
  refetch of the canonical page. A cursor or limit window adapter could later
  send `resync_required` whenever membership or ordering changes before the
  watched window.

## Source And History

- [x] Done: direct model changes through `ModelSource`.
- [x] Done: custom source adapters can map one source instance to one or more
  subject refs.
- [x] Done: source references can carry history model and history id metadata.
- [ ] Later: implement a `django-simple-history` source adapter.
- [ ] Later: implement a `django-pghistory` source adapter.
- [ ] Later: document source adapter examples for workflow-like downstream
  systems without importing downstream code.

## Denial Of Service Boundaries

- [x] Done: per-connection subscription cap through `max_subscriptions`.
- [x] Done: per-subscription membership cap through `max_member_pks`, checked
  with a bounded query before the pk set is materialized.
- [x] Done: reject a `subscription_id` that is already active, and discard the
  previous Channels groups when subscription groups are replaced.
- [ ] Next: inbound message validation limits for message size,
  `subscription_id` length, `pk` length, filter key count and value size,
  `ordering` length, and `shape` size.
- [ ] Later: document that registered FilterSets are a cost boundary, and that
  unbounded `icontains`, expensive joins, and large `in` lists should be
  avoided or capped by the integrator.
- [ ] Later: reduce fanout work for many subscriptions on one model, through
  grouping or a per-event membership cache, now that caps bound the worst case.
- [ ] Later: policy for repeated invalid messages and slow consumers, including
  a per-connection error budget and documented close codes.
- [ ] Later: guidance on `before`, `after`, and `metadata` payload size, given
  the library delivers notices rather than object state.
- [ ] Later: make the listener broadcast batch size configurable instead of the
  hard-coded 1000 in `object_streams_listen`.
- [ ] Later: document trigger capture volume for bulk writes, and the retention
  and listener capacity that follows from it.

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
- [x] Done: protocol error response coverage for every documented error code in
  both the sync and async runtimes.
- [x] Done: transport coverage proving visibility policies apply to the
  connection scope user.
- [x] Done: dedupe window bounds and subscription cap coverage.
- [ ] Later: add compatibility tests for non-integer primary keys.

## External Boundaries

- [ ] External: VUEDA integration belongs in a downstream adapter, not this
  package.
- [ ] External: application-specific visibility policies belong in the
  application or downstream integration package.
- [ ] External: workflow state mappings belong in a downstream source adapter.
