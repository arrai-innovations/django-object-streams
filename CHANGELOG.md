# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Entry conventions, including which commit types earn an entry, are in
[CONTRIBUTING](CONTRIBUTING.md#changelog).

## [Unreleased]

### Added

- Model registration with `django-filter` compatible FilterSets and
  permission-scoped visibility policies.
- Object, filter, and model subscription kinds, delivering subscription-relative
  `added`, `changed`, `removed`, and `deleted` events.
- Source adapters that separate the subject a subscriber cares about from the
  row that caused the event, and facets that classify which part changed.
- A replayable PostgreSQL outbox with commit-visible delivery cursors, object
  replay after a cursor, and a `resync_required` fallback when exact replay is
  unavailable.
- Producer helpers for immediate and transaction-on-commit outbox writes.
- Trigger-based capture through `django-pgtrigger`, covering deletes and bulk
  writes, available through the `triggers` extra.
- A Channels JSON WebSocket consumer with channel-layer fanout and PostgreSQL
  `LISTEN/NOTIFY` wakeups.
- `object_streams_listen`, which turns notifications into channel-layer fanout,
  and `object_streams_prune`, which enforces outbox retention limits.
- Per-connection limits for active subscriptions, subscription membership size,
  object replay, and the outbox id dedupe window.

[unreleased]: https://github.com/arrai-innovations/django-object-streams/commits/main/
