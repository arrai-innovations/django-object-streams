# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Entry conventions, including which commit types earn an entry, are in
[CONTRIBUTING](CONTRIBUTING.md#changelog).

## [Unreleased]

## [0.1.0a1] - 2026-11-04

### Added

- Size limits for incoming WebSocket messages. `ObjectStreamConsumer` rejects a
  frame larger than `max_message_bytes` before decoding it, with a
  `message_too_large` error. It rejects a `subscription_id`, `pk`, `filter`,
  `ordering`, or `shape` over its limit with a `field_too_large` error. Each
  limit is a consumer class attribute, and `None` disables it. The connection
  stays open after a rejected message.
- A system check that warns when a declared `ObjectStreamTrigger` is not
  installed in the database (`object_streams.W001`) or does not match its
  declaration (`object_streams.W002`). Run it with
  `manage.py check --database <alias>`.
- `ObjectStreamTrigger` can map the captured row to another subject, through
  `subject` for a foreign key path or `subject_content_type` and `subject_pk`
  for a generic relation. The changed row stays the source. Triggers without a
  mapping compile to the same SQL as before, so their migrations stay current.
- `StreamEvent.transaction_id`, which reads `metadata.transaction_id`, and
  `enqueue_outbox_events`, which schedules several events with one transaction
  id lookup. The protocol reference documents how a client coalesces the events
  of one transaction.
- `create_outbox_events`, which writes several events immediately in one
  transaction with one transaction id lookup.

### Changed

- The producer and outbox helpers record the Postgres transaction id in
  `metadata.transaction_id`, as capture triggers already did. The `enqueue_*`
  helpers read it when they are called, so the value names the caller's
  transaction rather than the one that writes the row after commit. Each call
  costs one extra query. A `transaction_id` passed in `metadata` is replaced.
- `create_source_events` writes all events of one call in one transaction, so
  they share one transaction id. Outside an atomic block, `enqueue_source_events`
  and `enqueue_outbox_events` do the same. Before, each event was written in a
  transaction of its own.

### Fixed

- A binary frame, invalid or deeply nested JSON, or a subscribe or unsubscribe
  field of the wrong JSON type now gets an `invalid_request` error and leaves
  the connection open. Before, the consumer raised and the connection ended
  with an unhandled exception. A string `ordering` is now rejected rather than
  split into single characters, and a fractional `cursor` is rejected rather
  than truncated. Integer `subscription_id` values are echoed back as strings.
- A subscribe message holding an unpaired surrogate, such as `"\ud800"` in a
  filter value, now gets an `invalid_request` error. Before, the session raised
  `UnicodeEncodeError` when it measured the value or queried with it.
- An object subscription whose `pk` the model cannot parse, such as `"abc"` for
  an integer primary key, now gets an `invalid_request` error. Before, the
  session raised `ValueError` from the membership query.
- An object subscription whose `pk` is a valid key in another text form, such
  as `"02"` or `" 2"` for primary key 2, now receives replay and live events.
  Before, it was acknowledged but received nothing. The session converts the
  `pk` to the model's canonical form, and `subscribed` echoes that form.
- A generated subscription id no longer replaces an active subscription whose
  client-chosen id has the same value, such as `sub_2`. The generator skips ids
  that are already active.

## [0.1.0a0] - 2026-09-02

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

[unreleased]: https://github.com/arrai-innovations/django-object-streams/compare/0.1.0a1...main
[0.1.0a1]: https://github.com/arrai-innovations/django-object-streams/releases/tag/0.1.0a1
[0.1.0a0]: https://github.com/arrai-innovations/django-object-streams/releases/tag/0.1.0a0
