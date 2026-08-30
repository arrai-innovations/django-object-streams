# django-object-streams Agent Guide

## Commands

- Bootstrap: `just bootstrap`
- Test: `just test`
- Coverage: `just coverage`
- Lint: `just check-ruff`
- Format check: `just check-format`
- Fix: `just fix`
- Django helper: `just manage <command>`

After bootstrap, prefer `uv run --no-sync` for commands.

## Architecture

- Core package: `object_streams/`
- Tests and sample Django app: `tests/`
- Runtime model: `object_streams.models.ObjectStreamEvent`, a replayable outbox cursor.
- PostgreSQL is the only database target.
- Public primitives: registrations, visibility policies, filterset application, subscription requests, and subscription-relative events.
- Transport adapters belong under `object_streams/transports/`.
- History adapters belong under `object_streams/history/`.
- Source adapters belong under `object_streams/sources/`.

The core package should not import VUEDA. VUEDA support should come through an adapter outside this package.

## Code Style

- Python 3.11 or newer.
- Django 5.2, 6.0, and 6.1 are the supported framework targets.
- Tests require local PostgreSQL and a role that can create test databases.
- Keep imports single-line, with `object_streams` and `tests` as first-party imports.
- Keep comments short and only use them when they clarify non-obvious behavior.
- Use `django-filter` FilterSet classes for subscription filters.
- Keep WebSocket transport details out of the registration and event domain model.

## Commit Message Style

We use a custom commitlint configuration based on [Conventional Commits](https://www.conventionalcommits.org/).
Issue titles, pull request titles, and commit messages follow the project rules
in `CONTRIBUTING.md`.

Valid types:

```text
build, ci, chore, content, docs, feat, fix, perf, refactor, remove, revert, style, test, wip
```

Example:

```text
fix(registry): reject duplicate model registrations
```

Scope should reference the affected filename without its extension, module, or concern.
