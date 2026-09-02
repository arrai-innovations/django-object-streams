# Contributing to django-object-streams

The root [README](README.md) explains how to install the workspace and run its
checks and tests.

## Issue and pull request titles

A title should identify the affected behavior and the intended outcome. It
should remain useful in search results, release notes, and cross-references
after the original discussion has been forgotten.

Prefer an active verb and a concrete object. Avoid vague titles such as `Fix
bug`, `Updates`, or `Cleanup`. Do not end a title with a period. Include an
implementation detail only when that implementation is the public contract or
the point of the work.

### Issue titles

Phrase an issue title as the outcome that closing the issue will deliver. Do
not add a Conventional Commit prefix such as `fix:` or `feat:`. Issue types and
labels classify proposed work.

Use `Investigate` only when evidence gathering or a decision is itself the
deliverable.

Examples:

- `Reject invalid subscription filters before registration`
- `Preserve cursor ordering across source adapters`
- `Investigate queryset window resync behavior after deletes`

### Pull request titles

Write a pull request title as the commit subject that should represent the
merged change:

```text
<type>(<optional-scope>): <outcome>
```

The allowed types are:

```text
build, ci, chore, content, docs, feat, fix, perf, refactor, remove, revert, style, test, wip
```

The scope should identify the affected filename without its extension, module,
package, or concern. A scope is optional.

The text after the prefix must describe the concrete outcome, not merely
classify the work. The title should account for the whole branch.

Examples:

- `feat(outbox): add replayable event cursor`
- `fix(registry): reject duplicate model registrations`
- `docs(subscriptions): explain filter membership semantics`

Avoid titles such as `fix: server changes` or `chore: updates`.

## Commit messages

Commit messages use the same Conventional Commit format, allowed types, and
scope guidance as pull request titles. Commitlint can be used locally to
validate commit messages.

## Changelog

[CHANGELOG.md](CHANGELOG.md) follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Add the entry in the same pull request as the change, under `## [Unreleased]`.

Write entries for the reader upgrading the package, not for the reviewer reading
the diff. Describe the behavior that changed and, for anything breaking, what to
do about it. An entry does not need to match the commit subject.

Only user-visible changes earn an entry. Map the commit type to a section:

| Type | Section |
|---|---|
| `feat` | `### Added`, or `### Changed` when it alters existing behavior |
| `fix` | `### Fixed`, or `### Security` for a vulnerability |
| `perf` | `### Changed` |
| `remove` | `### Removed`, after a release that listed it under `### Deprecated` |
| `revert` | The section the reverted change used |
| `build`, `ci`, `chore`, `docs`, `refactor`, `style`, `test`, `wip` | None, unless the change is visible to someone using the package |

Include only the sections that have entries. Do not add empty headings.

Use `### Deprecated` to announce something that still works but will be removed,
and keep it listed until the release that removes it.

### Releasing

At release, rename `## [Unreleased]` to the version and date, such as
`## [0.1.0a0] - 2026-09-01`, then open a fresh empty `## [Unreleased]` above it.
Update the link definitions at the bottom of the file so the new version points
at its tag and `unreleased` compares that tag against `main`.
