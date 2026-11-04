# Contributing to django-object-streams

The root [README](README.md) explains how to install the workspace and run its
checks and tests.

## Make a change

These steps follow one change from a working checkout to a pull request. Each
step links to the rules that it relies on.

1. Set up the workspace. Follow [Development](README.md#development) in the
   root README to run `just bootstrap` and create the test database. Run
   `just test` to confirm that the suite passes before you change anything.
2. Write a test that fails without the change. Tests live under `tests/` and
   run with `just test <path>`.
3. Make the change until the test passes. Run `just fix`, then `just check`.
   Run `just coverage` to confirm that the suite stays above the 80 percent
   floor that CI enforces.
4. Add a changelog entry when the change needs one. See
   [Changelog](#changelog).
5. Commit, then open the pull request. Follow
   [Commit messages](#commit-messages) and
   [Pull request titles](#pull-request-titles).

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

Mark a breaking change with `!` before the colon, as in
`feat(registry)!: require a visibility policy for every registration`. A change
is breaking when an integrator must change code, configuration, or data to keep
a project working after upgrading.

Examples:

- `feat(outbox): add replayable event cursor`
- `fix(registry): reject duplicate model registrations`
- `docs(subscriptions): explain filter membership semantics`

Avoid titles such as `fix: server changes` or `chore: updates`.

## Dependency audit fixes

Keep a dependency audit fix out of a pull request whose focus is other work.
Land it on its own, in a pull request whose focus is that fix.

An audit fix edits `uv.lock`, which most branches share. A pull request that
carries one conflicts with every other branch that touches the lockfile. Those
conflicts grow while the pull request waits for review and while other work
lands on `main`.

Run the audit from the repository root. It reads `uv.lock`, so it reports the
versions the workspace resolves rather than the lower bounds in
`pyproject.toml`:

```bash
uvx pysentry-rs . --compact --color never
```

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
