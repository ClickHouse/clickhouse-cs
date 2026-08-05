# Changelog fragments

Pending changelog entries live here, one file per change. At release time they are folded into
`CHANGELOG.md` and the files are deleted.

**Do not edit the `Unreleased` section of `CHANGELOG.md` directly, and do not edit
`RELEASENOTES.md` at all.** CI rejects both. `RELEASENOTES.md` is generated from `CHANGELOG.md`.

## Why

Two pull requests that both append to a shared `Unreleased` section conflict every time, and
GitHub does not honor `.gitattributes` merge drivers server-side, so `merge=union` cannot fix it
([github/community#9288](https://github.com/orgs/community/discussions/9288)). Union merge is not
the right answer locally either — it resolves by interleaving both sides' lines, which drops
entries under the wrong heading.

A fragment is a file only your branch adds, so two pull requests touch two distinct paths and git
has nothing to reconcile.

## Adding an entry

```bash
dotnet run scripts/changelog.cs -- --new fixes 512-variant-null
```

Then write the entry into the created file. The body is markdown, copied verbatim into the
changelog, so it must start with a top-level bullet:

```markdown
* Fixed writing NULL values to `Variant` columns. Writing null/DBNull now correctly emits the
  `None` discriminator (`0xFF`) for binary writes.
  - Nested sub-bullets and multi-line entries are fine.
```

Write in the past tense and describe the user-visible effect, not the implementation. Include an
issue number where there is one.

## Naming

```
changelog.d/<pr-or-issue-number>-<slug>.<category>.md
```

Prefix with the pull request or issue number: it is the identifier least likely to collide with
another branch's, and it sets the order entries appear in within their section. Use your branch
name if you do not have a number yet — anything unique to your branch works, since two branches
adding the same filename is the one way to reintroduce the conflict this scheme avoids.

| Category        | Renders as                            |
| --------------- | ------------------------------------- |
| `breaking`      | **Breaking Changes:**                 |
| `features`      | **New Features:**                     |
| `improvements`  | **Improvements:**                     |
| `internal`      | **Internal Improvements:**            |
| `deprecations`  | **Deprecations:**                     |
| `fixes`         | **Bug Fixes:**                        |
| `docs`          | **Documentation and Usage Examples:** |

Sections are emitted in that order, and empty ones are skipped.

## Commands

```bash
dotnet run scripts/changelog.cs -- --render       # preview the pending Unreleased section
dotnet run scripts/changelog.cs -- --check        # what the pull request gate runs
dotnet run scripts/changelog.cs -- --release v1.4.0
dotnet run scripts/changelog.cs -- --verify-release 1.4.0   # what the release gate runs
dotnet run scripts/changelog.cs -- --sync-notes   # regenerate RELEASENOTES.md
```

`--release` is a maintainer step: it rewrites `CHANGELOG.md`, regenerates `RELEASENOTES.md`, and
deletes the fragments. Run it on a release-prep branch and open it as a reviewable pull request
rather than pushing to `main`.
