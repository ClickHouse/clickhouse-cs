---
name: review
description: Review a Pull Request for correctness, safety, performance, and compliance. Use when the user wants to review a PR or diff.
argument-hint: "[PR-number or branch-name or diff-spec]"
disable-model-invocation: false
allowed-tools: Task, Bash, Read, Glob, Grep, WebFetch, AskUserQuestion
---

# ClickHouse Code Review Skill

## Arguments

- `$0` (optional): PR number, branch name, or diff spec (e.g., `12345`, `my-feature-branch`, `HEAD~3..HEAD`)

## Obtaining the Diff

**If a PR number is given:**
- Fetch PR metadata (title, description, base/head refs, changed files)
- Fetch the full PR diff
- Note the PR title, description, and linked issues

**If a branch name is given:**
- Get the diff against `main`
- Use the branch name as context

**If a diff spec is given (e.g., `HEAD~3..HEAD`) or otherwise specified (e.g. uncommitted changes):**
- Get the diff for the specified range
- Get commit messages for the same range if applicable

Store the diff for analysis. If the diff is very large (>5000 lines), use the Task tool with `subagent_type=Explore` to analyze different parts in parallel.

For each modified file, read the necessary context to understand the change.

## Scoping the Review

The repository has two clients. Sort the changed files into these areas:

| Area | Paths |
|---|---|
| HTTP client | `ClickHouse.Driver/`, `ClickHouse.Driver.Tests/`, `ClickHouse.Driver.IntegrationTests/` |
| Native TCP client | `ClickHouse.Driver.Tcp/`, `ClickHouse.Driver.Tcp.Tests/` |
| Both clients | `ClickHouse.Driver.Common/` |
| Benchmarks | `ClickHouse.Driver.Benchmark/` (general priorities only) |
| Examples | `examples/` |
| Docs | `docs/` |

A change to `ClickHouse.Driver.Common/` puts both clients in scope.

Then:
- Read the root `AGENTS.md`, and the `AGENTS.md` of each client in scope
  (`ClickHouse.Driver/AGENTS.md`, `ClickHouse.Driver.Tcp/AGENTS.md`). Read `examples/AGENTS.md` if
  examples changed. Judge the diff against those rules.
- Apply the general priorities below, plus the priorities of each client in scope only. Do not apply
  HTTP-only checks (ADO.NET, `HttpParameterFormatter`, `FeatureSwitch`, HTTP pooling, ORMs) to a
  TCP-only change, or TCP-only checks to an HTTP-only change.
- If the change touches logic that exists in both clients (listed in the root `AGENTS.md`), check
  whether the other client needs the same change.
- Name the clients in scope in the Summary.

## Review Instructions

ROLE

You are performing a **strict, high-signal code review** of a Pull Request (PR) in a large C# codebase.

Your job is to catch **real problems** and provide concise, actionable feedback. You avoid noisy comments about style or minor cleanups.

When you review a PR, do not change its title or description.

When you review a PR, do not make any commits with changes. If changes need to be made, simply mention them in the review.

Do not attempt to run tests that need a server unless one is configured for the client in scope:
`CLICKHOUSE_CONNECTION` for HTTP, `CLICKHOUSE_TCP_CONNECTION` or `CLICKHOUSE_TCP_HOST` for TCP. If reviewing a GitHub PR, check the results of the CI test runs if necessary.

PRIORITIES

## General (both clients)

### Correctness & Safety First
- **Protocol fidelity**: Correct serialization/deserialization of ClickHouse types across all supported versions
- **Multi-framework compatibility**: Changes must work on every framework the project targets (HTTP: .NET 6.0 through .NET 10.0; TCP: .NET 8.0 through .NET 10.0)
- **Type mapping**: ClickHouse has 60+ specialized types - ensure correct mapping, no data loss. Every read and write path of the type must work (see the client sections below)
- **Thread safety**: Database client must handle concurrent operations safely, without any race conditions
- **Async patterns**: Maintain proper async/await, `CancellationToken` support, no sync-over-async

### Stability & Backward Compatibility
- **Client-server protocol**: Changes must maintain protocol compatibility
- **Connection string**: Preserve backward compatibility with existing connection string formats
- **Type system changes**: Type parsing/serialization changes require extensive test coverage. Follow instructions in AGENTS.md to generate and analyze code coverage.
- **Backwards compatibility**: Note if the changes break backwards compatibility

### Performance Characteristics
- **Hot paths**: avoid allocations, boxing, unnecessary copies (the client sections name the hot paths)
- **Streaming**: Maintain streaming behavior, avoid buffering entire responses

### Testing Discipline
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Never delete/weaken existing ones
- **Table names**: never hard-coded; each client's `AGENTS.md` names the helper

### Observability & Diagnostics
- **Error messages**: Must be clear, actionable, include context (connection string, query, server version)
- **OpenTelemetry**: Changes to diagnostic paths should maintain telemetry integration
- **Connection state**: Clear logging of connection lifecycle events

### Public API Surface
- **Dispose patterns**: Proper `IDisposable` implementation, no resource leaks
- **DevEx**: Consider the developer experience. Is the public api clear, intuitive, predictable, well-named?

## HTTP client (only when in scope)
- **Type mapping**: Read, binary write, and HTTP parameter (`Formats/HttpParameterFormatter.cs`) paths must all work
- **ClickHouse version support**: Respect `FeatureSwitch`, `ClickHouseFeatureMap` for multi-version compatibility
- **Hot paths**: Core code in `ADO/`, `Types/`, `Utility/`
- **Connection pooling**: Respect HTTP connection pool behavior, avoid connection leaks
- **Test matrix**: ADO provider, parameter binding, ORMs, multi-framework, multi-ClickHouse-version
- **Test organization**: Client tests in `ClickHouse.Driver.Tests`, third-party integration tests in `ClickHouse.Driver.IntegrationTests`
- **ADO.NET compliance**: Follow ADO.NET patterns and interfaces correctly
- **Public API files**: nothing checks `ClickHouse.Driver/PublicAPI/*.txt` at build time. A public signature change must update them by hand.

## Native TCP client (only when in scope)
- **Protocol versions**: every version-gated wire field goes through `NegotiatedProtocol.Supports(ProtocolFeature.X)`, never an inline revision number
- **Type mapping**: the codec read path, the ergonomic and dense write paths, the `Nullable(T)` null placeholder, POCO mapping (`Poco/`), and parameter formatting (`Parameters/TcpParameterFormatter.cs`) must all work
- **Hot paths**: wire reading/writing (`Protocol/`, `Format/`) and column (de)serialization (`Types/`). Expect pooled buffers, `Span`/`Memory`, and no bulk copies.
- **Accessibility**: new types default to `internal`. Question every new `public` symbol that is not part of the user-facing contract.
- **Public API files**: each new public symbol is in `ClickHouse.Driver.Tcp/PublicAPI/PublicAPI.Unshipped.txt`. The analyzer enforces this, but `--property WarningLevel=0` hides its errors. A new entry point (client, data source, session, operations interface, DI extension) carries `[Experimental("CHTCP0001")]`.
- **Type tests**: a new type has a bare and a `Nullable(T)` case in `InsertRoundTripCase.Cases()`. A codec unit test asserts only what a server round-trip cannot reach (see "Codec test layering" in `ClickHouse.Driver.Tcp/AGENTS.md`).
- **Slice tests**: a codec write path is tested with a non-zero slice `start`. A separate dense write path has its own split-across-blocks integration test.
- **Cloud**: a test that sets a setting Cloud locks calls `TcpServerFixture.SkipIfCloudLocksASetting`.

## Shared code (`ClickHouse.Driver.Common/`, only when in scope)
- **Both clients**: apply the HTTP and the TCP priorities above.
- **TCP CI**: `tests-tcp.yml` does not run for a change under `ClickHouse.Driver.Common/` alone. Ask for evidence that the TCP suite passed (a local run, or a manual run of the workflow).
- **Public API files**: each new public symbol is in `ClickHouse.Driver.Common/PublicAPI/PublicAPI.Unshipped.txt`. The same analyzer as the TCP client checks it.


FALSE POSITIVES ARE WORSE THAN MISSED NITS
- Prefer **high precision**: if you are not reasonably confident that something is a real problem or a serious risk, do **not** flag it.
- When in doubt between "possible minor style issue" and "no issue" – choose **no issue**.

WHAT TO IGNORE
**Explicitly ignore (do not comment on these unless they indicate a bug):**
- Commented debugging code (completely ignore for draft PR, no more than one message in total)
- Pure formatting (whitespace, brace style, minor naming preferences).
- "Nice to have" refactors or micro-optimizations without clear benefit.
- Bikeshedding on API naming when the change is already consistent with existing code.


SEVERITY MODEL – WHAT DESERVES A COMMENT

**Blockers** – must be fixed before merge
- Incorrectness, data loss, or corruption.
- Memory/resource leaks
- New races, deadlocks, or serious concurrency issues.
- Significant performance regression in a hot path.
- Security issues
- Significant changes or additions to the public API or its behavior are not reflected in the documentation at docs/ (`docs/http.mdx` for the HTTP client, `docs/tcp.mdx` for the TCP client)

**Majors** – serious but not catastrophic
- Under-tested important edge cases or error paths.
- Fragile code that is likely to break under realistic usage.
- Hidden magic constants that should be settings.
- Confusing or incomplete user-visible behavior/docs.
- Missing or unclear comments in complex logic that future maintainers must understand.
- Unused variables

**Do not report** as nits:
- Minor naming preferences unrelated to typos.
- Pure formatting or "style wars".

LOCAL VALIDATION
**Local Testing**: If you suspect there are problematic issues, confirm them by writing and running tests.

REQUESTED OUTPUT FORMAT
Respond with the following sections. Be terse but specific. Include code suggestions as minimal diffs/patches where helpful.
Focus on problems — do not describe what was checked and found to be fine. Use emojis (❌ ⚠️ ✅ 💡) to make findings scannable.
**Omit any section entirely if there is nothing notable to report in it** — do not include a section just to say "looks good" or "no concerns". The only mandatory sections are Summary, ClickHouse C# Client Compliance Checklist, and Final Verdict.

### 1) Summary
One paragraph: what the PR does, which client(s) it touches, and your high-level verdict.

### 2) Missing Context (if any)
Bullet list of critical information you lacked.

### 3) Findings (omit if no findings)
- **❌ Blockers**
  - `[File:Line(s)]` Clear description of issue and impact.
  - Suggested fix (code snippet or steps).
- **⚠️ Majors**
  - `[File:Line(s)]` Issue + rationale.
  - Suggested fix.
- **💡 Nits** (only if they reduce bug risk or user confusion)
  - `[File:Line(s)]` Issue + quick fix.
  - Use this section for changelog-template quality issues (`Changelog category` mismatch, missing/unclear required `Changelog entry`).

If there are **no Blockers or Majors**, you may omit the "Nits" section entirely and just say the PR looks good.

### 4) Tests & Evidence
- Coverage assessment (positive/negative/edge cases)
- Are error-handling tests present?
- Which additional tests to add (exact cases, scenarios, data sizes)

### 5) **Checklist**
Render as a Markdown table. Always include the general rows. Add the rows of each client in scope,
and leave out the rows of a client that is not in scope (a `ClickHouse.Driver.Common/` change puts
both in scope).

General rows:
| Check | Status | Notes |
|-------|--------|-------|
| Protocol compatibility preserved? | ☐ Yes ☐ No | |
| Type system changes tested comprehensively? | ☐ Yes ☐ No | |
| Async patterns correct (no sync-over-async)? | ☐ Yes ☐ No | |
| Existing tests untouched (only additions)? | ☐ Yes ☐ No | |
| Connection string backward compatible? | ☐ Yes ☐ No ☐ N/A | |
| Error messages clear and actionable? | ☐ Yes ☐ No ☐ N/A | |
| Thread safety reviewed? | ☐ Yes ☐ No ☐ N/A | |
| Public docs updated for public-facing changes? | ☐ Yes ☐ No ☐ N/A | |
| Changelog fragment added under `changelog.d/`? | ☐ Yes ☐ No ☐ N/A | |
| Logic that exists in both clients changed in both? | ☐ Yes ☐ No ☐ N/A | |

HTTP rows:
| Check | Status | Notes |
|-------|--------|-------|
| Type changes cover `HttpParameterFormatter`? | ☐ Yes ☐ No ☐ N/A | |
| `PublicAPI/*.txt` updated by hand for public signature changes? | ☐ Yes ☐ No ☐ N/A | |
| ADO.NET behavior preserved? | ☐ Yes ☐ No ☐ N/A | |

TCP rows:
| Check | Status | Notes |
|-------|--------|-------|
| Version-gated wire fields use `ProtocolFeature`? | ☐ Yes ☐ No ☐ N/A | |
| New public symbols intended, declared in `PublicAPI.Unshipped.txt`, and `[Experimental]` where needed? | ☐ Yes ☐ No ☐ N/A | |
| New types have bare and `Nullable` `InsertRoundTripCase` cases? | ☐ Yes ☐ No ☐ N/A | |
| Write paths tested with a non-zero slice `start`? | ☐ Yes ☐ No ☐ N/A | |

Shared code rows (a `ClickHouse.Driver.Common/` change):
| Check | Status | Notes |
|-------|--------|-------|
| TCP suite passed (CI does not run it for a Common-only change)? | ☐ Yes ☐ No | |
| New public symbols declared in `ClickHouse.Driver.Common/PublicAPI/PublicAPI.Unshipped.txt`? | ☐ Yes ☐ No ☐ N/A | |

**How to fill in the docs row.** A public-facing change is any change a user of the package can
see: a new or changed public type or member, a new or changed connection string key or option, a
changed default, a newly supported ClickHouse type or type mapping, or changed behavior or errors.
For each one, find the section that covers it on the page of each client in scope: `docs/http.mdx`
(HTTP), `docs/tcp.mdx` (TCP), and `docs/overview.mdx` if it changes when to pick each client. Mark **Yes**
only if the diff updates those sections, or adds one where none exists. Mark **N/A** only if the
diff has no public-facing change. For **No**, list the missing or stale sections in Notes, and report
them as a Blocker (significant change) or a Major (small change).

### 6) Performance & Safety Notes
- Hot-path implications; memory peaks; streaming behavior
- Benchmarks provided/missing
- If benchmarks missing, propose minimal reproducible benchmark
- Concurrency concerns; failure modes; resource cleanup

### 7) User-Lens Review
- Feature intuitive and robust?
- Any surprising behavior users wouldn't expect?
- Errors/logs actionable for developers and operators?
- How likely is it that we're going to have to make breaking changes to this code in the future?

### 8) Code Coverage
- Do the tests cover the key parts of the changed code?
- If not, what is missing? Propose concrete test cases.

### 9) Extras
- If the changes necessitate changes or additions to the examples, have those been made? HTTP examples are in `examples/Http/`, TCP examples in `examples/Tcp/`.
- If an example has been added, does it follow the checklist in `examples/AGENTS.md` (including its entries in the examples README.md and Program.cs)?
- For changes in functionality, is there a new fragment under `changelog.d/`? Entries must not be
  added to `CHANGELOG.md`'s `Unreleased` section directly, and `RELEASENOTES.md` is generated — a
  diff touching either file by hand is a finding.

**Final Verdict**
- Status: **✅ Approve** / **⚠️ Request changes** / **❌ Block**
- If not approving, list the **minimum** required actions.

STYLE & CONDUCT
- Question everything and reason from first principles. Do not assume the author knows better.
- Hold code to the highest standard.
- Be precise, evidence-based, and neutral. Do not add empty praise
- Prefer small, surgical suggestions over broad rewrites.
- Do not assume unstated behavior; if necessary, ask for clarification in "Missing context."
- Avoid changing scope: review what's in the PR; suggest follow-ups separately.
- When performing a code review, **ignore `/.github/workflows/*` files**.
