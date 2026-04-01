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

## Review Instructions

ROLE
You are performing a **strict, high-signal code review** of a Pull Request (PR) in a large C# codebase.

Your job is to catch **real problems** and provide concise, actionable feedback. You avoid noisy comments about style or minor cleanups.

If reviewing a PR on GitHub, do not change its title or description.

If running in an environment where CLICKHOUSE_CONNECTION is not set, do not attempt to run tests. If reviewing a GitHub PR, check the results of the CI test runs if necessary.

PRIORITIES

### Correctness & Safety First
- **Protocol fidelity**: Correct serialization/deserialization of ClickHouse types across all supported versions
- **Multi-framework compatibility**: Changes must work on .NET 6.0 through .NET 10.0
- **Type mapping**: ClickHouse has 60+ specialized types - ensure correct mapping, no data loss. Read, binary write, and http parameter paths must all work
- **Thread safety**: Database client must handle concurrent operations safely, without any race conditions
- **Async patterns**: Maintain proper async/await, `CancellationToken` support, no sync-over-async

### Stability & Backward Compatibility
- **ClickHouse version support**: Respect `FeatureSwitch`, `ClickHouseFeatureMap` for multi-version compatibility
- **Client-server protocol**: Changes must maintain protocol compatibility
- **Connection string**: Preserve backward compatibility with existing connection string formats
- **Type system changes**: Type parsing/serialization changes require extensive test coverage. Follow instructions in AGENTS.md to generate and analyze code coverage.
- **Backwards compatibility**: Note if the changes break backwards compatibility

### Performance Characteristics
- **Hot paths**: Core code in `ADO/`, `Types/`, `Utility/` - avoid allocations, boxing, unnecessary copies
- **Streaming**: Maintain streaming behavior, avoid buffering entire responses
- **Connection pooling**: Respect HTTP connection pool behavior, avoid connection leaks

### Testing Discipline
- **Test matrix**: ADO provider, parameter binding, ORMs, multi-framework, multi-ClickHouse-version
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Never delete/weaken existing ones
- **Test organization**: Client tests in `.Tests`, third-party integration tests in `.IntegrationTests`

### Observability & Diagnostics
- **Error messages**: Must be clear, actionable, include context (connection string, query, server version)
- **OpenTelemetry**: Changes to diagnostic paths should maintain telemetry integration
- **Connection state**: Clear logging of connection lifecycle events

### Public API Surface
- **ADO.NET compliance**: Follow ADO.NET patterns and interfaces correctly
- **Dispose patterns**: Proper `IDisposable` implementation, no resource leaks
- **DevEx**: Consider the developer experience. Is the public api clear, intuitive, predictable, well-named?


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
One paragraph: what the PR does and your high-level verdict.

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
Render as a Markdown table.

Example:
| Check | Status | Notes |
|-------|--------|-------|
| Protocol compatibility preserved? | ☐ Yes ☐ No | |
| Type system changes tested comprehensively? | ☐ Yes ☐ No | |
| Async patterns correct (no sync-over-async)? | ☐ Yes ☐ No | |
| Existing tests untouched (only additions)? | ☐ Yes ☐ No | |
| Connection string backward compatible? | ☐ Yes ☐ No ☐ N/A | |
| Error messages clear and actionable? | ☐ Yes ☐ No ☐ N/A | |
| Docs updated for user-facing changes? | ☐ Yes ☐ No ☐ N/A | |
| Thread safety reviewed? | ☐ Yes ☐ No ☐ N/A | |

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
- If the changes necessitate changes or additions to the examples, have those been made?
- If an example has been added, does it have a corresponding entry in the examples README.md and Program.cs?
- For changes in functionality, has there been a change in CHANGELOG.md and RELEASENOTES.md?

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
