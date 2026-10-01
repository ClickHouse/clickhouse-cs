# ClickHouse.Driver Development Guide

This file has the rules for the whole repository. Each client, and the examples, has a second file
with the rules for that area. **Read the file for the area you change before you edit code or tests
in it.**

| You change | Read |
|---|---|
| The HTTP client: `ClickHouse.Driver/`, `ClickHouse.Driver.Tests/`, `ClickHouse.Driver.IntegrationTests/` | [`ClickHouse.Driver/AGENTS.md`](ClickHouse.Driver/AGENTS.md) |
| The native TCP client: `ClickHouse.Driver.Tcp/`, `ClickHouse.Driver.Tcp.Tests/` | [`ClickHouse.Driver.Tcp/AGENTS.md`](ClickHouse.Driver.Tcp/AGENTS.md) |
| The shared code in `ClickHouse.Driver.Common/` | Both client files. Both clients use this code, so run both test suites. |
| The examples: `examples/` | [`examples/AGENTS.md`](examples/AGENTS.md) |
| The benchmarks: `ClickHouse.Driver.Benchmark/` | This file only (see Benchmarks below). |

## Repository Overview

### Project Context
- **ClickHouse.Driver** is the official .NET client for ClickHouse. The repository has two clients:
  - The **HTTP client** (`ClickHouse.Driver`): `ClickHouseClient`, plus the ADO.NET
    `ClickHouseConnection` for ORMs. It sends RowBinary over HTTP. This is the main, stable client.
  - The **native TCP client** (`ClickHouse.Driver.Tcp`): `ClickHouseTcpClient`. It uses the ClickHouse
    native protocol and the columnar Native format. Its public API is experimental (compiler warning
    `CHTCP0001`).
- **One NuGet package**: the `ClickHouse.Driver` package contains `ClickHouse.Driver.Tcp.dll` and
  `ClickHouse.Driver.Common.dll`. Those two projects are not packages of their own.
- **Frameworks**: the HTTP client targets `net6.0`, `net8.0`, `net9.0`, `net10.0`. The TCP client
  targets `net8.0` and later, so the package's `net6.0` build does not contain it.
  `ClickHouse.Driver.IntegrationTests` and `ClickHouse.Driver.Benchmark` target `net10.0` only.
- **Critical priorities**: Stability, correctness, performance, and comprehensive testing
- **Supported ClickHouse versions**: the last 3 releases plus the last 2 LTS releases (a moving
  window, see `docs/http.mdx`). The authoritative list is the `version` matrix in
  `.github/workflows/tests.yml` (HTTP) and `.github/workflows/tests-tcp.yml` (TCP); its lowest
  entry is the current support floor. Behavior that only affects servers older than that floor is
  out of scope; don't add code paths or workarounds for it.

### Solution Structure
```
ClickHouse.Driver.sln
├── ClickHouse.Driver/                   # HTTP client; builds the NuGet package
├── ClickHouse.Driver.Tests/             # HTTP client tests (NUnit, multi-framework)
├── ClickHouse.Driver.IntegrationTests/  # HTTP client with third-party libraries (net10.0)
├── ClickHouse.Driver.Tcp/               # Native TCP client, packed into ClickHouse.Driver
├── ClickHouse.Driver.Tcp.Tests/         # Native TCP client tests (NUnit, net8.0+)
├── ClickHouse.Driver.Common/            # Compression code shared by both clients
└── ClickHouse.Driver.Benchmark/         # BenchmarkDotNet benchmarks for both clients
examples/                                # Runnable examples for both clients
docs/                                    # Public documentation (see Documentation below)
changelog.d/                             # Changelog fragments (see Changelog below)
```

Prefer using LSP to grep when navigating the codebase.

---

## Development Guidelines

These rules apply to both clients. The client files add the rules specific to each one.

### Correctness & Safety First
- **Protocol fidelity**: Correct serialization/deserialization of ClickHouse types across all supported versions
- **Multi-framework compatibility**: Changes must work on every framework the project targets
- **Type mapping**: ClickHouse has 60+ specialized types - ensure correct mapping, no data loss.
  Consider both the read path and the write path of a type.
- **Thread safety**: Database client must handle concurrent operations safely
- **Async patterns**: Maintain proper async/await, `CancellationToken` support, no sync-over-async
- **Culture invariance**: Make sure string and number comparisons are culture-invariant

### Logic that exists in both clients
The two clients do not share a type tree, so some logic is written twice. A fix in one copy usually
needs the same fix in the other:
- parameter formatting: `ClickHouse.Driver/Formats/HttpParameterFormatter.cs` and
  `ClickHouse.Driver.Tcp/Parameters/TcpParameterFormatter.cs`;
- SQL type-hint extraction: `SqlParameterTypeExtractor.cs` in both projects;
- server feature versions: `ClickHouse.Driver.ADO.Feature` and the TCP suite's `TcpFeature`.

### Stability & Backward Compatibility
- **Client-server protocol**: Changes must maintain protocol compatibility
- **Connection string**: Preserve backward compatibility with existing connection string formats
- **Type system changes**: Type parsing/serialization changes require extensive test coverage

### Performance Characteristics
- **Hot paths**: avoid allocations, boxing, unnecessary copies. Each client file names its hot paths.
- **Streaming**: Maintain streaming behavior, avoid buffering entire responses
- **Don't tax a common path for a niche case**: if a fix adds per-row or per-call work to a path
  everyone hits in order to serve an uncommon one, measure the cost and prefer an opt-in API over
  charging everybody for it.
- **Benchmarks**: measure performance-related changes with BenchmarkDotNet
  (`ClickHouse.Driver.Benchmark`, which has benchmarks for both clients) and put the numbers in the
  PR description. An ad-hoc benchmark written only to answer a question doesn't need to ship with
  the PR; commit one that is worth re-running later. A maintainer can also trigger a
  `/benchmark-compare` run on the PR.

### Testing Discipline

> **Table names: never hard-code one.** Test runs can share one server at the same time: a local
> `dotnet test` without `--framework` runs every framework at once against one server, and the HTTP
> and TCP Cloud jobs use one Cloud service. A fixed name lets one run drop, truncate or read another
> run's table. This is the most common way a new test becomes flaky. Each client file names the
> helper its suite uses.

- **Integration tests**: Strongly prefer tests that actually call the db over unit tests. A test that
  hand-builds wire bytes or mocks the server response only proves the code agrees with *your model*
  of the server. It keeps passing when the real server does something else. Assert against a real
  server.
- **Don't restate existing coverage**: each suite has a type matrix that already round-trips every
  type (the client file names it). Check there first, add tests only for what it doesn't reach, and
  say in the PR what that is. The usual offender is a "control" case pinning behavior your change
  never touched (the sibling type, the untouched overload); that is already covered, and you'll be
  asked to drop it.
- **Reading `system.query_log`: always go through the suite's `Utilities/QueryLog.cs`**, never a bare
  `SYSTEM FLUSH LOGS` followed by a single read. The server queues a query's QueryFinish record
  independently of the response reaching the client, so a flush issued right after the query can
  miss it. The lookup then matches fewer rows than expected, and the flake shows as a wrong-looking
  value, not a missing row. The helpers retry the flush and the read.
  Select an expression that is never NULL for an existing row (e.g. `mapContains(Settings, 'x')`,
  not `Settings['x']`, whose empty string for an absent key is indistinguishable from an
  unflushed row), and identify the query under test by `query_id` where you can. A lookup keyed on
  a marker in the query text (`query LIKE '%marker%'`) also matches the helper's own lookups, so it
  needs `AND query NOT LIKE '%system.query_log%'`. Don't paper over the race with `Task.Delay`.
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Only add new tests, never delete/weaken existing ones
- **Deterministic literals**: When asserting stored values, match a literal's precision to the
  column scale (e.g. an 8-digit fractional for `DateTime64(8)`) instead of relying on
  server-side rounding/truncation, which can vary by version/settings.
- **Parametrized tests**: When cases differ only in inputs/expected values, use an NUnit
  `TestCaseSource`/`[TestCase]` parametrized test rather than several near-identical methods.
- **Test naming**: The name of your test should consist of three parts:
  - Name of the method being tested
  - Scenario under which the method is being tested
  - Expected behavior when the scenario is invoked

### Code Style
- **Namespaces**: File-scoped namespaces (warning-level)
- **Analyzers**: Respect `.editorconfig`, StyleCop suppressions, nullable contexts
- **No redundant framework guards**: don't add an `#if` for a framework at or below the project's
  lowest target. `#if NET5_0_OR_GREATER` / `#if NET6_0_OR_GREATER` are always true in every project, and
  `#if NET8_0_OR_GREATER` is always true in `ClickHouse.Driver.Tcp`. Guard only APIs newer than the
  project's floor, with the narrowest symbol that applies.
- **Comments**: short, and only claims you have verified. Don't assert server or protocol behavior
  ("the separator is optional") without confirming it against a real server or the server source.

### Configuration & Settings
- **Feature flags**: Consider adding optional behavior behind connection string settings
- Each client file shows that client's settings, per-query options and parameters.

### Observability & Diagnostics
- **Error messages**: Must be clear, actionable, include context (connection string, query, server version)
- **OpenTelemetry**: Changes to diagnostic paths should maintain telemetry integration
- **Connection state**: Clear logging of connection lifecycle events

### Public API Surface
- **Breaking changes**: Must update the project's `PublicAPI/*.txt` files. Nothing checks the HTTP
  client's files (see its file). An analyzer checks the TCP client's files and
  `ClickHouse.Driver.Common`'s; the TCP file describes how, and the same rules apply to Common.
- **Dispose patterns**: Proper `IDisposable` implementation, no resource leaks

## PR Review Guidelines

Use review skill.

---

## Running Tests

Use `dotnet test <TestProject> --framework net9.0 --property WarningLevel=0`, where `<TestProject>` is
`ClickHouse.Driver.Tests` (HTTP) or `ClickHouse.Driver.Tcp.Tests` (TCP).

With optional `--filter "FullyQualifiedName~"` if you need it. Each client file says how its suite
finds a server.

`WarningLevel=0` also hides analyzer diagnostics, including the ones escalated to errors. To check
analyzer results (for example the TCP public API checks), build without it.

## Code Coverage

After completing a unit of work and adding tests, use a sub-agent to check coverage to catch important gaps. The goal is not blindly hitting 100%, it's making sure important code paths are exercised. ~85% line coverage is a reasonable target, but use judgment.

### Generating coverage

Run tests with coverlet.msbuild (produces cobertura XML). Both test projects support it:

```bash
dotnet test <TestProject> --framework net9.0 --property WarningLevel=0 /p:CollectCoverage=true /p:CoverletOutputFormat=cobertura
```

The cobertura XML file will be written to `<TestProject>/coverage.net9.0.cobertura.xml`.

### Analyzing coverage

**Per-file summary** (sorted worst-first):

```bash
# All files
python3 .claude/scripts/coverage-summary.py <TestProject>/coverage.*.cobertura.xml

# Only files changed in the working tree
python3 .claude/scripts/coverage-summary.py <TestProject>/coverage.*.cobertura.xml --changed

# Only files changed vs a specific ref (branch, commit, tag)
python3 .claude/scripts/coverage-summary.py <TestProject>/coverage.*.cobertura.xml --changed main
```

**Uncovered lines for a specific file**:

```bash
python3 .claude/scripts/coverage-uncovered.py <TestProject>/coverage.*.cobertura.xml TypeConverter.cs
```

Then read the uncovered lines in the source file to understand what's missing. If anything needs to be fixed, fix it.

---

## Review

After completing a unit of work and making sure code coverage is good, launch a sub-agent to perform a thorough review on the changes. The result of the review should be a prioritized list of issues (if any exist). Before fixing them, make sure to double-check that the issues are valid and prompt the user for the next steps.

## Documentation

Docs live in the `docs/` folder and are automatically synced to the public docs website (by
`.github/workflows/docs_sync.yml`, at each release, or at merge for a PR with the `sync-docs`
label). Any change to the public API or its behavior must be reflected in the documentation, in the
same PR:

- `docs/http.mdx`: the HTTP client.
- `docs/tcp.mdx`: the native TCP client.
- `docs/overview.mdx`: the landing page, which says when to use each client.

The pages are Mintlify MDX. Give every new heading an explicit anchor (`## Title {#title}`), as the
existing headings have.

## Changelog and release notes

After completing a unit of work, if it should be included in the changelog (any behavioral change in
the client should be), add a **fragment** under `changelog.d/` — do not edit `CHANGELOG.md` or
`RELEASENOTES.md`:

```bash
dotnet run scripts/changelog.cs -- --new fixes 512-variant-null
```

Then write the entry into the file it creates. Categories: `breaking`, `features`, `improvements`,
`internal`, `deprecations`, `fixes`, `docs`. Full contract in `changelog.d/README.md`.

Two rules the CI gate (`dotnet run scripts/changelog.cs -- --check`) enforces, so getting them wrong
fails the build:

- **Never edit the `Unreleased` section of `CHANGELOG.md`.** Concurrent pull requests editing one
  shared section conflict every time, and GitHub ignores `.gitattributes` merge drivers when merging
  pull requests, so `merge=union` cannot fix it. A fragment is a file only your branch adds, so
  there is nothing to reconcile. Maintainers fold fragments in at release time with `--release`.
- **Never edit `RELEASENOTES.md`.** It is generated from `CHANGELOG.md` (regenerate with
  `--sync-notes`) and ships inside the NuGet package via `PackageReleaseNotes`.

**Keep entries short** — one or two sentences on the user-visible change, plus the issue number. No
root-cause analysis, no benchmark tables, no implementation detail, and don't claim more than the
code actually guarantees. Everything else belongs in the PR description.

## CI workflows

`.github/workflows/zizmor.yml` fails a PR on any high-severity zizmor finding in the workflows. Pass
an input into a `run:` block through `env:`; do not interpolate `${{ inputs.x }}` into the script
(see the `Test` step of `reusable.yml`). To reproduce a finding, audit the tree CI audits, which is
the merge of the branch with `main`:

```bash
tree=$(git merge-tree --write-tree origin/main <branch>)
mkdir -p /tmp/zz && git archive "$tree" .github | tar -x -C /tmp/zz
(cd /tmp/zz && uvx zizmor@1.29.0 --min-severity high .)
```

`tests.yml` (HTTP) runs for every pull request. `tests-tcp.yml` runs only for its own paths, and
`ClickHouse.Driver.Common/` is not one of them: a change there alone does not start the TCP suite in
CI. Run it locally.
