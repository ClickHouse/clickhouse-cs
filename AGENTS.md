# ClickHouse.Driver Development Guide

## Repository Overview

### Project Context
- **ClickHouse.Driver** is the official .NET client for ClickHouse database
- **Primary API**: `ClickHouseClient` - thread-safe, singleton-friendly, recommended for most use cases
- **ADO.NET API**: `ClickHouseConnection`/`ClickHouseCommand` - for ORM compatibility (Dapper, EF Core, linq2db)
- **Critical priorities**: Stability, correctness, performance, and comprehensive testing
- **Tech stack**: C#/.NET targeting `net6.0`, `net8.0`, `net9.0`, `net10.0`
- **Tests run on**: `net6.0`, `net8.0`, `net9.0`, `net10.0`; Integration tests: `net10.0`; Benchmarks: `net10.0`
- **Supported ClickHouse versions**: `25.8` LTS and newer — the floor of the CI matrix in
  `.github/workflows/tests.yml`. Behavior that only affects older servers is out of scope; don't add
  code paths or workarounds for it.

### Solution Structure
```
ClickHouse.Driver.sln
├── ClickHouse.Driver/                   # Main library (NuGet package)
│   ├── Utility/                        # ClickHouseClient (primary API), schema, feature detection
│   ├── ADO/                            # ADO.NET layer (Connection, Command, DataReader, Parameters)
│   ├── Types/                          # 60+ ClickHouse type implementations + TypeConverter.cs
│   ├── Copy/                           # Binary serialization (used internally by ClickHouseClient)
│   ├── Http/                           # HTTP layer & connection pooling
│   └── PublicAPI/                      # Public API surface tracking (hand-maintained)
├── ClickHouse.Driver.Tests/            # NUnit tests (multi-framework)
├── ClickHouse.Driver.IntegrationTests/ # Integration tests (net10.0)
└── ClickHouse.Driver.Benchmark/        # BenchmarkDotNet performance tests
```

Prefer using LSP to grep when navigating the codebase.

### Key Files
- **Primary API**: `ClickHouseClient.cs` - main entry point for most applications
- **Type system**: `Types/TypeConverter.cs` (14KB, complex), `Types/Grammar/` (type parsing)
- **ADO.NET layer**: `ADO/ClickHouseConnection.cs`, `ADO/ClickHouseCommand.cs`, `ADO/Readers/`
- **Feature detection**: `Utility/ClickHouseFeatureMap.cs` (version-based capabilities)
- **Public API**: `PublicAPI/*.txt` (hand-maintained record of shipped signatures; the
  `Microsoft.CodeAnalysis.PublicApiAnalyzers` package is *not* referenced, so nothing checks these
  files at build time — keep them in sync yourself)
- **Config**: `.editorconfig` (file-scoped namespaces, StyleCop suppressions)

### API Architecture

**ClickHouseClient** (recommended):
```csharp
using var client = new ClickHouseClient("Host=localhost");
await client.ExecuteNonQueryAsync("CREATE TABLE ...");
await client.InsertBinaryAsync(tableName, columns, rows);  // High-performance bulk insert
using var reader = await client.ExecuteReaderAsync("SELECT ...");
var scalar = await client.ExecuteScalarAsync("SELECT count() ...");
```

**ClickHouseConnection** (for ORMs):
```csharp
// Use ClickHouseDataSource for proper connection lifetime management with ORMs
var dataSource = new ClickHouseDataSource("Host=localhost");
services.AddSingleton(dataSource);

// Dapper, EF Core, linq2db work with DbConnection
using var connection = dataSource.CreateConnection();
var users = connection.Query<User>("SELECT * FROM users");
```

**Key differences**:
- `ClickHouseClient`: Thread-safe, can be singleton, has `InsertBinaryAsync` for bulk inserts
- `ClickHouseConnection`: ADO.NET `DbConnection`, required for ORM compatibility
- `ClickHouseBulkCopy`: **Deprecated** - use `ClickHouseClient.InsertBinaryAsync` instead

---

## Development Guidelines

### Correctness & Safety First
- **Protocol fidelity**: Correct serialization/deserialization of ClickHouse types across all supported versions
- **Multi-framework compatibility**: Changes must work on .NET 6.0 through .NET 10.0
- **Type mapping**: ClickHouse has 60+ specialized types - ensure correct mapping, no data loss
- **Thread safety**: Database client must handle concurrent operations safely
- **Async patterns**: Maintain proper async/await, `CancellationToken` support, no sync-over-async
- **Read and write**: When making changes to types, consider both the binary read and write paths in the type class itself, as well as the HTTP parameter write path in HttpParameterFormatter.cs
- **Culture invariance**: Make sure string and number comparisons are culture-invariant

### Stability & Backward Compatibility
- **ClickHouse version support**: Respect `FeatureSwitch`, `ClickHouseFeatureMap` for multi-version compatibility
- **Client-server protocol**: Changes must maintain protocol compatibility
- **Connection string**: Preserve backward compatibility with existing connection string formats
- **Type system changes**: Type parsing/serialization changes require extensive test coverage

### Performance Characteristics
- **Hot paths**: Core code in `ADO/`, `Types/`, `Utility/` - avoid allocations, boxing, unnecessary copies
- **Streaming**: Maintain streaming behavior, avoid buffering entire responses
- **Connection pooling**: Respect HTTP connection pool behavior, avoid connection leaks
- **Don't tax a common path for a niche case**: if a fix adds per-row or per-call work to a path
  everyone hits in order to serve an uncommon one, measure the cost and prefer an opt-in API over
  charging everybody for it.
- **Benchmarks**: measure performance-related changes with BenchmarkDotNet
  (`ClickHouse.Driver.Benchmark`) and put the numbers in the PR description. An ad-hoc benchmark
  written only to answer a question doesn't need to ship with the PR; commit one that is worth
  re-running later. A maintainer can also trigger a `/benchmark-compare` run on the PR.

### Testing Discipline

> **Table names: never hard-code one.** Any test that touches a table must get its name from
> `CreateTableName(...)`. This is not a style preference — the `net6/8/9/10` suites run
> *simultaneously against one shared server*, so a fixed name lets one suite drop, truncate or read
> another suite's table. This is the single most common way a new test becomes flaky here.
>
> ```csharp
> // In a fixture deriving from AbstractConnectionTestFixture — preferred, cleans up for you:
> var table = CreateTableName();                  // test.MyTestMethod_net9_a1b2c3d4e5f6
>
> // Anywhere else — you own the cleanup:
> var table = TestUtilities.CreateTableName();    // + DROP TABLE IF EXISTS in your teardown
> ```

- **Integration tests**: Strongly prefer tests that actually call the db over unit tests. A test that
  hand-builds wire bytes or mocks the HTTP response only proves the code agrees with *your model* of
  the server — it keeps passing when the real server does something else. Assert against a real server.
- **Don't restate existing coverage**: `Utilities/TestCases.cs` (`GetDataTypeSamples()`) already
  round-trips every type — plus its `Nullable`/`Array`/composite forms — through the select,
  parameter, bulk-copy and serialisation suites. Check there first, add tests only for what those
  don't reach, and say in the PR what that is. The usual offender is a "control" case pinning
  behavior your change never touched (the sibling type, the untouched overload); that is already
  covered, and you'll be asked to drop it.
- **Test utilities**: before writing tests, read TestUtilities.cs to understand existing config and
  utility patterns — including `CreateTableName`/`SanitizeTableName` (see the note above).
- **Reading `system.query_log`: always go through `Utilities/QueryLog.cs`** (`QueryLog.ScalarAsync` /
  `QueryLog.CountAsync`), never a bare `SYSTEM FLUSH LOGS` followed by a single read. A query's
  QueryFinish record is queued independently of its HTTP response reaching the client, so a flush
  issued right after the query can miss it and the lookup then matches fewer rows than expected —
  a flake, and one that surfaces as a wrong-looking value rather than a missing row. The helpers
  retry the flush-and-read (3 attempts, 50 ms apart); `ScalarAsync` fails with a distinct
  "no row appeared" message, and `CountAsync` waits for `minimumCount` rows before reporting.
  Select an expression that is never NULL for an existing row (e.g. `mapContains(Settings, 'x')`,
  not `Settings['x']`, whose empty string for an absent key is indistinguishable from an
  unflushed row), and identify the query under test by `query_id` where you can — a lookup keyed on
  a marker in the query text (`query LIKE '%marker%'`) also matches the helper's own lookups, so it
  needs `AND query NOT LIKE '%system.query_log%'`. Don't paper over the race with `Task.Delay`.
- **Test matrix**: ADO provider, parameter binding, ORMs, multi-framework, multi-ClickHouse-version
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Only add new tests, never delete/weaken existing ones
- **Test organization**: Client tests in `.Tests`, third-party integration tests in `.IntegrationTests`
- **Table naming details**:
  - The inherited `AbstractConnectionTestFixture.CreateTableName()` registers the name so
    `[OneTimeTearDown]` drops it. Prefer it over the static helper whenever the fixture allows.
  - Pass a prefix only to carry a parametrized case, e.g. `CreateTableName($"bulk_{clickHouseType}")`.
    Prefixes are sanitized, so interpolating type names, timezone ids etc. is safe — no manual
    scrubbing needed.
  - Names are already `test.`-qualified. Never prepend `test.` yourself. Pass
    `database: "other_db"` to target another database, or `database: null` for an unqualified name
    (required for `CREATE TEMPORARY TABLE`, which cannot be qualified).
  - Because every name is unique, a plain `CREATE TABLE` cannot collide — don't add
    `IF NOT EXISTS`, don't `DROP` before the `CREATE`, and don't wrap a test in `try/finally` just to
    drop its table. `IF NOT EXISTS` on a unique name only hides a future isolation regression.
    Keep it (with a `DROP`) solely in the `[SetUp]` of a fixture that deliberately shares one table.
  - Gotcha: the server stores the *unqualified* name, so a test comparing against `system.tables` /
    `system.columns` / `DESCRIBE` output, or against a table name in an exception message, must
    split it: `var bare = name[(name.IndexOf('.') + 1)..];`.
  - Gotcha: passing a qualified name to an API whose database resolution you are testing (e.g.
    `InsertOptions.Database`, `QueryOptions.Database`) makes the override a no-op and silently voids
    the assertion. Pass the bare name there, keeping the qualified one for read-back.
  - A fixture may deliberately *share* one table across its tests and reset it in `[SetUp]`; that is
    fine — hoist one `CreateTableName(...)` into the fixture rather than splitting it per test.
  - `Utilities/TestTableNamingTests.cs` pins this contract; keep it passing.
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
- **No redundant framework guards**: the library floors at `net6.0`, so `#if NET5_0_OR_GREATER` /
  `#if NET6_0_OR_GREATER` are always true — don't add them. Guard only APIs newer than .NET 6, with
  the narrowest symbol that applies.
- **Comments**: short, and only claims you have verified. Don't assert server or protocol behavior
  ("the separator is optional") without confirming it against a real server or the server source.

### Configuration & Settings
- **Client configuration**: Connection string or `ClickHouseClientSettings` for client-level settings
- **Per-query options**: `QueryOptions` for query-specific settings (QueryId, CustomSettings, Roles, BearerToken)
- **Parameters**: Use `ClickHouseParameterCollection` with `ClickHouseDbParameter` for parameterized queries
- **Feature flags**: Consider adding optional behavior behind connection string settings

```csharp
// Client-level settings
var settings = new ClickHouseClientSettings("Host=localhost");
settings.CustomSettings.Add("max_threads", 4);
using var client = new ClickHouseClient(settings);

// Per-query options
var options = new QueryOptions
{
    QueryId = "my-query-id",
    CustomSettings = new Dictionary<string, object> { ["max_execution_time"] = 30 },
};
await client.ExecuteReaderAsync("SELECT ...", options: options);

// Parameters
var parameters = new ClickHouseParameterCollection();
parameters.AddParameter("id", 42UL);
await client.ExecuteReaderAsync("SELECT * FROM t WHERE id = {id:UInt64}", parameters);
```

### Query Parameters

Two parameter syntaxes are supported:

- **ClickHouse-native `{name:Type}`** — sent verbatim to the server. Preferred when writing queries by hand.
- **ADO.NET-style `@name`** — purely client-side. Rewritten to `{name:ResolvedType}` before the request is sent (ClickHouse never sees `@`). Required for ORMs like Dapper that emit `@`-style placeholders.

Both refer to parameters by name in `ClickHouseParameterCollection`. A `{name:Type}` hint and an `@name` placeholder for the same parameter are compatible — the hint informs type resolution.

**Type resolution precedence** (first match wins, in `ADO/Parameters/ParameterTypeResolution.cs`):

1. Explicit `ClickHouseDbParameter.ClickHouseType` on the parameter object
2. SQL type hint from `{name:Type}` in the query
3. Custom `IParameterTypeResolver` (per-query `QueryOptions.ParameterTypeResolver`, then client-level `ClickHouseClientSettings.ParameterTypeResolver`)
4. `decimal` special case — `Decimal128(scale)` where scale is read from the value's bits
5. `TypeConverter.ToClickHouseType(value)` — inferred from the .NET runtime value (not just the static type, so e.g. `IPAddress` is disambiguated into `IPv4`/`IPv6` by `AddressFamily`)

If the value is null/`DBNull` and no explicit type or hint is provided, resolution falls through to `Nullable(Nothing)`. Whether the server accepts that null sentinel depends on the expected column/type context; non-nullable targets may reject it. For nullable parameters, set `ClickHouseType` explicitly or include a `{name:Nullable(T)}` hint.

`DbType` on `ClickHouseDbParameter` is **not** part of the precedence chain — only `ClickHouseType` is. Setting `DbType` alone does not influence the resolved ClickHouse type.

**Data flow** (in `ClickHouseClient.PostSqlQueryAsync`):

1. `ClickHouseParameterCollection.ResolveTypeNames(sql, resolver)` — extracts `{name:Type}` hints via `SqlParameterTypeExtractor` (string/comment-aware), then runs the precedence chain once per parameter. Conflicting hints for the same name throw.
2. `ClickHouseParameterCollection.ReplacePlaceholders(sql, resolved)` — rewrites every `@name` to `{name:ResolvedType}`. Bypassable via the `ClickHouse.Driver.DisableReplacingParameters` AppContext switch.
3. `HttpParameterFormatter.Format(parameter, resolvedType, settings, customFormatter)` — culture-invariant value formatting for all 60+ types. Top-level `null`/`DBNull` parameter values become `\N` and skip the custom formatter; nullable values inside composite contexts may instead be emitted as the literal `null` by the type-specific formatter. `IParameterFormatter` (per-query or client-level) can override formatting; transparent wrappers (`Nullable`, `LowCardinality`, `Variant`) are unwrapped before the formatter is called.
4. Values are sent as `param_<name>` either in the URI query string (default) or as multipart form fields when `ClickHouseClientSettings.UseFormDataParameters` is true.

**When changing parameter behavior**, update both the read and write paths: the type's binary serialization in `Types/` and the HTTP write path in `Formats/HttpParameterFormatter.cs`.

### Observability & Diagnostics
- **Error messages**: Must be clear, actionable, include context (connection string, query, server version)
- **OpenTelemetry**: Changes to diagnostic paths should maintain telemetry integration
- **Connection state**: Clear logging of connection lifecycle events

### Public API Surface
- **Breaking changes**: Must update `PublicAPI/*.txt` files (by hand — no analyzer enforces this)
- **ADO.NET compliance**: Follow ADO.NET patterns and interfaces correctly
- **Dispose patterns**: Proper `IDisposable` implementation, no resource leaks

## PR Review Guidelines

Use review skill.

---

## Running Tests

Use `dotnet test --framework net9.0 --property WarningLevel=0`

With optional `--filter "FullyQualifiedName~"` if you need it.

## Code Coverage

After completing a unit of work and adding tests, use a sub-agent to check coverage to catch important gaps. The goal is not blindly hitting 100%, it's making sure important code paths are exercised. ~85% line coverage is a reasonable target, but use judgment.

### Generating coverage

Run tests with coverlet.msbuild (produces cobertura XML):

```bash
dotnet test ClickHouse.Driver.Tests --framework net9.0 --property WarningLevel=0 /p:CollectCoverage=true /p:CoverletOutputFormat=cobertura
```

The cobertura XML file will be written to `ClickHouse.Driver.Tests/coverage.net9.0.cobertura.xml`.

### Analyzing coverage

**Per-file summary** (sorted worst-first):

```bash
# All files
python3 .claude/scripts/coverage-summary.py ClickHouse.Driver.Tests/coverage.*.cobertura.xml

# Only files changed in the working tree
python3 .claude/scripts/coverage-summary.py ClickHouse.Driver.Tests/coverage.*.cobertura.xml --changed

# Only files changed vs a specific ref (branch, commit, tag)
python3 .claude/scripts/coverage-summary.py ClickHouse.Driver.Tests/coverage.*.cobertura.xml --changed main
```

**Uncovered lines for a specific file**:

```bash
python3 .claude/scripts/coverage-uncovered.py ClickHouse.Driver.Tests/coverage.*.cobertura.xml TypeConverter.cs
```

Then read the uncovered lines in the source file to understand what's missing. If anything needs to be fixed, fix it.

---

## Review

After completing a unit of work and making sure code coverage is good, launch a sub-agent to perform a thorough review on the changes. The result of the review should be a prioritized list of issues (if any exist). Before fixing them, make sure to double-check that the issues are valid and prompt the user for the next steps.

## Documentation

Docs live in the docs/ folder and are automatically synced to the public docs website. Any change to the public API or its behavior must be reflected in the documentation.

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

---

## Running Examples

```bash
cd examples

# Run all examples
dotnet run

# List available examples
dotnet run -- --list

# Run specific example(s) using fuzzy filter
dotnet run -- --filter basicusage
dotnet run -- --filter core001
dotnet run -- bulk
```
