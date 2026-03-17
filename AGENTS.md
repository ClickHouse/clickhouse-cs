# ClickHouse.Driver Development Guide

## Repository Overview

### Project Context
- **ClickHouse.Driver** is the official .NET client for ClickHouse database
- **Primary API**: `ClickHouseClient` - thread-safe, singleton-friendly, recommended for most use cases
- **ADO.NET API**: `ClickHouseConnection`/`ClickHouseCommand` - for ORM compatibility (Dapper, EF Core, linq2db)
- **Critical priorities**: Stability, correctness, performance, and comprehensive testing
- **Tech stack**: C#/.NET targeting `net6.0`, `net8.0`, `net9.0`, `net10.0`
- **Tests run on**: `net6.0`, `net8.0`, `net9.0`, `net10.0`; Integration tests: `net10.0`; Benchmarks: `net10.0`

### Solution Structure
```
ClickHouse.Driver.sln
├── ClickHouse.Driver/                   # Main library (NuGet package)
│   ├── Utility/                        # ClickHouseClient (primary API), schema, feature detection
│   ├── ADO/                            # ADO.NET layer (Connection, Command, DataReader, Parameters)
│   ├── Types/                          # 60+ ClickHouse type implementations + TypeConverter.cs
│   ├── Copy/                           # Binary serialization (used internally by ClickHouseClient)
│   ├── Http/                           # HTTP layer & connection pooling
│   └── PublicAPI/                      # Public API surface tracking (analyzer-enforced)
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
- **Public API**: `PublicAPI/*.txt` (Roslyn analyzer enforces shipped signatures)
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

### Testing Discipline
- **Test utilities**: before writing tests, read TestUtilities.cs to understand existing config and utility patterns.
- **Test matrix**: ADO provider, parameter binding, ORMs, multi-framework, multi-ClickHouse-version
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Only add new tests, never delete/weaken existing ones
- **Test organization**: Client tests in `.Tests`, third-party integration tests in `.IntegrationTests`
- **Test naming**: The name of your test should consist of three parts:
  - Name of the method being tested
  - Scenario under which the method is being tested
  - Expected behavior when the scenario is invoked

### Code Style
- **Namespaces**: File-scoped namespaces (warning-level)
- **Analyzers**: Respect `.editorconfig`, StyleCop suppressions, nullable contexts

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

### Observability & Diagnostics
- **Error messages**: Must be clear, actionable, include context (connection string, query, server version)
- **OpenTelemetry**: Changes to diagnostic paths should maintain telemetry integration
- **Connection state**: Clear logging of connection lifecycle events

### Public API Surface
- **Breaking changes**: Must update `PublicAPI/*.txt` files (analyzer enforces)
- **ADO.NET compliance**: Follow ADO.NET patterns and interfaces correctly
- **Dispose patterns**: Proper `IDisposable` implementation, no resource leaks

## PR Review Guidelines

Use the guidelines in .github/copilot-instructions.md

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

## Changelog and release notes

After completing a unit of work, if it should be included in the changelog (any behavioral change in the client should be), then update CHANGELOG.md and RELEASENOTES.md.

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
