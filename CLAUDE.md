# ClickHouse.Driver Development Guide

## Repository Overview

### Project Context
- **ClickHouse.Driver** is the official ADO.NET client for ClickHouse database
- **Critical priorities**: Stability, correctness, performance, and comprehensive testing
- **Tech stack**: C#/.NET targeting `net462`, `net48`, `netstandard2.1`, `net6.0`, `net8.0`, `net9.0`, `net10.0`
- **Tests run on**: `net6.0`, `net8.0`, `net9.0`, `net10.0`; Integration tests: `net10.0`; Benchmarks: `net10.0`

### Solution Structure
```
ClickHouse.Driver.sln
├── ClickHouse.Driver/                   # Main library (NuGet package)
│   ├── ADO/                            # Core ADO.NET (Connection, Command, DataReader, Parameters)
│   ├── Types/                          # 60+ ClickHouse type implementations + TypeConverter.cs
│   ├── Copy/                           # Bulk copy & binary serialization
│   ├── Http/                           # HTTP layer & connection pooling
│   ├── Utility/                        # Schema, feature detection, extensions
│   └── PublicAPI/                      # Public API surface tracking (analyzer-enforced)
├── ClickHouse.Driver.Tests/            # NUnit tests (multi-framework)
├── ClickHouse.Driver.IntegrationTests/ # Integration tests (net10.0)
└── ClickHouse.Driver.Benchmark/        # BenchmarkDotNet performance tests
```

### Key Files
- **Type system**: `Types/TypeConverter.cs` (14KB, complex), `Types/Grammar/` (type parsing)
- **Core ADO**: `ADO/ClickHouseConnection.cs`, `ADO/ClickHouseCommand.cs`, `ADO/Readers/`
- **Protocol**: Binary serialization in `Copy/Serializer/`, HTTP formatting in `Formats/`
- **Feature detection**: `Utility/ClickHouseFeatureMap.cs` (version-based capabilities)
- **Public API**: `PublicAPI/*.txt` (Roslyn analyzer enforces shipped signatures)
- **Config**: `.editorconfig` (file-scoped namespaces, StyleCop suppressions)

---

## Development Guidelines

### Correctness & Safety First
- **Protocol fidelity**: Correct serialization/deserialization of ClickHouse types across all supported versions
- **Multi-framework compatibility**: Changes must work on .NET Framework 4.6.2 through .NET 10.0
- **Type mapping**: ClickHouse has 60+ specialized types - ensure correct mapping, no data loss
- **Thread safety**: Database client must handle concurrent operations safely
- **Async patterns**: Maintain proper async/await, `CancellationToken` support, no sync-over-async

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
- **Test matrix**: ADO provider, parameter binding, ORMs, multi-framework, multi-ClickHouse-version
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Only add new tests, never delete/weaken existing ones
- **Test organization**: Client tests in `.Tests`, third-party integration tests in `.IntegrationTests`

### Code Style
- **Namespaces**: File-scoped namespaces (warning-level)
- **Analyzers**: Respect `.editorconfig`, StyleCop suppressions, nullable contexts

### Configuration & Settings
- **Configuration**: happens through connection string and ClickHouseClientSettings
- **Feature flags**: Consider adding optional behavior behind connection string settings

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
