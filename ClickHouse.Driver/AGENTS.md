# HTTP client (ClickHouse.Driver)

These rules add to the root [`AGENTS.md`](../AGENTS.md). They apply to `ClickHouse.Driver/`,
`ClickHouse.Driver.Tests/` and `ClickHouse.Driver.IntegrationTests/`.

This project also builds the `ClickHouse.Driver` NuGet package, which contains the native TCP client
and `ClickHouse.Driver.Common`. The TCP client has its own rules in
[`ClickHouse.Driver.Tcp/AGENTS.md`](../ClickHouse.Driver.Tcp/AGENTS.md).

## Project Context
- **Primary API**: `ClickHouseClient` - thread-safe, singleton-friendly, recommended for most use cases
- **ADO.NET API**: `ClickHouseConnection`/`ClickHouseCommand` - for ORM compatibility (Dapper, EF Core, linq2db)
- **Targets**: `net6.0`, `net8.0`, `net9.0`, `net10.0`. Tests run on all four;
  `ClickHouse.Driver.IntegrationTests` runs on `net10.0`.

### Structure
```
ClickHouse.Driver/
├── ClickHouseClient.cs, QueryOptions.cs, InsertOptions.cs   # Primary API
├── Utility/     # Schema, feature detection, extension methods
├── ADO/         # ADO.NET layer (Connection, Command, DataReader, Parameters)
├── Types/       # 60+ ClickHouse type implementations + TypeConverter.cs
├── Copy/        # Binary serialization (used internally by ClickHouseClient)
├── Formats/     # RowBinary readers/writers, HttpParameterFormatter
├── Http/        # HTTP layer & connection pooling
└── PublicAPI/   # Public API surface tracking (hand-maintained)
```

### Key Files
- **Primary API**: `ClickHouseClient.cs` - main entry point for most applications
- **Type system**: `Types/TypeConverter.cs` (complex), `Types/Grammar/` (type parsing)
- **ADO.NET layer**: `ADO/ClickHouseConnection.cs`, `ADO/ClickHouseCommand.cs`, `ADO/Readers/`
- **Feature detection**: `Utility/ClickHouseFeatureMap.cs` (version-based capabilities)
- **Parameter formatting**: `Formats/HttpParameterFormatter.cs`
- **Public API**: `PublicAPI/*.txt` (hand-maintained record of shipped signatures; the
  `Microsoft.CodeAnalysis.PublicApiAnalyzers` package is *not* referenced by this project, so nothing
  checks these files at build time. Keep them in sync yourself.)
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

- **Read and write**: When making changes to types, consider both the binary read and write paths in
  the type class itself, as well as the HTTP parameter write path in `HttpParameterFormatter.cs`.
  The TCP client has a copy of that formatter (see "Logic that exists in both clients" in the root
  file).
- **ClickHouse version support**: Respect `FeatureSwitch`, `ClickHouseFeatureMap` for multi-version compatibility
- **Hot paths**: Core code in `ADO/`, `Types/`, `Utility/` - avoid allocations, boxing, unnecessary copies
- **Connection pooling**: Respect HTTP connection pool behavior, avoid connection leaks
- **ADO.NET compliance**: Follow ADO.NET patterns and interfaces correctly

### Configuration & Settings
- **Client configuration**: Connection string or `ClickHouseClientSettings` for client-level settings
- **Per-query options**: `QueryOptions` for query-specific settings (QueryId, CustomSettings, Roles, BearerToken)
- **Parameters**: Use `ClickHouseParameterCollection` with `ClickHouseDbParameter` for parameterized queries

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

- **ClickHouse-native `{name:Type}`**: sent verbatim to the server. Preferred when writing queries by hand.
- **ADO.NET-style `@name`**: purely client-side. Rewritten to `{name:ResolvedType}` before the request
  is sent (ClickHouse never sees `@`). Required for ORMs like Dapper that emit `@`-style placeholders.

Both refer to parameters by name in `ClickHouseParameterCollection`. A `{name:Type}` hint and an
`@name` placeholder for the same parameter are compatible; the hint informs type resolution.

**Type resolution precedence** (first match wins, in `ADO/Parameters/ParameterTypeResolution.cs`):

1. Explicit `ClickHouseDbParameter.ClickHouseType` on the parameter object
2. SQL type hint from `{name:Type}` in the query
3. Custom `IParameterTypeResolver` (per-query `QueryOptions.ParameterTypeResolver`, then client-level `ClickHouseClientSettings.ParameterTypeResolver`)
4. `decimal` special case: `Decimal128(scale)` where scale is read from the value's bits.
   `ClickHouseDecimal` special case: `Decimal128(9)` if it holds the value exactly, else the value's
   scale without trailing zeros (raised to 9 where there is room) in `Decimal128` or, above 38 digits,
   `Decimal256`
5. `TypeConverter.ToClickHouseType(value)`: inferred from the .NET runtime value (not just the static
   type, so e.g. `IPAddress` is disambiguated into `IPv4`/`IPv6` by `AddressFamily`)

If the value is null/`DBNull` and no explicit type or hint is provided, resolution falls through to
`Nullable(Nothing)`. Whether the server accepts that null sentinel depends on the expected
column/type context; non-nullable targets may reject it. For nullable parameters, set
`ClickHouseType` explicitly or include a `{name:Nullable(T)}` hint.

`DbType` on `ClickHouseDbParameter` is **not** part of the precedence chain; only `ClickHouseType` is.
Setting `DbType` alone does not influence the resolved ClickHouse type.

**Data flow** (in `ClickHouseClient.PostSqlQueryAsync`):

1. `ClickHouseParameterCollection.ResolveTypeNames(sql, resolver)` extracts `{name:Type}` hints via
   `SqlParameterTypeExtractor` (string/comment-aware), then runs the precedence chain once per
   parameter. Conflicting hints for the same name throw.
2. `ClickHouseParameterCollection.ReplacePlaceholders(sql, resolved)` rewrites every `@name` to
   `{name:ResolvedType}`. Bypassable via the `ClickHouse.Driver.DisableReplacingParameters`
   AppContext switch.
3. `HttpParameterFormatter.Format(parameter, resolvedType, settings, customFormatter)` does
   culture-invariant value formatting for all 60+ types. Top-level `null`/`DBNull` parameter values
   become `\N` and skip the custom formatter; nullable values inside composite contexts may instead
   be emitted as the literal `null` by the type-specific formatter. `IParameterFormatter`
   (per-query or client-level) can override formatting; transparent wrappers (`Nullable`,
   `LowCardinality`, `Variant`) are unwrapped before the formatter is called.
4. Values are sent as `param_<name>` either in the URI query string (default) or as multipart form
   fields when `ClickHouseClientSettings.UseFormDataParameters` is true.

**When changing parameter behavior**, update both the read and write paths: the type's binary
serialization in `Types/` and the HTTP write path in `Formats/HttpParameterFormatter.cs`.

---

## Testing

### Running tests

```bash
dotnet test ClickHouse.Driver.Tests --framework net9.0 --property WarningLevel=0
```

The suite uses the server in `CLICKHOUSE_CONNECTION` (an HTTP connection string). When that is not
set and `CLICKHOUSE_TEST_ENVIRONMENT` is unset or `local_single_node`, it starts a ClickHouse
container with Testcontainers, which needs Docker. `CLICKHOUSE_TEST_ENVIRONMENT` names the kind of
server (see `TestEnv` in `Utilities/TestUtilities.cs`).

`CLICKHOUSE_VERSION` selects the container image tag, and it is also the version the version gates
use. The suite asks the server for its version only when the variable is unset, `latest` or `head`.
Do not set it to a version other than that of the server you point the suite at.

### Table names

Any test that touches a table must get its name from `CreateTableName(...)` (see the root file for
why):

```csharp
// In a fixture deriving from AbstractConnectionTestFixture (preferred, cleans up for you):
var table = CreateTableName();                  // test.MyTestMethod_net9_a1b2c3d4e5f6

// Anywhere else (you own the cleanup):
var table = TestUtilities.CreateTableName();    // + DROP TABLE IF EXISTS in your teardown
```

- The inherited `AbstractConnectionTestFixture.CreateTableName()` registers the name so
  `[OneTimeTearDown]` drops it. Prefer it over the static helper whenever the fixture allows.
- Pass a prefix only to carry a parametrized case, e.g. `CreateTableName($"bulk_{clickHouseType}")`.
  Prefixes are sanitized, so interpolating type names, timezone ids etc. is safe; no manual
  scrubbing needed.
- Names are already `test.`-qualified. Never prepend `test.` yourself. Pass
  `database: "other_db"` to target another database, or `database: null` for an unqualified name
  (required for `CREATE TEMPORARY TABLE`, which cannot be qualified).
- Because every name is unique, a plain `CREATE TABLE` cannot collide. Don't add
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
  fine. Hoist one `CreateTableName(...)` into the fixture rather than splitting it per test.
- `Utilities/TestTableNamingTests.cs` pins this contract; keep it passing.

### Suite conventions
- **Type matrix**: `Utilities/TestCases.cs` (`GetDataTypeSamples()`) already round-trips every type,
  plus its `Nullable`/`Array`/composite forms, through the select, parameter, bulk-copy and
  serialisation suites.
- **Test utilities**: before writing tests, read `Utilities/TestUtilities.cs` to understand existing
  config and utility patterns, including `CreateTableName`/`SanitizeTableName`.
- **`system.query_log`**: `QueryLog.ScalarAsync` fails with a distinct message when no row appears;
  `QueryLog.CountAsync` waits for `minimumCount` rows before reporting.
- **Server version gating**: `[FromVersion]`, `[IgnoreInVersion]` and `[RequiredFeature]` in
  `Attributes/`.
- **Test matrix**: ADO provider, parameter binding, ORMs, multi-framework, multi-ClickHouse-version
- **Test organization**: Client tests in `ClickHouse.Driver.Tests`, third-party integration tests in
  `ClickHouse.Driver.IntegrationTests`

---

## Documentation

User-facing behavior of this client is documented in `docs/http.mdx`.
