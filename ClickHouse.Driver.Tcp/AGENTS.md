# Native TCP client (ClickHouse.Driver.Tcp)

These rules add to the root [`AGENTS.md`](../AGENTS.md). They apply to `ClickHouse.Driver.Tcp/` and
`ClickHouse.Driver.Tcp.Tests/`.

## Project Context
- **Primary API**: `ClickHouseTcpClient`. It speaks the ClickHouse native protocol, and data moves as
  columnar Native-format blocks, not as RowBinary rows.
- **Experimental**: the entry points (client, data source, session, operations interfaces, and the
  dependency-injection extensions) carry `[Experimental("CHTCP0001")]`. A new entry point of that
  kind carries it too. The test, benchmark and examples projects suppress the warning.
- **Targets**: `net8.0`, `net9.0`, `net10.0`. There is no `net6.0` build.
- **Standalone**: this project references `ClickHouse.Driver.Common` only, never `ClickHouse.Driver`.
  `ClickHouse.Driver` packs this assembly into its NuGet package.
- **Reference sources**: the ClickHouse server source is the authority for wire behavior.
  [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) already implements the native
  protocol and is a useful second reference.

### Structure
```
ClickHouse.Driver.Tcp/
├── Client/               # ClickHouseTcpClient, options, data source, sessions, connection pool
├── Protocol/             # Connection, handshake, packets, ReadBuffer, binary reader/writer
├── Format/               # Native-format blocks: BlockReader, BlockWriter
├── Types/                # Column types; Types/Codecs/ has one codec per ClickHouse type (the wire layer),
│                         # Types/Converters/ the converter layer
├── Poco/                 # POCO read/write mapping
├── Parameters/           # Query parameter type inference and formatting
├── Compression/          # Compressed frames (the codecs are in ClickHouse.Driver.Common)
├── Diagnostic/, Logging/ # OpenTelemetry and ILogger integration
├── DependencyInjection/  # IServiceCollection extensions
├── Exceptions/, Numerics/
└── PublicAPI/            # Public API files, checked by an analyzer
```

---

## Development Guidelines

### Configuration & Parameters
- **Client configuration**: `ClickHouseTcpClientOptions`, or a connection string through
  `ClickHouseTcpConnectionStringBuilder`.
- **Per-call options**: `ClickHouseTcpQueryOptions` and `ClickHouseTcpInsertOptions`.
- **Parameters**: `ClickHouseTcpParameterCollection`, passed in `ClickHouseTcpQueryOptions.Parameters`.
  Only ClickHouse-native `{name:Type}` placeholders are supported. The client does not rewrite
  ADO.NET-style `@name` placeholders.

### Protocol versions
- Every version-conditional read or write asks `NegotiatedProtocol.Supports(ProtocolFeature.X)`
  (`Protocol/`). Do not compare against an inline revision number. `ProtocolFeature` mirrors the
  server's `DBMS_MIN_REVISION_WITH_*` constants; add a member there for a new version-gated field.

### Types
Two layers handle a column type:

- **The wire layer** (`Types/Codecs/`, one `IColumnCodec` for each ClickHouse type). `ReadColumnAsync` decodes a
  column into the class that a query of the type gives. `CanWrite` is true only for a column that the codec writes
  from its storage: that class, or a dense composite column (for example `ClickHouseTcpColumn.CreateArray` over a
  decoded column) whose child columns the child codecs write from their storage. A parameter that gives a stored value
  its meaning must be the same (the scale of a `DateTime64` or a `Time64`, the members of an `Enum`, the width of a
  `FixedString`, the layout of a `QBit`); a parameter that does not (a timezone) need not be. `BeginWrite`,
  `WriteStatePrefix` and `WriteColumn` write such a column with no conversion, so the bytes are the bytes that the
  server sent. `ElementType` is the canonical CLR type. `ClaimsValue` breaks a tie between `Variant` alternatives.
- **The converter layer** (`Types/Converters/`) gives every other reading and write.
  `ConverterDerivation.Derive(type, context, clrType, direction)` gives a cached tree of readers (`ColumnReader<T>`) or
  writers (`ColumnWriter<T>`), or a refusal with a reason. Every tier uses it: `Block.ReadAs<T>`, `QueryAsync<T>`,
  `InsertAsync` (through `Format/InsertColumnWrite.For`, which gives a storage column to the codec), `InsertRowsAsync<T>`,
  `InsertRowsAsync` with `object[]` rows, and `ClickHouseTcpTypes.CanRead`/`CanWrite`. A column that a query read as
  a type whose values mean other values in the target (another scale, other enum members) is written part by part
  (`ConverterDerivation.PlanDecodedPart`, `InsertColumnWrite.For`): only the parts whose values mean other values are
  converted through the meaning of their values (read as `DateTimeOffset`, `TimeSpan` or the label, then written); the
  offsets of an `Array` or a `Map`, the null map of a `Nullable` and every other part are written from their storage,
  so their bytes do not change (`Types/Converters/ConverterDerivation.DecodedColumns.cs`).

When you add or change a type, consider every path that touches it:

- **A leaf pair** (a leaf type and a CLR type that it reads as or is written from) goes into
  `Types/Converters/LeafTable.cs`, and only there. Its writer holds the placeholder that a marked position (the hidden
  value under a `NULL`) writes. A pair whose CLR type is not the stored value is a conversion (`conversion: true`).
- **A composite** is a combinator in `ConverterDerivation.Read.cs` and `ConverterDerivation.Write.cs`, built from the
  trees of its children: Lift (`Nullable`), Each (`Array`), Dictionary (`LowCardinality`), Map and Tuple. `Variant`,
  `Dynamic`, `Nested`, `JSON`, `QBit` and the geo types are nodes of their own. A combinator never names a leaf pair.
- **The read and write rules** (a CLR enum as its ordinal, a cast, a nullable value type both ways) are in
  `ReadRules.cs` and `WriteRules.cs`, and apply to the whole column type in every tier. A leaf or a combinator does not
  repeat them.
- **`Fill` and `Emit`.** Every reader has a bulk `Bind`/`Fill` and an `Emit` (an expression for one row, which the POCO
  tier compiles into its loop). The two give the same values and the same failures, and the differential tests run
  every tree both ways. `Fill` is the path of a runtime without dynamic code, so do not add a reader that has only an
  `Emit`.
- The codec: its read, and its write from storage. `CanWrite` is true for the column class that its read gives, and a
  composite codec asks the codec of each child about the child column.
- POCO mapping in `Poco/` uses the same trees.
- parameter formatting in `Parameters/TcpParameterFormatter.cs`. It duplicates the HTTP client's
  `HttpParameterFormatter`, because the two assemblies cannot share a type tree. Keep the shared
  cases aligned.

### Performance
This library is highly performance-sensitive. Minimize allocations and copies, most of all on the
hot paths: wire reading and writing (`Protocol/`, `Format/`) and column (de)serialization (`Types/`).
Prefer pooled buffers, `Span`/`Memory`, and zero-copy access. Do not bulk-copy an array where a
better data structure avoids it.

The read path uses a linear `ReadBuffer` (`Protocol/ReadBuffer.cs`, which documents its design). A
ring buffer was considered and rejected: this access pattern rarely pays the compaction cost that a
ring buffer removes, and a ring buffer adds a scratch copy on every read that crosses the end, split
reads from the socket, and more edge cases.

### Public API surface
- **Default new types to `internal`**, especially wire plumbing (buffers, readers/writers,
  packet/block encoders, connection internals). Make something `public` only when it is part of the
  user-facing contract: the client, options, exceptions, and the CLR value types users receive (such
  as `Int256`/`UInt256`). It is much easier to widen accessibility later than to take back a public
  API.
- The test project sees internals through `InternalsVisibleTo` in `AssemblyInfo.cs`. The assembly is
  strong-named, so the declaration carries the full public key.
- `Microsoft.CodeAnalysis.PublicApiAnalyzers` checks the surface. Add each new public symbol to
  `PublicAPI/PublicAPI.Unshipped.txt`. `.editorconfig` makes RS0016, RS0017, RS0024 and RS0048
  errors, but `--property WarningLevel=0` hides them. To check the surface, build without it:

  ```bash
  dotnet build ClickHouse.Driver.Tcp --framework net9.0 2>&1 | grep -E '(error|warning) RS[0-9]+'
  ```

- The analyzer reads exactly one `PublicAPI.Shipped.txt` + `PublicAPI.Unshipped.txt` pair. Do not add
  a second pair (for example a per-framework folder): the checks then stop without an error.

---

## Testing

### Running tests

```bash
dotnet test ClickHouse.Driver.Tcp.Tests --framework net9.0 --property WarningLevel=0
```

Tests outside the `Integration` namespace need no server. `Integration/TcpServerFixture.cs` finds a
server for the integration tests, in this order:

1. `CLICKHOUSE_TCP_CONNECTION`: a whole native connection string (TLS options included).
2. `CLICKHOUSE_TCP_HOST`, with `CLICKHOUSE_TCP_PORT` (default 9000), `CLICKHOUSE_TCP_USER` (default
   `default`) and `CLICKHOUSE_TCP_PASSWORD` (default `clickhouse`). A local server usually has an
   empty password:

   ```bash
   CLICKHOUSE_TCP_HOST=localhost CLICKHOUSE_TCP_PASSWORD="" dotnet test ClickHouse.Driver.Tcp.Tests --framework net9.0 --property WarningLevel=0
   ```

3. A Testcontainers container, which needs Docker. `CLICKHOUSE_VERSION` selects the image tag.

`CLICKHOUSE_VERSION` is also the version the version gates use (`Utilities/TcpServerFeatures.cs`).
The suite asks the server for its version only when the variable does not hold a version number.
Do not set it to a version other than that of the server you point the suite at.

### ClickHouse Cloud
- `CLICKHOUSE_TEST_ENVIRONMENT=cloud` marks a Cloud run (`TcpServerFixture.IsCloud`). Without it,
  `SkipIfCloudLocksASetting` does nothing.
- The Cloud workflow runs only fixtures marked `[Category("Cloud")]`. The standard matrix runs them
  too. The category is for behavior that a Cloud service changes (a load balancer, several
  replicas, TLS, locked settings), not for coverage. Mark a new fixture with it if it tests such
  behavior.
- Read the summary of `TcpServerFixture` before you write a test that runs on Cloud. `Memory` tables
  are local to one replica, so some test shapes read an empty table there.
- Cloud refuses some query settings. Call `TcpServerFixture.SkipIfCloudLocksASetting` for a test that
  sets them. The refusal also ends the connection, so the tests after it fail with
  `ObjectDisposedException`. Look at the first failure.

### Suite conventions
- **Table names**: give each test its own name with a `Guid` suffix, as the fixtures'
  `UniqueTableName()` helpers do (`$"tcp_insert_test_{Guid.NewGuid():N}"`), and drop it in a
  `finally`.
- **Server version gating**: `[RequiresServerFeature(TcpFeature.X)]`. `TcpFeature` mirrors the HTTP
  client's `ClickHouse.Driver.ADO.Feature`; keep the shared versions aligned.
- **`system.query_log`**: `Utilities/QueryLog.cs` (`QueryLog.ScalarAsync`). In the lookup SQL, write
  `QueryLog.Table` in place of `system.query_log`. Each replica keeps its own log, so on a server
  with a cluster (Cloud and the fixture's container) it reads every replica.

### Codec test layering

Per-type coverage is split between layers. A test in the wrong layer passes while it proves nothing that another layer
did not already prove.

**`InsertRoundTripCase` owns per-type values.** Adding a type means adding cases to
`InsertRoundTripCase.Cases()`: each creates a one-column table, inserts the column, selects it back,
and compares. That exercises the real write (the converter layer, or the codec for a decoded column) → server →
`ReadColumnAsync` path, so it subsumes any unit test that writes a column of values and reads it back.

Add a bare `T` case *and* a `Nullable(T)` case, with nulls between present values, for every type
that `Nullable` accepts. `Nullable` has its own write path (the null map plus a per-type placeholder)
that the bare type never exercises. If the type needs an enabling setting or a non-default
placeholder, the `Nullable` case is where that gets proven.

**The differential tests (`Differential/`) run every case through every tier, offline.** Their case list is every
`InsertRoundTripCase` case, `CompositeLiftMatrixTests.Cases()`, a table of column types with read targets, and the read
scenarios of `ColumnReadProjectionTests`. For each case they read through `Block.ReadAs<T>` and POCO mapping (both
tiers), write through the insert plan and the row inserts, and check that the tiers agree: `CanRead` and `CanWrite`
agree with the reads and the inserts, a decoded column writes the bytes that it was read from, and a slice that starts
after row 0 writes the values of its rows. A new `InsertRoundTripCase` case is a differential case too; the tests pin
the number of cases and facets, so a new case changes those counts.

**A codec unit test asserts only what the wire layer does:**

- reads of hand-written bytes, and their protocol errors;
- type resolution: `FormatException`/`NotSupportedException`, `ElementType`, `TypeName`, field and label metadata;
- the storage check `CanWrite`, most of all its rejections (a caller-built child, another arity, another order of
  `Variant` alternatives, a `Nothing` child);
- the write of a decoded or dense column: bytes, prefixes, slices. Write it with `CodecTestHarness.WriteStoredAsync`,
  which fails when the codec does not write the column from its storage, so the test cannot pass through the converter
  layer;
- column classes: constructors, `Values` caches, bounds past `RowCount`, `Dispose` and pooling, zero rows.

A test in a codec test file that writes a caller's column through `CodecTestHarness` (`WriteSliceAsync`,
`RoundTripAsync`) goes through the insert plan, so it tests the converter layer's write of that type. Keep such a test
only for bytes or a refusal that no case and no literal pin reaches.

**A converter unit test (`Types/Converters/`) asserts what no case reaches:** leaf pairs that no case has, segments and
marked positions, refusals (the exception type, the parameter name and the message, as literals), interners and
dictionary bytes, allocation bounds. Pin literal bytes, or decode the bytes with the codec's read and compare the values;
do not compare with another write implementation.

**Wire bytes that no round trip can observe** (key widths, dictionary order, discriminators, placeholders under `NULL`,
state prefixes) are literal pins: `Types/Converters/WireBytePinTests.cs`, `Format/InsertWritePinTests.cs`, and the
literal tests of the converter writers.

Two subtleties decide whether an accessor needs unit coverage:

- **`.Values` sometimes needs it, sometimes not.** It depends on whether `GetValue` delegates to it.
  For `ArrayColumn<T>`, `GetValue(row) => Values[row]`, so the per-row comparison in integration
  already covers it. For the `Nullable`, `LowCardinality`, `Map`, `Nested`, and `Tuple` columns,
  `GetValue` reads its own state directly, and `Values` is a separate pooled cache that integration
  never touches. Cover those.
- **Dense write paths are covered by integration.**
  `InsertAsync_DenseReadbackReinserted_RoundTripsThroughSelect` re-inserts the dense read-back of
  every case, so a unit test for "the dense column writes without rebuilding" is redundant unless it
  builds a shape the server cannot produce.

### Slices: always test a non-zero `start`

An insert larger than `maxRowsPerBlock` (default `ClickHouseTcpConnection.DefaultMaxRowsPerBlock`)
goes out as several blocks, so the codec writes a slice. A slice at `start = 0` does not exercise
the slice logic: every child run starts at offset 0, so a codec that ignores `start` still passes.
Only a non-zero `start` shows that the codec rebases its child offsets, run starts and element
ranges.

- **Converter write path**: `InsertSlicingIntegrationTests` inserts every `InsertRoundTripCase` one
  row per block, so a new case gets this coverage. Offline, the differential tests write every case from a row after
  row 0.
- **Dense write path**: it computes the slice differently, and no matrix test slices it. A codec with
  its own dense path needs an integration test that inserts with `maxRowsPerBlock` less than the
  row count and reads back every row. Follow
  `InsertAsync_DenseVariantColumnSplitAcrossBlocks_RoundTripsEveryRow`. Add an `id` column to pin the
  read-back order; the `Memory` engine does not guarantee it across blocks.
- **Codec unit test**: write a slice of the dense column with `start > 0` and assert the exact bytes.
  Put the slice after earlier values of the same type, so the expected child offset is not 0. Follow
  `VariantColumnCodecTests.WriteColumn_DenseColumnSliceAfterEarlierValues_StartsEachRunAtItsSliceOffset`.

---

## Documentation

User-facing behavior of this client is documented in `docs/tcp.mdx`. The TCP examples are in
`examples/Tcp/`.
