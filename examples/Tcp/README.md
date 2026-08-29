# Native protocol examples

These use `ClickHouseTcpClient`, which speaks ClickHouse's native TCP protocol, rather than the
`ClickHouseClient` / `ClickHouseConnection` pair in [../Http](../Http) that speaks HTTP. The index of
every example, and how to run one, is in [the top-level README](../README.md).

## Before you run them

**They need port 9000, not 8123.** The two interfaces are separate listeners, so a server reachable
over HTTP is not necessarily reachable here:

```bash
docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
```

`dotnet run -- --tcp` runs only these, and checks the endpoint before starting.

## The API is experimental

`ClickHouseTcpClient`, `ClickHouseTcpDataSource`, the three `IClickHouseTcp*` interfaces
(`IClickHouseTcpClient`, `IClickHouseTcpOperations`, `IClickHouseTcpSession`) and the
`AddClickHouseTcpDataSource` overloads carry `[Experimental("CHTCP0001")]`, so touching one is a
compile error until you acknowledge that the surface may change in a future release. The types around
them — the options record, the connection string builder, `Block`, the column interfaces and the
exceptions — carry nothing, so they can be named without the suppression.

```csharp
#pragma warning disable CHTCP0001 // The native protocol client's API is not yet stable.
```

Per file as above, or once for a project:

```xml
<NoWarn>$(NoWarn);CHTCP0001</NoWarn>
```

This examples project takes the project-wide route, which is why no file here opens with the pragma.

## What the native client does not do

Reach for the HTTP client instead when you need:

- **A format other than Native** — the protocol carries columnar blocks, so there is no CSV, JSONEachRow
  or Parquet ingestion or export, and no raw stream insert.
- **ADO.NET, and so any ORM.** There is no `DbConnection` implementation over this transport, so Dapper,
  EF Core and linq2db do not work with it.
- **JWT or bearer authentication.** Username and password only.
- **Custom HTTP headers**, which have no equivalent on the wire.
- **A parameter type resolver, a parameter formatter, or a read value converter.** The native client
  has no hook for any of the three; a parameter's type comes from the `{name:Type}` placeholder in
  the query or from `ClickHouseTcpParameter.ClickHouseType`.
- **A per-query role or database.** HTTP's `QueryOptions` carries both; `ClickHouseTcpQueryOptions`
  carries only `QueryId`, `Settings`, `Parameters` and `Callbacks`. Set the database on the client,
  and change roles with `SET ROLE` inside a session.

Also worth knowing before you read a timestamp: a `DateTime`, `DateTime64`, `Time` or `Time64` column
reaches the **row** tier as the integer the wire carried, not as a calendar type, because that is the
value the server sent. `QueryAsync<T>` into a POCO converts, and on the block tier the column
pattern-matches to `IDateTimeColumn` or `ITimeColumn`, which convert and report the timezone and
scale the column type declared.

## What only the native client does

- **Blocks and columns.** `StreamAsync` yields a `Block` whose typed columns expose `ReadOnlySpan<T>`
  over the server's own layout, so a read can avoid materializing rows at all — and a column read out
  of a block re-inserts without being rebuilt.
- **Real sessions.** `OpenSessionAsync` pins one connection, so a temporary table or a `SET` survives
  from one operation to the next without the caveats an HTTP session carries.
- **Progress, profile info and profile events while a query runs**, through
  `ClickHouseTcpQueryCallbacks`, rather than as headers after the fact.
- **Block compression** on the wire, LZ4 by default.
- **Bit-plane access to `QBit` columns**, through `IQBitColumn`.
- **W3C trace context propagation.** The client sends the current `Activity`'s trace and span ids with
  each query, so the spans the server records in `system.opentelemetry_span_log` join the same trace as
  the caller's. The HTTP transport sends no `traceparent`.
