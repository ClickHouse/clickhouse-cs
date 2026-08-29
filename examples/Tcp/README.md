# Native protocol examples

These examples use `ClickHouseTcpClient` and ClickHouse's native TCP protocol. The HTTP examples use
`ClickHouseClient` or `ClickHouseConnection` instead. See the [example index](../README.md) to choose
and run an example.

## Before you start

The native protocol normally listens on port 9000. Port 8123 is for HTTP.

```bash
docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
dotnet run -- --tcp
```

The native client API is experimental. Its main client, data source, session, operations interfaces,
and dependency-injection extensions produce warning `CHTCP0001`. This project acknowledges the
warning globally. In another project, add this setting while you evaluate the API:

```xml
<NoWarn>$(NoWarn);CHTCP0001</NoWarn>
```

## Choose the right transport

The native client provides:

- columnar block reads through `StreamAsync`;
- pinned sessions through `OpenSessionAsync`;
- progress, profile, log, totals, and extremes callbacks;
- native block compression and `QBit` plane access;
- W3C trace context propagation to ClickHouse.

Use the HTTP client when you need:

- ADO.NET or an ORM;
- CSV, JSONEachRow, Parquet, or raw stream input and output;
- bearer authentication or custom HTTP headers;
- custom parameter type resolution, parameter formatting, or read conversion;
- a per-query database or role.

One type detail is easy to miss: row reads return `DateTime`, `DateTime64`, `Time`, and `Time64` as
their wire integer values. `QueryAsync<T>` converts mapped POCO properties, while block reads expose
`IDateTimeColumn` and `ITimeColumn` for typed conversion.
