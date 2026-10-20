* Fixed `ClickHouseDataSource.OpenConnectionAsync()` without a cancellation token being typed as
  `DbConnection` instead of `ClickHouseConnection`
  ([#686](https://github.com/ClickHouse/clickhouse-cs/issues/686)). The documented ADO.NET examples
  now compile.
  - The call now returns `Task<ClickHouseConnection>` instead of `ValueTask<DbConnection>`. Code
    that stores it as a `ValueTask<DbConnection>`, or calls `.AsTask()` or `.Preserve()` on it, no
    longer compiles: await the call directly, or call it through `DbDataSource` to keep the old type.
