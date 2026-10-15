* Fixed `ClickHouseDataSource.OpenConnectionAsync()` without a cancellation token being typed as
  `DbConnection` instead of `ClickHouseConnection`
  ([#686](https://github.com/ClickHouse/clickhouse-cs/issues/686)). The documented ADO.NET examples
  now compile.
