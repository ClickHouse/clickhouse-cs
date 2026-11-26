* Fixed `ClickHouseConnection` not disposing the client that it owns when it was disposed through
  `DbConnection.Dispose()` or `DisposeAsync()` (`await using`), as ORMs such as EF Core do. The HTTP
  connections of a connection made from a connection string or from `ClickHouseClientSettings` then
  stayed open until garbage collection. ([#768](https://github.com/ClickHouse/clickhouse-cs/pull/768))
