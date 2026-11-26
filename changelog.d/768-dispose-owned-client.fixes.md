* Fixed `ClickHouseConnection` not disposing the client that it owns when it was disposed through
  `DbConnection.Dispose()` or `DisposeAsync()` (`await using`), as ORMs such as EF Core do. The client
  and its HTTP connections stayed open after the connection was disposed. A disposed connection is now
  closed, and a `Clone()` of a connection that owns its client stops working when the original is
  disposed. ([#768](https://github.com/ClickHouse/clickhouse-cs/pull/768))
