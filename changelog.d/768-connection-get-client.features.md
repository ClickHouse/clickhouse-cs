* Added `ClickHouseConnection.GetClient()`, which returns the `IClickHouseClient` that the connection
  uses. Libraries that get a `ClickHouseConnection`, such as ORMs, can now call `InsertBinaryAsync` with
  the settings and the HTTP connection pool of that connection. ([#768](https://github.com/ClickHouse/clickhouse-cs/pull/768))
