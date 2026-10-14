* Added the `BearerToken` connection string key, so a JWT or other bearer token can be set in the
  connection string. `ClickHouseConnection.ConnectionString` and `ClickHouseDataSource.ConnectionString`
  now keep a token that was set in `ClickHouseClientSettings`.
