* Added `CredentialsProvider` to `ClickHouseClientSettings` (HTTP) and `ClickHouseTcpClientOptions` (TCP): a
  callback that gives the client new credentials, for example a refreshed JWT, before they expire, and for the
  next request or connection after the server rejects them (ClickHouse/clickhouse-java#3145).
* Added JWT authentication to the native TCP client: `ClickHouseTcpClientOptions.BearerToken` and the
  `BearerToken` connection string key.
