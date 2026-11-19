* Fixed the OpenTelemetry `db.name` span tag reporting the client's configured database when a query is
  sent to a different one ([#685](https://github.com/ClickHouse/clickhouse-cs/issues/685)). The tag now
  reports the database sent with the request: `QueryOptions.Database` / `InsertOptions.Database` when
  set, or the database selected with `ClickHouseConnection.ChangeDatabase` for `ClickHouseCommand`.
