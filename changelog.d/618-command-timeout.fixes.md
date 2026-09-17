* Fixed `ClickHouseCommand.CommandTimeout` being ignored ([#618](https://github.com/ClickHouse/clickhouse-cs/issues/618)).
  A non-zero value is now sent as the `max_execution_time` setting, so the server cancels a query
  that exceeds it. Zero, the default, still means no limit, and an explicit `max_execution_time` in
  the command's `CustomSettings` takes precedence.
