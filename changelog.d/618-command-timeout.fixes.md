* Fixed `ClickHouseCommand.CommandTimeout` being ignored ([#618](https://github.com/ClickHouse/clickhouse-cs/issues/618)).
  A positive value is now sent as the `max_execution_time` setting, so the server cancels a query
  that exceeds it. Zero, the default, and negative values add no command-level limit and leave an
  inherited `max_execution_time` untouched, and an explicit `max_execution_time` in the command's
  `CustomSettings` takes precedence.
