* Fixed `ClickHouseCommand.CommandTimeout` being ignored ([#618](https://github.com/ClickHouse/clickhouse-cs/issues/618)).
  A positive value is now sent as `max_execution_time`, unless the command's `CustomSettings`
  sets it. Zero (the default) and negative values send nothing.
