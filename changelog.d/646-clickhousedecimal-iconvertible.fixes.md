* Fixed `Convert` conversions of `ClickHouseDecimal` ([#646](https://github.com/ClickHouse/clickhouse-cs/issues/646)).
  `InsertBinaryAsync` now stores the correct value in an `Int32` column, conversions to smaller
  integer types throw `OverflowException` instead of wrapping, and `Convert.ChangeType` to an
  unsupported type throws `InvalidCastException` instead of crashing the process.
