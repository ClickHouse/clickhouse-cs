* Fixed `ClickHouseDecimal.Floor()` rounding negative values with a fractional part toward zero:
  `-1.5` returned `-1` instead of `-2` ([#645](https://github.com/ClickHouse/clickhouse-cs/issues/645)).
