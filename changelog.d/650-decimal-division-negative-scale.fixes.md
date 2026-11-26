* Fixed `ClickHouseDecimal` division throwing `ArgumentException` ("Scale cannot be <0") for some quotients with more
  integer digits than `MaxDivisionPrecision` when the divisor has more fractional digits than the dividend, for
  example `10^60 / 0.3` ([#650](https://github.com/ClickHouse/clickhouse-cs/issues/650)). The quotient now keeps its
  full integer part.
