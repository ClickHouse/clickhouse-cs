* Fixed `ClickHouseDecimal.NumberOfDigits` returning one digit too few for powers of ten and for some
  large values just above them ([#642](https://github.com/ClickHouse/clickhouse-cs/issues/642)).
  `Truncate()` and conversion to `BigInteger` now return `0` for values such as `0.1`, `0.01` and
  `0.100`. `Truncate(precision)` and division now determine the same number of digits for such
  values as for any other value.
