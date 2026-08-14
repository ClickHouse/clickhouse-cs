* Removed the remaining per-value allocations on the read and write paths for `UUID`, `Array(...)` and
  `Decimal` columns ([#549](https://github.com/ClickHouse/clickhouse-cs/issues/549)). Reading or writing a
  UUID no longer allocates, value-type array elements are no longer boxed one at a time in either direction,
  and constructing a `ClickHouseDecimal` from a `decimal` allocates nothing for values that fit
  `BigInteger`'s inline representation.
