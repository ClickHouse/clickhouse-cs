* Fixed binary writes of `string` values into `Date`, `Date32`, `DateTime` and `DateTime64` columns,
  which previously threw a message-less `NotSupportedException` while every other scalar type
  accepted a string. This also unblocks `Map(Date, V)` / `Map(DateTime, V)` columns with string keys.
  An unsupported value now reports its type and the target column type ([#614](https://github.com/ClickHouse/clickhouse-cs/issues/614)).
