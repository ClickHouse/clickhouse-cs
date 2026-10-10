* Fixed the TCP client's `InsertAsync` writing a column that a query read into a column of another `DateTime64` or
  `Time64` scale, or of another `Enum8` or `Enum16` definition, as the raw counts or ordinals of the column that was
  read. The server then stored other instants, durations or labels. Such a column is converted through its values,
  also inside `Nullable`, `LowCardinality`, `Array`, `Map` and `Tuple`; a column of a scale above 7, or a `Variant`
  of such types, is refused when the target type differs
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
