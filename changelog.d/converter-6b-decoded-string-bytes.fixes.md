* Fixed the TCP client's `InsertAsync` writing a `String` column that a query read into a `LowCardinality(String)`,
  `LowCardinality(Nullable(String))` or `Nullable(String)` column through the text of its values, so the server stored
  U+FFFD for bytes that are not valid UTF-8. Such a column, also inside an `Array`, `Map` or `Tuple`, is written from its
  bytes. A `FixedString(N)` column takes it too, when each value is exactly N bytes
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
