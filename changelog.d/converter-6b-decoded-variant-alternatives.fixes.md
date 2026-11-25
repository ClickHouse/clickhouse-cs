* Fixed the TCP client's `InsertAsync` writing a `Variant` column that a query read into a `Variant` column of other
  alternatives value by value, through the CLR type of each value. A `String` value that is not valid UTF-8 was stored
  as U+FFFD, and a value with no alternative in the target failed after the insert had started. The column is now
  written from its storage when the target has each of its alternatives (also inside `Array`, `Map` and `Tuple`), so
  each value keeps its alternative and its bytes. When the target does not have an alternative of the column, the
  insert refuses the column before it sends a block, also when no row holds a value of that alternative; convert the
  values with a `CAST` in the query
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
