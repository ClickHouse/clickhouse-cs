* Changed the TCP client's `InsertAsync` to refuse a column that a query read, before it sends a block, when its type
  holds a `String` (also a `Dynamic` or a `JSON` column) and the target can take those strings only as text, so bytes
  that are not valid UTF-8 would change. Examples: a `Variant` with a `String` alternative into a `Dynamic`, a `Dynamic`
  into a `Variant`, a `LowCardinality(String)` into a `JSON`. A `String` into an `Enum8` or `Enum16` is still written.
  Convert the values with a `CAST` in the query, or insert a column of `string` values built with
  `ClickHouseTcpColumn.Create` ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
