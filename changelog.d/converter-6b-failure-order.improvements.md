* Changed the TCP client's `Block.ReadAs<T>` to fail on a `LowCardinality` dictionary entry that has no reading as
  `T` only when a row of the column uses it, as `QueryAsync<T>` already did
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)). When several values of a
  column cannot be read or written, the exception names one of them, which is not always the first and can differ
  between `Block.ReadAs<T>`, `QueryAsync<T>`, `InsertAsync` and the row inserts.
