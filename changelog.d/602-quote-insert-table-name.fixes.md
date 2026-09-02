* Fixed inserts into tables whose name is not a legal unquoted identifier (`my-table`, `user events`).
  `InsertBinaryAsync` and `InsertRawStreamAsync` now enclose the table name — and, for
  `InsertRawStreamAsync`, the column names — in backticks instead of concatenating them into the
  statement, so such a table no longer fails with a `SYNTAX_ERROR`. A name a caller already quoted is
  left as it is. ([#602](https://github.com/ClickHouse/clickhouse-cs/issues/602))
