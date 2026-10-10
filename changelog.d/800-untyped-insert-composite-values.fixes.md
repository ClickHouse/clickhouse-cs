* Fixed the TCP client's `InsertRowsAsync` with `object[]` rows refusing values of a CLR type that a composite column is
  written from but that is not its default type, for example `DateTime[]` for `Array(DateTime)`, `DateTime` for
  `LowCardinality(DateTime)` and `DateTime?[]` for `Array(Nullable(DateTime))`
  ([ClickHouse/integrations#800](https://github.com/ClickHouse/integrations/issues/800)).
