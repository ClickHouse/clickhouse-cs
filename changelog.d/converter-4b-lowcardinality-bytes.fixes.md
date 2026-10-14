* Fixed the TCP client refusing `byte[]` columns for `LowCardinality(String)`, `LowCardinality(Nullable(String))` and
  `Array(LowCardinality(String))` in `InsertAsync` and `ClickHouseTcpTypes.CanWrite`. The bytes are stored unchanged,
  also bytes that are not valid UTF-8
  ([ClickHouse/integrations#792](https://github.com/ClickHouse/integrations/issues/792)).
