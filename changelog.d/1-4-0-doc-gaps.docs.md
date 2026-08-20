* Documented the 1.4.0 changes that had shipped without docs: the built-in compressors and their
  levels, `IClickHouseCompressor` as an extension point, `Accept-Encoding` precedence,
  `TryGetEnumOrdinal`, `GetSchemaTable()` precision and scale, `GetSchema("Columns")` restrictions,
  the reader's new no-current-row rule, mid-stream and empty-body errors, JSON typed-path nulls,
  `ReadStringsAsByteArrays` inside JSON columns, `@name` placeholder rewriting, `TimeOnly`
  parameters, chunked binary inserts, and stream ownership on the raw insert paths.
