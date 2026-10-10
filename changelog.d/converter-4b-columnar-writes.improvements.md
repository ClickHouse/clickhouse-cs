* Changed the TCP client's inserts (`InsertAsync`, `InsertRowsAsync<T>` and `InsertRowsAsync` with `object[]` rows) and
  `ClickHouseTcpTypes.CanWrite` to write through the converter layer
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)):
  - `FixedString(N)` accepted `string` values, also inside `LowCardinality`, `Nullable`, `Array`, `Map` and `Tuple`: the
    UTF-8 bytes of the text, padded with zero bytes to `N`. A text of more than `N` bytes was refused.
  - A `Variant` value whose runtime type is the default CLR type of no alternative went to the alternative that is
    written from that type, for example a `byte[]` to a `String` alternative.
  - A `LowCardinality` dictionary held each distinct encoded value once, so two strings with the same UTF-8 bytes (two
    lone surrogates) shared one entry.
