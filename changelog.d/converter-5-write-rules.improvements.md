* Changed the TCP client's `InsertAsync`, `InsertRowsAsync<T>`, `InsertRowsAsync` with `object[]` rows, and
  `ClickHouseTcpTypes.CanWrite` to accept the same CLR types, with the write rules of POCO inserts
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)):
  - A CLR enum is written as its integer ordinal, a value as a type that it casts to (for example a `string` into a
    `Variant`), and a value type into a column of its nullable type (for example an `int` into `Nullable(Int32)`).
  - A nullable value type is written into a column that cannot hold `NULL` (for example an `int?` into `Int32`), and the
    insert throws at the first `NULL`. The row inserts find it before its block is sent; `InsertAsync` finds it while it
    writes the block, and the insert ends its connection.
  - The rules apply to every CLR type that the column type is written from, so `InsertRowsAsync<T>` also accepts, for
    example, a `DateTime?` property for `LowCardinality(DateTime)`.
  - The row inserts write `LowCardinality(String)` from `byte[]` and `FixedString(N)` from `string`, place a `Variant`
    value by the alternative that is written from its type, and keep each distinct encoded value of a `LowCardinality`
    dictionary once, as `InsertAsync` does.
