* Changed the TCP client's `InsertAsync` to write a column that implements `IColumn<T>` for more than one `T` as the
  first of those types in the list of CLR types that its refusal message names for the target type
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)). Such a `Nullable(Int32)`
  column threw an `InvalidOperationException`, and a `FixedString(N)` column of `string` values was refused.
  - A refused `QBit` vector was named by its row in the column, also in a block after the first.
