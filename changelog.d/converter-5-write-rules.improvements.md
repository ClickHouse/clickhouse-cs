* Changed the TCP client's inserts and `ClickHouseTcpTypes.CanWrite` to accept the same CLR types for each column type
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)). An enum was written as its
  integer ordinal, a value as a type that it casts to, an `int` into `Nullable(Int32)`, and an `int?` into `Int32`,
  where the insert throws at the first `NULL`.
