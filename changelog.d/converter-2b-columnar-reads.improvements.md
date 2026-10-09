* TCP client: `Block.ReadAs<T>` and `ClickHouseTcpTypes.CanRead` now accept the same readings as `QueryAsync<T>` POCO
  mapping: a CLR enum from its integer ordinal, a type that the values cast to (for example `object`), a nullable value
  type for a column that is not nullable (for example `ulong?` for `UInt64`), and a value type for a nullable column,
  which throws at the first `NULL` value ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
  - A `Block.ReadAs<T>` view that converts a column now converts the whole column on its first access to `Values` or to
    the indexer, and the block gives the same view for each later call with the same column and type.
