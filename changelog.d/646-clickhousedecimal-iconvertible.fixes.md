* Fixed `ClickHouseDecimal` conversions through `Convert` ([#646](https://github.com/ClickHouse/clickhouse-cs/issues/646)).
  `Convert.ToInt32` kept only the low 16 bits, so `InsertBinaryAsync` stored a wrong value in an
  `Int32` column; conversions to `sbyte`, `byte`, `short`, `ushort` and `char` wrapped instead of
  throwing `OverflowException` when the integer part is out of range; and `Convert.ChangeType` to
  a type such as `decimal?`, `Guid` or an enum crashed the process with a stack overflow instead of
  throwing `InvalidCastException`.
