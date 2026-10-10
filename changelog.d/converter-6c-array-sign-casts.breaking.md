* Changed the TCP client to refuse an array cast that gives each element another meaning, in every read and write:
  `Array(UInt32)` no longer reads as `int[]`, and a `uint[]` is no longer written into `Array(Int32)`, where values
  above `int.MaxValue` became negative numbers; the same applies to the other integer types of the other sign and to an
  enum array over an integer type of the other sign. An enum array still reads from and is written as an array of its
  underlying type, for example `Array(Enum8(...))` as `MyEnum[]`
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
