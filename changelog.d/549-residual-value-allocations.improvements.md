* Reduced the per-value allocations on the read and write paths for `UUID`, `Array(...)` and `Decimal`
  columns ([#549](https://github.com/ClickHouse/clickhouse-cs/issues/549)). Reading or writing a `UUID` no
  longer allocates. `Array(...)` elements are no longer boxed one at a time when the element has an exact
  typed path for the CLR type being read or written: reading covers the fixed-width scalars, `Decimal`,
  `DateTime`, `UUID` and `String`; writing covers those plus `Int128`/`UInt128`/`Int256`/`UInt256`
  (`BigInteger`), `ClickHouseDecimal`, `Time`/`Time64` (`TimeSpan`), `DateTimeOffset` and `DateOnly`.
  Everything else keeps the existing boxed per-element path unchanged — `Nullable(...)` elements in both
  directions, reads of element types without a typed reader (for example `Array(Int128)`), a CLR array whose
  element type the column only accepts by coercion (a `long[]` written into `Array(Int32)`), and composite or
  nested elements. Constructing a `ClickHouseDecimal` from a `decimal` allocates nothing for values that fit
  `BigInteger`'s inline representation.
