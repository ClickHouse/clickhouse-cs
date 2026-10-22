* Fixed the TCP client's `InsertAsync` failing on a column that a query read when a NULL row hides a value that the
  target type cannot hold, for example a `Nullable(Decimal(18, 2))` column after `nullIf` written into a
  `Nullable(Decimal(9, 2))` column (an `OverflowException`). A row that a NULL hides is written with the placeholder of
  the target, also inside a `Nullable(Tuple(...))` and in the `Array` and `Map` elements of such a row
  ([ClickHouse/integrations#801](https://github.com/ClickHouse/integrations/issues/801)).
