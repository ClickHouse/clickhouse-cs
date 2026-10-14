* Fixed `InsertBinaryAsync` attaching the caller's `InsertOptions.QueryId` to its internal schema
  probe (`SELECT ... WHERE 1=0`). The probe now uses an id of its own, so the caller's query id
  identifies only their insert batches (`{QueryId}-1`, `{QueryId}-2`, ...) in `system.query_log`
  ([#624](https://github.com/ClickHouse/clickhouse-cs/issues/624)).
