* Added a **Performance tuning** section to the docs, covering the materialization path
  (`QueryAsync<T>` against `MapTo<T>`), insert batch size and parallelism, compression by direction,
  `ReadBufferSize`, Server GC, connection reuse, and how to measure without misleading yourself
  ([#491](https://github.com/ClickHouse/clickhouse-cs/issues/491)).
  - Also corrected the `ReadBufferSize` row in the settings table, which still documented the old
    8 KiB default and a large-object-heap warning that no longer applies. The default is 64 KiB and
    the buffer is pooled.
