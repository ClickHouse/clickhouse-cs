* Fixed binary inserts silently storing a different timestamp for `DateTime64(9)` values before 1677-09-21 or
  after 2262-04-11 UTC, and for `DateTime64(8)` values after 4892-10-07 UTC. These values now throw
  `ArgumentOutOfRangeException` ([#711](https://github.com/ClickHouse/clickhouse-cs/issues/711)).
