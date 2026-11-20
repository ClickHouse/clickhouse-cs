* Fixed column names containing backslashes or backticks being emitted as malformed ClickHouse identifiers ([#316](https://github.com/ClickHouse/clickhouse-cs/issues/316)).
  Insert APIs now expect one raw column name per collection element and escape and enclose the names themselves; callers should remove manually added backticks.
