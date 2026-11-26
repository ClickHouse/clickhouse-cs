* Fixed the `ClickHouseRawResult` read members failing with an `IOException` instead of a `ClickHouseServerException` when a query failed after the server started to send the result ([#475](https://github.com/ClickHouse/clickhouse-cs/issues/475)).
  On a compressed response, only `ReadDecompressedStreamAsync` detects the error.
