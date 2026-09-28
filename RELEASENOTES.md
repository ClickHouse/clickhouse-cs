v1.5.0
---

**New Features:**
* **Experimental native TCP client.** `ClickHouseTcpClient` connects to ClickHouse over the native protocol (port 9000, or 9440 with TLS). Configure it with a connection string or `ClickHouseTcpClientOptions`. It reads rows, POCOs, or columnar blocks, and inserts rows, POCOs, or typed columns. In our benchmarks against the HTTP client, reads are about 2x faster, inserts are about 1.5x faster, and managed allocations are much lower. It ships in the `ClickHouse.Driver` package for .NET 8 and later. It is a separate API from `ClickHouseClient` and has no ADO.NET layer. The API can change in a future release, so using it causes warning `CHTCP0001`. See the [documentation](https://clickhouse.com/docs/integrations/csharp/tcp) and the [examples](https://github.com/ClickHouse/clickhouse-cs/tree/main/examples/Tcp).

**Improvements:**
* Added `RowIndex` to `ClickHouseBulkCopySerializationException` so binary-insert failures report the zero-based index of the offending row within the batch, making it easy to locate bad data in large inserts. (Thanks to @mbtolou - #606 and @Skyuzii - #608)

**Bug Fixes:**
* Fixed a wall-clock shift when reading a `DateTime`/`DateTime64` column whose `Fixed/UTC±HH:MM:SS` timezone name has a minute or second field above 59. The server carries such a field into the next one, so `Fixed/UTC+05:60:00` means +06:00; the driver rejected the name and read the value as UTC. (#612)
