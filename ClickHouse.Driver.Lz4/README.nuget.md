# ClickHouse.Driver.Lz4

LZ4 compression codec for [ClickHouse.Driver](https://www.nuget.org/packages/ClickHouse.Driver),
backed by [K4os.Compression.LZ4](https://www.nuget.org/packages/K4os.Compression.LZ4).

This is an **opt-in** package. The core `ClickHouse.Driver` package deliberately ships with a
Microsoft-only dependency set; installing this package adds the third-party K4os LZ4 dependency and
registers an `IClickHouseCompressor` implementation you can plug into binary inserts.

## Usage

```csharp
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Utility;

using var client = new ClickHouseClient("Host=localhost");

var options = new InsertOptions
{
    Compressor = Lz4Compressor.Default, // Content-Encoding: lz4
};

await client.InsertBinaryAsync(table, columns, rows, options);
```

LZ4 is much faster than GZip/Brotli at a lower compression ratio, which favours throughput-bound
inserts where CPU (not bandwidth) is the constraint. For the compression tradeoff and how to pick a
codec, see the [driver documentation](https://github.com/ClickHouse/clickhouse-cs).
