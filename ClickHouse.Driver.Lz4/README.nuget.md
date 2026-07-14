# ClickHouse.Driver.Lz4

LZ4 compression codec for [ClickHouse.Driver](https://www.nuget.org/packages/ClickHouse.Driver),
backed by [K4os.Compression.LZ4](https://www.nuget.org/packages/K4os.Compression.LZ4).

This is an **opt-in** package, kept separate so its third-party K4os dependency is only pulled in when
you actually use LZ4. Installing it provides an `IClickHouseCompressor` implementation — `Lz4Compressor`
— that you assign to `InsertOptions.Compressor` yourself. There is no automatic registration or DI wiring.

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
inserts where CPU (not bandwidth) is the constraint. It also imposes the least decompression load on the
ClickHouse server. Prefer `Lz4Compressor.Default` (fast mode) for almost all inserts — higher levels
cost markedly more CPU for little-to-no extra ratio on typical data. For the full compression tradeoff
and how to pick a codec, see the [driver documentation](https://github.com/ClickHouse/clickhouse-cs).
