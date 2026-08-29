using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Selects LZ4, Zstandard, or no native block compression.</summary>
public static class TcpCompression
{
    public static async Task Run()
    {
        // Compression applies to native data blocks; it does not change query results.
        foreach (string codec in new[] { "lz4", "zstd", "none" })
        {
            var builder = ExampleConfig.TcpBuilder();
            builder.Compression = codec;
            ClickHouseTcpClientOptions options = builder.ToOptions();

            await using var client = new ClickHouseTcpClient(options);
            object rows = await client.ExecuteScalarAsync("SELECT count() FROM numbers(10000)");

            string compressor = options.Compressor?.GetType().Name ?? "none";
            Console.WriteLine($"Compression={codec,-4} -> {compressor,-20}; rows={rows}");
        }

        Console.WriteLine("LZ4 is the default. Zstandard usually trades more CPU for smaller payloads.");
        Console.WriteLine("Choose a codec with measurements from your workload and network.");
    }
}
