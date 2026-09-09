using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Selects LZ4, Zstandard, or no native block compression.</summary>
public static class TcpCompression
{
    private const string TableName = "example_tcp_compression";

    public static async Task Run()
    {
        // Compression governs what an insert writes. It does not choose what the server sends: the request
        // carries a flag and no codec name, so the server frames its blocks with network_compression_method.
        var rows = new ulong[20000];
        for (int i = 0; i < rows.Length; i++)
        {
            rows[i] = (ulong)i;
        }

        foreach (string codec in new[] { "lz4", "zstd", "none" })
        {
            var builder = ExampleConfig.TcpBuilder();
            builder.Compression = codec;
            ClickHouseTcpClientOptions options = builder.ToOptions();

            await using var client = new ClickHouseTcpClient(options);
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            try
            {
                await client.ExecuteAsync($"CREATE TABLE {TableName} (id UInt64) ENGINE = Memory");

                long uncompressed = 0;
                long compressed = 0;
                await client.InsertAsync(
                    $"INSERT INTO {TableName} (id) VALUES",
                    new IColumn[] { ClickHouseTcpColumn.Create("id", rows) },
                    new ClickHouseTcpInsertOptions
                    {
                        Callbacks = new ClickHouseTcpQueryCallbacks
                        {
                            OnBlockWritten = block =>
                            {
                                uncompressed += block.UncompressedBytes;
                                compressed += block.CompressedBytes;
                            },
                        },
                    });

                string compressor = options.Compressor?.GetType().Name ?? "none";
                Console.WriteLine(
                    $"Compression={codec,-4} -> {compressor,-20}; insert wrote {uncompressed} bytes as {compressed}");
            }
            finally
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            }
        }

        // Set network_compression_method to change what a query result is compressed with.
        Console.WriteLine("LZ4 is the default. Zstandard usually trades more CPU for smaller payloads.");
        Console.WriteLine("Choose a codec with measurements from your workload and network.");
    }
}
