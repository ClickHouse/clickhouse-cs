using System.Net;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates negotiating a transport encoding per-query for a raw export.
/// Uses <see cref="QueryOptions.AcceptEncoding"/> to request gzip compression on
/// a single query and streams the compressed Parquet body straight to a file.
/// </summary>
public static class CompressedRawExport
{
    public static async Task Run()
    {
        var connectionString = "Host=localhost";
        var tableName = "example_compressed_export";

        // The default HttpClient enables AutomaticDecompression for gzip/deflate/brotli,
        // which transparently decompresses the body before the driver sees it.
        // For a raw export we want the bytes on the wire — turn it off.
        using var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var httpClient = new HttpClient(handler);
        using var client = new ClickHouseClient(new ClickHouseClientSettings(connectionString)
        {
            HttpClient = httpClient,
            // UseCompression=true (the default) makes the driver attach Accept-Encoding: gzip
            // to every request. Since this HttpClient won't decompress, the regular
            // CREATE/INSERT/DROP responses would come back as raw gzip and fail to parse.
            // Disable the client-wide default and negotiate compression per-query instead.
            UseCompression = false,
        });

        // Create and populate a test table
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                salary Float32
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice Johnson", 95000f },
            new object[] { 2UL, "Bob Smith", 75000f },
            new object[] { 3UL, "Carol White", 105000f },
        };
        await client.InsertBinaryAsync(tableName, new[] { "id", "name", "salary" }, rows);

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        var outputFile = Path.Combine(Path.GetTempPath(), $"clickhouse_export_{Guid.NewGuid()}.parquet.gz");

        try
        {
            // AcceptEncoding overrides the default `gzip, deflate` for this request and
            // forces enable_http_compression=1 on the URL so ClickHouse honours it.
            using var result = await client.ExecuteRawResultAsync(
                $"SELECT * FROM {tableName} FORMAT Parquet",
                options: new QueryOptions { AcceptEncoding = "gzip" });

            Console.WriteLine($"Content-Encoding from server: {result.ContentEncoding ?? "(none)"}");

            await using (var fileStream = File.Create(outputFile))
            {
                await result.CopyToAsync(fileStream);
            }

            var fileInfo = new FileInfo(outputFile);
            Console.WriteLine($"Exported gzipped Parquet to: {outputFile}");
            Console.WriteLine($"File size: {fileInfo.Length} bytes");
        }
        finally
        {
            if (File.Exists(outputFile))
            {
                File.Delete(outputFile);
                Console.WriteLine($"\nCleaned up temporary file: {outputFile}");
            }

            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"Table '{tableName}' dropped");
        }
    }
}
