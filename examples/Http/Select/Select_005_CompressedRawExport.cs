using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates streaming a compressed raw export straight to a file.
///
/// A raw result hands you the bytes exactly as they arrived, so the driver asks for no compression on
/// these requests and the server answers with plaintext. To get compressed bytes on disk, name a codec
/// with <see cref="QueryOptions.AcceptEncoding"/> — here `lz4`, which ClickHouse compresses cheaply.
///
/// Note the contrast with <see cref="ResponseCompression"/>: the reading APIs decompress for you, and
/// only these raw members hand back the body untouched.
/// </summary>
public static class CompressedRawExport
{
    public static async Task Run()
    {
        var connectionString = ExampleConfig.HttpConnectionString;
        var tableName = "example_compressed_export";

        // Nothing to configure for this to work: the driver decompresses responses itself, so a raw
        // export needs no special HttpClient. If you do supply your own, leave AutomaticDecompression at
        // its default (None) — .NET adds the codecs its mask covers to your Accept-Encoding and decodes
        // the answer behind your back, which would land plaintext in the .lz4 file below.
        using var client = new ClickHouseClient(connectionString);

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

        var outputFile = Path.Combine(Path.GetTempPath(), $"clickhouse_export_{Guid.NewGuid()}.parquet.lz4");

        try
        {
            // AcceptEncoding asks the server to compress this response, and switches on the server-side
            // setting ClickHouse needs before it will honour the request.
            using var result = await client.ExecuteRawResultAsync(
                $"SELECT * FROM {tableName} FORMAT Parquet",
                options: new QueryOptions { AcceptEncoding = "lz4" });

            Console.WriteLine($"Content-Encoding from server: {result.ContentEncoding ?? "(none)"}");

            // CopyToAsync (like ReadAsStreamAsync/ReadAsByteArrayAsync/ReadAsStringAsync) hands back the
            // body untouched, so what lands on disk is the compressed bytes. Use
            // ReadDecompressedStreamAsync instead when you want the driver to decompress it for you.
            await using (var fileStream = File.Create(outputFile))
            {
                await result.CopyToAsync(fileStream);
            }

            var fileInfo = new FileInfo(outputFile);
            Console.WriteLine($"Exported LZ4-compressed Parquet to: {outputFile}");
            Console.WriteLine($"File size: {fileInfo.Length} bytes");

            // "gzip", "deflate" and "br" work here too, and also land compressed.
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
