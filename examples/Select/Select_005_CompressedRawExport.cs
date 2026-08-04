using System.Net;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates streaming a compressed raw export straight to a file.
///
/// A raw result hands you the bytes as they arrived, so the driver only ever negotiates `gzip, deflate`
/// for it — the two codecs an <c>HttpClient</c> can decode for itself. To get compressed bytes on disk,
/// name a codec with <see cref="QueryOptions.AcceptEncoding"/>; here that is `lz4`, which ClickHouse
/// compresses cheaply.
///
/// Two things are needed to be sure of what lands on disk: naming the codec, and an <c>HttpClient</c> with
/// <c>AutomaticDecompression</c> off, so the framework neither widens the offer nor decodes the answer.
///
/// Note the contrast with <see cref="ResponseCompression"/>: the reading APIs decode transparently, and
/// only these raw members are verbatim.
/// </summary>
public static class CompressedRawExport
{
    public static async Task Run()
    {
        var connectionString = "Host=localhost";
        var tableName = "example_compressed_export";

        // A raw export wants the bytes exactly as they arrived, so turn off the framework's own
        // decoding: .NET otherwise adds the codecs its mask covers to Accept-Encoding, and a server
        // that did not offer lz4 would fall back to gzip and have it silently decoded — plaintext in
        // a .lz4 file. With the mask off, this request advertises lz4 and nothing else.
        using var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var httpClient = new HttpClient(handler);

        // Note what is NOT needed here: the CREATE/INSERT/DROP calls below still negotiate compression
        // as usual, because the driver decodes their responses itself rather than relying on the
        // HttpClient. Only the verbatim raw members hand you the bytes undecoded.
        using var client = new ClickHouseClient(new ClickHouseClientSettings(connectionString)
        {
            HttpClient = httpClient,
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

        var outputFile = Path.Combine(Path.GetTempPath(), $"clickhouse_export_{Guid.NewGuid()}.parquet.lz4");

        try
        {
            // AcceptEncoding asks the server to compress this response and forces
            // enable_http_compression=1 on the URL so ClickHouse honours it.
            using var result = await client.ExecuteRawResultAsync(
                $"SELECT * FROM {tableName} FORMAT Parquet",
                options: new QueryOptions { AcceptEncoding = "lz4" });

            Console.WriteLine($"Content-Encoding from server: {result.ContentEncoding ?? "(none)"}");

            // CopyToAsync (like ReadAsStreamAsync/ReadAsByteArrayAsync/ReadAsStringAsync) is a verbatim
            // pass-through, so what lands on disk is the compressed body. Use
            // ReadDecompressedStreamAsync instead when you want the driver to decode it for you.
            await using (var fileStream = File.Create(outputFile))
            {
                await result.CopyToAsync(fileStream);
            }

            var fileInfo = new FileInfo(outputFile);
            Console.WriteLine($"Exported LZ4-compressed Parquet to: {outputFile}");
            Console.WriteLine($"File size: {fileInfo.Length} bytes");

            // Because the mask is off, "gzip" or "deflate" would work here just as well and land
            // compressed. On a default HttpClient they would not: .NET decodes those two itself and
            // strips Content-Encoding, so the bytes would arrive already decompressed.
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
