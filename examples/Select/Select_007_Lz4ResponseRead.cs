using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates reading LZ4-compressed query responses.
///
/// LZ4 is the cheapest codec for the ClickHouse server to produce, but .NET's
/// AutomaticDecompression cannot decode it (the driver's handler only decodes gzip and deflate).
/// Set <see cref="ClickHouseClientSettings.ResponseCompressor"/> and the driver decodes the
/// response body itself, so every read API — reader, scalar, non-query, raw — works normally.
///
/// Response compression is strictly opt-in: with no compressor configured the driver still
/// negotiates the default `gzip, deflate` and nothing changes. Setting one adds the codec's token
/// to `Accept-Encoding` and forces `enable_http_compression=1` on the URL.
///
/// Note that the *response's* `Content-Encoding` decides whether (and how) the body is decoded,
/// never what the client asked for: ClickHouse picks the codec by its own preference order
/// (zstd > br > lz4 > gzip) and ignores client ordering and q-values. gzip, deflate and br
/// responses are decoded too, whichever compressor is configured.
/// </summary>
public static class Lz4ResponseRead
{
    public static async Task Run()
    {
        var tableName = "example_lz4_response_read";

        // No custom HttpClient needed: `lz4` is not in the handler's AutomaticDecompression mask,
        // so the framework leaves Content-Encoding: lz4 in place and the driver decodes it.
        using var client = new ClickHouseClient(new ClickHouseClientSettings("Host=localhost")
        {
            ResponseCompressor = Lz4Compressor.Default,
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

        try
        {
            // Example 1: read rows exactly as usual — the LZ4 decoding is transparent
            Console.WriteLine("1. Reading rows over an LZ4-compressed response:");
            using (var reader = await client.ExecuteReaderAsync(
                $"SELECT id, name, salary FROM {tableName} ORDER BY id"))
            {
                Console.WriteLine("   ID\tName\t\t\tSalary");
                Console.WriteLine("   --\t----\t\t\t------");

                while (reader.Read())
                {
                    Console.WriteLine($"   {reader.GetFieldValue<ulong>(0)}\t{reader.GetString(1),-20}\t${reader.GetFloat(2):F2}");
                }
            }

            var total = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
            Console.WriteLine($"   ExecuteScalarAsync also works: {total} rows\n");

            // Example 2: the same thing configured from a connection string, for ORM users
            // (Dapper, EF Core, linq2db) who never touch ClickHouseClientSettings directly.
            Console.WriteLine("2. Via the connection string (ResponseCompression=lz4|gzip|br|none):");
            using (var csClient = new ClickHouseClient("Host=localhost;ResponseCompression=lz4"))
            {
                var count = await csClient.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
                Console.WriteLine($"   Rows read with ResponseCompression=lz4: {count}\n");
            }

            // Example 3: per-query override. QueryOptions.ResponseCompressor wins over the
            // client-level setting, so LZ4 can be enabled for one heavy export only.
            Console.WriteLine("3. Per-query override via QueryOptions.ResponseCompressor:");
            using (var plainClient = new ClickHouseClient("Host=localhost"))
            {
                var count = await plainClient.ExecuteScalarAsync(
                    $"SELECT count() FROM {tableName}",
                    options: new QueryOptions { ResponseCompressor = Lz4Compressor.Default });
                Console.WriteLine($"   Rows read with a per-query LZ4 compressor: {count}\n");
            }

            // Example 4: raw formats. ReadAsStreamAsync is a verbatim pass-through of the bytes on
            // the wire; ReadDecompressedStreamAsync applies the same decoding rules as the reader.
            Console.WriteLine("4. Raw export decoded with ReadDecompressedStreamAsync:");
            using (var result = await client.ExecuteRawResultAsync(
                $"SELECT * FROM {tableName} ORDER BY id FORMAT JSONEachRow"))
            {
                Console.WriteLine($"   Content-Encoding from server: {result.ContentEncoding ?? "(none)"}");

                await using var body = await result.ReadDecompressedStreamAsync();
                using var bodyReader = new StreamReader(body);
                Console.WriteLine($"   Decoded body:\n{await bodyReader.ReadToEndAsync()}");
            }

            Console.WriteLine("Summary:");
            Console.WriteLine("   - ResponseCompressor is opt-in; the default Accept-Encoding stays `gzip, deflate`");
            Console.WriteLine("   - Setting it advertises the codec and forces enable_http_compression=1");
            Console.WriteLine("   - The response's Content-Encoding decides what is decoded, not the request");
            Console.WriteLine("   - An unsupported codec (e.g. zstd) raises an error naming it, never garbage rows");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nTable '{tableName}' dropped");
        }
    }
}
