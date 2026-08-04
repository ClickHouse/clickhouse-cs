using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates transport compression of query responses.
///
/// There is nothing to configure for the common case: the driver advertises the codecs it can decode
/// (`lz4, gzip, deflate`), ClickHouse answers with LZ4, and the driver decodes the body — so every read
/// API works normally over a compressed response. This example shows how to observe and override that.
///
/// The server picks the codec, not the client: ClickHouse scans `Accept-Encoding` for tokens in its own
/// fixed preference order (zstd > br > lz4 > snappy > gzip > deflate) and ignores both our ordering and
/// q-values, so the only way to steer it is which tokens are listed.
/// </summary>
public static class ResponseCompression
{
    public static async Task Run()
    {
        var tableName = "example_response_compression";

        // A default client. No custom HttpClient and no compression settings needed.
        using var client = new ClickHouseClient("Host=localhost");

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
            // Example 1: reading rows. The response is LZ4-compressed on the wire and decoded on the
            // way in; nothing about the reading code is different.
            Console.WriteLine("1. Reading rows over a compressed response (nothing configured):");
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

            // Example 2: which codec the server actually chose. ExecuteRawResultAsync is the only path
            // that exposes it, and it advertises no codec of its own, so ask for one explicitly here.
            Console.WriteLine("2. Observing the negotiated codec:");
            foreach (var acceptEncoding in new[] { "lz4, gzip, deflate", "br", "gzip", "identity" })
            {
                using var probe = await client.ExecuteRawResultAsync(
                    $"SELECT * FROM {tableName} FORMAT JSONEachRow",
                    options: new QueryOptions { AcceptEncoding = acceptEncoding });

                Console.WriteLine($"   Accept-Encoding: {acceptEncoding,-20} -> Content-Encoding: {probe.ContentEncoding ?? "(none)"}");
            }

            Console.WriteLine("   (each line is the codec the server chose; `identity` reports none because");
            Console.WriteLine("    the response really is uncompressed. Nothing strips Content-Encoding here:");
            Console.WriteLine("    the driver's HttpClient leaves AutomaticDecompression off and decodes itself.)");
            Console.WriteLine();

            // Example 3: overriding the codec client-wide. Brotli compresses far better than LZ4 at a
            // higher server-side cost, which is worth it on a slow link and not on a fast one.
            Console.WriteLine("3. Choosing brotli client-wide:");
            using (var brotliClient = new ClickHouseClient(new ClickHouseClientSettings("Host=localhost")
            {
                AcceptEncoding = "br",
            }))
            {
                var count = await brotliClient.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
                Console.WriteLine($"   Rows read over a brotli response: {count}");
            }

            // The same thing through a connection string, for ORM users (Dapper, EF Core, linq2db)
            // who never touch ClickHouseClientSettings directly.
            using (var csClient = new ClickHouseClient("Host=localhost;AcceptEncoding=br"))
            {
                var count = await csClient.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
                Console.WriteLine($"   Rows read with AcceptEncoding=br in the connection string: {count}\n");
            }

            // Example 4: opting a single query out, e.g. one already-tiny result where the round trip
            // through a codec buys nothing.
            Console.WriteLine("4. Opting one query out with `identity`:");
            var uncompressed = await client.ExecuteScalarAsync(
                $"SELECT count() FROM {tableName}",
                options: new QueryOptions { AcceptEncoding = "identity" });
            Console.WriteLine($"   Rows read uncompressed: {uncompressed}\n");

            // Example 5: raw exports. These are handed to you verbatim, so the driver asks for no codec
            // at all and the body below arrives as plaintext JSON — exactly as the server sent it. Ask
            // for a codec and you get those bytes untouched; ReadDecompressedStreamAsync will decode them
            // for you, ReadAsStreamAsync will not.
            Console.WriteLine("5. Raw exports stay readable unless you ask for a codec:");
            using (var result = await client.ExecuteRawResultAsync(
                $"SELECT * FROM {tableName} ORDER BY id FORMAT JSONEachRow"))
            {
                Console.WriteLine($"   Content-Encoding: {result.ContentEncoding ?? "(none)"}");
                Console.WriteLine($"   Body:\n{await result.ReadAsStringAsync()}");
            }

            using (var result = await client.ExecuteRawResultAsync(
                $"SELECT * FROM {tableName} ORDER BY id FORMAT JSONEachRow",
                options: new QueryOptions { AcceptEncoding = "lz4" }))
            {
                Console.WriteLine($"   With AcceptEncoding=lz4 -> Content-Encoding: {result.ContentEncoding ?? "(none)"}");

                await using var body = await result.ReadDecompressedStreamAsync();
                using var bodyReader = new StreamReader(body);
                Console.WriteLine($"   Decoded body:\n{await bodyReader.ReadToEndAsync()}");
            }

            Console.WriteLine("Summary:");
            Console.WriteLine("   - By default the driver advertises `lz4, gzip, deflate` and decodes whatever comes back");
            Console.WriteLine("   - The server chooses from that list by its own preference order, ignoring q-values");
            Console.WriteLine("   - Override with AcceptEncoding on the settings, the connection string, or one query");
            Console.WriteLine("   - Raw exports stay verbatim: no codec is asked for unless you set AcceptEncoding");
            Console.WriteLine("   - An undecodable codec (zstd, snappy) raises an error naming it, never garbage rows");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nTable '{tableName}' dropped");
        }
    }
}
