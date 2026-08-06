using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Explains the UseCompression setting and how compression works in the driver.
///
/// TL;DR: Compression is enabled by default and you rarely need to change it.
///
/// ## How It Works
///
/// UseCompression governs the **response** direction:
///
/// 1. **Responses (server → client)**, when UseCompression=true (the default):
///    - The driver sends enable_http_compression=true query parameter
///    - ClickHouse compresses the response with the codec it picks from Accept-Encoding — zstd by
///      default, since the driver advertises "zstd, lz4, gzip, deflate" and the server resolves that
///      by its own fixed preference order
///    - The driver decodes the body itself, from the response's Content-Encoding
///
/// 2. **Requests (client → server)** are not governed by this setting:
///    - A binary insert compresses its body with InsertOptions.Compressor — ZSTD level 3 by default —
///      and declares it with Content-Encoding; set that to null to send the body uncompressed
///    - An ordinary query sends its SQL uncompressed either way
///
/// ## When to Disable Compression
///
/// - **Low-latency/local connections**: Compression is a trade-off between CPU time and network time.
///   On localhost or fast networks, uncompressed may be faster for small payloads.
/// - **Already compressed data**: If you're inserting pre-compressed data or
///   data that doesn't compress well (random bytes, encrypted data).
///
/// ## Important: Custom HttpClient Configuration
///
/// If you provide your own HttpClient, leave AutomaticDecompression off. The driver decodes
/// compressed responses itself, so it is not needed — and it is not only a response-side setting:
/// at send time the handler also ADDS every algorithm in its mask to the outgoing Accept-Encoding,
/// so a mask widens what the driver advertised and ClickHouse may answer with a codec you did not
/// ask for. The HttpClient the driver builds for itself leaves the mask at DecompressionMethods.None.
///
/// ## InsertBinaryAsync
///
/// Note: InsertBinaryAsync compresses its uploads regardless of the UseCompression setting, because
/// bulk inserts benefit significantly from compression given the data volumes involved. The codec is
/// InsertOptions.Compressor — ZSTD level 3 by default, with GZip, Brotli and LZ4 also built in.
/// See Insert_002_BulkInsert for switching it.
/// </summary>
public static class Compression
{
    public static async Task Run()
    {
        Console.WriteLine("Compression Setting\n");

        // Default: compression enabled
        Console.WriteLine("1. Default behavior (compression enabled):");
        using (var client = new ClickHouseClient("Host=localhost"))
        {
            // The driver will:
            // - Request compressed responses via enable_http_compression=true
            // - Advertise "zstd, lz4, gzip, deflate" and decode whatever comes back
            var result = await client.ExecuteScalarAsync("SELECT 'Compressed response'");
            Console.WriteLine($"   Result: {result}");
            Console.WriteLine("   Response was zstd compressed and decoded by the driver\n");
        }

        // Compression disabled
        Console.WriteLine("2. Compression disabled:");
        using (var client = new ClickHouseClient("Host=localhost;Compression=false"))
        {
            // The driver will:
            // - Set enable_http_compression=false and advertise no codec (uncompressed responses)
            var result = await client.ExecuteScalarAsync("SELECT 'Uncompressed response'");
            Console.WriteLine($"   Result: {result}");
            Console.WriteLine("   Response was uncompressed\n");
        }

        // Using ClickHouseClientSettings
        Console.WriteLine("3. Via ClickHouseClientSettings:");
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            UseCompression = false  // Disable compression
        };
        using (var client = new ClickHouseClient(settings))
        {
            var result = await client.ExecuteScalarAsync("SELECT 1");
            Console.WriteLine($"   UseCompression = {settings.UseCompression}");
            Console.WriteLine($"   Result: {result}\n");
        }

        Console.WriteLine("Summary:");
        Console.WriteLine("   - Default: UseCompression=true (recommended for most cases)");
        Console.WriteLine("   - Reduces response bandwidth; request bodies are InsertOptions.Compressor's job");
        Console.WriteLine("   - Consider disabling for localhost or small, frequent queries");
        Console.WriteLine("   - Custom HttpClient: leave AutomaticDecompression off; the driver decodes responses itself");
    }
}
