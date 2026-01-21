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
/// When UseCompression=true (the default):
///
/// 1. **Requests (client → server)**:
///    - The driver compresses the request body using GZip
///    - Adds Content-Encoding: gzip header
///
/// 2. **Responses (server → client)**:
///    - The driver sends enable_http_compression=true query parameter
///    - ClickHouse compresses the response with GZip
///    - HttpClient automatically decompresses it (via AutomaticDecompression)
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
/// If you provide your own HttpClient, you MUST configure AutomaticDecompression
/// when compression is enabled, otherwise you'll get an error when reading responses:
///
///     var handler = new HttpClientHandler
///     {
///         AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
///     };
///     var httpClient = new HttpClient(handler);
///
/// The driver's default HttpClient already has this configured.
///
/// ## ClickHouseBulkCopy
///
/// Note: ClickHouseBulkCopy always uses GZip compression for uploads regardless
/// of the UseCompression setting. This is because bulk inserts benefit significantly
/// from compression due to the large data volumes involved.
/// </summary>
public static class Compression
{
    public static async Task Run()
    {
        Console.WriteLine("Compression Setting\n");

        // Default: compression enabled
        Console.WriteLine("1. Default behavior (compression enabled):");
        using (var connection = new ClickHouseConnection("Host=localhost"))
        {
            await connection.OpenAsync();

            // The driver will:
            // - Compress request bodies with GZip
            // - Request compressed responses via enable_http_compression=true
            var result = await connection.ExecuteScalarAsync("SELECT 'Compressed request and response'");
            Console.WriteLine($"   Result: {result}");
            Console.WriteLine("   Request was GZip compressed, response was GZip compressed\n");
        }

        // Compression disabled
        Console.WriteLine("2. Compression disabled:");
        using (var connection = new ClickHouseConnection("Host=localhost;Compression=false"))
        {
            await connection.OpenAsync();

            // The driver will:
            // - Send uncompressed request bodies
            // - Set enable_http_compression=false (uncompressed responses)
            var result = await connection.ExecuteScalarAsync("SELECT 'Uncompressed request and response'");
            Console.WriteLine($"   Result: {result}");
            Console.WriteLine("   Request was uncompressed, response was uncompressed\n");
        }

        // Using ClickHouseClientSettings
        Console.WriteLine("3. Via ClickHouseClientSettings:");
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            UseCompression = false  // Disable compression
        };
        using (var connection = new ClickHouseConnection(settings))
        {
            await connection.OpenAsync();
            var result = await connection.ExecuteScalarAsync("SELECT 1");
            Console.WriteLine($"   UseCompression = {settings.UseCompression}");
            Console.WriteLine($"   Result: {result}\n");
        }

        Console.WriteLine("Summary:");
        Console.WriteLine("   - Default: UseCompression=true (recommended for most cases)");
        Console.WriteLine("   - Reduces bandwidth for both requests and responses");
        Console.WriteLine("   - Consider disabling for localhost or small, frequent queries");
        Console.WriteLine("   - Custom HttpClient must have AutomaticDecompression configured");
    }
}
