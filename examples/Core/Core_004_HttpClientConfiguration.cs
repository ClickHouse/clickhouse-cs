using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to provide your own HttpClient or IHttpClientFactory to ClickHouse connections.
/// This is important for:
/// - Custom SSL/TLS configuration (certificates, validation)
/// - Proxy configuration
/// - Custom timeout settings
/// - Connection pooling control
/// - Integration with dependency injection
///
/// IMPORTANT LIMITATIONS:
/// - When providing your own HttpClient, YOU are responsible for:
///   * Enabling automatic decompression (required if compression is not disabled)
///   * Setting appropriate timeouts
///   * Certificate validation
///   * Disposal (if not using DI).
/// It is recommended that you reuse and do not dispose HttpSocketsHandler/HttpClientHandler,
/// as they maintain internal connection pools.
///
/// IMPORTANT: Connection Idle Timeout
/// - If you provide your own HttpClient/HttpClientFactory, ensure that the client-side
///   idle connection timeout is SMALLER than the server's keep_alive_timeout setting.
/// - ClickHouse Cloud default keep_alive_timeout: 10 seconds
/// - If client timeout >= server timeout, you may encounter half-open connections that
///   fail with "connection was closed" errors
/// </summary>
public static class HttpClientConfiguration
{
    public static async Task Run()
    {
        Console.WriteLine("HttpClient Configuration Examples\n");

        // Example 1: Basic custom HttpClient
        Console.WriteLine("1. Providing a custom HttpClient:");
        await Example1_CustomHttpClient();

        // Example 2: SSL/TLS configuration
        Console.WriteLine("\n2. Custom SSL/TLS configuration:");
        await Example2_SslConfiguration();

        // Example 3: Proxy configuration
        Console.WriteLine("\n3. Proxy configuration:");
        await Example3_ProxyConfiguration();

        // Example 4: Using IHttpClientFactory (without DI)
        Console.WriteLine("\n4. Using IHttpClientFactory directly:");
        await Example4_HttpClientFactory();

        Console.WriteLine("\nAll HttpClient configuration examples completed!");
    }

    private static async Task Example1_CustomHttpClient()
    {
        // Create a custom HttpClient with specific configuration
        var handler = new SocketsHttpHandler
        {
            // REQUIRED: Enable automatic decompression for ClickHouse compression
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,

            // Optional: Configure other handler settings
            MaxConnectionsPerServer = 10,
            UseProxy = false,

            // IMPORTANT: Set idle timeout smaller than server's keep_alive_timeout
            // ClickHouse Cloud default keep_alive_timeout: 10 seconds
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5),
        };

        var httpClient = new HttpClient(handler)
        {
            // Set custom timeout for individual requests
            Timeout = TimeSpan.FromMinutes(5),
        };

        // Pass the HttpClient to the connection
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            HttpClient = httpClient,
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var version = await connection.ExecuteScalarAsync("SELECT version()");
        Console.WriteLine($"   Connected to ClickHouse version: {version}");
    }

    private static async Task Example2_SslConfiguration()
    {
        Console.WriteLine("   Configuring SSL/TLS for secure connections:");

        // Example A: Skip certificate validation (NOT RECOMMENDED for production)
        Console.WriteLine("\n   A. Skip certificate validation (development/testing only):");
        var insecureSettings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Protocol = "https",
            SkipServerCertificateValidation = true, // Use built-in option
        };

        Console.WriteLine("     Using SkipServerCertificateValidation = true");

        // Example B: Custom certificate validation
        Console.WriteLine("\n   B. Custom certificate validation:");

        var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            ServerCertificateCustomValidationCallback = (message, cert, chain, sslPolicyErrors) =>
            {
                // Example: Accept specific certificate thumbprint
                if (cert?.Thumbprint == "YOUR_EXPECTED_THUMBPRINT")
                {
                    return true;
                }

                // Example: Accept certificates from specific issuer
                if (cert?.Issuer.Contains("YourOrganization") == true)
                {
                    return true;
                }

                // Example: Log and accept self-signed certificates in development
                if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
                {
                    Console.WriteLine($"       Certificate validation: {cert?.Subject}");
                    // return true; // Uncomment to accept in development
                }

                // Default: Use standard validation
                return sslPolicyErrors == SslPolicyErrors.None;
            },
        };

        var httpClient = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

        var secureSettings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Protocol = "https",
            HttpClient = httpClient,
        };

        // Note: Not executing this example as it requires HTTPS setup
        // using var connection = new ClickHouseConnection(secureSettings);
        // await connection.OpenAsync();
    }

    private static async Task Example3_ProxyConfiguration()
    {
        Console.WriteLine("   Configuring HTTP proxy:");

        var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,

            // Enable proxy
            UseProxy = true,
            Proxy = new WebProxy
            {
                Address = new Uri("http://proxy.example.com:8080"),

                // Proxy credentials if required
                Credentials = new NetworkCredential("proxyuser", "proxypassword"),

                // Bypass proxy for local addresses
                BypassProxyOnLocal = true,

                // Specific addresses to bypass
                BypassList = new[] { "localhost", "127.0.0.1", "internal.company.com" },
            },
        };

        var httpClient = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

        var settings = new ClickHouseClientSettings
        {
            Host = "clickhouse.example.com",
            HttpClient = httpClient,
        };

        // Note: Not executing this example as it requires proxy setup
        // using var connection = new ClickHouseConnection(settings);
        // await connection.OpenAsync();
    }

    private static async Task Example4_HttpClientFactory()
    {
        Console.WriteLine("   Using IHttpClientFactory without DI:");
        Console.WriteLine("   (See Core_003_DependencyInjection.cs for DI integration)\n");

        // You can create a simple factory implementation
        var factory = new SimpleHttpClientFactory();

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            HttpClientFactory = factory,
            HttpClientName = "ClickHouseClient", // Optional: factory can use this name
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var version = await connection.ExecuteScalarAsync("SELECT version()");
        Console.WriteLine($"   Connected using IHttpClientFactory: {version}");

        // Factory handles disposal
    }

    // Simple IHttpClientFactory implementation for demonstration
    private class SimpleHttpClientFactory : IHttpClientFactory
    {
        private readonly HttpClient _client;

        public SimpleHttpClientFactory()
        {
            var handler = new SocketsHttpHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                MaxConnectionsPerServer = 10, // Controls connection pool size
                PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5), // Always set to a value lower than server-side idle timeout (10s for Cloud)
            };

            _client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromMinutes(5),
            };
        }

        public HttpClient CreateClient(string name)
        {
            return _client;
        }
    }
}
