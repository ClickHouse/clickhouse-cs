using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Builds native-protocol connection options from a connection string.</summary>
public static class TcpConnectionString
{
    public static async Task Run()
    {
        // Use a builder when configuration starts as a connection string but needs code-level changes.
        var builder = ExampleConfig.TcpBuilder();
        builder.Compression = "zstd";
        builder.MaxPoolSize = 4;
        builder.IdleTimeout = TimeSpan.FromMinutes(1);

        // Prefix server settings with set_ when they should apply to every query.
        builder["set_max_threads"] = 2;

        ClickHouseTcpClientOptions options = builder.ToOptions();
        Console.WriteLine($"Endpoint: {options.Host}:{options.Port ?? 9000}/{options.Database}");
        Console.WriteLine($"Compression: {options.Compressor?.GetType().Name ?? "none"}");
        Console.WriteLine($"Max pool size: {options.MaxPoolSize}");

        // Options are immutable records. Use with to derive a configuration variant.
        ClickHouseTcpClientOptions systemOptions = options with { Database = "system" };

        await using var client = new ClickHouseTcpClient(systemOptions);
        object database = await client.ExecuteScalarAsync("SELECT currentDatabase()");
        object maxThreads = await client.ExecuteScalarAsync("SELECT getSetting('max_threads')");

        Console.WriteLine($"Connected database: {database}");
        Console.WriteLine($"max_threads: {maxThreads}");
    }
}
