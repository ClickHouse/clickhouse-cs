using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>Enables diagnostic logs for client, connection, and pool activity.</summary>
public static class TcpLogging
{
    public static async Task Run()
    {
        using ILoggerFactory loggerFactory = LoggerFactory.Create(logging => logging
            .AddFilter((category, level) => category switch
            {
                ClickHouseTcpDiagnostics.ClientLogCategory => level >= LogLevel.Debug,
                ClickHouseTcpDiagnostics.ConnectionLogCategory => level >= LogLevel.Debug,
                ClickHouseTcpDiagnostics.PoolLogCategory => level >= LogLevel.Trace,
                _ => false,
            })
            .AddSimpleConsole(console => console.SingleLine = true)
            .SetMinimumLevel(LogLevel.Trace));

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions() with
        {
            LoggerFactory = loggerFactory,
            StatementMaxLength = 80,
        };

        await using var client = new ClickHouseTcpClient(options);
        await client.PingAsync();
        object value = await client.ExecuteScalarAsync("SELECT 'logged query'");
        Console.WriteLine($"Query result: {value}");

        Console.WriteLine($"Client category: {ClickHouseTcpDiagnostics.ClientLogCategory}");
        Console.WriteLine($"Connection category: {ClickHouseTcpDiagnostics.ConnectionLogCategory}");
        Console.WriteLine($"Pool category: {ClickHouseTcpDiagnostics.PoolLogCategory}");

        // Debug logs can contain SQL. Set StatementMaxLength to 0 to omit statement text.
    }
}
