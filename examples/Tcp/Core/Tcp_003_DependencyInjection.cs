using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.Driver.Examples;

/// <summary>Registers the native client with Microsoft dependency injection.</summary>
public static class TcpDependencyInjection
{
    public static async Task Run()
    {
        await RegisterOneClient();
        await RegisterMultipleClients();
    }

    private static async Task RegisterOneClient()
    {
        var services = new ServiceCollection();
        services.AddClickHouseTcpDataSource(ExampleConfig.TcpConnectionString);
        services.AddSingleton<ServerProbe>();

        await using ServiceProvider provider = services.BuildServiceProvider();

        var probe = provider.GetRequiredService<ServerProbe>();
        Console.WriteLine(await probe.GetVersionAsync());

        var dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();
        var client = provider.GetRequiredService<IClickHouseTcpClient>();
        Console.WriteLine($"The data source owns the injected client: " +
                          $"{ReferenceEquals(client, dataSource.GetClient())}");

        // The service provider owns the data source and its pool. Do not dispose injected clients.
    }

    private static async Task RegisterMultipleClients()
    {
        var services = new ServiceCollection();
        services.AddClickHouseTcpDataSource(
            ExampleConfig.TcpConnectionString,
            serviceKey: "ingest");
        services.AddClickHouseTcpDataSource(
            ExampleConfig.TcpBuilder().ToOptions() with { MaxPoolSize = 2 },
            serviceKey: "reporting");

        await using ServiceProvider provider = services.BuildServiceProvider();

        var ingest = provider.GetRequiredKeyedService<IClickHouseTcpClient>("ingest");
        var reporting = provider.GetRequiredKeyedService<IClickHouseTcpClient>("reporting");

        await ingest.PingAsync();
        await reporting.PingAsync();
        Console.WriteLine("Both keyed clients connected successfully.");
    }

    private sealed class ServerProbe(IClickHouseTcpClient client)
    {
        public async Task<string> GetVersionAsync()
        {
            ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();
            return $"ClickHouse {server.Version}";
        }
    }
}
