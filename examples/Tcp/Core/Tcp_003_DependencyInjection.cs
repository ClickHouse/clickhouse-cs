using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Registering the native-protocol client in an <see cref="IServiceCollection"/> with
/// <c>AddClickHouseTcpDataSource</c>: the connection-string and options overloads, injecting
/// <see cref="IClickHouseTcpClient"/> into a consumer, the keyed overload for two clusters, and the one rule that
/// matters — the pool is a singleton, so nothing injected may dispose it.
/// </summary>
public static class TcpDependencyInjection
{
    public static async Task Run()
    {
        Console.WriteLine("One call registers three services, all singletons:\n");
        Console.WriteLine("  ClickHouseTcpDataSource    owns the client and its connection pool; the container disposes it");
        Console.WriteLine("  IClickHouseTcpClient       the client that data source owns — queries, inserts, sessions");
        Console.WriteLine("  IClickHouseTcpOperations   the same object again, for code that only runs operations");

        await FromConnectionString();
        await FromOptions();
        await TwoClusters();
        await WhoDisposesWhat();
    }

    private static async Task FromConnectionString()
    {
        Console.WriteLine("\n1. From a connection string:\n");

        var services = new ServiceCollection();
        services.AddClickHouseTcpDataSource(ExampleConfig.TcpConnectionString);

        // A consumer takes the interface. Registered through a factory here only because this example's consumer
        // is a private nested type; a normal AddSingleton<ServerProbe>() reaches the same client, and a keyed one
        // is reached with [FromKeyedServices("key")] on the constructor parameter.
        services.AddSingleton(sp => new ServerProbe(sp.GetRequiredService<IClickHouseTcpClient>()));

        await using ServiceProvider provider = services.BuildServiceProvider();

        var probe = provider.GetRequiredService<ServerProbe>();
        Console.WriteLine($"   ServerProbe (injected IClickHouseTcpClient): {await probe.DescribeAsync()}");

        // Every registration resolves the one client the data source owns, so there is one pool per registration
        // however many consumers there are.
        var dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();
        var client = provider.GetRequiredService<IClickHouseTcpClient>();
        var operations = provider.GetRequiredService<IClickHouseTcpOperations>();

        Console.WriteLine($"   IClickHouseTcpClient is dataSource.GetClient():  {ReferenceEquals(client, dataSource.GetClient())}");
        Console.WriteLine($"   IClickHouseTcpOperations is the same object:     {ReferenceEquals(client, operations)}");
    }

    private static async Task FromOptions()
    {
        Console.WriteLine("\n2. From options, with the container's logging:\n");

        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Options are a record, so the shape that differs from the connection string is a 'with' away.
        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions() with
        {
            MaxPoolSize = 4,
            IdleTimeout = TimeSpan.FromSeconds(30),
        };

        services.AddClickHouseTcpDataSource(options);

        await using ServiceProvider provider = services.BuildServiceProvider();

        var dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();
        Console.WriteLine($"   {dataSource.Options}");
        Console.WriteLine($"   MaxPoolSize {dataSource.Options.MaxPoolSize}, IdleTimeout {dataSource.Options.IdleTimeout}");

        // The registration fills in a null LoggerFactory from the container, on a copy: the options object passed
        // in is left alone, so the two are no longer the same instance.
        Console.WriteLine($"   LoggerFactory on the options passed in:   {options.LoggerFactory?.GetType().Name ?? "(null)"}");
        Console.WriteLine($"   LoggerFactory the data source runs with:  {dataSource.Options.LoggerFactory?.GetType().Name ?? "(null)"}");

        // There is also an overload taking Func<IServiceProvider, ClickHouseTcpClientOptions>, for options that
        // need something else out of the container, and one taking a Func that builds the data source itself.
        object value = await provider.GetRequiredService<IClickHouseTcpClient>().ExecuteScalarAsync("SELECT 'registered from options'");
        Console.WriteLine($"   SELECT returned: {value}");
    }

    private static async Task TwoClusters()
    {
        Console.WriteLine("\n3. Two clusters, told apart by service key:\n");

        var services = new ServiceCollection();

        // A second unkeyed call would be a no-op: every service is added with TryAdd, so the first registration
        // of a service and key wins. A key is what makes the second registration a different service.
        services.AddClickHouseTcpDataSource(ExampleConfig.TcpConnectionString, serviceKey: "ingest");
        services.AddClickHouseTcpDataSource(
            ExampleConfig.TcpBuilder().ToOptions() with { MaxPoolSize = 2 },
            serviceKey: "reporting");

        // Both point at this example's one server; in a real application they would be different endpoints.
        services.AddSingleton(sp => new ServerProbe(sp.GetRequiredKeyedService<IClickHouseTcpClient>("reporting")));

        await using ServiceProvider provider = services.BuildServiceProvider();

        var ingest = provider.GetRequiredKeyedService<IClickHouseTcpClient>("ingest");
        var reporting = provider.GetRequiredKeyedService<IClickHouseTcpClient>("reporting");

        Console.WriteLine($"   'ingest'     MaxPoolSize {ingest.Options.MaxPoolSize}, pool of its own: {!ReferenceEquals(ingest, reporting)}");
        Console.WriteLine($"   'reporting'  MaxPoolSize {reporting.Options.MaxPoolSize}");
        Console.WriteLine($"   ServerProbe holding the 'reporting' client: {await provider.GetRequiredService<ServerProbe>().DescribeAsync()}");

        // Keyed registrations are not also unkeyed, so plain injection finds nothing. Key every consumer, or
        // register one of the endpoints without a key as well.
        Console.WriteLine($"   An unkeyed IClickHouseTcpClient is registered: {provider.GetService<IClickHouseTcpClient>() is not null}");
    }

    private static async Task WhoDisposesWhat()
    {
        Console.WriteLine("\n4. Who disposes what:\n");
        Console.WriteLine("   The data source owns the pool and the container owns the data source, so the pool");
        Console.WriteLine("   closes once, at shutdown, when the provider is disposed. Prefer DisposeAsync where the");
        Console.WriteLine("   call site can await it, as a generic host does.");
        Console.WriteLine();
        Console.WriteLine("   Never dispose an injected client. It offers DisposeAsync because a session needs one,");
        Console.WriteLine("   but disposing it closes the shared pool and every other consumer's next operation");
        Console.WriteLine("   fails. A session from OpenSessionAsync is the opposite: it is yours to dispose.");

        ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(ExampleConfig.TcpConnectionString)
            .BuildServiceProvider();

        var client = provider.GetRequiredService<IClickHouseTcpClient>();
        await client.PingAsync();
        Console.WriteLine("\n   Ping before shutdown: answered");

        await provider.DisposeAsync();

        // What a consumer that disposed the client would leave behind for everyone else.
        try
        {
            await client.PingAsync();
        }
        catch (ObjectDisposedException)
        {
            Console.WriteLine("   Ping after the provider was disposed: ObjectDisposedException, as it should be");
        }
    }

    // Takes IClickHouseTcpClient rather than the concrete client, so a test can substitute a double. Holds no
    // disposal logic: the container owns the client's lifetime.
    private sealed class ServerProbe(IClickHouseTcpClient client)
    {
        public async Task<string> DescribeAsync()
        {
            ClickHouseTcpServerInfo info = await client.GetServerInfoAsync();
            return $"{info}, protocol revision {info.ProtocolRevision}";
        }
    }
}
