using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace ClickHouse.Driver.Tcp.Tests.DependencyInjection;

/// <summary>
/// Covers the registrations themselves: what is registered, at which lifetime, and who owns the pool. A data
/// source dials nothing until an operation runs, so none of this needs a server.
/// </summary>
[TestFixture]
public class ClickHouseTcpServiceCollectionExtensionsTests
{
    private const string ConnectionString = "Host=clickhouse.invalid;Port=9123;Username=someone;Database=somewhere";

    private static ClickHouseTcpClientOptions Options() => new() { Host = "clickhouse.invalid", Port = 9123 };

    [Test]
    public async Task AddClickHouseTcpDataSource_WithConnectionString_ConfiguresTheDataSourceFromIt()
    {
        await using ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(ConnectionString)
            .BuildServiceProvider();

        ClickHouseTcpClientOptions options = provider.GetRequiredService<ClickHouseTcpDataSource>().Options;

        Assert.Multiple(() =>
        {
            Assert.That(options.Host, Is.EqualTo("clickhouse.invalid"));
            Assert.That(options.Port, Is.EqualTo(9123));
            Assert.That(options.Username, Is.EqualTo("someone"));
            Assert.That(options.Database, Is.EqualTo("somewhere"));
        });
    }

    [Test]
    public void AddClickHouseTcpDataSource_WithOptions_RegistersOnlySingletons()
    {
        IServiceCollection services = new ServiceCollection().AddClickHouseTcpDataSource(Options());

        Assert.That(
            services.Select(descriptor => (descriptor.ServiceType, descriptor.Lifetime)),
            Is.EquivalentTo(new[]
            {
                (typeof(ClickHouseTcpDataSource), ServiceLifetime.Singleton),
                (typeof(IClickHouseTcpClient), ServiceLifetime.Singleton),
                (typeof(IClickHouseTcpOperations), ServiceLifetime.Singleton),
            }));
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithOptions_ResolvesTheClientTheDataSourceOwns()
    {
        await using ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options())
            .BuildServiceProvider();

        var dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<ClickHouseTcpDataSource>(), Is.SameAs(dataSource));
            Assert.That(provider.GetRequiredService<IClickHouseTcpClient>(), Is.SameAs(dataSource.GetClient()));
            Assert.That(provider.GetRequiredService<IClickHouseTcpOperations>(), Is.SameAs(dataSource.GetClient()));
        });
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithoutALoggerFactoryInTheOptions_TakesTheProviderOne()
    {
        await using ServiceProvider provider = new ServiceCollection()
            .AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance)
            .AddClickHouseTcpDataSource(Options())
            .BuildServiceProvider();

        ClickHouseTcpDataSource dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();

        Assert.That(dataSource.Options.LoggerFactory, Is.SameAs(NullLoggerFactory.Instance));
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithALoggerFactoryInTheOptions_KeepsIt()
    {
        var configured = Substitute.For<ILoggerFactory>();

        await using ServiceProvider provider = new ServiceCollection()
            .AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance)
            .AddClickHouseTcpDataSource(Options() with { LoggerFactory = configured })
            .BuildServiceProvider();

        ClickHouseTcpDataSource dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();

        Assert.That(dataSource.Options.LoggerFactory, Is.SameAs(configured));
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithAnOptionsFactory_RunsItOnceWithTheProvider()
    {
        int calls = 0;

        await using ServiceProvider provider = new ServiceCollection()
            .AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance)
            .AddClickHouseTcpDataSource(serviceProvider =>
            {
                calls++;
                return Options() with { Database = serviceProvider.GetRequiredService<ILoggerFactory>().GetType().Name };
            })
            .BuildServiceProvider();

        ClickHouseTcpDataSource dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();
        _ = provider.GetRequiredService<IClickHouseTcpClient>();

        Assert.Multiple(() =>
        {
            Assert.That(dataSource.Options.Database, Is.EqualTo(nameof(NullLoggerFactory)));
            Assert.That(calls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithADataSourceFactory_UsesWhatTheFactoryReturns()
    {
        var built = new ClickHouseTcpDataSource(Options() with { Database = "from_factory" });

        await using ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource((_, _) => built)
            .BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ClickHouseTcpDataSource>(), Is.SameAs(built));
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithAServiceKey_RegistersKeyedServicesOnly()
    {
        await using ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options(), serviceKey: "reporting")
            .BuildServiceProvider();

        var dataSource = provider.GetRequiredKeyedService<ClickHouseTcpDataSource>("reporting");

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredKeyedService<IClickHouseTcpClient>("reporting"), Is.SameAs(dataSource.GetClient()));
            Assert.That(provider.GetRequiredKeyedService<IClickHouseTcpOperations>("reporting"), Is.SameAs(dataSource.GetClient()));
            Assert.That(provider.GetService<ClickHouseTcpDataSource>(), Is.Null);
            Assert.That(provider.GetService<IClickHouseTcpClient>(), Is.Null);
        });
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_WithTwoServiceKeys_KeepsThePoolsApart()
    {
        await using ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options() with { Database = "first" }, serviceKey: "first")
            .AddClickHouseTcpDataSource(Options() with { Database = "second" }, serviceKey: "second")
            .BuildServiceProvider();

        var first = provider.GetRequiredKeyedService<ClickHouseTcpDataSource>("first");
        var second = provider.GetRequiredKeyedService<ClickHouseTcpDataSource>("second");

        Assert.Multiple(() =>
        {
            Assert.That(first.Options.Database, Is.EqualTo("first"));
            Assert.That(second.Options.Database, Is.EqualTo("second"));
            Assert.That(provider.GetRequiredKeyedService<IClickHouseTcpClient>("second"), Is.SameAs(second.GetClient()));
        });
    }

    [Test]
    public async Task AddClickHouseTcpDataSource_CalledTwiceWithoutAKey_KeepsTheFirstRegistration()
    {
        await using ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options() with { Database = "first" })
            .AddClickHouseTcpDataSource(Options() with { Database = "second" })
            .BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ClickHouseTcpDataSource>().Options.Database, Is.EqualTo("first"));
    }

    [Test]
    public void AddClickHouseTcpDataSource_WithANullArgument_Throws()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpServiceCollectionExtensions.AddClickHouseTcpDataSource(null, ConnectionString));
            Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddClickHouseTcpDataSource((string)null));
            Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddClickHouseTcpDataSource((ClickHouseTcpClientOptions)null));
            Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddClickHouseTcpDataSource((Func<IServiceProvider, ClickHouseTcpClientOptions>)null));
            Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddClickHouseTcpDataSource((Func<IServiceProvider, object, ClickHouseTcpDataSource>)null));
        });
    }

    [Test]
    public async Task DisposeAsync_OnTheProvider_ClosesThePoolOnceAndWithoutThrowing()
    {
        ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options())
            .BuildServiceProvider();

        var dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();
        IClickHouseTcpClient client = provider.GetRequiredService<IClickHouseTcpClient>();

        // The container holds both the data source and the client it owns, so it disposes the same pool twice.
        await provider.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PingAsync());
            Assert.DoesNotThrowAsync(async () => await dataSource.DisposeAsync());
        });
    }

    [Test]
    public void Dispose_OnTheProviderWithOnlyTheDataSourceResolved_ClosesThePool()
    {
        ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options())
            .BuildServiceProvider();

        IClickHouseTcpClient client = provider.GetRequiredService<ClickHouseTcpDataSource>().GetClient();

        provider.Dispose();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PingAsync());
    }

    [Test]
    public void Dispose_OnTheProviderWithTheClientResolved_ClosesThePool()
    {
        // The container tracks the resolved client, and a synchronous ServiceProvider.Dispose() rejects a
        // tracked service that offers only IAsyncDisposable — rejecting it instead of disposing the rest of
        // its list. ClickHouseTcpClient.Dispose exists so that neither happens here.
        ServiceProvider provider = new ServiceCollection()
            .AddClickHouseTcpDataSource(Options())
            .BuildServiceProvider();

        using ClickHouseTcpDataSource dataSource = provider.GetRequiredService<ClickHouseTcpDataSource>();
        IClickHouseTcpClient client = provider.GetRequiredService<IClickHouseTcpClient>();

        Assert.DoesNotThrow(provider.Dispose);
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PingAsync());
    }
}
