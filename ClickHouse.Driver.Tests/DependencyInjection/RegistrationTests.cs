using System.Net;
using System.Net.Http;
using ClickHouse.Driver.ADO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.DependencyInjection;

public class RegistrationTests
{
#if NET7_0_OR_GREATER
    [Test]
    public void CanAddClickHouseDataSource()
    {
        const string connectionString = "Host=localhost;Port=1234";
        using var services = new ServiceCollection()
                             .AddClickHouseDataSource(connectionString)
                             .BuildServiceProvider();
        var dataSource = services.GetRequiredService<IClickHouseDataSource>();
        Assert.That(dataSource.ConnectionString, Contains.Substring(connectionString));

        using var fromService = services.GetRequiredService<IClickHouseConnection>();
        using var rawConnection = new ClickHouseConnection(connectionString);
        Assert.That(fromService.ConnectionString, Is.EqualTo(rawConnection.ConnectionString));
    }

    [Test]
    public void CanAddClickHouseDataSourceWithSettings()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Port = 9999,
            Database = "test_db",
            Username = "test_user",
            Password = "test_password"
        };

        using var services = new ServiceCollection()
                             .AddClickHouseDataSource(settings)
                             .BuildServiceProvider();

        var dataSource = services.GetRequiredService<IClickHouseDataSource>();
        Assert.That(dataSource.ConnectionString, Does.Contain("Host=localhost"));
        Assert.That(dataSource.ConnectionString, Does.Contain("Port=9999"));
        Assert.That(dataSource.ConnectionString, Does.Contain("Database=test_db"));

        using var connection = services.GetRequiredService<IClickHouseConnection>();
        Assert.That(connection.Database, Is.EqualTo("test_db"));
    }

    [Test]
    public void CanAddClickHouseDataSourceWithSettingsFactory()
    {
        using var services = new ServiceCollection()
                             .AddSingleton<ILoggerFactory, LoggerFactory>()
                             .AddClickHouseDataSource(sp => new ClickHouseClientSettings
                             {
                                 Host = "localhost",
                                 Port = 8888,
                                 Database = "factory_db",
                                 LoggerFactory = sp.GetService<ILoggerFactory>()
                             })
                             .BuildServiceProvider();

        var dataSource = services.GetRequiredService<ClickHouseDataSource>();
        Assert.That(dataSource.ConnectionString, Does.Contain("Host=localhost"));
        Assert.That(dataSource.ConnectionString, Does.Contain("Port=8888"));
        Assert.That(dataSource.LoggerFactory, Is.Not.Null);

        using var connection = services.GetRequiredService<IClickHouseConnection>();
        Assert.That(connection.Database, Is.EqualTo("factory_db"));
    }

    [Test]
    public void DataSourceInjectsLoggerFactoryFromDI()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Port = 7777
        };

        using var services = new ServiceCollection()
                             .AddSingleton<ILoggerFactory, LoggerFactory>()
                             .AddClickHouseDataSource(settings)
                             .BuildServiceProvider();

        var dataSource = services.GetRequiredService<ClickHouseDataSource>();
        Assert.That(dataSource.LoggerFactory, Is.Not.Null);
    }

    [Test]
    public void CanAddClickHouseDataSourceWithHttpClientFactory()
    {
        const string connectionString = "Host=localhost;Port=5555;Database=factory_db";
        const string clientName = "test-clickhouse-client";

        var serviceCollection = new ServiceCollection()
                             .AddHttpClient(clientName)
                             .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
                             {
                                 AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
                             })
                             .Services;

        // Build a temporary service provider to get the IHttpClientFactory
        var tempProvider = serviceCollection.BuildServiceProvider();
        var httpClientFactory = tempProvider.GetRequiredService<IHttpClientFactory>();

        using var services = serviceCollection
                             .AddSingleton(httpClientFactory)
                             .AddSingleton<ILoggerFactory, LoggerFactory>()
                             .AddClickHouseDataSource(
                                 connectionString,
                                 httpClientFactory,
                                 clientName)
                             .BuildServiceProvider();

        var dataSource = services.GetRequiredService<ClickHouseDataSource>();
        Assert.That(dataSource.ConnectionString, Does.Contain("Host=localhost"));
        Assert.That(dataSource.ConnectionString, Does.Contain("Port=5555"));
        Assert.That(dataSource.ConnectionString, Does.Contain("Database=factory_db"));
        Assert.That(dataSource.LoggerFactory, Is.Not.Null, "LoggerFactory should be injected from DI");
        
        using var connection = services.GetRequiredService<IClickHouseConnection>();
        Assert.That(connection.Database, Is.EqualTo("factory_db"));
    }
#endif
}
