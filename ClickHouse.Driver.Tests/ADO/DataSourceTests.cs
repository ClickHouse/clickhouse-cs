#if NET7_0_OR_GREATER
using ClickHouse.Driver.ADO;
using NUnit.Framework;
using System;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.Tests.Utilities;

namespace ClickHouse.Driver.Tests.ADO;

public class DataSourceTests
{
    [Test]
    public void CanCreateConnection()
    {
        var connectionString = new ClickHouseConnection("Host=localhost").ConnectionString;
        using var dataSource = new ClickHouseDataSource(connectionString);
        using var connection = dataSource.CreateConnection();
        Assert.That(connectionString, Is.EqualTo(connection.ConnectionString));
    }

    [Test]
    public async Task DisposeAsync_DisposesOwnedHttpClient()
    {
        var handler = new TrackingHandler(new HttpClientHandler());
    
        var httpClient = new HttpClient(handler);
    
        var dataSource = new ClickHouseDataSource(
            "Host=localhost",
            httpClient,
            disposeHttpClient: true);
    
        await dataSource.DisposeAsync();
    
        Assert.That(handler.IsDisposed, Is.True);
    }

    [Test]
    public void Dispose_DisposesOwnedHttpClient()
    {
        var handler = new TrackingHandler(new HttpClientHandler());
        var httpClient = new HttpClient(handler);

        var dataSource = new ClickHouseDataSource(
            "Host=localhost",
            httpClient,
            disposeHttpClient: true);

        dataSource.Dispose();

        Assert.That(handler.IsDisposed, Is.True);
    }

    [Test]
    public async Task DisposeAsync_DisposesDefaultClient()
    {
        var dataSource = new ClickHouseDataSource("Host=localhost");

        await dataSource.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await dataSource.GetClient().ExecuteScalarAsync("SELECT 1"));
    }

    [Test]
    public void Dispose_DisposesDefaultClient()
    {
        var dataSource = new ClickHouseDataSource("Host=localhost");

        dataSource.Dispose();

        Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await dataSource.GetClient().ExecuteScalarAsync("SELECT 1"));
    }

    [Test]
    public async Task DisposeAsync_DoesNotDisposeHttpClientWhenOwnershipIsDisabled()
    {
        var handler = new TrackingHandler(new HttpClientHandler());
        var httpClient = new HttpClient(handler);

        var dataSource = new ClickHouseDataSource(
            "Host=localhost",
            httpClient,
            disposeHttpClient: false);

        await dataSource.DisposeAsync();

        Assert.That(handler.IsDisposed, Is.False);

        httpClient.Dispose();
    }

    [Test]
    public void Dispose_DoesNotDisposeHttpClientWhenOwnershipIsDisabled()
    {
        var handler = new TrackingHandler(new HttpClientHandler());
        var httpClient = new HttpClient(handler);

        var dataSource = new ClickHouseDataSource(
            "Host=localhost",
            httpClient,
            disposeHttpClient: false);

        dataSource.Dispose();

        Assert.That(handler.IsDisposed, Is.False);

        httpClient.Dispose();
    }

    [Test]
    public async Task AwaitUsing_DisposesOwnedHttpClient()
    {
        var handler = new TrackingHandler(new HttpClientHandler());
        var httpClient = new HttpClient(handler);

        await using (var dataSource = new ClickHouseDataSource(
            "Host=localhost",
            httpClient,
            disposeHttpClient: true))
        {
        }

        Assert.That(handler.IsDisposed, Is.True);
    }
}
#endif
