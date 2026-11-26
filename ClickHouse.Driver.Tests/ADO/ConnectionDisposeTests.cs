using System;
using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// A connection disposes the client that it owns on every dispose path, and leaves a shared
/// client open. ORMs such as EF Core dispose a connection as a <see cref="DbConnection"/>.
/// </summary>
public class ConnectionDisposeTests : AbstractConnectionTestFixture
{
    public enum DisposeCall
    {
        IDisposable,
        DbConnection,
        DisposeAsync,
    }

    private static async Task DisposeAsync(ClickHouseConnection target, DisposeCall call)
    {
        switch (call)
        {
            case DisposeCall.IDisposable:
                ((IDisposable)target).Dispose();
                break;
            case DisposeCall.DbConnection:
                ((DbConnection)target).Dispose();
                break;
            default:
                await target.DisposeAsync();
                break;
        }
    }

    [TestCase(DisposeCall.IDisposable)]
    [TestCase(DisposeCall.DbConnection)]
    [TestCase(DisposeCall.DisposeAsync)]
    public async Task Dispose_ConnectionFromSettings_DisposesOwnedClient(DisposeCall call)
    {
        var owner = new ClickHouseConnection(TestUtilities.GetTestClickHouseClientSettings());
        var ownedClient = owner.GetClient();
        Assert.That(Convert.ToInt32(await ownedClient.ExecuteScalarAsync("SELECT 1")), Is.EqualTo(1));

        await DisposeAsync(owner, call);

        Assert.That(async () => await ownedClient.ExecuteScalarAsync("SELECT 1"), Throws.InstanceOf<ObjectDisposedException>());
    }

    [TestCase(DisposeCall.IDisposable)]
    [TestCase(DisposeCall.DbConnection)]
    [TestCase(DisposeCall.DisposeAsync)]
    public async Task Dispose_ConnectionAfterSettingsChange_DisposesOwnedClient(DisposeCall call)
    {
        // A settings change gives a connection from a shared client its own client.
        var target = (ClickHouseConnection)connection.Clone();
        target.Settings = new ClickHouseClientSettings(target.Settings);
        var ownedClient = target.GetClient();
        Assert.That(ownedClient, Is.Not.SameAs(client));

        await DisposeAsync(target, call);

        Assert.That(async () => await ownedClient.ExecuteScalarAsync("SELECT 1"), Throws.InstanceOf<ObjectDisposedException>());
        Assert.That(Convert.ToInt32(await client.ExecuteScalarAsync("SELECT 1")), Is.EqualTo(1));
    }

    [TestCase(DisposeCall.IDisposable)]
    [TestCase(DisposeCall.DbConnection)]
    [TestCase(DisposeCall.DisposeAsync)]
    public async Task Dispose_ConnectionFromClient_KeepsSharedClient(DisposeCall call)
    {
        var shared = (ClickHouseConnection)connection.Clone();

        await DisposeAsync(shared, call);

        Assert.That(Convert.ToInt32(await client.ExecuteScalarAsync("SELECT 1")), Is.EqualTo(1));
    }

#if NET7_0_OR_GREATER
    [TestCase(DisposeCall.IDisposable)]
    [TestCase(DisposeCall.DbConnection)]
    [TestCase(DisposeCall.DisposeAsync)]
    public async Task Dispose_ConnectionFromDataSource_KeepsDataSourceClient(DisposeCall call)
    {
        using var dataSource = new ClickHouseDataSource(TestUtilities.GetTestClickHouseClientSettings());
        var dataSourceConnection = dataSource.CreateConnection();

        await DisposeAsync(dataSourceConnection, call);

        Assert.That(Convert.ToInt32(await dataSource.GetClient().ExecuteScalarAsync("SELECT 1")), Is.EqualTo(1));
    }
#endif

    [Test]
    public void Dispose_Twice_DoesNotThrow()
    {
        var owner = new ClickHouseConnection(TestUtilities.GetTestClickHouseClientSettings());

        ((DbConnection)owner).Dispose();

        Assert.That(() => ((IDisposable)owner).Dispose(), Throws.Nothing);
    }
}
