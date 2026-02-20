using System;
using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.Driver.Tests;

public class ClickHouseServiceCollectionExtensionsTests
{
    [Test]
    public void AddClickHouseClient_ShouldRegisterSingletonService()
    {
        // Arrange
        var services = new ServiceCollection();
        const string connectionString = "Host=localhost";

        // Act
        services.AddClickHouseClient(connectionString);
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var client1 = serviceProvider.GetService<IClickHouseClient>();
        var client2 = serviceProvider.GetService<IClickHouseClient>();

        Assert.That(client1, Is.Not.Null, "Service should be registered");
        Assert.That(client1, Is.InstanceOf<ClickHouseClient>(), "Should resolve to ClickHouseClient");
        Assert.That(client1, Is.SameAs(client2), "Should return same instance (singleton)");
    }

    [Test]
    public void AddClickHouseClient_WithNullConnectionString_ShouldThrow()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            services.AddClickHouseClient(null!));
    }
}
