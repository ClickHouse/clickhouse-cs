using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[SetUpFixture]
public class TestContainerFixture
{
    [OneTimeSetUp]
    public async Task SetUp()
    {
        // Creating the test database here (rather than per-fixture) avoids hammering
        // the distributed DDL queue on multi-replica deployments,
        // which was causing issues with the cloud tests.
        using var client = TestUtilities.GetTestClickHouseClient();
        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test");
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        var container = TestUtilities.TestContainer;
        if (container is not null)
            await container.DisposeAsync();
    }
}
