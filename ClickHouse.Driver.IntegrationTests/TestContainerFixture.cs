using System.Threading.Tasks;
using ClickHouse.Driver.Tests;
using NUnit.Framework;

namespace ClickHouse.Driver.IntegrationTests;

[SetUpFixture]
public class TestContainerFixture
{
    [OneTimeSetUp]
    public async Task SetUp()
    {
        // The Tests project creates this in its own [SetUpFixture], but NUnit only runs those for the
        // assembly under test, so this project has to create the database it puts its tables in too.
        using var client = TestUtilities.GetTestClickHouseClient();
        await client.ExecuteNonQueryAsync($"CREATE DATABASE IF NOT EXISTS {TestUtilities.TestDatabase}");
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        var container = TestUtilities.TestContainer;
        if (container is not null)
            await container.DisposeAsync();
    }
}
