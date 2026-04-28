using System.Threading.Tasks;
using ClickHouse.Driver.Tests;
using NUnit.Framework;

namespace ClickHouse.Driver.IntegrationTests;

[SetUpFixture]
public class TestContainerFixture
{
    [OneTimeTearDown]
    public async Task TearDown()
    {
        var container = TestUtilities.TestContainer;
        if (container is not null)
            await container.DisposeAsync();
    }
}
