using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

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
