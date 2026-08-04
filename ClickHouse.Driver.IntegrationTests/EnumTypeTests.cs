using System.Threading.Tasks;
using ClickHouse.Driver.Tests;
using NUnit.Framework;

namespace ClickHouse.Driver.IntegrationTests;

public class EnumTypeTests
{
    [Test]
    public async Task GetDataTypeName_Enum16Column_RoundTripsThroughServer()
    {
        using var client = TestUtilities.GetTestClickHouseClient();
        using var reader = await client.ExecuteReaderAsync(
            "SELECT CAST('Low' AS Enum16('Low' = -32768, 'High' = 32767)) AS value");

        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetDataTypeName(0), Is.EqualTo("Enum16('Low' = -32768, 'High' = 32767)"));
            Assert.That(reader.GetString(0), Is.EqualTo("Low"));
        });
    }
}
