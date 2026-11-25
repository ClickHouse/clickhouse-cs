using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Types;

public class TimeWritesIntegrationTests : AbstractConnectionTestFixture
{
    private static IEnumerable<TestCaseData> TimeCases()
    {
        yield return new TestCaseData("Time", TimeSpan.FromTicks(-37_239_876_543), "-01:02:03");
        yield return new TestCaseData("Time", TimeOnly.MaxValue, "23:59:59");
        yield return new TestCaseData("Time64(3)", TimeSpan.FromTicks(37_234_567_891), "01:02:03.456");
        yield return new TestCaseData("Time64(3)", TimeSpan.FromTicks(-37_234_567_891), "-01:02:03.456");
        yield return new TestCaseData("Time64(3)", TimeOnly.MaxValue, "23:59:59.999");
        yield return new TestCaseData("Time64(7)", TimeOnly.MaxValue, "23:59:59.9999999");
        yield return new TestCaseData("Time64(9)", TimeSpan.FromTicks(-37_234_567_891), "-01:02:03.456789100");
    }

    [RequiredFeature(Feature.Time)]
    [TestCaseSource(nameof(TimeCases))]
    public async Task WriteTime_WithFractionalTicks_MatchesTheColumnPrecision(
        string clickHouseType, object value, string expected)
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (value {clickHouseType}) ENGINE = Memory");
        await client.InsertBinaryAsync(table, ["value"], [[value]]);
        var stored = await client.ExecuteScalarAsync($"SELECT toString(value) FROM {table}");

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", value);
        var parameter = await client.ExecuteScalarAsync($"SELECT toString({{p:{clickHouseType}}})", parameters);

        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.EqualTo(expected), "binary insert");
            Assert.That(parameter, Is.EqualTo(expected), "typed query parameter");
        });
    }
}
