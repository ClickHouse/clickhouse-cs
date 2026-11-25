using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

[TestFixture]
[Category("Integration")]
[RequiresServerFeature(TcpFeature.Time)]
public class TimeParameterPrecisionTests
{
    private static IEnumerable<TestCaseData> TimeCases()
    {
        yield return new TestCaseData("Time", TimeSpan.FromTicks(-37_239_876_543), "-01:02:03");
        yield return new TestCaseData("Time", TimeOnly.MaxValue, "23:59:59");
        yield return new TestCaseData("Time64(3)", TimeSpan.FromTicks(37_234_567_891), "01:02:03.456");
        yield return new TestCaseData("Time64(3)", TimeSpan.FromTicks(-37_234_567_891), "-01:02:03.456");
        yield return new TestCaseData("Time64(3)", TimeOnly.MaxValue, "23:59:59.999");
    }

    [TestCaseSource(nameof(TimeCases))]
    public async Task QueryAsync_TimeParameter_TruncatesTowardZero(string clickHouseType, object value, string expected)
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", value } },
        };

        var results = new List<object>();
        await foreach (var row in client.QueryAsync($"SELECT toString({{p:{clickHouseType}}})", options))
            results.Add(row[0]);

        Assert.That(results, Is.EqualTo(new[] { expected }));
    }
}
