namespace ClickHouse.Driver.Tests.SQL;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

[TestFixture(true)]
[TestFixture(false)]
public class SqlParameterizedSelectWithFormDataTests
{
    private readonly ClickHouseConnection connection;

    public SqlParameterizedSelectWithFormDataTests(bool useCompression)
    {
        connection = TestUtilities.GetTestClickHouseConnection(useCompression, useFormDataParameters: true);
        connection.Open();
    }

    public static IEnumerable<TestCaseData> TypedQueryParameters => TestUtilities.GetDataTypeSamples()
        // DB::Exception: There are no UInt128 literals in SQL
        .Where(sample => !sample.ClickHouseType.Contains("UUID") || TestUtilities.SupportedFeatures.HasFlag(Feature.UUIDParameters))
        // DB::Exception: Serialization is not implemented
        .Where(sample => sample.ClickHouseType != "Nothing")
        .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

    [Test]
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
    public async Task ShouldExecuteParameterizedCompareWithTypeDetection(string exampleExpression, string clickHouseType, object value)
    {
        if (clickHouseType.StartsWith("DateTime64") || clickHouseType == "Date" || clickHouseType == "Date32" || clickHouseType == "Time")
            Assert.Pass("Automatic type detection does not work for " + clickHouseType);
        if (clickHouseType.StartsWith("Enum"))
            clickHouseType = "String";

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {exampleExpression} as expected, {{var:{clickHouseType}}} as actual, expected = actual as equals";
        command.AddParameter("var", value);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        TestUtilities.AssertEqual(result[1], result[0]);

        if (value is null || value is DBNull)
        {
            ClassicAssert.IsInstanceOf<DBNull>(result[2]);
        }
    }


    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
    }
}
