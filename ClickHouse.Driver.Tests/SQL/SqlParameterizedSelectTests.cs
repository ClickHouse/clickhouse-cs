using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

[TestFixture(true)]
[TestFixture(false)]
public class SqlParameterizedSelectTests : IDisposable
{
    private readonly ClickHouseConnection connection;

    public SqlParameterizedSelectTests(bool useCompression)
    {
        connection = TestUtilities.GetTestClickHouseConnection(useCompression);
        connection.Open();
    }

    public static IEnumerable<TestCaseData> TypedQueryParameters => TestUtilities.GetDataTypeSamples()
        // DB::Exception: There are no UInt128 literals in SQL
        .Where(sample => !sample.ClickHouseType.Contains("UUID") || TestUtilities.SupportedFeatures.HasFlag(Feature.UUIDParameters))
        // DB::Exception: Serialization is not implemented
        .Where(sample => sample.ClickHouseType != "Nothing")
        .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

    [Test]
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
//TODO step through this test and see exactly what happens with HttpParameterFormatter and ClickHouseDbParameter, they seem to be doing duplicate work
        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        TestUtilities.AssertEqual(result[0], result[1]);

        if (value is null || value is DBNull)
        {
            Assert.That(result[2], Is.InstanceOf<DBNull>());
        }
        //else
        //{
        //    Assert.AreEqual(1, result[2], $"Equality check in ClickHouse failed: {result[0]} {result[1]}");
        //}
    }

    [Test]
    [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
    public async Task ShouldExecuteParameterizedSelectWithExplicitType(string _, string clickHouseType, object value)
    {
        if (clickHouseType.StartsWith("Enum"))
            clickHouseType = "String";
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {{var:{clickHouseType}}} as res";
        command.AddParameter("var", clickHouseType, value);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        TestUtilities.AssertEqual(result, value);
    }

    [Test]
    [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
    public async Task ShouldExecuteParameterizedCompareWithExplicitType(string exampleExpression, string clickHouseType, object value)
    {
        if (clickHouseType.StartsWith("Enum"))
            clickHouseType = "String";

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {exampleExpression} as expected, {{var:{clickHouseType}}} as actual, expected = actual as equals";
        command.AddParameter("var", clickHouseType, value);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        TestUtilities.AssertEqual(result[0], result[1]);

        if (value is null || value is DBNull)
        {
            Assert.That(result[2], Is.InstanceOf<DBNull>());
        }
        // else
        // {
        //     Assert.AreEqual(1, result[2], $"Equality check in ClickHouse failed: {result[0]} {result[1]}");
        // }
    }


    [Test]
    public async Task ShouldExecuteSelectWithTupleParameter()
    {
        var sql = @"
                SELECT 1
                FROM (SELECT tuple(1, 'a', NULL) AS res)
                WHERE res.1 = tupleElement({var:Tuple(Int32, String, Nullable(Int32))}, 1)
                  AND res.2 = tupleElement({var:Tuple(Int32, String, Nullable(Int32))}, 2)
                  AND res.3 is NULL 
                  AND tupleElement({var:Tuple(Int32, String, Nullable(Int32))}, 3) is NULL";
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.AddParameter("var", Tuple.Create<int, string, int?>(1, "a", null));

        var result = await command.ExecuteReaderAsync();
        result.GetEnsureSingleRow();
    }

    [Test]
    public async Task ShouldExecuteSelectWithUnderlyingTupleParameter()
    {
        var sql = @"
                SELECT 1
                FROM (SELECT tuple(123, tuple(5, 'a', 7)) AS res)
                WHERE res.1 = tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 1)
                  AND res.2.1 = tupleElement(tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 2), 1)
                  AND res.2.2 = tupleElement(tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 2), 2)
                  AND res.2.3 = tupleElement(tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 2), 3)";
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.AddParameter("var", Tuple.Create(123, Tuple.Create((byte)5, "a", 7)));

        var result = await command.ExecuteReaderAsync();
        result.GetEnsureSingleRow();
    }

    public void Dispose() => connection?.Dispose();
}
