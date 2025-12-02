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
        if (clickHouseType.StartsWith("DateTime64") || clickHouseType == "Date" || clickHouseType == "Date32" || clickHouseType == "Time" || clickHouseType.Contains("FixedString"))
            Assert.Pass("Automatic type detection does not work for " + clickHouseType);
        if (clickHouseType.StartsWith("Enum"))
            clickHouseType = "String";
        

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {exampleExpression} as expected, {{var:{clickHouseType}}} as actual, expected = actual as equals";
        command.AddParameter("var", value);

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
    [TestCase(null, "FixedString(4)", "asdf", TestName = "Parametrized select with string for FixedString")]
    [TestCase(null, "FixedString(4)", new byte[] { 91, 92, 93, 94}, TestName = "Parametrized select with byte array for FixedString")]
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
    [TestCase("String")]
    [TestCase("Int32")]
    [TestCase("Int64")]
    [TestCase("Float64")]
    [TestCase("UUID")]
    [TestCase("Date")]
    [TestCase("DateTime")]
    [TestCase("Bool")]
    public async Task ShouldExecuteSelectWithNullParameterWithoutExplicitType(string underlyingType)
    {
        // Regression test: When adding a parameter with null value and not specifying the type,
        // HttpParameterFormatter.Format would throw NullReferenceException
        // trying to call parameter.Value.GetType() on null
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {{SomeField:Nullable({underlyingType})}} as res";
        command.AddParameter("SomeField", null);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.InstanceOf<DBNull>());
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
