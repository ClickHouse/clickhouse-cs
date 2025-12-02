using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;
using Dapper;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ORM;

public class DapperTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
        .Where(s => ShouldBeSupportedByDapper(s.ClickHouseType))
        .Where(s => s.ExampleValue != DBNull.Value)
        .Where(s => !s.ClickHouseType.StartsWith("Array")) // Dapper issue, see ShouldExecuteSelectWithParameters test
        .Select(sample => new TestCaseData($"SELECT {{value:{sample.ClickHouseType}}}", sample.ExampleValue));

    static DapperTests()
    {
        SqlMapper.AddTypeHandler(new ClickHouseDecimalHandler());
        SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
#if NET48 || NET5_0_OR_GREATER
        SqlMapper.AddTypeHandler(new ITupleHandler());
#endif
        SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime2);
        SqlMapper.AddTypeMap(typeof(DateTimeOffset), DbType.DateTime2);
        SqlMapper.AddTypeHandler(new ClickHouseIpHandler());
    }

    // "The member value of type <xxxxxxxx> cannot be used as a parameter value"
    private static bool ShouldBeSupportedByDapper(string clickHouseType)
    {
        if (clickHouseType.Contains("Tuple"))
            return false;
        if (clickHouseType.Contains("Map"))
            return false;
        if (clickHouseType.Contains("Int128"))
            return false;
        if (clickHouseType.Contains("Int256"))
            return false;
        if (clickHouseType.Contains("Nested"))
            return false;
        if (clickHouseType.Contains("FixedString"))
            return false;
        switch (clickHouseType)
        {
            case "UUID":
            case "Date":
            case "Date32":
            case "Nothing":
            case "Point":
            case "Ring":
            case "Geometry":
            case "LineString":
            case "MultiLineString":
            case "Polygon":
            case "MultiPolygon":
            case "Time":
                return false;
            default:
                return true;
        }
    }

#if NET48 || NET5_0_OR_GREATER
    private class ITupleHandler : SqlMapper.TypeHandler<ITuple>
    {
        public override void SetValue(IDbDataParameter parameter, ITuple value) => parameter.Value = value;

        public override ITuple Parse(object value) => value as ITuple ?? throw new NotSupportedException();
    }

    [Test]
    public async Task ShouldExecuteSelectReturningTuple()
    {
        string sql = "SELECT tuple(1,2,3)";
        var result = (await connection.QueryAsync<ITuple>(sql)).Single();
        ClassicAssert.IsInstanceOf<ITuple>(result);
        Assert.That(result.AsEnumerable(), Is.EqualTo(new[] { 1, 2, 3 }).AsCollection);
    }
#endif

    private class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
    {
        public override void SetValue(IDbDataParameter parameter, DateTimeOffset value) => parameter.Value = value.UtcDateTime;

        public override DateTimeOffset Parse(object value)
        {
            switch (value)
            {
                case DateTimeOffset dt:
                    return dt;
                case string s:
                    return DateTimeOffset.Parse(s);
                default:
                    throw new ArgumentException("Cannot convert value to DateTimeOffset", nameof(value));
            }
        }
    }

    private class ClickHouseDecimalHandler : SqlMapper.TypeHandler<ClickHouseDecimal>
    {
        public override void SetValue(IDbDataParameter parameter, ClickHouseDecimal value) => parameter.Value = value.ToString(CultureInfo.InvariantCulture);

        public override ClickHouseDecimal Parse(object value) => value switch
        {
            ClickHouseDecimal chd => chd,
            IConvertible ic => Convert.ToDecimal(ic),
            _ => throw new ArgumentException(nameof(value))
        };
    }

    private class ClickHouseIpHandler : SqlMapper.TypeHandler<IPAddress>
    {
        public override void SetValue(IDbDataParameter parameter, IPAddress value)
        {
            parameter.Value = value;
        }

        public override IPAddress Parse(object value)
        {
            return IPAddress.Parse((string)value);
        }
    }

    [Test]
    public async Task ShouldExecuteSimpleSelect()
    {
        string sql = "SELECT * FROM system.table_functions";

        var functions = (await connection.QueryAsync<string>(sql)).ToList();
        Assert.That(functions, Is.Not.Empty);
        Assert.That(functions, Is.All.Not.Null);
    }

    [Test]
    [TestCaseSource(typeof(DapperTests), nameof(SimpleSelectQueries))]
    public async Task ShouldExecuteSelectStringWithSingleParameterValue(string sql, object value)
    {
        if (value is JsonObject or IPAddress or Guid or TimeSpan)
        {
            Assert.Ignore("Dapper does not support selecting this type as string");
        }

        var parameters = new Dictionary<string, object> { { "value", value } };
        var results = await connection.QueryAsync<string>(sql, parameters);
        Assert.That(results.Single(), Is.EqualTo(Convert.ToString(value, CultureInfo.InvariantCulture)));
    }

    [Test]
    [TestCaseSource(typeof(DapperTests), nameof(SimpleSelectQueries))]
    public async Task ShouldExecuteSelectWithSingleParameterValue(string sql, object expected)
    {
        var parameters = new Dictionary<string, object> { { "value", expected } };
        var rows = await connection.QueryAsync(sql, parameters);
        IDictionary<string, object> row = rows.Single();

        // Workaround: Dapper does not specify type, so
        // DateTime is always mapped as CH's 32-bit DateTime
        if (expected is DateTime dt)
            expected = dt.AddTicks(-dt.Ticks % TimeSpan.TicksPerSecond);

        TestUtilities.AssertEqual(expected, row.Single().Value);
    }

    [Test]
    public async Task ShouldExecuteSelectWithArrayParameter()
    {
        var parameters = new Dictionary<string, object> { { "names", new[] { "mysql", "odbc" } } };
        string sql = "SELECT * FROM system.table_functions WHERE has({names:Array(String)}, name)";

        var functions = (await connection.QueryAsync<string>(sql, parameters)).ToList();
        Assert.That(functions, Is.Not.Empty);
        Assert.That(functions, Is.All.Not.Null);
    }

    [Test]
    public async Task ShouldExecuteSelectReturningNullable()
    {
        string sql = "SELECT toNullable(5)";
        var result = (await connection.QueryAsync<int?>(sql)).Single();
        Assert.That(result, Is.EqualTo(5));
    }

    [Test]
    public async Task ShouldExecuteSelectReturningArray()
    {
        string sql = "SELECT array(1,2,3)";
        var result = (await connection.QueryAsync<int[]>(sql)).Single();
        Assert.That(result, Is.Not.Empty);
        Assert.That(result, Is.All.Not.Null);
    }

    [Test]
    public async Task ShouldExecuteSelectReturningDecimal()
    {
        string sql = "SELECT toDecimal128(0.0001, 8)";
        var result = (await connection.QueryAsync<decimal>(sql)).Single();
        ClassicAssert.IsInstanceOf<decimal>(result);
        Assert.That(result, Is.EqualTo(0.0001m));
    }

    [Test]
    [TestCase(100)]
    [TestCase(1000000000)]
    [TestCase(123.456)]
    [TestCase(0.0001)]
    public async Task ShouldWriteDecimalWithTypeInference(decimal expected)
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_decimal");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_decimal (balance Decimal64(4)) ENGINE Memory");


        var sql = @"INSERT INTO test.dapper_decimal (balance) VALUES (@balance)";
        await connection.ExecuteAsync(sql, new { balance = expected });

        var actual = (ClickHouseDecimal) await connection.ExecuteScalarAsync("SELECT * FROM test.dapper_decimal");
        Assert.That(actual.ToDecimal(CultureInfo.InvariantCulture), Is.EqualTo(expected));
    }

    [Test]
    public async Task ShouldWriteTwoFieldsWithTheSamePrefix()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_prefixes");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_prefixes (testField Int32, testFieldWithSuffix Int32) ENGINE Memory");

        const string sql = "INSERT INTO test.dapper_prefixes (testField, testFieldWithSuffix) VALUES (@testField, @testFieldWithSuffix)";
        await connection.ExecuteAsync(sql, new { testField = 1, testFieldWithSuffix = 2 });
    }

    [Test]
    [TestCase(1.0)]
    [TestCase(null)]
    public async Task ShouldWriteNullableDoubleWithTypeInference(double? expected)
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_nullable_double");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_nullable_double (balance Nullable(Float64)) ENGINE Memory");

        var sql = @"INSERT INTO test.dapper_nullable_double (balance) VALUES (@balance)";
        await connection.ExecuteAsync(sql, new { balance = expected });

        var actual = await connection.ExecuteScalarAsync("SELECT * FROM test.dapper_nullable_double");
        if (expected is null)
            Assert.That(actual, Is.InstanceOf<DBNull>());
        else
            Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task ShouldInsertWithExceptSyntax()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_except");
        await connection.ExecuteStatementAsync(@"
            CREATE TABLE IF NOT EXISTS test.dapper_except (
                id UInt32,
                name String,
                value Float64,
                created DateTime DEFAULT now(),
                updated DateTime DEFAULT now()
            ) ENGINE Memory
        ");

        // Insert using Dapper with EXCEPT syntax
        var sql = "INSERT INTO test.dapper_except (* EXCEPT (created, updated)) VALUES (@id, @name, @value)";
        await connection.ExecuteAsync(sql, new { id = 100, name = "dapper-test", value = 123.45 });

        // Verify the insert worked and defaults were applied
        var result = await connection.QueryAsync("SELECT * FROM test.dapper_except");
        var row = result.Single() as IDictionary<string, object>;
        
        Assert.That(row, Is.Not.Null);
        Assert.That(row.Count, Is.EqualTo(5)); // All 5 columns should be present
        Assert.That(row["id"], Is.EqualTo(100));
        Assert.That(row["name"], Is.EqualTo("dapper-test"));
        Assert.That(row["value"], Is.EqualTo(123.45));
        
        // Verify default timestamps were set
        var created = (DateTime)row["created"];
        var updated = (DateTime)row["updated"];
        Assert.That(created, Is.GreaterThan(DateTime.UtcNow.AddMinutes(-1)));
        Assert.That(updated, Is.GreaterThan(DateTime.UtcNow.AddMinutes(-1)));
    }
}
