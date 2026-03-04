using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
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
        .Select(sample => new TestCaseData($"SELECT {{value:{sample.ClickHouseType}}}", sample.ExampleValue));

    public static IEnumerable<TestCaseData> SimpleSelectQueriesForStringConversion => TestUtilities.GetDataTypeSamples()
        .Where(s => ShouldBeSupportedByDapper(s.ClickHouseType))
        .Where(s => ShouldSupportStringConversion(s.ClickHouseType))
        .Where(s => s.ExampleValue != DBNull.Value)
        .Select(sample => new TestCaseData($"SELECT {{value:{sample.ClickHouseType}}}", sample.ExampleValue));

    static DapperTests()
    {
        SqlMapper.AddTypeHandler(new ClickHouseDecimalHandler());
        SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
        SqlMapper.AddTypeHandler(new ITupleHandler());
        SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime2);
        SqlMapper.AddTypeHandler(new ClickHouseIpHandler());
        SqlMapper.AddTypeHandler(new BigIntegerHandler());
    }

    // "The member value of type <xxxxxxxx> cannot be used as a parameter value"
    private static bool ShouldBeSupportedByDapper(string clickHouseType)
    {
        if (clickHouseType.Contains("Tuple"))
            return false;
        if (clickHouseType.Contains("Nested"))
            return false;
        switch (clickHouseType)
        {
            case "Nothing":
            case "Point":
            case "Ring":
            case "Geometry":
            case "LineString":
            case "MultiLineString":
            case "Polygon":
            case "MultiPolygon":
                return false;
            default:
                return true;
        }
    }
    
    private static bool ShouldSupportStringConversion(string clickHouseType)
    {
        // Dapper does not support selecting these as string
        if (clickHouseType == "Time" ||
            clickHouseType.Contains("Array") || 
            clickHouseType.Contains("Int128") || 
            clickHouseType.Contains("Int256") || 
            clickHouseType.Contains("QBit") ||
            clickHouseType.Contains("Json") ||
            clickHouseType.Contains("IPv4") ||
            clickHouseType.Contains("IPv6") ||
            clickHouseType.Contains("UUID") ||
            clickHouseType.Contains("Map") ||
            clickHouseType.Contains("Time64") ||
            clickHouseType.Contains("Tuple") ||
            clickHouseType.Contains("FixedString"))
        {
            return false;
        }

        return true;
    }

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

    private class BigIntegerHandler : SqlMapper.TypeHandler<BigInteger>
    {
        public override void SetValue(IDbDataParameter parameter, BigInteger value) => parameter.Value = value;

        public override BigInteger Parse(object value) => value switch
        {
            BigInteger bi => bi,
            long l => new BigInteger(l),
            ulong ul => new BigInteger(ul),
            decimal d => new BigInteger(d),
            string s => BigInteger.Parse(s, CultureInfo.InvariantCulture),
            _ => throw new ArgumentException($"Cannot convert {value.GetType()} to BigInteger", nameof(value)),
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
    [TestCaseSource(typeof(DapperTests), nameof(SimpleSelectQueriesForStringConversion))]
    public async Task ShouldExecuteSelectStringWithSingleParameterValue(string sql, object value)
    {
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

    // Used as both Dapper parameter object and query result mapping target
    private class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    [Test]
    public async Task ShouldInsertAndSelectWithAnonymousObject()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_anon");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_anon (id Int32, name String, value Float64) ENGINE Memory");

        // Anonymous object as parameter source - the most common Dapper pattern
        await connection.ExecuteAsync(
            "INSERT INTO test.dapper_anon (id, name, value) VALUES (@Id, @Name, @Value)",
            new { Id = 1, Name = "alice", Value = 3.14 });

        var rows = (await connection.QueryAsync<SimpleRow>("SELECT id, name, value FROM test.dapper_anon")).ToList();
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Id, Is.EqualTo(1));
        Assert.That(rows[0].Name, Is.EqualTo("alice"));
        Assert.That(rows[0].Value, Is.EqualTo(3.14));
    }

    [Test]
    public async Task ShouldInsertWithPocoParameters()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_poco");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_poco (id Int32, name String, value Float64) ENGINE Memory");

        // POCO class as parameter source - Dapper reflects its properties identically to anonymous objects
        var param = new SimpleRow { Id = 42, Name = "bob", Value = 99.9 };
        await connection.ExecuteAsync(
            "INSERT INTO test.dapper_poco (id, name, value) VALUES (@Id, @Name, @Value)", param);

        var rows = (await connection.QueryAsync<SimpleRow>("SELECT id, name, value FROM test.dapper_poco")).ToList();
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Id, Is.EqualTo(42));
        Assert.That(rows[0].Name, Is.EqualTo("bob"));
    }

    [Test]
    public async Task ShouldSelectWithDynamicParametersFromDictionary()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_dynparams");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_dynparams (id Int32, name String) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_dynparams VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

        // DynamicParameters constructed from a dictionary
        var dict = new Dictionary<string, object> { { "Id", 2 } };
        var dynParams = new DynamicParameters(dict);

        var rows = (await connection.QueryAsync<SimpleRow>(
            "SELECT id, name FROM test.dapper_dynparams WHERE id = @Id", dynParams)).ToList();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Name, Is.EqualTo("bob"));
    }

    [Test]
    public async Task ShouldSelectWithDynamicParametersFromAnonymousObject()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_dynparams2");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_dynparams2 (id Int32, name String) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_dynparams2 VALUES (1, 'alice'), (2, 'bob')");

        // DynamicParameters constructed from an anonymous object
        var dynParams = new DynamicParameters(new { Id = 1 });

        var rows = (await connection.QueryAsync<SimpleRow>(
            "SELECT id, name FROM test.dapper_dynparams2 WHERE id = @Id", dynParams)).ToList();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Name, Is.EqualTo("alice"));
    }

    [Test]
    public async Task ShouldSelectPureNoParameters_MappingToPoco()
    {
        // Pure SELECT with no parameters, mapping to a POCO - basic Dapper use case
        var rows = (await connection.QueryAsync<SimpleRow>(
            "SELECT 1 as id, 'hello' as name, 2.5 as value")).ToList();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Id, Is.EqualTo(1));
        Assert.That(rows[0].Name, Is.EqualTo("hello"));
        Assert.That(rows[0].Value, Is.EqualTo(2.5));
    }

    [Test]
    public async Task ShouldSelectPureFromTable_MappingToPoco()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_pure");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_pure (id Int32, name String, value Float64) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_pure VALUES (10, 'test', 1.5), (20, 'test2', 2.5)");

        var rows = (await connection.QueryAsync<SimpleRow>("SELECT id, name, value FROM test.dapper_pure ORDER BY id")).ToList();

        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows[0].Id, Is.EqualTo(10));
        Assert.That(rows[1].Id, Is.EqualTo(20));
    }

    [Test]
    public async Task ShouldSelectWithMultipleAnonymousParameters()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_multi");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_multi (id Int32, name String, value Float64) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_multi VALUES (1, 'alice', 1.0), (2, 'bob', 2.0), (3, 'carol', 3.0)");

        // Multiple parameters from a single anonymous object
        var rows = (await connection.QueryAsync<SimpleRow>(
            "SELECT id, name, value FROM test.dapper_multi WHERE id >= @MinId AND name = @Name",
            new { MinId = 1, Name = "bob" })).ToList();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Id, Is.EqualTo(2));
    }

    // Object with a Tuple field - for testing Tuple-in-object scenarios
    private class RowWithTuple
    {
        public int Id { get; set; }
        public ITuple Coords { get; set; }
    }

    [Test]
    public async Task ShouldSelectReturningObjectWithTupleColumn()
    {
        // Pure SELECT returning a tuple column - no tuple PARAMETER, just tuple in RESULT
        // This tests whether Dapper can materialize a tuple column into a POCO
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_tuple_col");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_tuple_col (id Int32, coords Tuple(Int32, Int32)) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_tuple_col VALUES (1, (10, 20))");

        var rows = (await connection.QueryAsync<RowWithTuple>(
            "SELECT id, coords FROM test.dapper_tuple_col")).ToList();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Id, Is.EqualTo(1));
        Assert.That(rows[0].Coords, Is.Not.Null);
        Assert.That(rows[0].Coords[0], Is.EqualTo(10));
        Assert.That(rows[0].Coords[1], Is.EqualTo(20));
    }

    [Test]
    public async Task ShouldSelectTupleFromLiteral()
    {
        // Simplest tuple SELECT - no table, no params
        var result = (await connection.QueryAsync<ITuple>("SELECT tuple(42, 'hello')")).Single();
        Assert.That(result[0], Is.EqualTo(42));
        Assert.That(result[1], Is.EqualTo("hello"));
    }

    [Test]
    public async Task ShouldSelectWithWhereInUsingHasAndArrayParam()
    {
        // ClickHouse doesn't support Dapper's automatic IN expansion (@Ids -> @Ids1, @Ids2, ...).
        // Instead, use has() with an Array parameter and ClickHouse native {param:Type} syntax.
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_in");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_in (id Int32, name String) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_in VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave')");

        var parameters = new Dictionary<string, object> { { "ids", new[] { 1, 3 } } };
        var rows = (await connection.QueryAsync<SimpleRow>(
            "SELECT id, name FROM test.dapper_in WHERE has({ids:Array(Int32)}, id) ORDER BY id",
            parameters)).ToList();

        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows[0].Name, Is.EqualTo("alice"));
        Assert.That(rows[1].Name, Is.EqualTo("carol"));
    }

    [Test]
    public async Task ShouldSelectWithWhereInDapperExpansion()
    {
        // Test Dapper's native IN expansion: WHERE id IN @Ids
        // Dapper rewrites this to WHERE id IN (@Ids1, @Ids2, ...) with individual params.
        // Each @IdsN then gets replaced by {IdsN:Type} via ReplacePlaceholders.
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_in2");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_in2 (id Int32, name String) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_in2 VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

        var rows = (await connection.QueryAsync<SimpleRow>(
            "SELECT id, name FROM test.dapper_in2 WHERE id IN @Ids ORDER BY id",
            new { Ids = new[] { 1, 3 } })).ToList();

        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows[0].Name, Is.EqualTo("alice"));
        Assert.That(rows[1].Name, Is.EqualTo("carol"));
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
