using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

[TestFixture(true)]
[TestFixture(false)]
public class SqlParameterizedSelectTests : IDisposable
{
    private readonly ClickHouseConnection connection;
    private readonly ConcurrentQueue<string> createdTables = new();

    public SqlParameterizedSelectTests(bool useCompression)
    {
        connection = TestUtilities.GetTestClickHouseConnection(useCompression);
        connection.Open();
    }

    /// <summary>
    /// This fixture does not derive from <see cref="AbstractConnectionTestFixture"/>, so it tracks
    /// the names it hands out and drops them itself in <see cref="Dispose"/>.
    /// </summary>
    private string CreateTableName(string prefix = null, [CallerMemberName] string testName = null)
    {
        var name = TestUtilities.CreateTableName(prefix, testName: testName);
        createdTables.Enqueue(name);
        return name;
    }

    public static IEnumerable<TestCaseData> TypedQueryParameters => TestCases.GetDataTypeSamples()
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
        {
            clickHouseType = "String";
        }
        if (clickHouseType.StartsWith("QBit"))
        {
            Assert.Ignore("QBit does not support comparing for equality.");
        }
        

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
        command.AddParameter("var", value);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        TestUtilities.AssertEqual(result, value);
    }

    // Issue #483: byte[] and TimeOnly parameter binding over the HTTP parameter path.

    [Test]
    public async Task ShouldExecuteParameterizedSelectWithByteArrayForString()
    {
        // A byte[] used to be formatted as the literal text "System.Byte[]".
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {var:String} as res";
        command.AddParameter("var", new byte[] { 0x41, 0x42, 0x43 }); // "ABC"

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo("ABC"));
    }

    [Test]
    public async Task ShouldRoundTripNonUtf8ByteArrayThroughStringParameter()
    {
        // Payload mixes a null byte, printable ASCII, the escape-sensitive ' and \ bytes, a control
        // byte and invalid-UTF-8 bytes; hex() asserts on the exact bytes the server received.
        var payload = new byte[] { 0x00, 0x41, 0xFF, 0xFE, 0x27, 0x5C, 0x0A, 0x80, 0xC3, 0x28 };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT hex({var:String}) as res";
        command.AddParameter("var", payload);

        var result = (string)(await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo(Convert.ToHexString(payload)));
    }

    // TimeOnly used to throw InvalidCastException (Time) or hit the default throw (Time64).
    [RequiredFeature(Feature.Time)]
    [TestCase("Time", 14, 30, 0, 0)]
    [TestCase("Time64(3)", 14, 30, 0, 500)]
    public async Task ShouldExecuteParameterizedSelectWithTimeOnly(string clickHouseType, int hour, int minute, int second, int millisecond)
    {
        var value = new TimeOnly(hour, minute, second, millisecond);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {{var:{clickHouseType}}} as res";
        command.AddParameter("var", value);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo(value.ToTimeSpan()).Within(TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
    public async Task ShouldExecuteParameterizedCompareWithExplicitType(string exampleExpression, string clickHouseType, object value)
    {
        if (clickHouseType.StartsWith("Enum"))
        {
            clickHouseType = "String";
        }
        if (clickHouseType.StartsWith("QBit"))
        {
            Assert.Ignore("QBit does not support comparing for equality.");
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {exampleExpression} as expected, {{var:{clickHouseType}}} as actual, expected = actual as equals";
        command.AddParameter("var", value);

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
    public async Task AddParameter_NullValueWithNonNullableStringSqlTypeHint_ReturnsEmptyString()
    {
        // When null is passed to a non-nullable String type hint, ClickHouse interprets \N as empty string
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {val:String} as res";
        command.AddParameter("val", null);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo(""));
    }

    [Test]
    [TestCase("Int32")]
    [TestCase("DateTime")]
    public void AddParameter_NullValueWithNonNullableNonStringSqlTypeHint_ThrowsServerException(string typeHint)
    {
        // When null is passed to non-nullable non-string types, the server rejects \N
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {{val:{typeHint}}} as res";
        command.AddParameter("val", null);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await command.ExecuteReaderAsync());
    }

    [Test]
    public void AddParameter_InvalidTypeHint_ThrowsArgumentException()
    {
        string invalidType = "NotARealType";
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {{val:{invalidType}}} as res";
        command.AddParameter("val", 123);

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await command.ExecuteReaderAsync());
        Assert.That(ex.Message, Does.Contain($"Unknown type: {invalidType}"));
    }

    [Test]
    [TestCase("SELECT {val:Int32} AS res // {val:String}")]
    [TestCase("SELECT {val:Int32} AS res /* a /* b */ {val:String} */")]
    [TestCase("SELECT {val:Int32} AS res, $$ {val:String} $$ AS heredoc")]
    public async Task AddParameter_ConflictingTypeHintInsideCommentOrHeredoc_UsesRealHint(string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.AddParameter("val", 42);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        Assert.That(result[0], Is.EqualTo(42));
    }

    [Test]
    [TestCase("SELECT 1 AS `a--b`, toString({val:DateTime64(3, 'UTC')}) AS res")]
    [TestCase("SELECT 1 AS \"a'b\", toString({val:DateTime64(3, 'UTC')}) AS res")]
    [TestCase("SELECT $$--$$ AS heredoc, toString({val:DateTime64(3, 'UTC')}) AS res")]
    public async Task AddParameter_TypeHintAfterQuotedIdentifierOrHeredoc_UsesTypeHint(string sql)
    {
        // Without the hint the value would be formatted as DateTime (no sub-second part)
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.AddParameter("val", new DateTime(2020, 1, 2, 3, 4, 5, 123, DateTimeKind.Utc));

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        Assert.That(result[1], Is.EqualTo("2020-01-02 03:04:05.123"));
    }

    [Test]
    public async Task AddParameter_EscapedQuoteInsideEnumTypeHint_UsesTypeHint()
    {
        // A `\'` escape inside the Enum label left the old scanner believing it was still inside the
        // string literal, so the hint ended at the `}` in the label and the type came out truncated
        using var command = connection.CreateCommand();
        command.CommandText = @"SELECT {val:Enum8('a\'}b' = 1)} AS res";
        command.AddParameter("val", "a'}b");

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo("a'}b"));
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

    [Test]
    public async Task AddParameter_IdentifierTypeBindsColumnName_ResolvesColumnValue()
    {
        // Regression for the missing server-side {name:Identifier} parameter type
        // (https://github.com/ClickHouse/clickhouse-go/issues/1635). Identifier binds the value
        // as a bare SQL identifier (here a column name), unlike String which binds a quoted literal.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {col:Identifier} FROM system.numbers LIMIT 1";
        command.AddParameter("col", "number");

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo(0UL)); // value of the `number` column, not the literal "number"
    }

    [Test]
    public async Task AddParameter_IdentifierWithEmbeddedBacktick_ResolvesColumnSafely()
    {
        // Security/adversarial: an identifier containing a backtick must round-trip. The client sends
        // the value verbatim and the server applies its own backtick quoting/escaping, so a backtick
        // cannot break out. If the client escaped the value (e.g. via Escape()), this column would not
        // resolve.
        var table = CreateTableName("poly_ident");
        using (var createCmd = connection.CreateCommand())
        {
            // Column literally named  weird`col  (inner backtick doubled per ClickHouse DDL quoting).
            createCmd.CommandText = $"CREATE TABLE {table} (`weird``col` Int32, normal Int32) ENGINE = Memory";
            await createCmd.ExecuteNonQueryAsync();
        }

        using (var insertCmd = connection.CreateCommand())
        {
            insertCmd.CommandText = $"INSERT INTO {table} VALUES (7, 9)";
            await insertCmd.ExecuteNonQueryAsync();
        }

        using var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = $"SELECT {{c:Identifier}} FROM {table}";
        selectCmd.AddParameter("c", "weird`col"); // raw value, single backtick, no client-side escaping

        var result = (await selectCmd.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo(7));
    }

    [Test]
    public async Task ExecuteNonQuery_CreateDatabaseWithIdentifierParameter_CreatesDatabase()
    {
        // The original use case from the source issue: bind a database name in DDL without falling
        // back to string interpolation. {name:String} would substitute a quoted literal and fail.
        var dbName = $"poly_ident_db_{Guid.NewGuid():N}";
        try
        {
            using (var createCmd = connection.CreateCommand())
            {
                createCmd.CommandText = "CREATE DATABASE {name:Identifier}";
                createCmd.AddParameter("name", dbName);
                await createCmd.ExecuteNonQueryAsync();
            }

            using var checkCmd = connection.CreateCommand();
            checkCmd.CommandText = "SELECT count() FROM system.databases WHERE name = {n:String}";
            checkCmd.AddParameter("n", dbName);
            var count = await checkCmd.ExecuteScalarAsync();
            Assert.That(Convert.ToInt32(count), Is.EqualTo(1));
        }
        finally
        {
            using var dropCmd = connection.CreateCommand();
            dropCmd.CommandText = "DROP DATABASE IF EXISTS {name:Identifier}";
            dropCmd.AddParameter("name", dbName);
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    [Test]
    public async Task AddParameterWithTypeOverride_IdentifierViaExplicitTypeAndAdoPlaceholder_ResolvesColumnValue()
    {
        // Covers the other entry point named in the bug: the Identifier type set explicitly on the
        // parameter object (not via a {col:Identifier} SQL hint). The SQL uses an ADO-style @col
        // placeholder, which the driver rewrites to {col:Identifier} before sending the request.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT @col FROM system.numbers LIMIT 1";
        command.AddParameterWithTypeOverride("col", "Identifier", "number");

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
        Assert.That(result, Is.EqualTo(0UL)); // value of the `number` column, not the literal "number"
    }

    [Test]
    public async Task AddParameter_AdoPlaceholderInsideStringLiteral_LeavesLiteralIntact()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'user@id' AS s, @id AS v";
        command.AddParameter("id", 42);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        Assert.Multiple(() =>
        {
            Assert.That(result[0], Is.EqualTo("user@id"));
            Assert.That(result[1], Is.EqualTo(42));
        });
    }

    /// <summary>
    /// Drops every name handed out by <see cref="CreateTableName"/>. Best-effort: a table that
    /// cannot be dropped must not fail an otherwise passing fixture.
    /// </summary>
    private void DropCreatedTables()
    {
        while (createdTables.TryDequeue(out var table))
        {
            try
            {
                connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}").GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                TestContext.Progress.WriteLine($"Failed to drop test table {table}: {e.Message}");
            }
        }
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        // NUnit both invokes [OneTimeTearDown] and disposes the fixture instance, so this can run
        // twice; draining the queue and disposing the connection are both idempotent.
        DropCreatedTables();
        connection?.Dispose();
    }
}
