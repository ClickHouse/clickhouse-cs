using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class TupleTypeTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldSelectTuple([Range(1, 24, 4)] int count)
    {
        var items = string.Join(",", Enumerable.Range(1, count));
        var result = await connection.ExecuteScalarAsync($"select tuple({items})");
        ClassicAssert.IsInstanceOf<ITuple>(result);
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(count));
            Assert.That(AsEnumerable(tuple), Is.EqualTo(Enumerable.Range(1, count)).AsCollection);
        });
    }

    private static IEnumerable<object> AsEnumerable(ITuple tuple) => Enumerable.Range(0, tuple.Length).Select(i => tuple[i]);

    [Test]
    public async Task ShouldReadSmallTuple_WithinFastPathArity_ReturnsSystemTupleWithValues([Range(1, 7)] int count)
    {
        var items = string.Join(",", Enumerable.Range(1, count).Select(i => $"toInt32({i})"));
        var result = await connection.ExecuteScalarAsync($"select tuple({items})");
        var tuple = result as ITuple;
        // Small tuples must materialize as exactly System.Tuple<int, ...> of the matching arity —
        // never a ValueTuple, a LargeTuple, or a Tuple`8 with TRest nesting.
        var expectedType = Type.GetType("System.Tuple`" + count)
            .MakeGenericType(Enumerable.Repeat(typeof(int), count).ToArray());
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.Not.Null);
            Assert.That(result.GetType(), Is.EqualTo(expectedType));
            Assert.That(tuple.Length, Is.EqualTo(count));
            Assert.That(AsEnumerable(tuple), Is.EqualTo(Enumerable.Range(1, count)).AsCollection);
        });
    }

    [Test]
    public async Task ShouldReadLargeTuple_AboveFastPathArity_ReturnsLargeTupleWithValues()
    {
        var items = string.Join(",", Enumerable.Range(1, 8).Select(i => $"toInt32({i})"));
        var result = await connection.ExecuteScalarAsync($"select tuple({items})");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            // 8+ element tuples have no System.Tuple<...> arity and stay on the LargeTuple path.
            Assert.That(result, Is.InstanceOf<LargeTuple>());
            Assert.That(tuple.Length, Is.EqualTo(8));
            Assert.That(AsEnumerable(tuple), Is.EqualTo(Enumerable.Range(1, 8)).AsCollection);
        });
    }

    [Test]
    public async Task ShouldReadTuple_WithNullableElement_PreservesNullsAsNull()
    {
        var result = await connection.ExecuteScalarAsync(
            "select tuple(CAST(NULL AS Nullable(Int32)), CAST(42 AS Nullable(Int32)), 'x')");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(result.GetType().FullName, Does.StartWith("System.Tuple`"));
            Assert.That(tuple.Length, Is.EqualTo(3));
            Assert.That(tuple[0], Is.Null);
            Assert.That(tuple[1], Is.EqualTo(42));
            Assert.That(tuple[2], Is.EqualTo("x"));
        });
    }

    [Test]
    public async Task ShouldReadTuple_WithNestedCompositeElements_ReturnsElements()
    {
        var result = await connection.ExecuteScalarAsync(
            "select tuple([toInt32(1), toInt32(2), toInt32(3)], map('a', toUInt64(7)))");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(result.GetType().FullName, Does.StartWith("System.Tuple`"));
            Assert.That(tuple.Length, Is.EqualTo(2));
            Assert.That(tuple[0], Is.EqualTo(new[] { 1, 2, 3 }));
            Assert.That(((IDictionary)tuple[1])["a"], Is.EqualTo(7UL));
        });
    }

    [Test]
    public async Task ShouldReadDynamicWrappedTuple_AcrossMultipleRows_ReturnsSystemTuplePerRow()
    {
        // Every value in a Dynamic column carries its own type header, so a fresh TupleType is
        // decoded per row. Reading several rows exercises the factory cache shared across those
        // instances rather than a single decode.
        var expectedStrings = new[] { "0", "1", "2" };
        var values = new List<ITuple>();
        using (var reader = await connection.ExecuteReaderAsync(
            "select tuple(toInt32(number), toString(number))::Dynamic from system.numbers limit 3"))
        {
            while (reader.Read())
                values.Add((ITuple)reader.GetValue(0));
        }

        Assert.Multiple(() =>
        {
            Assert.That(values, Has.Count.EqualTo(3));
            for (var i = 0; i < values.Count; i++)
            {
                Assert.That(values[i].GetType(), Is.EqualTo(typeof(Tuple<int, string>)));
                Assert.That(values[i][0], Is.EqualTo(i));
                Assert.That(values[i][1], Is.EqualTo(expectedStrings[i]));
            }
        });
    }

    [Test]
    public async Task ShouldReadTuple_WithNullableNothingElement_ReturnsNullElement()
    {
        // select tuple(NULL) yields Tuple(Nullable(Nothing)) — the one element whose framework type
        // is a reference type that only ever carries null.
        var result = await connection.ExecuteScalarAsync("select tuple(NULL)");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.Not.Null);
            Assert.That(result.GetType().FullName, Does.StartWith("System.Tuple`"));
            Assert.That(tuple.Length, Is.EqualTo(1));
            Assert.That(tuple[0], Is.Null);
        });
    }

    [Test]
    public async Task ShouldReadMap_WithTupleKey_ReturnsDictionaryKeyedByTuple()
    {
        // A tuple used as a Map key is materialized by the same read path and then used as a
        // dictionary key, so a wrong CLR type here would throw from Dictionary rather than compare.
        var result = await connection.ExecuteScalarAsync(
            "select map(tuple(toInt32(1), toInt32(2)), toInt32(3))");
        var dictionary = result as IDictionary;
        Assert.Multiple(() =>
        {
            Assert.That(dictionary, Is.Not.Null);
            Assert.That(dictionary.Count, Is.EqualTo(1));
            Assert.That(dictionary[Tuple.Create(1, 2)], Is.EqualTo(3));
        });
    }

    [Test]
    public void MakeTuple_WithSmallTupleType_ReturnsSystemTuplePassingValuesThrough()
    {
        // Read no longer routes small tuples through MakeTuple, so pin MakeTuple's retained
        // behaviour directly: values are passed to the constructor unchanged, and an arity mismatch
        // still throws.
        var tupleType = (TupleType)TypeConverter.ParseClickHouseType("Tuple(Int32, String)", TypeSettings.Default);
        var tuple = tupleType.MakeTuple(1, "a");
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.TypeOf<Tuple<int, string>>());
            Assert.That(tuple[0], Is.EqualTo(1));
            Assert.That(tuple[1], Is.EqualTo("a"));
            Assert.Throws<ArgumentException>(() => tupleType.MakeTuple(1));
        });
    }

    [Test]
    public void ParseClickHouseType_EmptyTuple_ReturnsTupleWithoutUnderlyingTypes()
    {
        var type = TypeConverter.ParseClickHouseType("Tuple()", TypeSettings.Default);
        ClassicAssert.IsInstanceOf<TupleType>(type);
        Assert.That(((TupleType)type).UnderlyingTypes, Is.Empty);
    }

    [Test]
    public async Task ShouldReadEmptyTupleColumn_FollowedByAnotherColumn_ReturnsBothValues()
    {
        // The server accepts Tuple() as a column type and reports it back verbatim, so the client has
        // to parse and read it. An empty tuple has no System.Tuple arity and occupies no bytes on the
        // wire, so it materializes as a tuple of length zero and the following column stays aligned.
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (t Tuple(), value Int32) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {targetTable} VALUES (tuple(), 5)");

        using var reader = await client.ExecuteReaderAsync($"SELECT t, value FROM {targetTable}");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(((ITuple)reader.GetValue(0)).Length, Is.EqualTo(0));
            Assert.That(reader.GetValue(1), Is.EqualTo(5));
        });
    }

    [Test]
    public async Task ShouldReadDynamicWrappedEmptyTuple_ReturnsTupleWithoutElements()
    {
        // A value in a Dynamic column carries its own binary type header, which is decoded
        // structurally rather than through the type parser: a second entry point which reaches an
        // element count of zero on its own.
        using var reader = await client.ExecuteReaderAsync("SELECT tuple()::Dynamic AS d, toInt32(5) AS value");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(((ITuple)reader.GetValue(0)).Length, Is.EqualTo(0));
            Assert.That(reader.GetValue(1), Is.EqualTo(5));
        });
    }

    [Test]
    public async Task ShouldInsertBinary_IntoEmptyTupleColumn_RoundTripsRow()
    {
        // The insert path resolves the destination column types from the server, so it parses
        // Tuple() too; an empty tuple writes no bytes, leaving the next column's bytes in place.
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (t Tuple(), value Int32) ENGINE Memory");

        await client.InsertBinaryAsync(targetTable, ["t", "value"], [[Array.Empty<object>(), 5]]);

        using var reader = await client.ExecuteReaderAsync($"SELECT t, value FROM {targetTable}");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(((ITuple)reader.GetValue(0)).Length, Is.EqualTo(0));
            Assert.That(reader.GetValue(1), Is.EqualTo(5));
        });
    }

    [Test]
    [TestCase("Tuple(String, Int32)")]
    [TestCase("Tuple(name String, age Int32)")]
    public void ShouldParseNamedTupleFields(string typeString)
    {
        var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
        ClassicAssert.IsInstanceOf<TupleType>(type);
    }

    [Test]
    [TestCase("Tuple(name String, status Enum8('Active' = 0, 'Inactive' = 1))")]
    [TestCase("Tuple(id Int32, value Decimal(10, 2))")]
    [TestCase("Tuple(timestamp DateTime64(3, 'UTC'), value Float64)")]
    [TestCase("Tuple(code FixedString(5), count Int32)")]
    [TestCase("Tuple(name String, tags Array(String))")]
    [TestCase("Tuple(name String, optional Nullable(Int32))")]
    [TestCase("Tuple(key String, value LowCardinality(String))")]
    public void ShouldParseNamedTupleWithParameterizedTypes(string typeString)
    {
        // Named tuple fields with parameterized types should parse without throwing
        Assert.DoesNotThrow(() =>
        {
            var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
            ClassicAssert.IsInstanceOf<TupleType>(type);
            var tupleType = (TupleType)type;
            Assert.That(tupleType.UnderlyingTypes.Length, Is.EqualTo(2));
        });
    }

    /// <summary>
    /// Element names which the server requires to be backtick-quoted, in the spelling it both
    /// accepts in DDL and reports back in the column type. The escape forms are the server's own:
    /// <c>\`</c> for a backtick, <c>\'</c> for a single quote, <c>\n</c>/<c>\t</c>/<c>\r</c> for
    /// whitespace and <c>\\</c> for a backslash.
    /// </summary>
    private static readonly string[] QuotedElementNames =
    [
        "`a b`",
        "`a b c`",
        "`a,b`",
        "`a(b)`",
        "`a.b c`",
        @"`a\'b`",
        @"`a\`b c`",
        @"`a\nb c`",
        @"`a\tb c`",
        @"`a\rb c`",
        @"`a\\`",
    ];

    [Test]
    [TestCaseSource(nameof(QuotedElementNames))]
    public async Task ShouldRoundTripTupleColumn_WithBacktickQuotedElementName_ReturnsElements(string elementName)
    {
        var targetTable = CreateTableName($"quoted_element_{elementName}");
        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (t Tuple({elementName} Int64, r String)) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {targetTable} VALUES ((1, 'a'))");

        var result = await client.ExecuteScalarAsync($"SELECT t FROM {targetTable}");

        var tuple = result as ITuple;
        Assert.That(tuple, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(2));
            Assert.That(tuple[0], Is.EqualTo(1L));
            Assert.That(tuple[1], Is.EqualTo("a"));
        });
    }

    [Test]
    [TestCaseSource(nameof(QuotedElementNames))]
    public async Task ShouldRoundTripNested_WithBacktickQuotedElementName_ReturnsElements(string elementName)
    {
        // The element type is parameterized on purpose: a Nested element whose type is a bare name
        // is resolved by NestedType itself, so only a parameterized one reaches the type parser.
        var result = await client.ExecuteScalarAsync(
            $"SELECT CAST([tuple(toDecimal64(1.25, 2), 'a')] AS Nested({elementName} Decimal(10, 2), r String))");

        var rows = (IList)result;
        Assert.That(rows, Has.Count.EqualTo(1));
        var tuple = rows[0] as ITuple;
        Assert.That(tuple, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(2));
            Assert.That(tuple[0], Is.EqualTo(new ClickHouseDecimal(1.25m)));
            Assert.That(tuple[1], Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task ShouldRoundTripTupleColumn_WithQuotedElementNamesAndParameterizedElementTypes_ReturnsElements()
    {
        // A quoted element name next to element types which carry their own arguments, including
        // single-quoted ones containing a space: both quote kinds occur in the same declaration.
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (t Tuple(`a b` Decimal(10, 2), `c d` Map(String, Array(Int32)), `e f` Enum8('x y' = 1, 'z' = 2))) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {targetTable} VALUES ((1.25, map('k', [1, 2]), 'x y'))");

        var result = await client.ExecuteScalarAsync($"SELECT t FROM {targetTable}");

        var tuple = result as ITuple;
        Assert.That(tuple, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(3));
            Assert.That(tuple[0], Is.EqualTo(new ClickHouseDecimal(1.25m)));
            Assert.That(((IDictionary)tuple[1])["k"], Is.EqualTo(new[] { 1, 2 }).AsCollection);
            Assert.That(tuple[2], Is.EqualTo("x y"));
        });
    }

    [Test]
    public async Task ShouldRoundTripColumns_WithQuotedTupleElementNameWrappedInAnotherType_ReturnsElements()
    {
        // The quoted element name also has to be resolved when the named tuple is not the outermost
        // type: inside an Array, as a Map value, and as an element of an enclosing named tuple.
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync(
            $@"CREATE TABLE {targetTable} (
                    a Array(Tuple(`a b` Int64, c String)),
                    m Map(String, Tuple(`d e` Int64, f String)),
                    n Tuple(`g h` Tuple(`i j` Int64, k String)),
                    s Tuple(`l m` Int64)) ENGINE Memory");
        await client.ExecuteNonQueryAsync(
            $"INSERT INTO {targetTable} VALUES ([(1, 'x')], map('k', (2, 'y')), ((3, 'z')), tuple(4))");

        using var reader = await client.ExecuteReaderAsync($"SELECT a, m, n, s FROM {targetTable}");
        Assert.That(reader.Read(), Is.True);

        var array = (IList)reader.GetValue(0);
        var map = (IDictionary)reader.GetValue(1);
        var nested = (ITuple)reader.GetValue(2);
        var single = (ITuple)reader.GetValue(3);
        Assert.Multiple(() =>
        {
            Assert.That(((ITuple)array[0])[0], Is.EqualTo(1L));
            Assert.That(((ITuple)array[0])[1], Is.EqualTo("x"));
            Assert.That(((ITuple)map["k"])[0], Is.EqualTo(2L));
            Assert.That(((ITuple)map["k"])[1], Is.EqualTo("y"));
            Assert.That(((ITuple)nested[0])[0], Is.EqualTo(3L));
            Assert.That(((ITuple)nested[0])[1], Is.EqualTo("z"));
            Assert.That(single[0], Is.EqualTo(4L));
        });
    }

    [Test]
    [TestCaseSource(nameof(QuotedElementNames))]
    public async Task ShouldInsertBinary_IntoTupleColumnWithBacktickQuotedElementName_RoundTripsElements(string elementName)
    {
        // The insert path resolves the destination column types from the server too, so it parses
        // the same quoted element name as the read path.
        var targetTable = CreateTableName($"insert_quoted_element_{elementName}");
        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (id Int32, t Tuple({elementName} Int64, r String)) ENGINE Memory");

        await client.InsertBinaryAsync(
            targetTable,
            ["id", "t"],
            [[1, Tuple.Create(2L, "a")]]);

        var result = await client.ExecuteScalarAsync($"SELECT t FROM {targetTable}");

        var tuple = result as ITuple;
        Assert.That(tuple, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(2));
            Assert.That(tuple[0], Is.EqualTo(2L));
            Assert.That(tuple[1], Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task ShouldRoundTripDynamicWrappedTuple_WithBacktickQuotedElementName_ReturnsElements()
    {
        // A value in a Dynamic column carries its own type header — the server sends the quoted name
        // there too. That header is decoded structurally rather than through ExtractTypeName, so this
        // is a contrast case: it resolved before this change and has to keep resolving after it.
        var result = await client.ExecuteScalarAsync(
            "SELECT CAST(tuple(toInt64(1), 'a') AS Tuple(`p q` Int64, r String))::Dynamic");

        var tuple = result as ITuple;
        Assert.That(tuple, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(2));
            Assert.That(tuple[0], Is.EqualTo(1L));
            Assert.That(tuple[1], Is.EqualTo("a"));
        });
    }

    [Test]
    [TestCase("`p q`")]
    [TestCase(@"`p\`q r`")]
    [TestCase("`p``q r`")]
    public async Task ShouldRoundTripTupleParameter_WithBacktickQuotedElementNameInTypeHint_ReturnsElement(string elementName)
    {
        // A parameter type hint is written by hand rather than reported by the server, so it reaches
        // the parser through the parameter formatter instead of a result-set header — and may double
        // the backtick the way SQL does rather than escaping it the way the server does.
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT tupleElement({{var:Tuple({elementName} Int64, r String)}}, 2)";
        command.AddParameter("var", Tuple.Create(1L, "a"));

        var result = await command.ExecuteScalarAsync();

        Assert.That(result, Is.EqualTo("a"));
    }

    [Test]
    [TestCase("Tuple(`a b Int64, c String)")]
    [TestCase("Nested(`a b`, c String)")]
    public void ParseClickHouseType_ElementQuotingIsMalformed_ThrowsArgumentException(string typeString)
    {
        // Not a shape the server ever sends, so it has no round-trip form: an unterminated or
        // type-less element declaration must still be rejected rather than silently mis-parsed.
        Assert.Throws<ArgumentException>(
            () => TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default));
    }
}
