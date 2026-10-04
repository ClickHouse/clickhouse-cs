using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// <c>GetFieldValue&lt;T&gt;</c> for tuple targets the materialized value is not: <see cref="ValueTuple"/> of any
/// arity and <c>Tuple`8</c> (more than seven elements through <c>TRest</c>), as themselves, as array elements and
/// nested in a tuple.
/// </summary>
[TestFixture]
public class TupleFieldValueTests : AbstractConnectionTestFixture
{
    // A ten-element tuple mixing the element types of a typical aggregated report row.
    private const string WideTupleSql =
        "tuple(toInt32(1), toInt64(2), toInt64(3), toInt64(4), toInt64(-1), toInt32(6), " +
        "toDate('2025-01-15'), toDate('2025-01-31'), toDate('2025-02-01'), toInt8(2))";

    private async Task<T> ReadFieldAsync<T>(string sql)
    {
        using var reader = await client.ExecuteReaderAsync(sql);
        Assert.That(reader.Read(), Is.True);
        return reader.GetFieldValue<T>(0);
    }

    [Test]
    public async Task GetFieldValue_SingleElementValueTuple_ReturnsValueTuple()
    {
        var value = await ReadFieldAsync<ValueTuple<int>>("SELECT tuple(toInt32(5))");
        Assert.That(value, Is.EqualTo(new ValueTuple<int>(5)));
    }

    [Test]
    public async Task GetFieldValue_ValueTupleOfMixedElements_ReturnsValueTuple()
    {
        var value = await ReadFieldAsync<(int Id, string Name, DateTime Day)>(
            "SELECT tuple(toInt32(7), 'seven', toDate('2025-01-15'))");
        Assert.That(value, Is.EqualTo((7, "seven", new DateTime(2025, 1, 15))));
    }

    [Test]
    public async Task GetFieldValue_SevenElementValueTuple_ReturnsValueTuple()
    {
        var value = await ReadFieldAsync<(int, int, int, int, int, int, int)>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7))");
        Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, 7)));
    }

    [Test]
    public async Task GetFieldValue_EightElementValueTuple_FillsRestFromEighthElement()
    {
        var value = await ReadFieldAsync<(int, int, int, int, int, int, int, int)>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))");
        Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, 7, 8)));
    }

    [Test]
    public async Task GetFieldValue_TenElementValueTupleOfMixedElements_ReturnsEveryElement()
    {
        var value = await ReadFieldAsync<(int CategoryId, long DescriptiveCategoryId, long DescriptiveTypeId,
            long WarehouseId, long SupplyId, int Quantity,
            DateTime BeginDate, DateTime EndDate, DateTime NextPaidDate, sbyte ItemTag)>($"SELECT {WideTupleSql}");

        Assert.That(value, Is.EqualTo((1, 2L, 3L, 4L, -1L, 6,
            new DateTime(2025, 1, 15), new DateTime(2025, 1, 31), new DateTime(2025, 2, 1), (sbyte)2)));
    }

    [Test]
    public async Task GetFieldValue_FifteenElementValueTuple_FillsNestedRest()
    {
        // Fifteen elements nest TRest twice: ValueTuple`8<7 elements, ValueTuple`8<7 elements, ValueTuple<string>>>.
        var value = await ReadFieldAsync<(int, int, int, int, int, int, int, int, int, int, int, int, int, int, string)>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), " +
            "toInt32(8), toInt32(9), toInt32(10), toInt32(11), toInt32(12), toInt32(13), toInt32(14), 'fifteen')");
        Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, "fifteen")));
    }

    [Test]
    public async Task GetFieldValue_SystemTupleWithRest_ReturnsTupleOfEightTypeArguments()
    {
        var value = await ReadFieldAsync<Tuple<int, int, int, int, int, int, int, Tuple<int, string>>>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8), 'nine')");

        var expected = new Tuple<int, int, int, int, int, int, int, Tuple<int, string>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, "nine"));
        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected));
            Assert.That(((ITuple)value).Length, Is.EqualTo(9));
        });
    }

    [Test]
    public async Task GetFieldValue_ArrayOfTenElementValueTuples_ReturnsArray()
    {
        var value = await ReadFieldAsync<(int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte)[]>(
            "SELECT groupArray(tuple(toInt32(number), toInt64(number * 10), toInt64(3), toInt64(4), toInt64(-1), " +
            "toInt32(6), toDate('2025-01-15') + number, toDate('2025-01-31'), toDate('2025-02-01'), toInt8(number))) " +
            "FROM (SELECT number FROM numbers(3) ORDER BY number)");

        var expected = new (int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte)[3];
        for (var i = 0; i < expected.Length; i++)
        {
            expected[i] = (i, i * 10L, 3L, 4L, -1L, 6,
                new DateTime(2025, 1, 15 + i), new DateTime(2025, 1, 31), new DateTime(2025, 2, 1), (sbyte)i);
        }
        Assert.That(value, Is.EqualTo(expected));
    }

    [Test]
    public async Task GetFieldValue_EmptyArrayOfTenElementTuples_ReturnsEmptyArray()
    {
        var value = await ReadFieldAsync<(int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte)[]>(
            $"SELECT arrayFilter(x -> 0, [{WideTupleSql}])");
        Assert.That(value, Is.Empty);
    }

    [Test]
    public async Task GetFieldValue_ArrayOfSmallValueTuples_ReturnsArray()
    {
        var value = await ReadFieldAsync<(int, string)[]>("SELECT [tuple(toInt32(1), 'a'), tuple(toInt32(2), 'b')]");
        Assert.That(value, Is.EqualTo(new[] { (1, "a"), (2, "b") }));
    }

    [Test]
    public async Task GetFieldValue_WideTupleColumnAcrossRows_ReturnsEachRow()
    {
        using var reader = await client.ExecuteReaderAsync(
            "SELECT tuple(toInt32(number), toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toString(number)) " +
            "FROM (SELECT number FROM numbers(3) ORDER BY number)");

        var values = new List<(int, int, int, int, int, int, int, string)>();
        while (reader.Read())
            values.Add(reader.GetFieldValue<(int, int, int, int, int, int, int, string)>(0));

        Assert.That(values, Is.EqualTo(new[]
        {
            (0, 1, 2, 3, 4, 5, 6, "0"),
            (1, 1, 2, 3, 4, 5, 6, "1"),
            (2, 1, 2, 3, 4, 5, 6, "2"),
        }));
    }

    [Test]
    public async Task GetFieldValue_TupleNestedAsElement_ConvertsInnerTuple()
    {
        var value = await ReadFieldAsync<(int, (string, byte))>("SELECT tuple(toInt32(1), tuple('a', toUInt8(2)))");
        Assert.That(value, Is.EqualTo((1, ("a", (byte)2))));
    }

    [Test]
    public async Task GetFieldValue_TupleNestedAsEighthElement_IsNotTakenForRest()
    {
        // The eighth element is itself a one-element tuple, so TRest holds a ValueTuple<ValueTuple<string>>.
        var value = await ReadFieldAsync<(int, int, int, int, int, int, int, ValueTuple<string>)>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), tuple('x'))");
        Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, 7, new ValueTuple<string>("x"))));
    }

    [Test]
    public async Task GetFieldValue_WideTupleInsideSystemTuple_ConvertsOnlyTheWideTuple()
    {
        var value = await ReadFieldAsync<Tuple<string, (int, int, int, int, int, int, int, int)>>(
            "SELECT tuple('outer', tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8)))");
        Assert.That(value, Is.EqualTo(Tuple.Create("outer", (1, 2, 3, 4, 5, 6, 7, 8))));
    }

    [Test]
    public async Task GetFieldValue_TupleHoldingArrayOfWideTuples_ConvertsArrayElements()
    {
        var value = await ReadFieldAsync<(string, (int, int, int, int, int, int, int, int)[])>(
            "SELECT tuple('k', [tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))])");
        Assert.Multiple(() =>
        {
            Assert.That(value.Item1, Is.EqualTo("k"));
            Assert.That(value.Item2, Is.EqualTo(new[] { (1, 2, 3, 4, 5, 6, 7, 8) }));
        });
    }

    [Test]
    public async Task GetFieldValue_NullableElements_ReturnsNullAndValue()
    {
        var value = await ReadFieldAsync<(int?, int?, string)>(
            "SELECT tuple(CAST(NULL AS Nullable(Int32)), CAST(42 AS Nullable(Int32)), CAST(NULL AS Nullable(String)))");
        Assert.That(value, Is.EqualTo(((int?)null, (int?)42, (string)null)));
    }

    [Test]
    public async Task GetFieldValue_WideTupleWithNullableElements_ReturnsNullAndValue()
    {
        var value = await ReadFieldAsync<(int, int, int, int, int, int, int?, string, int?)>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), " +
            "CAST(NULL AS Nullable(Int32)), CAST(NULL AS Nullable(String)), CAST(9 AS Nullable(Int32)))");
        Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, (int?)null, (string)null, (int?)9)));
    }

    [Test]
    public async Task GetFieldValue_NullableTargetOverNonNullableElement_ReturnsValue()
    {
        // The same cast GetFieldValue<int?> accepts over an Int32 column.
        var value = await ReadFieldAsync<(int?, string)>("SELECT tuple(toInt32(3), 'c')");
        Assert.That(value, Is.EqualTo(((int?)3, "c")));
    }

    [Test]
    public async Task GetFieldValue_NamedTuple_ReturnsElementsByPosition()
    {
        var value = await ReadFieldAsync<(int, string, int, int, int, int, int, int)>(
            "SELECT CAST(tuple(1, 'b', 3, 4, 5, 6, 7, 8) AS " +
            "Tuple(a Int32, b String, c Int32, d Int32, e Int32, f Int32, g Int32, h Int32))");
        Assert.That(value, Is.EqualTo((1, "b", 3, 4, 5, 6, 7, 8)));
    }

    [Test]
    public async Task GetFieldValue_LowCardinalityElement_ReturnsString()
    {
        var value = await ReadFieldAsync<(int, int, int, int, int, int, int, string)>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), " +
            "toLowCardinality('lc'))");
        Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, 7, "lc")));
    }

    [Test]
    public async Task GetFieldValue_ElementCountMismatch_ThrowsInvalidCastException()
    {
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<(int, int, int)>("SELECT tuple(toInt32(1), toInt32(2))"));
        Assert.That(ex.Message, Does.Contain("Column [0]").And.Contain("2 elements"));
    }

    [Test]
    public void GetFieldValue_ElementTypeMismatch_ThrowsInvalidCastException()
    {
        // No numeric widening: an Int32 element is not read as long, as a whole Int32 column is not.
        Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<(long, string)>("SELECT tuple(toInt32(1), 'a')"));
    }

    [Test]
    public void GetFieldValue_NullIntoNonNullableElement_ThrowsInvalidCastException()
    {
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<(int, string)>("SELECT tuple(CAST(NULL AS Nullable(Int32)), 'a')"));
        Assert.That(ex.Message, Does.Contain("non-nullable"));
    }

    [Test]
    public void GetFieldValue_NonTupleColumn_ThrowsInvalidCastException()
    {
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<(int, int)>("SELECT toInt32(1)"));
        Assert.That(ex.Message, Does.Contain("not a tuple"));
    }

    [Test]
    public void GetFieldValue_RestThatIsNotATuple_ThrowsInvalidCastException()
    {
        // ValueTuple`8 only constrains TRest to a struct, so this type can be named, but its constructor rejects it.
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<ValueTuple<int, int, int, int, int, int, int, int>>(
                "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))"));
        Assert.That(ex.Message, Does.Contain("not a valid tuple type"));
    }

    [Test]
    public void GetFieldValue_RestOfTheOtherTupleKind_ThrowsInvalidCastException()
    {
        // Tuple`8 does not constrain TRest, but its constructor accepts only a System.Tuple there.
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<Tuple<int, int, int, int, int, int, int, ValueTuple<int>>>(
                "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))"));
        Assert.That(ex.Message, Does.Contain("not a valid tuple type"));
    }

    [Test]
    public void GetFieldValue_NullColumn_ThrowsInvalidCastException()
    {
        var ex = Assert.ThrowsAsync<InvalidCastException>(() => ReadFieldAsync<(int, int)>("SELECT NULL"));
        Assert.That(ex.Message, Does.Contain("A null value"));
    }

    [Test]
    public void GetFieldValue_ArrayTargetOverNonArrayColumn_ThrowsInvalidCastException()
    {
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<(int, string)[]>("SELECT tuple(toInt32(1), 'a')"));
        Assert.That(ex.Message, Does.Contain("not an array"));
    }

    [Test]
    public async Task GetFieldValue_EmptyTupleColumn_ReturnsEmptyValueTuple()
    {
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (t Tuple(), ts Array(Tuple())) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {targetTable} VALUES (tuple(), [tuple(), tuple()])");

        using var reader = await client.ExecuteReaderAsync($"SELECT t, ts FROM {targetTable}");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetFieldValue<ValueTuple>(0), Is.EqualTo(default(ValueTuple)));
            Assert.That(reader.GetFieldValue<ValueTuple[]>(1), Is.EqualTo(new[] { default(ValueTuple), default(ValueTuple) }));
        });
    }

    [Test]
    public async Task GetFieldValue_SevenElementTupleWithEmptyRest_ReturnsValueTuple()
    {
        // ValueTuple`8 accepts the empty ValueTuple as TRest, which leaves seven elements.
        var value = await ReadFieldAsync<ValueTuple<int, int, int, int, int, int, int, ValueTuple>>(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7))");
        Assert.That(value, Is.EqualTo(new ValueTuple<int, int, int, int, int, int, int, ValueTuple>(1, 2, 3, 4, 5, 6, 7, default)));
    }

    [Test]
    public async Task GetFieldValue_NullArrayElement_ReturnsNull()
    {
        // A NULL element converts to null for a reference-type target, as a NULL String element does.
        var value = await ReadFieldAsync<((int, int)[], int)>("SELECT tuple(NULL, toInt32(1))");
        Assert.That(value, Is.EqualTo((((int, int)[])null, 1)));
    }

    [Test]
    public async Task GetFieldValue_NullSystemTupleElement_ReturnsNull()
    {
        var value = await ReadFieldAsync<(Tuple<int, (int, int)>, int)>("SELECT tuple(NULL, toInt32(1))");
        Assert.That(value, Is.EqualTo(((Tuple<int, (int, int)>)null, 1)));
    }

    [Test]
    public void GetFieldValue_NullValueTupleElement_ThrowsInvalidCastException()
    {
        var ex = Assert.ThrowsAsync<InvalidCastException>(() =>
            ReadFieldAsync<((int, int), int)>("SELECT tuple(NULL, toInt32(1))"));
        Assert.That(ex.Message, Does.Contain("A null value"));
    }

    [Test]
    public async Task GetFieldValue_EmptyArrayOfMatchingSmallTuples_ReturnsEmptyArray()
    {
        var value = await ReadFieldAsync<(int, string)[]>("SELECT arrayFilter(x -> 0, [tuple(toInt32(1), 'a')])");
        Assert.That(value, Is.Empty);
    }

    private static IEnumerable<TestCaseData> MismatchedEmptyArrays()
    {
        yield return new TestCaseData(
            "SELECT CAST([] AS Array(Int32))",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, string)[]>(0)))
            .SetArgDisplayNames("ElementsAreNotTuples");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1), 'a')])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(long, string)[]>(0)))
            .SetArgDisplayNames("ElementTypeDiffers");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1), 'a')])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, string, int)[]>(0)))
            .SetArgDisplayNames("ElementCountDiffers");
        yield return new TestCaseData(
            "SELECT CAST([] AS Array(Array(Int32)))",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, int)[][]>(0)))
            .SetArgDisplayNames("InnerElementsAreNotTuples");
        yield return new TestCaseData(
            "SELECT CAST([] AS Array(Int32))",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, int)[][]>(0)))
            .SetArgDisplayNames("ElementsAreNotArrays");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, int, int, int, int, int, int, int)[]>(0)))
            .SetArgDisplayNames("ElementCountDiffersInRest");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(NULL, toInt32(1))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<((int, int), int)[]>(0)))
            .SetArgDisplayNames("NullElementIntoValueTuple");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<ValueTuple<DayOfWeek?>[]>(0)))
            .SetArgDisplayNames("UnderlyingTypeIntoNullableEnum");
    }

    private static IEnumerable<TestCaseData> TargetsHoldingAnInvalidTuple()
    {
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<ValueTuple<int, int, int, int, int, int, int, int>[]>(0)))
            .SetArgDisplayNames("RestIsNotATuple");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<ValueTuple<int, int, int, int, int, int, int, int>[]>(0)))
            .SetArgDisplayNames("RestIsNotATupleOverWideTuples");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(0), tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8)))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, ValueTuple<int, int, int, int, int, int, int, int>)[]>(0)))
            .SetArgDisplayNames("NestedRestIsNotATuple");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [NULL])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<Tuple<int, int, int, int, int, int, int, ValueTuple<int>>[]>(0)))
            .SetArgDisplayNames("RestOfTheOtherKindOverNullElements");
        yield return new TestCaseData(
            "SELECT [tuple(NULL, toInt32(1))]",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(ValueTuple<int, int, int, int, int, int, int, int>[], int)[]>(0)))
            .SetArgDisplayNames("NestedRestUnderANullElement");
        yield return new TestCaseData(
            "SELECT arrayFilter(x -> 0, [tuple(NULL, toInt32(1))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(ValueTuple<int, int, int, int, int, int, int, int>[], int)[]>(0)))
            .SetArgDisplayNames("NestedRestUnderANullElementInAnEmptyArray");
        yield return new TestCaseData(
            "SELECT tuple(NULL, toInt32(1))",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(ValueTuple<int, int, int, int, int, int, int, int>?, int)>(0)))
            .SetArgDisplayNames("NullableRestUnderANullElement");
    }

    [TestCaseSource(nameof(TargetsHoldingAnInvalidTuple))]
    public async Task GetFieldValue_TargetHoldingAnInvalidTuple_ThrowsWhateverTheData(
        string sql, Func<ClickHouseDataReader, object> read)
    {
        // Such a target is not a valid tuple shape, so it is rejected before the data is looked at: a NULL in place
        // of the invalid part, or an empty array, does not let it through.
        using var reader = await client.ExecuteReaderAsync(sql);
        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidCastException>(() => read(reader));
        Assert.That(ex.Message, Does.Contain("not a valid tuple type"));
    }

    [Test]
    public async Task GetFieldValue_NullableElementAsNullableOfAnotherType_ReadsNullsAndEmptyArraysButRejectsAValue()
    {
        // A NULL converts to any target that holds null, so an empty array passes as an array of NULLs does; a
        // non-null Int64 still does not cast to int?.
        using var reader = await client.ExecuteReaderAsync(
            "SELECT [tuple(CAST(NULL AS Nullable(Int64)))], arrayFilter(x -> 0, [tuple(CAST(NULL AS Nullable(Int64)))]), " +
            "[tuple(CAST(1 AS Nullable(Int64)))], arrayFilter(x -> 0, [tuple(CAST(NULL AS Nullable(Int64)), toInt32(1))])");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetFieldValue<ValueTuple<int?>[]>(0), Is.EqualTo(new[] { new ValueTuple<int?>(null) }));
            Assert.That(reader.GetFieldValue<ValueTuple<int?>[]>(1), Is.Empty);
            Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<ValueTuple<int?>[]>(2));
            Assert.That(reader.GetFieldValue<((int, int)[], int)[]>(3), Is.Empty);
        });
    }

    private static IEnumerable<TestCaseData> MisplacedElements()
    {
        yield return new TestCaseData(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8), toInt32(9))",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(int, int, int, int, int, int, int, int, long)>(0)),
            "Item9: ")
            .SetArgDisplayNames("ElementInRest");
        yield return new TestCaseData(
            "SELECT tuple('k', [tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))])",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(string, (int, int, int, int, int, int, int, long)[])>(0)),
            "Item2[].Item8: ")
            .SetArgDisplayNames("ElementOfANestedArray");
        yield return new TestCaseData(
            "SELECT [tuple(toInt32(1), 'a')]",
            (Func<ClickHouseDataReader, object>)(r => r.GetFieldValue<(long, string)[]>(0)),
            "[].Item1: ")
            .SetArgDisplayNames("ElementOfAnArray");
    }

    [TestCaseSource(nameof(MisplacedElements))]
    public async Task GetFieldValue_ElementOfTheWrongType_NamesItsPosition(
        string sql, Func<ClickHouseDataReader, object> read, string position)
    {
        using var reader = await client.ExecuteReaderAsync(sql);
        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidCastException>(() => read(reader));
        Assert.That(ex.Message, Does.Contain("Column [0]").And.Contain(position));
    }

    [Test]
    public async Task GetFieldValue_InterfaceElementTargetOverNullableElement_ReadsEmptyAndNonEmptyArrays()
    {
        // A non-null Nullable(Int32) element is a boxed int, which casts to an interface int implements.
        using var reader = await client.ExecuteReaderAsync(
            "SELECT arrayFilter(x -> 0, [tuple(CAST(1 AS Nullable(Int32)), 'a')]), [tuple(CAST(1 AS Nullable(Int32)), 'a')]");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetFieldValue<(IComparable, string)[]>(0), Is.Empty);
            Assert.That(reader.GetFieldValue<(IComparable, string)[]>(1), Is.EqualTo(new[] { ((IComparable)1, "a") }));
        });
    }

    [Test]
    public async Task GetFieldValue_EmptyArrayWithNullElement_ReturnsEmptyArray()
    {
        // A NULL element converts to null for a reference-type target, so an empty array of them passes too.
        var value = await ReadFieldAsync<((int, int)[], int)[]>("SELECT arrayFilter(x -> 0, [tuple(NULL, toInt32(1))])");
        Assert.That(value, Is.Empty);
    }

    [Test]
    public async Task GetFieldValue_EmptyArrayOfUntypedElements_ReturnsEmptyArray()
    {
        // A Dynamic element has no static type to check against, so an empty array passes.
        var value = await ReadFieldAsync<(int, string)[]>("SELECT arrayFilter(x -> 0, [1::Dynamic])");
        Assert.That(value, Is.Empty);
    }

    [Test]
    public async Task GetFieldValue_EnumElementTargetOverItsUnderlyingType_ReadsEmptyAndNonEmptyArrays()
    {
        // Unboxing reads an Int32 as an int-backed enum; the empty-array check accepts what the cast accepts.
        using var reader = await client.ExecuteReaderAsync(
            "SELECT arrayFilter(x -> 0, [tuple(toInt32(1), 'a')]), [tuple(toInt32(1), 'a')]");
        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetFieldValue<(DayOfWeek, string)[]>(0), Is.Empty);
            Assert.That(reader.GetFieldValue<(DayOfWeek, string)[]>(1), Is.EqualTo(new[] { (DayOfWeek.Monday, "a") }));
        });
    }

    [TestCaseSource(nameof(MismatchedEmptyArrays))]
    public async Task GetFieldValue_EmptyArrayOfMismatchedElements_ThrowsInvalidCastException(
        string sql, Func<ClickHouseDataReader, object> read)
    {
        // The plain cast rejects int[] as long[] even when empty; an empty array here is rejected the same way.
        using var reader = await client.ExecuteReaderAsync(sql);
        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidCastException>(() => read(reader));
        Assert.That(ex.Message, Does.Contain("An empty array"));
    }

    [Test]
    public async Task GetFieldValue_EmptyMapReadAsPairs_ThrowsInvalidCastException()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.MapReadMode = MapReadMode.KeyValuePairs;
        using var pairClient = new ClickHouseClient(new ClickHouseClientSettings(builder));
        using var reader = await pairClient.ExecuteReaderAsync("SELECT CAST(map() AS Map(String, Int32))");
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<(string, int)[]>(0));
        Assert.That(ex.Message, Does.Contain("An empty array"));
    }

    [Test]
    public async Task GetFieldValue_SmallSystemTupleTarget_ReturnsTheMaterializedInstance()
    {
        // A target with no ValueTuple or Tuple`8 stays on the plain cast: no conversion, no copy.
        using var reader = await client.ExecuteReaderAsync("SELECT tuple(toInt32(1), 'a')");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<Tuple<int, string>>(0), Is.SameAs(reader.GetValue(0)));
    }

    [Test]
    public async Task GetFieldValue_WithReadValueConverter_ConvertsOnceWithTargetType()
    {
        var converter = new RecordingConverter();
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            ReadValueConverter = converter,
        };
        using var recordingClient = new ClickHouseClient(settings);
        using var reader = await recordingClient.ExecuteReaderAsync(
            "SELECT tuple(toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5), toInt32(6), toInt32(7), toInt32(8))");
        Assert.That(reader.Read(), Is.True);

        var value = reader.GetFieldValue<(int, int, int, int, int, int, int, int)>(0);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo((1, 2, 3, 4, 5, 6, 7, 8)));
            Assert.That(converter.TypedCalls, Is.EqualTo(new[] { typeof((int, int, int, int, int, int, int, int)) }));
            Assert.That(converter.BoxedCalls, Is.Zero);
        });
    }

    [TestCase(typeof(int), ExpectedResult = false)]
    [TestCase(typeof(int[]), ExpectedResult = false)]
    [TestCase(typeof(object), ExpectedResult = false)]
    [TestCase(typeof(ITuple), ExpectedResult = false)]
    [TestCase(typeof(Tuple<int, string>), ExpectedResult = false)]
    [TestCase(typeof(Tuple<int, string>[]), ExpectedResult = false)]
    [TestCase(typeof(Tuple<int, Tuple<int, string>>), ExpectedResult = false)]
    [TestCase(typeof(ValueTuple), ExpectedResult = true)]
    [TestCase(typeof(ValueTuple[]), ExpectedResult = true)]
    [TestCase(typeof(ValueTuple<int>), ExpectedResult = true)]
    [TestCase(typeof((int, string)[]), ExpectedResult = true)]
    [TestCase(typeof((int, string)[][]), ExpectedResult = true)]
    [TestCase(typeof(Tuple<int, int, int, int, int, int, int, Tuple<int>>), ExpectedResult = true)]
    [TestCase(typeof(Tuple<int, (int, string)>), ExpectedResult = true)]
    [TestCase(typeof(Tuple<int, (int, string)[]>), ExpectedResult = true)]
    [TestCase(typeof((int, string)[,]), ExpectedResult = false)]
    [TestCase(typeof((int, string)?), ExpectedResult = false)]
    [TestCase(typeof(List<(int, string)>), ExpectedResult = false)]
    public bool RequiresConversion_TargetType_ReturnsWhetherItHoldsValueTupleOrTupleOfEight(Type type) =>
        TupleFieldConverter.RequiresConversion(type);

    private sealed class RecordingConverter : IReadValueConverter
    {
        public List<Type> TypedCalls { get; } = new();

        public int BoxedCalls { get; private set; }

        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            BoxedCalls++;
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            TypedCalls.Add(typeof(T));
            return value;
        }
    }
}
