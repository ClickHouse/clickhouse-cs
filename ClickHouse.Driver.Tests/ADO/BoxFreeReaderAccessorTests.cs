using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Pins the accessor semantics of the typed-column-slot reader against a live server.
///
/// <para><see cref="ColumnSlotTests"/> proves a slot decodes the same bytes to the same value as the boxed
/// reader. What it cannot see is the layer above: which slot each accessor reaches for, and — more
/// importantly — that the cases the fast path deliberately declines still fail in exactly the way they used
/// to. Widening, reading a NULL as a non-nullable target, and <c>T = U?</c> all fall through to the boxed
/// cast, and their <see cref="InvalidCastException"/>s are part of the ADO.NET contract callers rely on.</para>
/// </summary>
[TestFixture]
public class BoxFreeReaderAccessorTests : AbstractConnectionTestFixture
{
    private async Task<ClickHouseDataReader> ReadOneAsync(string selectList)
    {
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {selectList}");
        Assert.That(reader.Read(), Is.True);
        return reader;
    }

    // ---- GetFieldValue<T>: the typed slot must produce exactly what the boxed cast produced ----

    [TestCase("toInt8(-8)", typeof(sbyte), (sbyte)-8)]
    [TestCase("toInt16(-16)", typeof(short), (short)-16)]
    [TestCase("toInt32(-32)", typeof(int), -32)]
    [TestCase("toInt64(-64)", typeof(long), -64L)]
    [TestCase("toUInt8(8)", typeof(byte), (byte)8)]
    [TestCase("toUInt16(16)", typeof(ushort), (ushort)16)]
    [TestCase("toUInt32(32)", typeof(uint), 32u)]
    [TestCase("toUInt64(64)", typeof(ulong), 64ul)]
    [TestCase("toFloat32(1.5)", typeof(float), 1.5f)]
    [TestCase("toFloat64(2.5)", typeof(double), 2.5d)]
    [TestCase("true", typeof(bool), true)]
    [TestCase("'abc'", typeof(string), "abc")]
    public async Task GetFieldValue_ExactTargetType_ReturnsValue(string expression, Type target, object expected)
    {
        using var reader = await ReadOneAsync($"{expression} AS c");
        Assert.That(GetFieldValueDynamic(reader, target), Is.EqualTo(expected));
    }

    // Invariant in T, as the boxed unbox-any was: no widening, ever.
    [TestCase("toInt32(1)", typeof(long))]
    [TestCase("toFloat32(1)", typeof(double))]
    [TestCase("toUInt8(1)", typeof(int))]
    [TestCase("toInt64(1)", typeof(string))]
    public async Task GetFieldValue_WideningTarget_ThrowsInvalidCast(string expression, Type target)
    {
        using var reader = await ReadOneAsync($"{expression} AS c");
        Assert.Throws<InvalidCastException>(() => GetFieldValueDynamic(reader, target));
    }

    // A Nullable<U> target over a non-nullable column has no typed slot (the slot holds U, not U?), but the
    // boxed fallback lifts it, so it keeps working.
    [Test]
    public async Task GetFieldValue_NullableTargetOverNonNullableColumn_LiftsThroughBoxedFallback()
    {
        using var reader = await ReadOneAsync("toInt64(9) AS c");
        Assert.That(reader.GetFieldValue<long?>(0), Is.EqualTo(9L));
    }

    [Test]
    public async Task GetFieldValue_NullableColumnWithValue_ReadsUnderlyingType()
    {
        using var reader = await ReadOneAsync("toInt64OrNull('7') AS c");
        Assert.Multiple(() =>
        {
            Assert.That(reader.IsDBNull(0), Is.False);
            Assert.That(reader.GetFieldValue<long>(0), Is.EqualTo(7L));
            // T = U? is deliberately left to the boxed fallback (a generic-interface dispatch that would
            // serve both costs 3.5-5.5x the sealed-class check). It must still work.
            Assert.That(reader.GetFieldValue<long?>(0), Is.EqualTo(7L));
        });
    }

    // A NULL cell has no value to hand back as a non-nullable T, so the boxed DBNull cast throws — the
    // pre-slot behaviour, preserved by letting the null case fall through to GetBoxed().
    [Test]
    public async Task GetFieldValue_NullCellAsNonNullableTarget_ThrowsInvalidCast()
    {
        using var reader = await ReadOneAsync("CAST(NULL AS Nullable(Int64)) AS c");
        Assert.That(reader.IsDBNull(0), Is.True);
        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<long>(0));
    }

    // Also pre-slot behaviour, and the surprising one: DBNull does not unbox to Nullable<T> either.
    [Test]
    public async Task GetFieldValue_NullCellAsNullableTarget_ThrowsInvalidCast()
    {
        using var reader = await ReadOneAsync("CAST(NULL AS Nullable(Int64)) AS c");
        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<long?>(0));
    }

    [Test]
    public async Task GetFieldValue_ObjectTarget_ReturnsTheBoxedValueAndDBNullForNull()
    {
        using var reader = await ReadOneAsync("toInt64(5) AS a, CAST(NULL AS Nullable(Int64)) AS b");
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetFieldValue<object>(0), Is.EqualTo(5L));
            Assert.That(reader.GetFieldValue<object>(1), Is.EqualTo(DBNull.Value));
        });
    }

    // A composite has no typed slot; GetFieldValue must keep working through the boxed fallback.
    [Test]
    public async Task GetFieldValue_CompositeColumn_ReadsThroughBoxedFallback()
    {
        using var reader = await ReadOneAsync("array(toInt32(1), toInt32(2), toInt32(3)) AS c");
        Assert.That(reader.GetFieldValue<int[]>(0), Is.EqualTo(new[] { 1, 2, 3 }));
    }

    // Pins the user-visible contract for an AggregateFunction column: the reader opens, FieldCount answers,
    // and the failure arrives on the row with the message that tells you to use xMerge().
    //
    // Deliberately not the guard on ColumnSlotFactory's bail-out ordering, though it reads like one. Slots are
    // built on the first Read(), so an ordering regression would throw the same exception with the same
    // message from this same call; only the unit test
    // (ColumnSlotTests.Create_AggregateFunctionColumn_FallsBackToBoxedWithoutEvaluatingFrameworkType) can
    // distinguish them.
    [Test]
    public async Task Read_AggregateFunctionColumn_OpensTheReaderAndFailsOnlyOnTheValue()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT quantileState(0.5)(number) AS c FROM numbers(10)");

        Assert.That(reader.FieldCount, Is.EqualTo(1));
        var ex = Assert.Throws<AggregateFunctionType.AggregateFunctionException>(() => reader.Read());
        Assert.That(ex.Message, Does.Contain("Merge()"));
    }

    // ---- No current row ----
    //
    // Slots hold typed storage, so without this guard a non-nullable value column would read back as a
    // perfectly plausible 0 / false / Guid.Empty before the first Read() — data-shaped, and indistinguishable
    // from a real value. (The old object[] buffer started all-null, so GetValue returned null and a typed
    // accessor threw NullReferenceException.) Every value accessor now reports the mistake instead.

    private static IEnumerable<TestCaseData> ValueAccessors()
    {
        yield return Accessor("GetValue", r => r.GetValue(0));
        yield return Accessor("Indexer", r => r[0]);
        yield return Accessor("IndexerByName", r => r["a"]);
        yield return Accessor("GetValues", r => r.GetValues(new object[3]));
        yield return Accessor("GetFieldValue", r => r.GetFieldValue<long>(0));
        yield return Accessor("IsDBNull", r => r.IsDBNull(0));
        yield return Accessor("GetInt64", r => r.GetInt64(0));
        yield return Accessor("GetString", r => r.GetString(1));
        yield return Accessor("GetBoolean", r => r.GetBoolean(0));
        yield return Accessor("GetDecimal", r => r.GetDecimal(2));
        yield return Accessor("GetDateTime", r => r.GetDateTime(0));

        static TestCaseData Accessor(string name, Func<ClickHouseDataReader, object> read)
            => new TestCaseData(read).SetArgDisplayNames(name);
    }

    private const string ThreeColumns = "toInt64(1) AS a, 's' AS b, toDecimal64(1.5, 2) AS c";

    [TestCaseSource(nameof(ValueAccessors))]
    public async Task ValueAccessor_BeforeFirstRead_ThrowsInvalidOperation(Func<ClickHouseDataReader, object> read)
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {ThreeColumns}");

        var ex = Assert.Throws<InvalidOperationException>(() => read(reader));
        Assert.That(ex.Message, Does.Contain("Read()"));
    }

    [TestCaseSource(nameof(ValueAccessors))]
    public async Task ValueAccessor_AfterReadReturnsFalse_ThrowsInvalidOperation(Func<ClickHouseDataReader, object> read)
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {ThreeColumns}");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.Read(), Is.False);

        Assert.Throws<InvalidOperationException>(() => read(reader));
    }

    // Column metadata does not depend on a row and must stay reachable — this is what a caller inspecting the
    // shape of an empty result set needs, and what DataTable.Load asks for before its first Read().
    [Test]
    public async Task ColumnMetadata_WithNoCurrentRow_IsStillAvailable()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            $"SELECT {ThreeColumns} FROM system.numbers WHERE 0");

        Assert.Multiple(() =>
        {
            Assert.That(reader.FieldCount, Is.EqualTo(3));
            Assert.That(reader.GetName(0), Is.EqualTo("a"));
            Assert.That(reader.GetOrdinal("b"), Is.EqualTo(1));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(long)));
            Assert.That(reader.GetDataTypeName(0), Is.EqualTo("Int64"));
            Assert.That(reader.GetSchemaTable().Rows, Has.Count.EqualTo(3));
        });
    }

    // ---- IsDBNull now answers from the slot's presence flag rather than inspecting a boxed value ----

    [Test]
    public async Task IsDBNull_AcrossColumnKinds_MatchesNullability()
    {
        using var reader = await ReadOneAsync(
            "toInt64(1) AS nonNullableValue, " +
            "'s' AS nonNullableRef, " +
            "toInt64OrNull('1') AS nullableWithValue, " +
            "CAST(NULL AS Nullable(Int64)) AS nullableValue, " +
            "CAST(NULL AS Nullable(String)) AS nullableRef, " +
            "array(1) AS composite, " +
            "CAST(NULL AS Nullable(UUID)) AS nullableUuid");

        Assert.Multiple(() =>
        {
            Assert.That(reader.IsDBNull(0), Is.False);
            Assert.That(reader.IsDBNull(1), Is.False);
            Assert.That(reader.IsDBNull(2), Is.False);
            Assert.That(reader.IsDBNull(3), Is.True);
            Assert.That(reader.IsDBNull(4), Is.True);
            Assert.That(reader.IsDBNull(5), Is.False);
            Assert.That(reader.IsDBNull(6), Is.True);
        });
    }

    // A slot is reused across rows, so presence has to be re-decoded every row rather than latched.
    [Test]
    public async Task IsDBNull_AlternatingNullsAcrossRows_TracksEachRow()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT if(number % 2 = 0, NULL, toInt64(number)) AS c FROM system.numbers LIMIT 6");

        for (var row = 0; row < 6; row++)
        {
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.IsDBNull(0), Is.EqualTo(row % 2 == 0), $"row {row}");
            if (row % 2 != 0)
                Assert.That(reader.GetFieldValue<long>(0), Is.EqualTo((long)row), $"row {row}");
        }
    }

    // ---- GetValues / GetValue keep the boxed contract the BCL data-binding stack depends on ----

    [Test]
    public async Task GetValues_MixedRow_ProducesTheSameBoxedValuesAsGetValue()
    {
        using var reader = await ReadOneAsync(
            "toInt64(1) AS a, 'b' AS b, CAST(NULL AS Nullable(Int64)) AS c, array(1, 2) AS d");

        var values = new object[reader.FieldCount];
        Assert.That(reader.GetValues(values), Is.EqualTo(4));
        Assert.Multiple(() =>
        {
            Assert.That(values[0], Is.EqualTo(1L));
            Assert.That(values[1], Is.EqualTo("b"));
            Assert.That(values[2], Is.EqualTo(DBNull.Value));
            Assert.That(values[3], Is.EqualTo(new[] { 1, 2 }));
            for (var i = 0; i < values.Length; i++)
                Assert.That(values[i], Is.EqualTo(reader.GetValue(i)), $"column {i}");
        });
    }

    // GetValues writes as many cells as the caller's array has room for, and no more.
    [Test]
    public async Task GetValues_ShorterDestinationArray_FillsOnlyWhatFits()
    {
        using var reader = await ReadOneAsync("toInt64(1) AS a, toInt64(2) AS b, toInt64(3) AS c");

        var values = new object[2];
        Assert.That(reader.GetValues(values), Is.EqualTo(2));
        Assert.That(values, Is.EqualTo(new object[] { 1L, 2L }));
    }

    // Boxing is now per call rather than once per row, so the same cell yields two distinct boxes. They must
    // still compare equal — that is the comparison ADO.NET consumers and DataTable actually use.
    [Test]
    public async Task GetValue_CalledTwiceOnAValueTypeCell_ReturnsEqualValues()
    {
        using var reader = await ReadOneAsync("toInt64(42) AS c");
        Assert.That(reader.GetValue(0), Is.EqualTo(reader.GetValue(0)));
    }

    // Values handed out must not be invalidated by advancing the reader: GetValue returns a fresh box, and a
    // reference-typed cell hands out the reference the caller then owns. DataReaderTests exercises the same
    // shape via LINQ over IDataRecord; this states the guarantee directly.
    [Test]
    public async Task GetValue_RetainedAcrossRead_KeepsTheOriginalRowsValue()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT toInt64(number) AS n, toString(number) AS s FROM system.numbers LIMIT 2");

        Assert.That(reader.Read(), Is.True);
        var firstNumber = reader.GetValue(0);
        var firstString = reader.GetValue(1);

        Assert.That(reader.Read(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(firstNumber, Is.EqualTo(0L));
            Assert.That(firstString, Is.EqualTo("0"));
            Assert.That(reader.GetValue(0), Is.EqualTo(1L));
        });
    }

    // ---- Typed accessors: what linq2db's compiled mapper actually calls, one per column per row ----

    [Test]
    public async Task TypedAccessors_ExactColumnTypes_ReturnValues()
    {
        using var reader = await ReadOneAsync(
            "toInt8(-8) AS a, toInt16(-16) AS b, toInt32(-32) AS c, toInt64(-64) AS d, " +
            "toUInt8(8) AS e, toUInt16(16) AS f, toUInt32(32) AS g, toUInt64(64) AS h, " +
            "toFloat32(1.5) AS i, toFloat64(2.5) AS j, true AS k, 'abc' AS l, " +
            "toUUID('11223344-5566-7788-99aa-bbccddeeff00') AS m, " +
            "toIPv6('2001:db8::1') AS n, toInt256(-256) AS o, " +
            "toDateTime('2025-01-15 12:00:00', 'UTC') AS p");

        Assert.Multiple(() =>
        {
            Assert.That(reader.GetSByte(0), Is.EqualTo((sbyte)-8));
            Assert.That(reader.GetInt16(1), Is.EqualTo((short)-16));
            Assert.That(reader.GetInt32(2), Is.EqualTo(-32));
            Assert.That(reader.GetInt64(3), Is.EqualTo(-64L));
            Assert.That(reader.GetByte(4), Is.EqualTo((byte)8));
            Assert.That(reader.GetUInt16(5), Is.EqualTo((ushort)16));
            Assert.That(reader.GetUInt32(6), Is.EqualTo(32u));
            Assert.That(reader.GetUInt64(7), Is.EqualTo(64ul));
            Assert.That(reader.GetFloat(8), Is.EqualTo(1.5f));
            Assert.That(reader.GetDouble(9), Is.EqualTo(2.5d));
            Assert.That(reader.GetBoolean(10), Is.True);
            Assert.That(reader.GetString(11), Is.EqualTo("abc"));
            Assert.That(reader.GetGuid(12), Is.EqualTo(Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00")));
            Assert.That(reader.GetIPAddress(13), Is.EqualTo(System.Net.IPAddress.Parse("2001:db8::1")));
            Assert.That(reader.GetBigInteger(14), Is.EqualTo(new System.Numerics.BigInteger(-256)));
            Assert.That(reader.GetDateTime(15), Is.EqualTo(new DateTime(2025, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
            Assert.That(reader.GetDateTimeOffset(15), Is.EqualTo(new DateTimeOffset(2025, 1, 15, 12, 0, 0, TimeSpan.Zero)));
        });
    }

    // A Nullable(T) slot holds the underlying T, so the typed accessors reach it without a box.
    [Test]
    public async Task TypedAccessors_NullableColumnsWithValues_ReturnUnderlyingValues()
    {
        using var reader = await ReadOneAsync(
            "toInt64OrNull('7') AS a, toFloat64OrNull('1.5') AS b, " +
            "CAST('s' AS Nullable(String)) AS c, CAST(true AS Nullable(Bool)) AS d, " +
            "CAST(false AS Nullable(Bool)) AS e");

        Assert.Multiple(() =>
        {
            Assert.That(reader.GetInt64(0), Is.EqualTo(7L));
            Assert.That(reader.GetDouble(1), Is.EqualTo(1.5d));
            Assert.That(reader.GetString(2), Is.EqualTo("s"));

            // Both polarities, so the assertion cannot be satisfied by a branch that hard-codes one.
            Assert.That(reader.GetBoolean(3), Is.True);
            Assert.That(reader.GetBoolean(4), Is.False);
        });
    }

    // Pre-slot behaviour: the typed accessors were all `(T)GetValue(ordinal)`, so a NULL threw.
    [Test]
    public async Task TypedAccessors_NullCell_ThrowInvalidCast()
    {
        using var reader = await ReadOneAsync(
            "CAST(NULL AS Nullable(Int64)) AS a, CAST(NULL AS Nullable(Float64)) AS b, " +
            "CAST(NULL AS Nullable(UUID)) AS c, CAST(NULL AS Nullable(Bool)) AS d");

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
            Assert.Throws<InvalidCastException>(() => reader.GetDouble(1));
            Assert.Throws<InvalidCastException>(() => reader.GetGuid(2));
            Assert.Throws<InvalidCastException>(() => reader.GetBoolean(3));
        });
    }

    // GetString coerces rather than casts, and DBNull.Value.ToString() is "". Surprising, but pre-existing
    // and load-bearing for anyone relying on it — which is why the fast path only covers non-nullable String.
    [Test]
    public async Task GetString_NullCell_ReturnsEmptyStringNotNull()
    {
        using var reader = await ReadOneAsync("CAST(NULL AS Nullable(String)) AS c");
        Assert.That(reader.GetString(0), Is.EqualTo(string.Empty));
    }

    // GetBoolean is the one accessor that widens; only an exact Bool column short-circuits, so the
    // Convert.ToBoolean coercion has to survive for everything else.
    [TestCase("toUInt8(1)", true)]
    [TestCase("toUInt8(0)", false)]
    [TestCase("toInt32(5)", true)]
    [TestCase("toFloat64(0)", false)]
    public async Task GetBoolean_NonBoolColumn_StillCoerces(string expression, bool expected)
    {
        using var reader = await ReadOneAsync($"{expression} AS c");
        Assert.That(reader.GetBoolean(0), Is.EqualTo(expected));
    }

    // GetString coerces too, and must keep doing so for columns that are not String at all.
    [Test]
    public async Task GetString_NonStringColumn_StillCoerces()
    {
        using var reader = await ReadOneAsync("toInt64(42) AS c");
        Assert.That(reader.GetString(0), Is.EqualTo("42"));
    }

    // A Decimal column resolves to a decimal or a ClickHouseDecimal slot depending on UseCustomDecimals;
    // GetDecimal has to reach the same decimal either way. Nullable() doubles that again — it is a different
    // slot kind, so a different branch — and a NULL cell has to keep throwing off the boxed fallback.
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetDecimal_UnderEitherDecimalRepresentation_ReturnsTheSameValue(bool useCustomDecimals)
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { UseCustomDecimals = useCustomDecimals };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDecimal64(12.34, 2) AS c, toDecimal64OrNull('56.78', 2) AS n, " +
            "CAST(NULL AS Nullable(Decimal64(2))) AS z");
        Assert.That(reader.Read(), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(reader.GetDecimal(0), Is.EqualTo(12.34m));
            Assert.That(reader.GetDecimal(1), Is.EqualTo(56.78m));
            Assert.Throws<InvalidCastException>(() => reader.GetDecimal(2));
        });
    }

    // ---- Converter routing ----

    // Two paths, two overloads, both unchanged by column slots: the typed accessors were
    // `(T)GetValue(ordinal)` and so saw ConvertValue(object, ...), while GetFieldValue<T> called
    // ConvertValue<T>. De-boxing the accessors would have switched them onto ConvertValue<T>, which is
    // observable to a converter whose two overloads disagree, so with a converter configured they stay on the
    // boxed route. This pins both halves of that decision.
    [Test]
    public async Task WithConverter_TypedAccessorUsesObjectOverloadWhileGetFieldValueUsesGeneric()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = new ObjectOnlyDoublingConverter() };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toInt64(21) AS c");
        Assert.That(reader.Read(), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(reader.GetInt64(0), Is.EqualTo(42L), "GetInt64 must still route through ConvertValue(object, ...)");
            Assert.That(reader.GetValue(0), Is.EqualTo(42L));
            Assert.That(reader.GetFieldValue<long>(0), Is.EqualTo(21L), "GetFieldValue<T> routes through ConvertValue<T>, which this converter leaves alone");
        });
    }

    // GetDecimal's fast path is skipped when a converter is configured, so this exercises its boxed
    // fallback — including the ClickHouseDecimal branch, which is what a Decimal column boxes as by default.
    [Test]
    public async Task GetDecimal_WithConverter_FallsBackToTheBoxedPath()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = new ObjectOnlyDoublingConverter() };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDecimal64(12.34, 2) AS c");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetDecimal(0), Is.EqualTo(12.34m));
    }

    // Doubles longs in the object overload only, so which overload an accessor picks is directly observable.
    private sealed class ObjectOnlyDoublingConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
            => value is long l ? l * 2 : value;

        public T ConvertValue<T>(T value, string columnName, string clickHouseType) => value;
    }

    // Reflection is the only way to parametrize over T. Unwraps TargetInvocationException so the assertions
    // above see the exception the caller would actually get.
    private static object GetFieldValueDynamic(ClickHouseDataReader reader, Type target)
    {
        var method = typeof(ClickHouseDataReader)
            .GetMethod(nameof(ClickHouseDataReader.GetFieldValue), [typeof(int)])
            .MakeGenericMethod(target);
        try
        {
            return method.Invoke(reader, [0]);
        }
        catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException != null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw; // unreachable
        }
    }
}
