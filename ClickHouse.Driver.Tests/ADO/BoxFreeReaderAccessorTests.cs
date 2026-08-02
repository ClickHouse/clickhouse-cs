using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;
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
