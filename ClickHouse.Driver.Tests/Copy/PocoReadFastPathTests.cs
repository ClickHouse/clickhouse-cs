using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// Tests for the box-free POCO read fast path (#509): <see cref="ClickHouseClient.QueryAsync{T}"/> materializes
/// rows straight from the stream, bypassing the boxing <c>Read()</c> + <c>MapTo&lt;T&gt;</c> path. Covers value
/// parity with the boxed path, the multiple-read-type bindings that only the fast path supports
/// (DateTimeOffset/DateOnly/byte[]/native Int128), and per-column mixing with boxed fallback for composites.
/// </summary>
[TestFixture]
public class PocoReadFastPathTests : AbstractConnectionTestFixture
{
    public class ScalarPoco
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    public class AllScalarsPoco
    {
        public sbyte I8 { get; set; }
        public short I16 { get; set; }
        public int I32 { get; set; }
        public long I64 { get; set; }
        public byte U8 { get; set; }
        public ushort U16 { get; set; }
        public uint U32 { get; set; }
        public ulong U64 { get; set; }
        public float F32 { get; set; }
        public double F64 { get; set; }
        public bool B { get; set; }
    }

    [Test]
    public async Task QueryAsync_ScalarShape_SelectsFastPath()
    {
        client.RegisterPocoType<ScalarPoco>();

        using var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(
            "SELECT toInt64(1) AS Id, 'a' AS Name, toFloat64(2.0) AS Value");

        Assert.That(reader.TryGetRowMaterializer<ScalarPoco>(out var materializers, out var ctor), Is.True);
        Assert.That(materializers, Is.Not.Null.And.Length.EqualTo(3));
        Assert.That(ctor, Is.Not.Null);
    }

    [Test]
    public async Task QueryAsync_ScalarShape_ReturnsSameValuesAsBoxedPath()
    {
        client.RegisterPocoType<ScalarPoco>();
        var sql = "SELECT toInt64(number) AS Id, concat('n', toString(number)) AS Name, toFloat64(number) * 0.5 AS Value FROM system.numbers LIMIT 1000";

        var expected = new List<(long, string, double)>();
        using (var reader = await client.ExecuteReaderAsync(sql))
        {
            while (reader.Read())
                expected.Add(((long)reader.GetValue(0), (string)reader.GetValue(1), (double)reader.GetValue(2)));
        }

        var actual = new List<(long, string, double)>();
        await foreach (var row in client.QueryAsync<ScalarPoco>(sql))
            actual.Add((row.Id, row.Name, row.Value));

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task QueryAsync_AllFixedScalars_ReadBoxFreeWithCorrectValues()
    {
        client.RegisterPocoType<AllScalarsPoco>();
        const string sql =
            "SELECT toInt8(-8) AS I8, toInt16(-16) AS I16, toInt32(-32) AS I32, toInt64(-64) AS I64, " +
            "toUInt8(8) AS U8, toUInt16(16) AS U16, toUInt32(32) AS U32, toUInt64(64) AS U64, " +
            "toFloat32(1.5) AS F32, toFloat64(2.5) AS F64, true AS B";

        // Every column is a value type with a typed reader, so the whole row is box-free.
        using (var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(sql))
            Assert.That(reader.TryGetRowMaterializer<AllScalarsPoco>(out _, out _), Is.True);

        AllScalarsPoco row = null;
        await foreach (var r in client.QueryAsync<AllScalarsPoco>(sql))
            row = r;

        Assert.That(row, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(row.I8, Is.EqualTo((sbyte)-8));
            Assert.That(row.I16, Is.EqualTo((short)-16));
            Assert.That(row.I32, Is.EqualTo(-32));
            Assert.That(row.I64, Is.EqualTo(-64L));
            Assert.That(row.U8, Is.EqualTo((byte)8));
            Assert.That(row.U16, Is.EqualTo((ushort)16));
            Assert.That(row.U32, Is.EqualTo(32u));
            Assert.That(row.U64, Is.EqualTo(64ul));
            Assert.That(row.F32, Is.EqualTo(1.5f));
            Assert.That(row.F64, Is.EqualTo(2.5d));
            Assert.That(row.B, Is.True);
        });
    }

    // ---- Multiple read representations: bindings the boxed MapTo<T> rejects, but the fast path supports ----

    public class DateTimeOffsetPoco
    {
        public DateTimeOffset Ts { get; set; }
    }

    public class DateOnlyPoco
    {
        public DateOnly D { get; set; }
    }

    public class ByteArrayPoco
    {
        public long Id { get; set; }
        public byte[] Data { get; set; }
    }

    public class DecimalPoco
    {
        public decimal Amount { get; set; }
    }

    [Test]
    public async Task QueryAsync_DateTimeColumn_ReadsAsDateTimeOffset()
    {
        client.RegisterPocoType<DateTimeOffsetPoco>();
        const string sql = "SELECT toDateTime64('2021-06-15 12:30:00.000', 3, 'UTC') AS Ts";

        DateTimeOffsetPoco row = null;
        await foreach (var r in client.QueryAsync<DateTimeOffsetPoco>(sql))
            row = r;

        Assert.That(row.Ts.UtcDateTime, Is.EqualTo(new DateTime(2021, 6, 15, 12, 30, 0, DateTimeKind.Utc)));
        Assert.That(row.Ts.Offset, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public async Task QueryAsync_DateColumn_ReadsAsDateOnly()
    {
        client.RegisterPocoType<DateOnlyPoco>();

        DateOnlyPoco row = null;
        await foreach (var r in client.QueryAsync<DateOnlyPoco>("SELECT toDate('2023-03-14') AS D"))
            row = r;

        Assert.That(row.D, Is.EqualTo(new DateOnly(2023, 3, 14)));
    }

    [Test]
    public async Task QueryAsync_StringColumn_ReadsAsByteArray()
    {
        client.RegisterPocoType<ByteArrayPoco>();

        ByteArrayPoco row = null;
        await foreach (var r in client.QueryAsync<ByteArrayPoco>("SELECT toInt64(1) AS Id, 'hello' AS Data"))
            row = r;

        Assert.That(row.Id, Is.EqualTo(1L));
        Assert.That(row.Data, Is.EqualTo(new byte[] { (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' }));
    }

    [Test]
    public async Task QueryAsync_DecimalColumn_ReadsAsDecimal()
    {
        client.RegisterPocoType<DecimalPoco>();

        DecimalPoco row = null;
        await foreach (var r in client.QueryAsync<DecimalPoco>("SELECT CAST('123.45', 'Decimal(10, 2)') AS Amount"))
            row = r;

        Assert.That(row.Amount, Is.EqualTo(123.45m));
    }

#if NET8_0_OR_GREATER
    public class Int128Poco
    {
        public Int128 Big { get; set; }
    }

    [Test]
    public async Task QueryAsync_Int128Column_ReadsAsNativeInt128()
    {
        client.RegisterPocoType<Int128Poco>();
        // A value beyond Int64 range to exercise the full 128-bit little-endian decode.
        const string sql = "SELECT toInt128('170141183460469231731687303715884105727') AS Big"; // Int128.MaxValue

        Int128Poco row = null;
        await foreach (var r in client.QueryAsync<Int128Poco>(sql))
            row = r;

        Assert.That(row.Big, Is.EqualTo(Int128.MaxValue));
    }
#endif

    // ---- Nullable ----

    public class NullableLongPoco
    {
        public long? Id { get; set; }
        public string Name { get; set; }
    }

    [Test]
    public async Task QueryAsync_NullableColumn_ReadsValuesAndNulls()
    {
        client.RegisterPocoType<NullableLongPoco>();
        // Rows 0..4 with every other Id null.
        const string sql =
            "SELECT if(number % 2 = 0, CAST(number, 'Nullable(Int64)'), CAST(NULL, 'Nullable(Int64)')) AS Id, " +
            "concat('n', toString(number)) AS Name FROM system.numbers LIMIT 5";

        var rows = new List<NullableLongPoco>();
        await foreach (var r in client.QueryAsync<NullableLongPoco>(sql))
            rows.Add(r);

        Assert.That(rows.Select(r => r.Id), Is.EqualTo(new long?[] { 0, null, 2, null, 4 }));
        Assert.That(rows.Select(r => r.Name), Is.EqualTo(new[] { "n0", "n1", "n2", "n3", "n4" }));
    }

    // ---- Per-column mixing: fast columns alongside a composite column that falls back to the boxed path ----

    public class MixedPoco
    {
        public DateTimeOffset Ts { get; set; }  // fast path (extended binding)
        public long[] Values { get; set; }      // composite -> boxed fallback
        public string Name { get; set; }         // fast path
    }

    [Test]
    public async Task QueryAsync_FastAndCompositeColumns_MixPerColumn()
    {
        client.RegisterPocoType<MixedPoco>();
        const string sql =
            "SELECT toDateTime64('2020-01-02 03:04:05.000', 3, 'UTC') AS Ts, " +
            "[toInt64(1), toInt64(2), toInt64(3)] AS Values, 'mixed' AS Name";

        // Fast path is still selected even though one bound column (the array) has no typed read.
        using (var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(sql))
            Assert.That(reader.TryGetRowMaterializer<MixedPoco>(out _, out _), Is.True);

        MixedPoco row = null;
        await foreach (var r in client.QueryAsync<MixedPoco>(sql))
            row = r;

        Assert.That(row.Ts.UtcDateTime, Is.EqualTo(new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc)));
        Assert.That(row.Values, Is.EqualTo(new long[] { 1, 2, 3 }));
        Assert.That(row.Name, Is.EqualTo("mixed"));
    }

    // ---- Fallback correctness: unmapped columns stay aligned; unregistered types still throw ----

    [Test]
    public async Task QueryAsync_UnmappedExtraColumn_StaysAligned()
    {
        client.RegisterPocoType<ScalarPoco>();
        const string sql =
            "SELECT toInt64(number) AS Id, concat('skip', toString(number)) AS Unmapped, " +
            "concat('n', toString(number)) AS Name, toFloat64(number) AS Value FROM system.numbers LIMIT 500";

        var rows = new List<ScalarPoco>();
        await foreach (var row in client.QueryAsync<ScalarPoco>(sql))
            rows.Add(row);

        Assert.That(rows, Has.Count.EqualTo(500));
        for (var i = 0; i < rows.Count; i++)
        {
            Assert.That(rows[i].Id, Is.EqualTo(i));
            Assert.That(rows[i].Name, Is.EqualTo($"n{i}"));
            Assert.That(rows[i].Value, Is.EqualTo((double)i));
        }
    }

    [Test]
    public async Task QueryAsync_LowCardinalityColumn_ReadsUnderlyingBoxFree()
    {
        client.RegisterPocoType<ScalarPoco>();
        const string sql =
            "SELECT toInt64(number) AS Id, CAST(concat('n', toString(number)), 'LowCardinality(String)') AS Name, " +
            "toFloat64(number) AS Value FROM system.numbers LIMIT 20";

        using (var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(sql))
            Assert.That(reader.TryGetRowMaterializer<ScalarPoco>(out _, out _), Is.True);

        var rows = new List<ScalarPoco>();
        await foreach (var row in client.QueryAsync<ScalarPoco>(sql))
            rows.Add(row);

        Assert.That(rows, Has.Count.EqualTo(20));
        Assert.That(rows[7].Name, Is.EqualTo("n7"));
    }

    public class NullablePropPoco
    {
        public long? Id { get; set; }
    }

    [Test]
    public async Task QueryAsync_NullablePropertyOnNonNullableColumn_Reads()
    {
        client.RegisterPocoType<NullablePropPoco>();

        NullablePropPoco row = null;
        await foreach (var r in client.QueryAsync<NullablePropPoco>("SELECT toInt64(5) AS Id"))
            row = r;

        Assert.That(row.Id, Is.EqualTo(5L));
    }

    public class NonNullablePropPoco
    {
        public long Id { get; set; }
    }

    [Test]
    public async Task QueryAsync_NonNullablePropertyOnNullableColumn_ReadsNonNullValues()
    {
        // long (non-nullable) property on a Nullable(Int64) column: no fast path (a null could not be
        // represented), so it falls back to the boxed reader, which reads the non-null value fine.
        client.RegisterPocoType<NonNullablePropPoco>();

        NonNullablePropPoco row = null;
        await foreach (var r in client.QueryAsync<NonNullablePropPoco>("SELECT CAST(7, 'Nullable(Int64)') AS Id"))
            row = r;

        Assert.That(row.Id, Is.EqualTo(7L));
    }

    public class InvalidCompositeBindingPoco
    {
        public string Values { get; set; } // bound to an Array column -> not assignable
    }

    [Test]
    public void QueryAsync_CompositeColumnNotAssignableToProperty_ThrowsFailFast()
    {
        client.RegisterPocoType<InvalidCompositeBindingPoco>();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in client.QueryAsync<InvalidCompositeBindingPoco>(
                "SELECT [toInt64(1), toInt64(2)] AS Values"))
            {
            }
        });

        Assert.That(ex.Message, Does.Contain("Values"));
    }

    public class UnregisteredScalarPoco
    {
        public long Id { get; set; }
    }

    [Test]
    public void QueryAsync_UnregisteredType_Throws()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in client.QueryAsync<UnregisteredScalarPoco>("SELECT toInt64(1) AS Id"))
            {
            }
        });

        Assert.That(ex.Message, Does.Contain("UnregisteredScalarPoco"));
        Assert.That(ex.Message, Does.Contain("RegisterPocoType"));
    }
}
