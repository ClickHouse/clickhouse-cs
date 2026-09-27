using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// The regression guard for the typed-column-slot reader. Every other test would still pass if boxing crept
/// back in — the values would be identical, only the garbage would return — so this is the one that fails.
///
/// <para>Deliberately server-free and synchronous: the payload is a pre-built RowBinaryWithNamesAndTypes
/// buffer served from a <see cref="ByteArrayContent"/>, so <c>Read()</c> and the accessors run entirely on
/// the test thread and <see cref="GC.GetAllocatedBytesForCurrentThread"/> is exact rather than a sample of
/// whatever else the process is doing.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public class BoxFreeReaderAllocationTests
{
    private const int Rows = 5000;
    private const int Columns = 4;

    // Four value-type columns, all with typed readers, none of whose decode allocates. That isolates the
    // measurement to boxing: anything left is the thing under test.
    private static readonly string[] ColumnTypes = ["Int64", "Float64", "Int32", "UInt64"];

    private static byte[] BuildPayload() => BuildPayload(ColumnTypes, TypeSettings.Default, WriteNumericRow);

    private static void WriteNumericRow(ExtendedBinaryWriter writer, ClickHouseType[] types, int row)
    {
        types[0].Write(writer, (long)row);
        types[1].Write(writer, (double)row);
        types[2].Write(writer, row);
        types[3].Write(writer, (ulong)row);
    }

    private static byte[] BuildPayload(string[] columnTypes, TypeSettings settings, Action<ExtendedBinaryWriter, ClickHouseType[], int> writeRow, int rows = Rows)
    {
        var types = Array.ConvertAll(columnTypes, t => TypeConverter.ParseClickHouseType(t, settings));

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        writer.Write7BitEncodedInt(columnTypes.Length);
        for (var i = 0; i < columnTypes.Length; i++)
            writer.Write($"c{i}");
        foreach (var name in columnTypes)
            writer.Write(name);

        for (var row = 0; row < rows; row++)
            writeRow(writer, types, row);

        writer.Flush();
        return stream.ToArray();
    }

    private static Task<ClickHouseDataReader> CreateReaderAsync(byte[] payload) => CreateReaderAsync(payload, TypeSettings.Default);

    private static Task<ClickHouseDataReader> CreateReaderAsync(byte[] payload, TypeSettings settings)
        => ClickHouseDataReader.FromHttpResponseAsync(
            new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(payload) },
            settings);

    // Returns bytes allocated while draining the reader. Sums the values into `checksum` so nothing the
    // accessors produce can be optimized away as dead.
    private static long Measure(ClickHouseDataReader reader, Func<ClickHouseDataReader, double> readRow, out double checksum)
    {
        var sum = 0d;
        var before = GC.GetAllocatedBytesForCurrentThread();
        var rows = 0;
        while (reader.Read())
        {
            sum += readRow(reader);
            rows++;
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(rows, Is.EqualTo(Rows), "the payload did not decode to the expected row count");
        checksum = sum;
        return allocated;
    }

    private static double Scan(ClickHouseDataReader reader) => 0d;

    private static double ReadTyped(ClickHouseDataReader reader)
        => reader.GetInt64(0) + reader.GetDouble(1) + reader.GetInt32(2) + reader.GetUInt64(3);

    private static double ReadFieldValue(ClickHouseDataReader reader)
        => reader.GetFieldValue<long>(0) + reader.GetFieldValue<double>(1)
            + reader.GetFieldValue<int>(2) + reader.GetFieldValue<ulong>(3);

    private static double ReadBoxed(ClickHouseDataReader reader)
        => (long)reader.GetValue(0) + (double)reader.GetValue(1)
            + (int)reader.GetValue(2) + (ulong)reader.GetValue(3);

    [Test]
    public async Task Read_ValueTypeColumns_AllocatesNothingPerRow()
    {
        var payload = BuildPayload();

        // First pass per shape pays for JIT and generic instantiation. Neither is per-row, and neither should
        // be attributed to the measurement.
        foreach (var warmup in new[] { Scan, ReadTyped, ReadFieldValue, ReadBoxed })
        {
            using var reader = await CreateReaderAsync(payload);
            Measure(reader, warmup, out _);
        }

        long scan, typed, fieldValue, boxed;
        using (var reader = await CreateReaderAsync(payload))
            scan = Measure(reader, Scan, out _);
        using (var reader = await CreateReaderAsync(payload))
            typed = Measure(reader, ReadTyped, out _);
        using (var reader = await CreateReaderAsync(payload))
            fieldValue = Measure(reader, ReadFieldValue, out _);
        using (var reader = await CreateReaderAsync(payload))
            boxed = Measure(reader, ReadBoxed, out _);

        TestContext.Out.WriteLine(
            $"scan={PerRow(scan)} typed={PerRow(typed)} fieldValue={PerRow(fieldValue)} boxed={PerRow(boxed)} (bytes/row)");

        // One box per value-type cell, and a box is 24 bytes on 64-bit. Requiring even half of that is a wide
        // margin against allocation the reader does for other reasons, while still failing outright if any of
        // the three de-boxed paths starts boxing again.
        const long BoxedFloor = Rows * Columns * 12;

        Assert.Multiple(() =>
        {
            // The headline: Read() used to box every cell of every row whether or not anyone looked at it.
            Assert.That(scan, Is.LessThan(Rows), $"Read() must not allocate per row, saw {PerRow(scan)} B/row");
            Assert.That(typed, Is.LessThan(Rows), $"typed accessors must not box, saw {PerRow(typed)} B/row");
            Assert.That(fieldValue, Is.LessThan(Rows), $"GetFieldValue<T> must not box, saw {PerRow(fieldValue)} B/row");

            // And the control: the untyped path still boxes, so the comparison above is measuring something.
            Assert.That(boxed, Is.GreaterThan(BoxedFloor),
                $"GetValue is expected to still box; saw only {PerRow(boxed)} B/row, so this test is not measuring boxing");
        });
    }

    // Bool and Decimal(10,2), read through the two accessors that coerce rather than cast.
    private static readonly string[] CoercedColumnTypes = ["Bool", "Decimal(10,2)"];
    private static readonly string[] NullableCoercedColumnTypes = ["Nullable(Bool)", "Nullable(Decimal(10,2))"];

    // Every cell is populated: a NULL costs nothing on either path (DBNull.Value is a singleton), so it is
    // the present value that can be left boxing.
    private static void WriteCoercedRow(ExtendedBinaryWriter writer, ClickHouseType[] types, int row)
    {
        types[0].Write(writer, row % 2 == 0);
        types[1].Write(writer, row / 100m);
    }

    private static double ReadCoerced(ClickHouseDataReader reader)
        => (reader.GetBoolean(0) ? 1d : 0d) + (double)reader.GetDecimal(1);

    // The control has to do the same *work* as ReadCoerced, not merely touch the same cells, or it measures
    // the decimal conversion rather than the box. This is the pre-slot body of both accessors verbatim, so
    // the difference between the two is exactly the two boxes.
    private static double ReadCoercedBoxed(ClickHouseDataReader reader)
    {
        var flag = Convert.ToBoolean(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture);
        var raw = reader.GetValue(1);
        var value = raw is ClickHouseDecimal chd
            ? chd.ToDecimal(System.Globalization.CultureInfo.InvariantCulture)
            : (decimal)raw;
        return (flag ? 1d : 0d) + (double)value;
    }

    /// <summary>
    /// <see cref="ClickHouseDataReader.GetBoolean"/> and <see cref="ClickHouseDataReader.GetDecimal"/> coerce
    /// rather than cast, so neither can use the shared <c>GetSlotValue&lt;T&gt;</c> body and both match their
    /// slot kinds by hand — which is how they came to recognise only the non-nullable <c>ValueSlot&lt;T&gt;</c>
    /// and box every populated <c>Nullable(Bool)</c>/<c>Nullable(Decimal)</c> cell, on column types the
    /// feature claims to cover.
    /// </summary>
    /// <remarks>
    /// Measured against the identical non-nullable shape rather than against zero, because zero is not the
    /// right answer here: under <c>useBigDecimal</c> the <c>ClickHouseDecimal</c>-to-<c>decimal</c> conversion
    /// allocates two byte arrays per call (~72 B/row) whatever the reader does, and that cost is common to
    /// both sides. The difference isolates exactly the property under test — a <c>Nullable</c> column must
    /// cost no more than its non-nullable twin. Both decimal representations are covered because
    /// <c>useBigDecimal</c> picks between two different slots reached by two different branches.
    /// </remarks>
    [TestCase(true)]
    [TestCase(false)]
    public async Task GetBooleanAndGetDecimal_PopulatedNullableCells_AllocateNoMoreThanNonNullable(bool useBigDecimal)
    {
        var settings = TypeSettings.Default with { useBigDecimal = useBigDecimal };
        var plain = BuildPayload(CoercedColumnTypes, settings, WriteCoercedRow);
        var nullable = BuildPayload(NullableCoercedColumnTypes, settings, WriteCoercedRow);

        foreach (var payload in new[] { plain, nullable, nullable })
        {
            using var warmup = await CreateReaderAsync(payload, settings);
            Measure(warmup, ReadCoerced, out _);
            using var warmupBoxed = await CreateReaderAsync(payload, settings);
            Measure(warmupBoxed, ReadCoercedBoxed, out _);
        }

        long plainAllocated, nullableAllocated, nullableBoxed;
        using (var reader = await CreateReaderAsync(plain, settings))
            plainAllocated = Measure(reader, ReadCoerced, out _);
        using (var reader = await CreateReaderAsync(nullable, settings))
            nullableAllocated = Measure(reader, ReadCoerced, out _);
        using (var reader = await CreateReaderAsync(nullable, settings))
            nullableBoxed = Measure(reader, ReadCoercedBoxed, out _);

        TestContext.Out.WriteLine(
            $"useBigDecimal={useBigDecimal} plain={PerRow(plainAllocated)} nullable={PerRow(nullableAllocated)} " +
            $"nullableBoxed={PerRow(nullableBoxed)} (bytes/row)");

        Assert.Multiple(() =>
        {
            // One byte per row of slack, which is far below the 24-byte box either accessor would take.
            Assert.That(nullableAllocated, Is.LessThanOrEqualTo(plainAllocated + Rows),
                $"a populated Nullable cell must cost no more than a non-nullable one; saw " +
                $"{PerRow(nullableAllocated)} B/row against {PerRow(plainAllocated)} B/row");

            // The control: the same two cells through GetValue do box, so the comparison above is measuring
            // something rather than two equally-zero numbers.
            Assert.That(nullableBoxed, Is.GreaterThan(nullableAllocated + (Rows * 12)),
                $"GetValue is expected to box both cells; saw only {PerRow(nullableBoxed)} B/row, so this " +
                $"test is not measuring boxing");
        });
    }

    /// <summary>
    /// The column slots are the reader's only per-column allocation, and <c>QueryAsync&lt;T&gt;</c>'s box-free
    /// POCO path never reads one — it materializes straight from the stream. Building them in the constructor
    /// would put one permanently dead object per column on the driver's primary read API, and on every empty
    /// or metadata-only reader besides.
    /// </summary>
    /// <remarks>
    /// Reached by reflection because the effect under test is an absence. No public surface reports whether
    /// the storage exists, and measuring it in bytes would mean resolving a few hundred bytes against the row
    /// materialization it is supposed to be dwarfed by.
    /// </remarks>
    [Test]
    public async Task Read_BeforeFirstRow_HasNotBuiltColumnSlots()
    {
        var slotsField = SlotsField();

        using var reader = await CreateReaderAsync(BuildPayload());
        Assert.That(slotsField.GetValue(reader), Is.Null, "column slots must not be built before the first Read()");

        Assert.That(reader.Read(), Is.True);
        Assert.That(slotsField.GetValue(reader), Is.Not.Null, "the first Read() must build the column slots");
    }

    [Test]
    public async Task Read_EmptyResult_BuildsNoColumnSlotsAndKeepsMetadata()
    {
        var slotsField = SlotsField();
        var payload = BuildPayload(ColumnTypes, TypeSettings.Default, WriteNumericRow, rows: 0);

        using var reader = await CreateReaderAsync(payload);
        Assert.That(reader.Read(), Is.False);

        Assert.Multiple(() =>
        {
            Assert.That(slotsField.GetValue(reader), Is.Null, "an empty result must not build column slots");

            // Metadata stays available with no row, which is what makes skipping the storage safe.
            Assert.That(reader.FieldCount, Is.EqualTo(Columns));
            Assert.That(reader.GetName(0), Is.EqualTo("c0"));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(long)));
        });
    }

    private static readonly Guid Uuid = new("2ee6b16f-1b03-4b1e-a1a5-99f7ae6a1c2c");

    private static void WriteUuidRow(ExtendedBinaryWriter writer, ClickHouseType[] types, int row) => types[0].Write(writer, Uuid);

    /// <summary>
    /// A UUID column decodes into a <see cref="ValueSlot{T}"/> like any other fixed-width column, so no cell of
    /// it is boxed — but the decode itself took a 16-byte scratch array per value, which no boxing test can see.
    /// </summary>
    [Test]
    public async Task Read_UuidColumn_AllocatesNothingPerRow()
    {
        string[] columnTypes = ["UUID"];
        var payload = BuildPayload(columnTypes, TypeSettings.Default, WriteUuidRow);

        using (var warmup = await CreateReaderAsync(payload))
            Measure(warmup, ReadUuid, out _);

        long typed;
        using (var reader = await CreateReaderAsync(payload))
            typed = Measure(reader, ReadUuid, out _);

        TestContext.Out.WriteLine($"uuid={PerRow(typed)} (bytes/row)");

        Assert.That(typed, Is.LessThan(Rows), $"reading a UUID must not allocate per row, saw {PerRow(typed)} B/row");
    }

    private static double ReadUuid(ClickHouseDataReader reader) => reader.GetGuid(0).GetHashCode();

    private static void WriteInt32ArrayRow(ExtendedBinaryWriter writer, ClickHouseType[] types, int row)
        => types[0].Write(writer, new[] { row, row + 1, row + 2 });

    private static void WriteNullableInt32ArrayRow(ExtendedBinaryWriter writer, ClickHouseType[] types, int row)
        => types[0].Write(writer, new int?[] { row, row + 1, row + 2 });

    private static double ReadArraySum(ClickHouseDataReader reader)
    {
        var sum = 0d;
        foreach (var value in (int[])reader.GetValue(0))
            sum += value;
        return sum;
    }

    private static double ReadNullableArraySum(ClickHouseDataReader reader)
    {
        var sum = 0d;
        foreach (var value in (int?[])reader.GetValue(0))
            sum += value ?? 0;
        return sum;
    }

    /// <summary>
    /// An array column allocates the <c>T[]</c> it hands back, and nothing else: the elements are decoded
    /// straight into it rather than boxed one at a time on the way in.
    /// </summary>
    /// <remarks>
    /// Measured against the array itself rather than against zero, since that allocation is the result. The
    /// control is the same column made <c>Nullable</c>, whose elements have no typed reader and so keep going
    /// through the boxed decode — without it, both numbers could be "the array plus three boxes" and the test
    /// would still pass.
    /// </remarks>
    [Test]
    public async Task Read_Int32ArrayColumn_AllocatesOnlyTheArrayPerRow()
    {
        const int Elements = 3;
        var payload = BuildPayload(["Array(Int32)"], TypeSettings.Default, WriteInt32ArrayRow);
        var nullablePayload = BuildPayload(["Array(Nullable(Int32))"], TypeSettings.Default, WriteNullableInt32ArrayRow);

        using (var warmup = await CreateReaderAsync(payload))
            Measure(warmup, ReadArraySum, out _);
        using (var warmup = await CreateReaderAsync(nullablePayload))
            Measure(warmup, ReadNullableArraySum, out _);

        long typed, boxedElements, arrayOnly;
        using (var reader = await CreateReaderAsync(payload))
            typed = Measure(reader, ReadArraySum, out _);
        using (var reader = await CreateReaderAsync(nullablePayload))
            boxedElements = Measure(reader, ReadNullableArraySum, out _);

        // What an int[3] costs on this runtime, measured the same way rather than assumed.
        var arrays = new int[Rows][];
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Rows; i++)
            arrays[i] = new int[Elements];
        arrayOnly = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(arrays);

        TestContext.Out.WriteLine(
            $"typed={PerRow(typed)} arrayOnly={PerRow(arrayOnly)} nullable={PerRow(boxedElements)} (bytes/row)");

        Assert.Multiple(() =>
        {
            Assert.That(typed, Is.LessThanOrEqualTo(arrayOnly + Rows),
                $"an Int32 array must cost its own storage and no more; saw {PerRow(typed)} B/row against " +
                $"{PerRow(arrayOnly)} B/row for the bare array");

            // The control: Nullable elements still box, so the comparison above is measuring boxing.
            Assert.That(boxedElements, Is.GreaterThan(typed + (Rows * Elements * 12)),
                $"Nullable elements are expected to still box; saw only {PerRow(boxedElements)} B/row");
        });
    }

    private static FieldInfo SlotsField()
    {
        var field = typeof(ClickHouseDataReader).GetField("slots", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "the reader's column-slot storage was renamed; update these tests");
        return field;
    }

    private static string PerRow(long allocated) => (allocated / (double)Rows).ToString("F1", System.Globalization.CultureInfo.InvariantCulture);
}
