using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Formats;
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

    private static byte[] BuildPayload()
    {
        var types = Array.ConvertAll(ColumnTypes, t => TypeConverter.ParseClickHouseType(t, TypeSettings.Default));

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        writer.Write7BitEncodedInt(ColumnTypes.Length);
        for (var i = 0; i < ColumnTypes.Length; i++)
            writer.Write($"c{i}");
        foreach (var name in ColumnTypes)
            writer.Write(name);

        for (var row = 0; row < Rows; row++)
        {
            types[0].Write(writer, (long)row);
            types[1].Write(writer, (double)row);
            types[2].Write(writer, row);
            types[3].Write(writer, (ulong)row);
        }

        writer.Flush();
        return stream.ToArray();
    }

    private static Task<ClickHouseDataReader> CreateReaderAsync(byte[] payload)
        => ClickHouseDataReader.FromHttpResponseAsync(
            new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(payload) },
            TypeSettings.Default);

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

        // First pass per shape pays for JIT, generic instantiation and the slot factory's one-off reflection.
        // None of that is per-row, and none of it should be attributed to the measurement.
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

    private static string PerRow(long allocated) => (allocated / (double)Rows).ToString("F1", System.Globalization.CultureInfo.InvariantCulture);
}
