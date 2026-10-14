using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// The regression guard for the write path's per-value allocations. Every other test would still pass if the
/// scratch arrays came back — the bytes are identical either way, only the garbage returns — so this is the
/// one that fails.
///
/// <para>Deliberately server-free and synchronous: values are serialized into a <see cref="MemoryStream"/> on
/// the test thread, so <see cref="GC.GetAllocatedBytesForCurrentThread"/> measures the driver and nothing
/// else. The stream is rewound rather than replaced between iterations, so its own growth is not counted.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public class BoxFreeWriteAllocationTests
{
    private const int Values = 5000;

    private static readonly Guid Uuid = new("2ee6b16f-1b03-4b1e-a1a5-99f7ae6a1c2c");

    /// <summary>
    /// Runs <paramref name="write"/> <paramref name="iterations"/> times (by default <see cref="Values"/>) to settle
    /// JIT and stream growth, then measures the bytes allocated over as many further iterations.
    /// </summary>
    private static long Measure(Action<ExtendedBinaryWriter> write, int iterations = Values)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        for (var i = 0; i < iterations; i++)
            write(writer);
        writer.Flush();

        stream.Position = 0;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
            write(writer);
        writer.Flush();
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static string PerValue(long allocated) => (allocated / (double)Values).ToString("F1", CultureInfo.InvariantCulture);

    /// <summary>
    /// A UUID is 16 fixed bytes in a known order, which the writer used to build in a fresh array per value.
    /// </summary>
    [Test]
    public void WriteValue_Uuid_AllocatesNothingPerValue()
    {
        var type = (ITypedWriter<Guid>)TypeConverter.ParseClickHouseType("UUID", TypeSettings.Default);

        var allocated = Measure(writer => type.WriteValue(writer, Uuid));

        TestContext.Out.WriteLine($"uuid={PerValue(allocated)} (bytes/value)");
        Assert.That(allocated, Is.LessThan(Values), $"writing a UUID must not allocate, saw {PerValue(allocated)} B/value");
    }

    // Each case writes a 3-element array whose elements the column can serialize box-free.
    private static readonly object[] BoxFreeElementCases =
    [
        new object[] { "Array(Int32)", new[] { 1, 2, 3 } },
        new object[] { "Array(Int64)", new[] { 1L, 2L, 3L } },
        new object[] { "Array(Float64)", new[] { 1d, 2d, 3d } },
        new object[] { "Array(Bool)", new[] { true, false, true } },
        new object[] { "Array(UUID)", new[] { Uuid, Uuid, Uuid } },
        new object[] { "Array(DateTime('UTC'))", new[] { new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) } },
    ];

    /// <summary>
    /// The array is written element by element through the column's own serializer; on the <see cref="System.Collections.IList"/>
    /// path each of those elements is boxed on the way out.
    /// </summary>
    [Test]
    [TestCaseSource(nameof(BoxFreeElementCases))]
    public void Write_ValueTypeElementArray_AllocatesNothingPerValue(string clickHouseType, object array)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);

        var allocated = Measure(writer => type.Write(writer, array));

        TestContext.Out.WriteLine($"{clickHouseType}={PerValue(allocated)} (bytes/value)");
        Assert.That(allocated, Is.LessThan(Values),
            $"writing a {clickHouseType} must not allocate per element, saw {PerValue(allocated)} B/value");
    }

    /// <summary>
    /// The control for the case above: an element CLR type the column cannot write box-free keeps the boxed
    /// loop, which is both what preserves its <c>Convert</c> coercion and proof that the measurement above is
    /// measuring boxing rather than two equally-zero numbers.
    /// </summary>
    [Test]
    public void Write_ElementArrayNeedingCoercion_StillBoxesPerElement()
    {
        var type = TypeConverter.ParseClickHouseType("Array(Int32)", TypeSettings.Default);
        var widening = new[] { 1L, 2L, 3L };

        var allocated = Measure(writer => type.Write(writer, widening));

        TestContext.Out.WriteLine($"coerced={PerValue(allocated)} (bytes/value)");
        Assert.That(allocated, Is.GreaterThan(Values * widening.Length * 12L),
            $"a coerced element array is expected to still box; saw only {PerValue(allocated)} B/value");
    }

    // Each array is written fewer times than Values, as it has many elements.
    private const int ArrayWrites = 200;

    // Scale 9 with 2 integer digits, so Array(Decimal(18, 16)). The mantissas exceed 31 bits, so converting one
    // to ClickHouseDecimal allocates.
    private static readonly decimal[] Decimals = Enumerable.Range(0, 1000).Select(i => 12.345678901m + (i * 0.000000001m)).ToArray();

    private static readonly object[] DynamicDecimalArrayCases =
    [
        new object[] { Decimals, "Array(Decimal(18, 16))" },
        new object[] { Decimals.Select(d => new ClickHouseDecimal(d)).ToArray(), "Array(Decimal(18, 16))" },
    ];

    /// <summary>
    /// Dynamic infers the Decimal type of these arrays from their elements, and then writes them through the
    /// typed path of <see cref="ArrayType"/>. The inference must not box or convert the elements. The difference
    /// to writing the array as its inferred type directly is the cost of the inference and the type header, which
    /// must not grow with the number of elements.
    /// </summary>
    [Test]
    [TestCaseSource(nameof(DynamicDecimalArrayCases))]
    public void Write_DecimalArrayToDynamic_AllocatesNothingPerElementToInferType(Array array, string inferredType)
    {
        var dynamic = TypeConverter.ParseClickHouseType("Dynamic", TypeSettings.Default);
        var inferred = TypeConverter.ParseClickHouseType(inferredType, TypeSettings.Default);

        var perWrite = (Measure(writer => dynamic.Write(writer, array), ArrayWrites)
            - Measure(writer => inferred.Write(writer, array), ArrayWrites)) / ArrayWrites;

        TestContext.Out.WriteLine($"{array.GetType().Name} inference={perWrite} (bytes/array)");
        Assert.That(perWrite, Is.LessThan(array.Length),
            $"inferring the type of a {array.GetType().Name} must not allocate per element, saw {perWrite} B for {array.Length} elements");
    }

    /// <summary>
    /// <see cref="ClickHouseDecimal"/> is constructed from a <see cref="decimal"/> on every write of a Decimal
    /// column, and used to take an <c>int[4]</c> plus a <c>byte[13]</c> to get there. A mantissa small enough
    /// for <see cref="System.Numerics.BigInteger"/>'s inline representation now costs nothing at all.
    /// </summary>
    [Test]
    public void Construct_FromDecimalWithSmallMantissa_AllocatesNothing()
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        var scale = 0;
        for (var i = 0; i < Values; i++)
            scale += new ClickHouseDecimal(1.23m).Scale;
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(scale, Is.EqualTo(Values * 2));
        TestContext.Out.WriteLine($"smallMantissa={PerValue(allocated)} (bytes/value)");
        Assert.That(allocated, Is.LessThan(Values),
            $"a small mantissa must not allocate, saw {PerValue(allocated)} B/value");
    }

    /// <summary>
    /// A mantissa too large for that inline representation still allocates the one array
    /// <see cref="System.Numerics.BigInteger"/> keeps it in — but not the two scratch arrays on the way.
    /// </summary>
    [Test]
    public void Construct_FromDecimalWithLargeMantissa_AllocatesOnlyTheBigIntegerStorage()
    {
        // 96 bits of mantissa: three uints, so BigInteger holds it in a uint[3] and nothing smaller will do.
        var before = GC.GetAllocatedBytesForCurrentThread();
        var scale = 0;
        for (var i = 0; i < Values; i++)
            scale += new ClickHouseDecimal(decimal.MaxValue).Scale;
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(scale, Is.Zero);

        // The mantissa is what the rewritten bit-shuffle produces; a wrong endianness or sign treatment
        // would still allocate the same amount.
        Assert.That(new ClickHouseDecimal(decimal.MaxValue).Mantissa, Is.EqualTo(BigInteger.Parse("79228162514264337593543950335", CultureInfo.InvariantCulture)));
        Assert.That(new ClickHouseDecimal(decimal.MinValue).Mantissa, Is.EqualTo(BigInteger.Parse("-79228162514264337593543950335", CultureInfo.InvariantCulture)));
        Assert.That(new ClickHouseDecimal(-1.23m).Mantissa, Is.EqualTo(new BigInteger(-123)));
        Assert.That(new ClickHouseDecimal(0m).Mantissa, Is.EqualTo(BigInteger.Zero));

        TestContext.Out.WriteLine($"largeMantissa={PerValue(allocated)} (bytes/value)");

        // The old int[4] and byte[13] alone were 88 bytes before the BigInteger's own storage.
        Assert.That(allocated, Is.LessThan(Values * 64L),
            $"only the BigInteger's own storage should remain, saw {PerValue(allocated)} B/value");
    }
}
