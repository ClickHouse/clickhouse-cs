using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The write derivation of the composite types: it succeeds for exactly the CLR types that the codecs write today
/// (<see cref="IColumnCodec.CanWriteElementType"/>), and for the writes that it adds (<see cref="Additions"/>); a cached
/// tree keeps no buffer sized by a type parameter; one tree serves concurrent writes.
/// </summary>
[TestFixture]
public class WriteDerivationTests
{
    // Composite types, with leaves of every kind of canonical value under them.
    private static readonly string[] Types =
    {
        "Nullable(Int32)", "Nullable(String)", "Nullable(FixedString(4))", "Nullable(DateTime('UTC'))", "Nullable(Enum8('a' = 1))",
        "Nullable(Tuple(Int32, String))", "Nullable(Decimal(9, 2))",
        "Array(Int32)", "Array(String)", "Array(Nullable(String))", "Array(Array(DateTime('UTC')))", "Array(LowCardinality(String))",
        "Array(FixedString(4))", "Array(Tuple(Int32, String))", "Array(Nested(a Int32))",
        "LowCardinality(String)", "LowCardinality(Nullable(String))", "LowCardinality(FixedString(4))", "LowCardinality(Nullable(FixedString(4)))",
        "LowCardinality(Int32)", "LowCardinality(Nullable(Int32))", "LowCardinality(DateTime('UTC'))", "LowCardinality(Nullable(Enum8('a' = 1)))",
        "LowCardinality(UUID)", "LowCardinality(IPv6)", "LowCardinality(Decimal(9, 2))", "LowCardinality(Float32)", "LowCardinality(Bool)",
        "LowCardinality(Date32)", "LowCardinality(Nullable(DateTime64(3)))", "LowCardinality(Time)", "LowCardinality(BFloat16)", "LowCardinality(JSON)",
        "Map(String, Int32)", "Map(String, Nullable(String))", "Map(LowCardinality(String), Array(FixedString(4)))",
        "Tuple(Int32)", "Tuple(Int32, String)", "Tuple(a DateTime('UTC'), b FixedString(4))", "Tuple(Int32, Tuple())",
        "Variant(String, UInt64)", "Variant(Nothing, String)", "Dynamic", "Nested(a Int32, b String)", "QBit(Float32, 4)",
        "Point", "Ring", "Polygon", "Geometry", "Tuple()", "SimpleAggregateFunction(anyLast, Nullable(String))",
    };

    // CLR types of every shape that the composites take: leaves, nullable values, arrays, pairs and tuples.
    private static readonly Type[] Candidates =
    {
        typeof(int), typeof(int?), typeof(string), typeof(byte[]), typeof(uint), typeof(uint?), typeof(DateTimeOffset), typeof(DateTimeOffset?),
        typeof(DateTime), typeof(DateTime?), typeof(sbyte), typeof(sbyte?), typeof(Guid), typeof(IPAddress), typeof(decimal), typeof(decimal?),
        typeof(float), typeof(float?), typeof(bool), typeof(DateOnly), typeof(DateOnly?), typeof(long), typeof(long?), typeof(TimeSpan), typeof(object),
        typeof(int[]), typeof(int?[]), typeof(string[]), typeof(byte[][]), typeof(DateTimeOffset[]), typeof(DateTimeOffset[][]), typeof(uint[][]),
        typeof(object[]), typeof(object[][]), typeof(float[]), typeof(double[]), typeof(sbyte[]),
        typeof(KeyValuePair<string, int>[]), typeof(KeyValuePair<string, string>[]), typeof(KeyValuePair<string, int?>[]),
        typeof(KeyValuePair<string, byte[][]>[]), typeof(KeyValuePair<string, string[]>[]), typeof(KeyValuePair<byte[], byte[][]>[]),
        typeof(KeyValuePair<byte[], string[]>[]), typeof(KeyValuePair<object, object>[]),
        typeof(ValueTuple), typeof(ValueTuple<int>), typeof((int, string)), typeof((int, byte[])), typeof((int, string)?), typeof((int, string)[]),
        typeof((DateTimeOffset, byte[])), typeof((DateTimeOffset, string)), typeof((uint, byte[])), typeof((int, ValueTuple)), typeof((int, object)),
        typeof((double, double)), typeof((double, double)[]), typeof((double, double)[][]),
    };

    /// <summary>
    /// The writes that the derivation adds to the current codecs (decision D7): <c>LowCardinality(String)</c> from
    /// <see cref="T:byte[]"/> (ClickHouse/integrations#792), and <c>FixedString</c> from <see cref="string"/> in every
    /// position.
    /// </summary>
    internal static readonly (string Type, Type ClrType)[] Additions =
    {
        ("LowCardinality(String)", typeof(byte[])),
        ("LowCardinality(Nullable(String))", typeof(byte[])),
        ("Array(LowCardinality(String))", typeof(byte[][])),
        ("Map(LowCardinality(String), Array(FixedString(4)))", typeof(KeyValuePair<byte[], byte[][]>[])),
        ("Map(LowCardinality(String), Array(FixedString(4)))", typeof(KeyValuePair<byte[], string[]>[])),
        ("Map(LowCardinality(String), Array(FixedString(4)))", typeof(KeyValuePair<string, string[]>[])),
        ("Nullable(FixedString(4))", typeof(string)),
        ("Array(FixedString(4))", typeof(string[])),
        ("LowCardinality(FixedString(4))", typeof(string)),
        ("LowCardinality(Nullable(FixedString(4)))", typeof(string)),
        ("Tuple(a DateTime('UTC'), b FixedString(4))", typeof((DateTimeOffset, string))),
    };

    public static IEnumerable<string> CompositeTypes => Types;

    /// <summary>
    /// The derivation writes a composite type from exactly the CLR types that its codec writes today, and from the
    /// <see cref="Additions"/>.
    /// </summary>
    [TestCaseSource(nameof(CompositeTypes))]
    public void Derive_EveryCandidateType_AgreesWithTheCurrentCodec(string type)
    {
        IColumnCodec codec = ConverterHarness.Codec(type);
        var disagreements = new List<string>();
        foreach (Type candidate in Candidates)
        {
            bool derived = ConverterDerivation.Default.Derive(type, ConverterHarness.Context, candidate, ConversionDirection.Write).Succeeded;
            bool expected = codec.CanWriteElementType(candidate) || Additions.Contains((type, candidate));
            if (derived != expected)
            {
                disagreements.Add($"write from {candidate}: derived {derived}");
            }
        }

        Assert.That(disagreements, Is.Empty);
    }

    [Test]
    public void Additions_EveryEntry_IsATypeAndCandidateOfTheMatrix()
    {
        foreach ((string type, Type clrType) in Additions)
        {
            Assert.That(Types, Does.Contain(type));
            Assert.That(Candidates, Does.Contain(clrType));
        }
    }

    /// <summary>
    /// A cached tree keeps the width of a <c>FixedString</c>, not a buffer of that width, also under a composite, so
    /// deriving a very wide one allocates little.
    /// </summary>
    [TestCase("LowCardinality(FixedString({0}))", typeof(byte[]))]
    [TestCase("LowCardinality(Nullable(FixedString({0})))", typeof(string))]
    [TestCase("Array(Nullable(FixedString({0})))", typeof(byte[][]))]
    [TestCase("Map(FixedString({0}), FixedString({0}))", typeof(KeyValuePair<string, byte[]>[]))]
    [TestCase("Tuple(FixedString({0}), Nullable(FixedString({0})))", typeof((byte[], string)))]
    [TestCase("Variant(FixedString({0}), String)", typeof(object))]
    public void Derive_VeryWideFixedStringUnderAComposite_AllocatesLittle(string pattern, Type clrType)
    {
        var derivation = new ConverterDerivation(ColumnCodecRegistry.Default);

        // Run the code once at a small width first, so the measured call allocates no JIT or static state.
        Assert.That(derivation.Derive(string.Format(System.Globalization.CultureInfo.InvariantCulture, pattern, 8), ConverterHarness.Context, clrType, ConversionDirection.Write).Succeeded, Is.True);
        long before = GC.GetAllocatedBytesForCurrentThread();
        Derivation wide = derivation.Derive(string.Format(System.Globalization.CultureInfo.InvariantCulture, pattern, 268435456), ConverterHarness.Context, clrType, ConversionDirection.Write);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(wide.Succeeded, Is.True);
            Assert.That(allocated, Is.LessThan(64 * 1024), "bytes allocated to derive the type with FixedString(268435456)");
        });
    }

    /// <summary>
    /// One cached tree serves writes on several threads at once: each write keeps its state in its own buffers, and a
    /// Variant tree derives the alternatives for a new CLR type on first use, safely.
    /// </summary>
    [TestCase("Variant(String, UInt64, Array(String))")]
    [TestCase("Array(LowCardinality(Nullable(String)))")]
    [TestCase("Map(String, Nullable(Tuple(Int32, String)))")]
    public async Task Write_OneTreeOnEightThreads_GivesTheBytesOfOneWrite(string type)
    {
        Array values = ValuesFor(type);
        var derivation = new ConverterDerivation(ColumnCodecRegistry.Default);
        Type clrType = values.GetType().GetElementType();
        object writer = derivation.Derive(type, ConverterHarness.Context, clrType, ConversionDirection.Write).Converter;
        byte[] expected = await WriteAsync(writer, clrType, values);

        Task<byte[]>[] writes = Enumerable.Range(0, 8).Select(_ => Task.Run(() => WriteAsync(writer, clrType, values))).ToArray();
        byte[][] results = await Task.WhenAll(writes);

        Assert.That(results.Select(Convert.ToHexString), Is.All.EqualTo(Convert.ToHexString(expected)));
    }

    private static Array ValuesFor(string type) => type switch
    {
        "Variant(String, UInt64, Array(String))" => Enumerable.Range(0, 2000).Select(i => (i % 4) switch
        {
            0 => (object)$"s{i % 7}",
            1 => (ulong)i,
            2 => new byte[] { (byte)i, 0xFF },
            _ => new[] { "a", $"b{i % 3}" },
        }).ToArray(),
        "Array(LowCardinality(Nullable(String)))" => Enumerable.Range(0, 2000).Select(i => new[] { $"v{i % 50}", i % 3 == 0 ? null : "x" }).ToArray(),
        _ => Enumerable.Range(0, 2000).Select(i => new[] { new KeyValuePair<string, (int, string)?>($"k{i}", i % 2 == 0 ? (i, "v") : null) }).ToArray(),
    };

    private static Task<byte[]> WriteAsync(object writer, Type clrType, Array values)
        => (Task<byte[]>)ConverterHarness.InvokeGeneric(typeof(WriteDerivationTests), nameof(WriteTypedAsync), new[] { clrType }, writer, values);

    private static Task<byte[]> WriteTypedAsync<T>(ColumnWriter<T> writer, T[] values) => ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);
}
