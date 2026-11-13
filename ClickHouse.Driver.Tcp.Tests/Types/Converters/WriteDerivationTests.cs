using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The write derivation of the composite types: a cached tree keeps no buffer sized by a type parameter; one tree serves
/// concurrent writes. What each type is written from is pinned by <see cref="WriteRulesTests"/> and
/// <see cref="SuggestedTypesTests"/>.
/// </summary>
[TestFixture]
public class WriteDerivationTests
{
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
