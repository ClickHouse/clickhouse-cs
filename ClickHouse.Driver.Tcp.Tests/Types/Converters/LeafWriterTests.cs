using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Compares each write pair of the leaf table with the current codec write: the same bytes for a whole column, for
/// a slice that starts after row 0, and for the same values given as segments. Under marked positions the leaf
/// writes its canonical placeholder, which gives the bytes of the current <c>Nullable</c> write.
/// </summary>
[TestFixture]
public class LeafWriterTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    public static IEnumerable<TestCaseData> WritePairs()
    {
        foreach (string type in LeafSamples.Types)
        {
            IColumnCodec codec = ConverterHarness.Codec(type);
            foreach (Type clrType in LeafTableTests.LeafOf(type).WriteTypes(codec))
            {
                yield return new TestCaseData(type, clrType).SetArgDisplayNames(type, clrType.Name);
            }
        }
    }

    [TestCaseSource(nameof(WritePairs))]
    public Task Write_LeafPair_GivesTheCurrentBytes(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafWriterTests), nameof(AssertWritesLikeTheCurrentPathAsync), new[] { clrType }, type);

    [TestCaseSource(nameof(WritePairs))]
    public Task Write_MarkedPositions_GiveTheBytesOfTheCurrentNullableWrite(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(LeafWriterTests),
            clrType.IsValueType ? nameof(AssertMarksLikeNullableValuesAsync) : nameof(AssertMarksLikeNullableReferencesAsync),
            new[] { clrType },
            type);

    /// <summary>A value that the leaf cannot store fails as the current write does: type, message and parameter name.</summary>
    [TestCaseSource(nameof(RefusedValues))]
    public Task Write_ValueThatTheLeafCannotStore_FailsAsTheCurrentWriteDoes(string type, Array values, int start)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(LeafWriterTests),
            nameof(AssertFailsLikeTheCurrentPathAsync),
            new[] { values.GetType().GetElementType() },
            type,
            values,
            start);

    /// <summary>
    /// In a segmented source each segment is the array of one row. A refusal there names the position as the current
    /// <c>Array</c> write does: the element of the row for <c>FixedString</c>, the flat position for <c>String</c>.
    /// </summary>
    [TestCase("FixedString(2)")]
    [TestCase("String")]
    public async Task Write_RefusedValueInASegment_NamesThePositionAsTheCurrentArrayWriteDoes(string type)
    {
        byte[][][] rows = { new[] { "ab"u8.ToArray() }, new[] { "cd"u8.ToArray(), type == "String" ? null : "e"u8.ToArray() } };
        ColumnWriter<byte[]> writer = Derivation.Writer<byte[]>(type, ConverterHarness.Context);

        Exception expected = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteOldAsync($"Array({type})", rows, 0, rows.Length));
        Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteSegmentsAsync(writer, rows));

        ConverterHarness.AssertSameFailure(expected, actual, "segments");
    }

    [Test]
    public async Task Write_Json_WritesTheVersionPrefix()
    {
        ColumnWriter<string> writer = Derivation.Writer<string>("JSON", ConverterHarness.Context);
        byte[] bytes = await ConverterHarness.WriteNewAsync(writer, new[] { "{}" }, 0, 1);

        Assert.Multiple(() =>
        {
            Assert.That(writer.HasPrefix, Is.True);
            Assert.That(bytes, Is.EqualTo(new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 2, (byte)'{', (byte)'}' }));
        });
    }

    [Test]
    public void HasPrefix_LeafOtherThanJson_IsFalse()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Derivation.Writer<string>("String", ConverterHarness.Context).HasPrefix, Is.False);
            Assert.That(Derivation.Writer<DateTimeOffset>("DateTime", ConverterHarness.Context).HasPrefix, Is.False);
        });
    }

    /// <summary>
    /// The canonical placeholder is one value for each leaf, the same whatever CLR type the leaf writes from: the
    /// first declared member of an enum, N zero bytes of a FixedString, an empty object of JSON, zero for the others.
    /// </summary>
    [Test]
    public void Placeholder_EveryWriteTypeOfALeaf_IsTheSameCanonicalValue()
    {
        static object Canonical(ColumnWriter writer) => writer switch
        {
            FixedLeafWriter<sbyte, sbyte> w => w.Placeholder,
            FixedLeafWriter<string, sbyte> w => w.Placeholder,
            FixedLeafWriter<uint, uint> w => w.Placeholder,
            FixedLeafWriter<DateTimeOffset, uint> w => w.Placeholder,
            FixedLeafWriter<DateTime, uint> w => w.Placeholder,
            BytesLeafWriter<string> w => Convert.ToHexString(w.Placeholder),
            BytesLeafWriter<byte[]> w => Convert.ToHexString(w.Placeholder),
            _ => throw new ArgumentException($"No placeholder accessor for {writer.GetType()}."),
        };

        const string enumType = "Enum8('b' = 5, 'a' = 1)";
        Assert.Multiple(() =>
        {
            Assert.That(Canonical(Derivation.Writer<sbyte>(enumType, ConverterHarness.Context)), Is.EqualTo((sbyte)5));
            Assert.That(Canonical(Derivation.Writer<string>(enumType, ConverterHarness.Context)), Is.EqualTo((sbyte)5));
            Assert.That(Canonical(Derivation.Writer<uint>("DateTime", ConverterHarness.Context)), Is.EqualTo(0u));
            Assert.That(Canonical(Derivation.Writer<DateTimeOffset>("DateTime", ConverterHarness.Context)), Is.EqualTo(0u));
            Assert.That(Canonical(Derivation.Writer<DateTime>("DateTime", ConverterHarness.Context)), Is.EqualTo(0u));
            Assert.That(Canonical(Derivation.Writer<string>("String", ConverterHarness.Context)), Is.EqualTo(string.Empty));
            Assert.That(Canonical(Derivation.Writer<byte[]>("String", ConverterHarness.Context)), Is.EqualTo(string.Empty));
            Assert.That(Canonical(Derivation.Writer<byte[]>("FixedString(3)", ConverterHarness.Context)), Is.EqualTo("000000"));
            Assert.That(Canonical(Derivation.Writer<string>("JSON", ConverterHarness.Context)), Is.EqualTo("7B7D"));
        });
    }

    /// <summary>
    /// The canonical value of a fixed-width leaf is its wire value, so its bytes in memory are the bytes that a write
    /// gives. The dictionary of a LowCardinality write relies on this.
    /// </summary>
    [TestCase("UUID")]
    [TestCase("IPv6")]
    [TestCase("IPv4")]
    [TestCase("Date")]
    [TestCase("Decimal(38, 10)")]
    [TestCase("Float64")]
    public Task Encode_CanonicalValues_GiveTheBytesOfTheWrite(string type)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(LeafWriterTests),
            nameof(AssertCanonicalEncodesLikeTheWriteAsync),
            new[] { ConverterHarness.Codec(type).ElementType, CanonicalType(type) },
            type);

    [Test]
    public void ToCanonical_DestinationShorterThanTheValues_Throws()
    {
        var writer = (FixedLeafWriter<DateTimeOffset, uint>)Derivation.Writer<DateTimeOffset>("DateTime", ConverterHarness.Context);

        var thrown = Assert.Throws<ArgumentException>(() => writer.ToCanonical(new[] { DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch }, new uint[1], 0));
        Assert.That(thrown.ParamName, Is.EqualTo("destination"));
    }

    private static Type CanonicalType(string type) => type switch
    {
        "UUID" or "IPv6" => typeof(UInt128),
        "IPv4" => typeof(uint),
        "Date" => typeof(ushort),
        "Decimal(38, 10)" => typeof(Int128),
        "Float64" => typeof(ulong),
        _ => throw new ArgumentException(type),
    };

    private static IEnumerable<TestCaseData> RefusedValues()
    {
        TestCaseData Case(string type, Array values, int start = 0, string label = null)
            => new TestCaseData(type, values, start).SetArgDisplayNames(type, values.GetType().GetElementType().Name + label, start.ToString(System.Globalization.CultureInfo.InvariantCulture));

        yield return Case("DateTime", new[] { DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddSeconds(-1) });
        yield return Case("DateTime", new[] { DateTimeOffset.FromUnixTimeSeconds(uint.MaxValue).AddSeconds(1) });
        yield return Case("DateTime('UTC')", new[] { new DateTime(1960, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        yield return Case("DateTime", new[] { new DateTime(2024, 3, 10, 2, 30, 0, DateTimeKind.Unspecified) }, label: " [skipped wall clock]");
        yield return Case("DateTime64(3)", new[] { DateTimeOffset.UnixEpoch.AddTicks(1) });
        yield return Case("DateTime64(9)", new[] { DateTimeOffset.MaxValue });
        yield return Case("DateTime64(9)", new[] { new DateTime(1600, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        yield return Case("Date", new[] { new DateOnly(2149, 6, 7) });
        yield return Case("Date", new[] { new DateOnly(1969, 12, 31) });
        yield return Case("Date32", new[] { new DateOnly(2300, 1, 1) });
        yield return Case("Time", new[] { new TimeSpan(1000, 0, 0) });
        yield return Case("Time64(3)", new[] { -new TimeSpan(1000, 0, 0) });
        yield return Case("Decimal(9, 2)", new[] { 0m, 1m, 10_000_000m }, start: 1);
        yield return Case("Decimal(9, 2)", new[] { 0.001m });
        yield return Case("Decimal(38, 10)", new[] { new ClickHouseTcpDecimal(System.Numerics.BigInteger.Pow(10, 30), 0) });
        yield return Case("Enum8('a' = 1)", new[] { "a", "b" });
        yield return Case("Enum8('a' = 1)", new string[] { null });
        yield return Case("IPv4", new[] { IPAddress.IPv6Loopback });
        yield return Case("IPv4", new IPAddress[] { null });
        yield return Case("IPv6", new IPAddress[] { null });
        yield return Case("String", new[] { "a", null });
        yield return Case("String", new[] { "a"u8.ToArray(), null }, start: 1);
        yield return Case("FixedString(2)", new[] { "ab"u8.ToArray(), "a"u8.ToArray() }, start: 1);
        yield return Case("FixedString(2)", new[] { "abc"u8.ToArray() });
        yield return Case("FixedString(2)", new byte[][] { null });
        yield return Case("JSON", new string[] { null });
    }

    private static async Task AssertWritesLikeTheCurrentPathAsync<T>(string type)
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        Assert.That(values, Has.Length.GreaterThanOrEqualTo(3), "a sample needs a slice that starts after row 0");
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);

        byte[] whole = await ConverterHarness.WriteOldAsync(type, values, 0, values.Length);
        byte[] slice = await ConverterHarness.WriteOldAsync(type, values, 1, values.Length - 1);

        // Segments of 1, of the middle rows, empty, and of the last row, as an Array writes its rows.
        T[][] segments = { values[..1], values[1..^1], Array.Empty<T>(), values[^1..] };

        byte[] newWhole = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);
        byte[] newSlice = await ConverterHarness.WriteNewAsync(writer, values, 1, values.Length - 1);
        byte[] newSegments = await ConverterHarness.WriteSegmentsAsync(writer, segments);

        Assert.Multiple(() =>
        {
            Assert.That(newWhole, Is.EqualTo(whole), "whole column");
            Assert.That(newSlice, Is.EqualTo(slice), "slice from row 1");
            Assert.That(newSegments, Is.EqualTo(whole), "segments");
        });
    }

    // The current Nullable write: the inner prefix, the null map, then the inner values with the placeholder of the
    // write type under each NULL. The leaf gives the same bytes when the NULL positions are marked.
    private static async Task AssertMarksLikeNullableValuesAsync<T>(string type)
        where T : struct
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        byte[] marks = Marks(values.Length);
        T?[] nullable = values.Select((value, i) => marks[i] != 0 ? (T?)null : value).ToArray();
        byte[] expected = await ConverterHarness.WriteOldAsync($"Nullable({type})", nullable, 0, nullable.Length);

        // The marked positions hold default values. The leaf must not read them: default(DateTimeOffset) is outside
        // the DateTime range, for example.
        T[] present = values.Select((value, i) => marks[i] != 0 ? default : value).ToArray();
        await AssertMarkedWriteAsync(type, present, marks, expected);
    }

    private static async Task AssertMarksLikeNullableReferencesAsync<T>(string type)
        where T : class
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        byte[] marks = Marks(values.Length);
        T[] withNulls = values.Select((value, i) => marks[i] != 0 ? null : value).ToArray();
        byte[] expected = await ConverterHarness.WriteOldAsync($"Nullable({type})", withNulls, 0, withNulls.Length);

        // The marked positions hold null. The leaf must not read them: a String refuses a null value.
        await AssertMarkedWriteAsync(type, withNulls, marks, expected);
    }

    private static async Task AssertMarkedWriteAsync<T>(string type, T[] values, byte[] marks, byte[] expected)
    {
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);
        T[][] segments = { values[..2], values[2..] };

        byte[] fromSpan = await CodecTestHarness.WriteAsync(w =>
        {
            ValueSource<T> source = ValueSource<T>.Of(values).WithAbsent(marks);
            IColumnWriteState state = writer.Begin(source);
            writer.WritePrefix(w, source, state);
            w.WriteBytes(marks);
            writer.Write(w, source, state);
            state?.Dispose();
        });
        byte[] fromSegments = await CodecTestHarness.WriteAsync(w =>
        {
            ValueSource<T> source = ValueSource<T>.OfSegments(segments).WithAbsent(marks);
            writer.WritePrefix(w, source, null);
            w.WriteBytes(marks);
            writer.Write(w, source, null);
        });

        Assert.Multiple(() =>
        {
            Assert.That(fromSpan, Is.EqualTo(expected), "span");
            Assert.That(fromSegments, Is.EqualTo(expected), "segments");
        });
    }

    private static async Task AssertFailsLikeTheCurrentPathAsync<T>(string type, T[] values, int start)
    {
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);
        Exception expected = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteOldAsync(type, values, start, values.Length - start));
        Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteNewAsync(writer, values, start, values.Length - start));
        ConverterHarness.AssertSameFailure(expected, actual, "write");
    }

    private static async Task AssertCanonicalEncodesLikeTheWriteAsync<T, TCanon>(string type)
        where TCanon : unmanaged, IEquatable<TCanon>
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        var writer = (FixedLeafWriter<T, TCanon>)Derivation.Writer<T>(type, ConverterHarness.Context);
        var canonical = new TCanon[values.Length];
        writer.ToCanonical(values, canonical, 0);

        byte[] encoded = await CodecTestHarness.WriteAsync(w => FixedLeafWriter<T, TCanon>.Encode(w, canonical));
        byte[] one = await CodecTestHarness.WriteAsync(w => FixedLeafWriter<T, TCanon>.Encode(w, new[] { writer.ToCanonical(values[1], 1) }));
        byte[] written = await ConverterHarness.WriteOldAsync(type, values, 0, values.Length);
        byte[] writtenOne = await ConverterHarness.WriteOldAsync(type, values, 1, 1);

        Assert.Multiple(() =>
        {
            Assert.That(encoded, Is.EqualTo(written), "bulk");
            Assert.That(one, Is.EqualTo(writtenOne), "one value");
        });
    }

    // Every third position, from the second, has no value.
    private static byte[] Marks(int count) => Enumerable.Range(0, count).Select(i => (byte)(i % 3 == 1 ? 1 : 0)).ToArray();
}
