using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The write tests of the leaves that the differential tests (<see cref="LeafConverterRegistration"/>) do not run: the
/// pairs that no differential case reaches, compared here with the current codec write (a whole column and a slice
/// that starts after row 0); segments against one span; marked positions against the current <c>Nullable</c> write;
/// refused values (type, message and parameter name); <c>FixedString</c> from text, which no codec writes.
/// </summary>
[TestFixture]
public class LeafWriterTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    // Texts of at most 4 UTF-8 bytes. A lone surrogate encodes as the 3 bytes EF BF BD.
    private static readonly string[] FixedStringTexts = { string.Empty, "a", "abcd", "é", "\uD800", "ab", "a" };

    public static IEnumerable<TestCaseData> WritePairs() => Pairs(static (_, _, _) => true);

    public static IEnumerable<TestCaseData> WritePairsOfTheCodecs() => Pairs(static (_, codec, clrType) => codec.CanWriteElementType(clrType));

    public static IEnumerable<TestCaseData> WritePairsNotInTheCaseList()
        => Pairs(static (leaf, _, clrType) => LeafConverterRegistrationTests.NotInTheCaseList.Contains(new LeafPairKey(leaf.Name, clrType, ConversionDirection.Write)));

    [TestCaseSource(nameof(WritePairsNotInTheCaseList))]
    public Task Write_LeafPairNotInTheCaseList_GivesTheCurrentBytes(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafWriterTests), nameof(AssertWritesLikeTheCurrentPathAsync), new[] { clrType }, type);

    /// <summary>Segments (the row arrays of an <c>Array</c>) give the bytes of the same values in one span.</summary>
    [TestCaseSource(nameof(WritePairs))]
    public Task Write_Segments_GiveTheBytesOfOneSpan(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafWriterTests), nameof(AssertSegmentsWriteLikeOneSpanAsync), new[] { clrType }, type);

    [TestCaseSource(nameof(WritePairsOfTheCodecs))]
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

    /// <summary>
    /// Under marks, the values of a segmented source come from a <c>Nullable</c> child of an <c>Array</c>. The current
    /// write reaches the leaf through a flat view there, so a refusal names the flat row.
    /// </summary>
    [Test]
    public async Task Write_RefusedValueInMarkedSegments_NamesTheFlatRowAsTheCurrentNullableArrayWriteDoes()
    {
        byte[][][] rows = { new[] { "ab"u8.ToArray(), null }, new[] { "c"u8.ToArray() } };
        byte[] marks = { 0, 1, 0 };
        ColumnWriter<byte[]> writer = Derivation.Writer<byte[]>("FixedString(2)", ConverterHarness.Context);

        Exception expected = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteOldAsync("Array(Nullable(FixedString(2)))", rows, 0, rows.Length));
        Exception actual = await ConverterHarness.CatchAsync(
            () => CodecTestHarness.WriteAsync(w => writer.Write(w, ValueSource<byte[]>.OfSegments(rows).WithAbsent(marks), null)));

        ConverterHarness.AssertSameFailure(expected, actual, "marked segments");
    }

    /// <summary>
    /// The leaf converts in chunks of 4,096 bytes. Values, marks and a refusal past the first chunk give the bytes and
    /// the position of the current write.
    /// </summary>
    [Test]
    public async Task Write_MoreValuesThanOneChunk_GivesTheCurrentBytesAndPositions()
    {
        DateTimeOffset[] instants = Enumerable.Range(0, 2_500).Select(i => DateTimeOffset.FromUnixTimeSeconds(i * 1_000L)).ToArray();
        ColumnWriter<DateTimeOffset> writer = Derivation.Writer<DateTimeOffset>("DateTime", ConverterHarness.Context);
        byte[] whole = await ConverterHarness.WriteOldAsync("DateTime", instants, 7, instants.Length - 7);
        byte[] newWhole = await ConverterHarness.WriteNewAsync(writer, instants, 7, instants.Length - 7);

        byte[] marks = Marks(instants.Length);
        DateTimeOffset?[] nullable = instants.Select((value, i) => marks[i] != 0 ? (DateTimeOffset?)null : value).ToArray();
        byte[] marked = await ConverterHarness.WriteOldAsync("Nullable(DateTime)", nullable, 0, nullable.Length);
        byte[] newMarked = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteBytes(marks);
            writer.Write(w, ValueSource<DateTimeOffset>.Of(instants).WithAbsent(marks), null);
        });

        decimal[] amounts = Enumerable.Range(0, 1_500).Select(i => i == 1_300 ? 10_000_000m : i / 4m).ToArray();
        ColumnWriter<decimal> decimals = Derivation.Writer<decimal>("Decimal(9, 2)", ConverterHarness.Context);
        Exception expected = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteOldAsync("Decimal(9, 2)", amounts, 5, amounts.Length - 5));
        Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteNewAsync(decimals, amounts, 5, amounts.Length - 5));

        Assert.Multiple(() =>
        {
            Assert.That(newWhole, Is.EqualTo(whole), "slice from row 7");
            Assert.That(newMarked, Is.EqualTo(marked), "marks");
            ConverterHarness.AssertSameFailure(expected, actual, "refusal past the first chunk");
            Assert.That(actual?.Message, Does.Contain("index 1295"));
        });
    }

    /// <summary>
    /// <c>FixedString(N)</c> from text writes the UTF-8 bytes padded with zero bytes to N: the bytes of the current
    /// write of the padded bytes, for a whole column, a slice and marked positions.
    /// </summary>
    [Test]
    public async Task Write_FixedStringFromText_GivesTheBytesOfTheTextPaddedWithZeros()
    {
        byte[][] padded = FixedStringTexts.Select(text => Padded(text, 4)).ToArray();
        ColumnWriter<string> writer = Derivation.Writer<string>("FixedString(4)", ConverterHarness.Context);
        byte[] marks = Marks(FixedStringTexts.Length);

        byte[] whole = await ConverterHarness.WriteOldAsync("FixedString(4)", padded, 0, padded.Length);
        byte[] slice = await ConverterHarness.WriteOldAsync("FixedString(4)", padded, 2, padded.Length - 2);
        byte[] marked = await ConverterHarness.WriteOldAsync(
            "Nullable(FixedString(4))",
            padded.Select((value, i) => marks[i] != 0 ? null : value).ToArray(),
            0,
            padded.Length);
        byte[] newWhole = await ConverterHarness.WriteNewAsync(writer, FixedStringTexts, 0, FixedStringTexts.Length);
        byte[] newSlice = await ConverterHarness.WriteNewAsync(writer, FixedStringTexts, 2, FixedStringTexts.Length - 2);

        Assert.Multiple(() =>
        {
            Assert.That(newWhole, Is.EqualTo(whole), "whole column");
            Assert.That(newSlice, Is.EqualTo(slice), "slice from row 2");
        });
        await AssertMarkedWriteAsync("FixedString(4)", FixedStringTexts.Select((value, i) => marks[i] != 0 ? null : value).ToArray(), marks, marked);
    }

    /// <summary>A text of more than N UTF-8 bytes is refused with its byte count, as a wrong-width byte array is.</summary>
    [TestCase("abcde", 5)]
    [TestCase("ééé", 6)]
    public void Write_FixedStringFromTextLongerThanN_IsRefusedWithItsByteCount(string text, int byteCount)
    {
        ColumnWriter<string> writer = Derivation.Writer<string>("FixedString(4)", ConverterHarness.Context);
        string[] values = { "a", "b", text };

        Exception thrown = ConverterHarness.Catch(
            () => CodecTestHarness.WriteAsync(w => writer.Write(w, ValueSource<string>.Of(values.AsSpan(1), firstRow: 1), null)).GetAwaiter().GetResult());

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.TypeOf<ArgumentException>());
            Assert.That(
                thrown?.Message,
                Does.StartWith($"A FixedString(4) value at row 2 is {byteCount} bytes in UTF-8; a text value can have at most 4 bytes."));
            Assert.That((thrown as ArgumentException)?.ParamName, Is.EqualTo("value"));
        });
    }

    /// <summary>A null, and a refused text in a segment, are named as for raw bytes.</summary>
    [Test]
    public void Write_FixedStringFromTextNullOrInASegment_NamesThePositionAsForBytes()
    {
        ColumnWriter<string> writer = Derivation.Writer<string>("FixedString(2)", ConverterHarness.Context);
        string[] withNull = { "a", null };
        string[][] segments = { new[] { "a" }, new[] { "b", "abc" } };

        Exception nullValue = ConverterHarness.Catch(
            () => CodecTestHarness.WriteAsync(w => writer.Write(w, ValueSource<string>.Of(withNull), null)).GetAwaiter().GetResult());
        Exception inSegment = ConverterHarness.Catch(
            () => CodecTestHarness.WriteAsync(w => writer.Write(w, ValueSource<string>.OfSegments(segments), null)).GetAwaiter().GetResult());

        Assert.Multiple(() =>
        {
            Assert.That(nullValue?.Message, Does.StartWith("A FixedString(2) column cannot hold a null value (at row 1); wrap the type in Nullable to write nulls."));
            Assert.That(inSegment?.Message, Does.StartWith("A FixedString(2) value at element 1 is 3 bytes in UTF-8;"));
        });
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
            BytesLeafWriter<string> w => Convert.ToHexString(Placeholder(w)),
            BytesLeafWriter<byte[]> w => Convert.ToHexString(Placeholder(w)),
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
            Assert.That(Canonical(Derivation.Writer<string>("FixedString(3)", ConverterHarness.Context)), Is.EqualTo("000000"));
            Assert.That(Canonical(Derivation.Writer<string>("JSON", ConverterHarness.Context)), Is.EqualTo("7B7D"));
        });
    }

    /// <summary>
    /// A wide <c>FixedString</c> writes its placeholder in chunks: under marks, from bytes and from text, the leaf gives
    /// the bytes of the current <c>Nullable</c> write, and its <c>WritePlaceholder</c> gives N zero bytes.
    /// </summary>
    [Test]
    public async Task Write_WideFixedStringUnderMarks_GivesTheCurrentNullableBytes()
    {
        const int size = 10_000;
        const string type = "FixedString(10000)";
        string[] texts = { "a", "b", new string('x', size), "c", "é" };
        byte[][] padded = texts.Select(text => Padded(text, size)).ToArray();
        byte[] marks = Marks(texts.Length);
        byte[] expected = await ConverterHarness.WriteOldAsync(
            $"Nullable({type})",
            padded.Select((value, i) => marks[i] != 0 ? null : value).ToArray(),
            0,
            padded.Length);
        var bytesLeaf = (BytesLeafWriter<byte[]>)Derivation.Writer<byte[]>(type, ConverterHarness.Context);
        byte[] placeholder = await CodecTestHarness.WriteAsync(bytesLeaf.WritePlaceholder);

        await AssertMarkedWriteAsync(type, padded.Select((value, i) => marks[i] != 0 ? null : value).ToArray(), marks, expected);
        await AssertMarkedWriteAsync(type, texts.Select((value, i) => marks[i] != 0 ? null : value).ToArray(), marks, expected);
        Assert.That(placeholder, Is.EqualTo(new byte[size]));
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
        byte[] newWhole = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);
        byte[] newSlice = await ConverterHarness.WriteNewAsync(writer, values, 1, values.Length - 1);

        Assert.Multiple(() =>
        {
            Assert.That(newWhole, Is.EqualTo(whole), "whole column");
            Assert.That(newSlice, Is.EqualTo(slice), "slice from row 1");
        });
    }

    private static async Task AssertSegmentsWriteLikeOneSpanAsync<T>(string type)
    {
        T[] values = typeof(T) == typeof(string) && LeafTableTests.LeafOf(type).Name == "FixedString"
            ? (T[])(object)FixedStringTexts
            : (T[])LeafSamples.Writable(type, typeof(T));
        Assert.That(values, Has.Length.GreaterThanOrEqualTo(3), "a sample needs a first, a middle and a last segment");
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);

        // Segments of 1, of the middle rows, empty, and of the last row, as an Array writes its rows.
        T[][] segments = { values[..1], values[1..^1], Array.Empty<T>(), values[^1..] };
        byte[] span = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);
        byte[] fromSegments = await ConverterHarness.WriteSegmentsAsync(writer, segments);

        Assert.That(fromSegments, Is.EqualTo(span));
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

    private static IEnumerable<TestCaseData> Pairs(Func<Leaf, IColumnCodec, Type, bool> include)
    {
        foreach (string type in LeafSamples.Types)
        {
            IColumnCodec codec = ConverterHarness.Codec(type);
            Leaf leaf = LeafTableTests.LeafOf(type);
            foreach (Type clrType in leaf.WriteTypes(codec))
            {
                if (include(leaf, codec, clrType))
                {
                    yield return new TestCaseData(type, clrType).SetArgDisplayNames(type, clrType.Name);
                }
            }
        }
    }

    private static byte[] Placeholder<T>(BytesLeafWriter<T> leaf)
    {
        byte[] scratch = Array.Empty<byte>();
        return leaf.GetPlaceholder(ref scratch).ToArray();
    }

    private static byte[] Padded(string text, int size)
    {
        var bytes = new byte[size];
        Encoding.UTF8.GetBytes(text, bytes);
        return bytes;
    }

    // Every third position, from the second, has no value.
    private static byte[] Marks(int count) => Enumerable.Range(0, count).Select(i => (byte)(i % 3 == 1 ? 1 : 0)).ToArray();
}
