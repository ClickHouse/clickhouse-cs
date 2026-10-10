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
/// The write tests of the leaves that the differential tests (<see cref="LeafConverterRegistrationTests"/>) do not
/// reach: the pairs that no differential case reaches, as pinned bytes (a whole column and a slice that starts after
/// row 0); segments against one span; marked positions as pinned bytes for each kind of leaf writer; refused values
/// (type, message and parameter name); <c>FixedString</c> from text.
/// </summary>
[TestFixture]
public class LeafWriterTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    // The error of each case of RefusedValues, in order: the exception type, the parameter name and the message. The
    // messages format values with the invariant culture.
    private static readonly (string Exception, string Parameter, string Message)[] RefusedErrors =
    {
        ("ArgumentOutOfRangeException", "utc", "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC). (Parameter 'utc')\nActual value was 12/31/1969 23:59:59."), // 0 DateTime
        ("ArgumentOutOfRangeException", "utc", "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC). (Parameter 'utc')\nActual value was 02/07/2106 06:28:16."), // 1 DateTime
        ("ArgumentOutOfRangeException", "utc", "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC). (Parameter 'utc')\nActual value was 01/01/1960 00:00:00."), // 2 DateTime('UTC')
        ("ArgumentException", "value", "2024-03-10 02:30:00 does not exist in 'America/New_York': a daylight-saving change skips it. Pass a DateTimeOffset, or a DateTime with Kind=Utc, to name the instant you mean. (Parameter 'value')"), // 3 DateTime
        ("ArgumentException", "value", "1970-01-01T00:00:00.0000001+00:00 cannot be written to DateTime64(3) (scale 3) without losing precision. (Parameter 'value')"), // 4 DateTime64(3)
        ("ArgumentOutOfRangeException", "value", "9999-12-31T23:59:59.9999999+00:00 cannot be written to DateTime64(9) (scale 9): the count of sub-second units since 1970-01-01 does not fit in an Int64. (Parameter 'value')\nActual value was 12/31/9999 23:59:59 +00:00."), // 5 DateTime64(9)
        ("ArgumentOutOfRangeException", "value", "1600-01-01T00:00:00.0000000+00:00 cannot be written to DateTime64(9) (scale 9): the count of sub-second units since 1970-01-01 does not fit in an Int64. (Parameter 'value')\nActual value was 01/01/1600 00:00:00 +00:00."), // 6 DateTime64(9)
        ("ArgumentOutOfRangeException", "column", "Date is outside the range ClickHouse Date can hold (1970-01-01 to 2149-06-06). (Parameter 'column')\nActual value was 06/07/2149."), // 7 Date
        ("ArgumentOutOfRangeException", "column", "Date is outside the range ClickHouse Date can hold (1970-01-01 to 2149-06-06). (Parameter 'column')\nActual value was 12/31/1969."), // 8 Date
        ("ArgumentOutOfRangeException", "column", "Date32 is outside the range ClickHouse Date32 can hold (1900-01-01 to 2299-12-31). (Parameter 'column')\nActual value was 01/01/2300."), // 9 Date32
        ("ArgumentOutOfRangeException", "value", "Time is outside the range ClickHouse Time can hold ([-999:59:59, 999:59:59]). (Parameter 'value')\nActual value was 41.16:00:00."), // 10 Time
        ("ArgumentOutOfRangeException", "value", "Time64 is outside the range ClickHouse Time64 can hold ([-999:59:59, 999:59:59]). (Parameter 'value')\nActual value was -41.16:00:00."), // 11 Time64(3)
        ("OverflowException", null, "Value at index 1 exceeds the declared precision 9 of decimal type 'Decimal(9, 2)'."), // 12 Decimal(9, 2)
        ("ArgumentException", null, "Value 0.001 cannot be represented exactly at scale 2."), // 13 Decimal(9, 2)
        ("OverflowException", null, "Value was either too large or too small for an Int128."), // 14 Decimal(38, 10)
        ("ArgumentException", "label", "'b' is not a label of 'Enum8('a' = 1)'. Its labels are: 'a'. (Parameter 'label')"), // 15 Enum8('a' = 1)
        ("ArgumentException", "label", "A null is not a label of 'Enum8('a' = 1)'. Declare the target Nullable to carry nulls. (Parameter 'label')"), // 16 Enum8('a' = 1)
        ("ArgumentException", "value", "An IPv4 column requires IPv4 addresses; got '::1'. (Parameter 'value')"), // 17 IPv4
        ("ArgumentException", "value", "An IPv4 column requires IPv4 addresses; got ''. (Parameter 'value')"), // 18 IPv4
        ("ArgumentException", "value", "An IPv6 column requires IPv6 addresses; got ''. (Parameter 'value')"), // 19 IPv6
        ("ArgumentNullException", "value", "Value cannot be null. (Parameter 'value')"), // 20 String
        ("ArgumentException", "column", "A String column cannot hold a null value (at row 1); wrap the type in Nullable to write nulls. (Parameter 'column')"), // 21 String
        ("ArgumentException", "value", "A FixedString(2) value at row 1 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 22 FixedString(2)
        ("ArgumentException", "value", "A FixedString(2) value at row 0 is 3 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 23 FixedString(2)
        ("ArgumentException", "value", "A FixedString(2) column cannot hold a null value (at row 0); wrap the type in Nullable to write nulls. (Parameter 'value')"), // 24 FixedString(2)
        ("ArgumentNullException", "value", "Value cannot be null. (Parameter 'value')"), // 25 JSON
        ("ArgumentOutOfRangeException", "utc", "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC). (Parameter 'utc')\nActual value was 01/01/2200 00:00:00."), // 26 DateTime
        ("ArgumentOutOfRangeException", "value", "Time64 is outside the range ClickHouse Time64 can hold ([-999:59:59, 999:59:59]). (Parameter 'value')\nActual value was 41.16:00:00."), // 27 Time64(3)
    };

    // Texts of at most 4 UTF-8 bytes. A lone surrogate encodes as the 3 bytes EF BF BD.
    private static readonly string[] FixedStringTexts = { string.Empty, "a", "abcd", "é", "\uD800", "ab", "a" };

    public static IEnumerable<TestCaseData> WritePairs() => Pairs(static (_, _) => true);

    public static IEnumerable<TestCaseData> WritePairsNotInTheCaseList()
        => Pairs(static (leaf, clrType) => LeafConverterRegistrationTests.NotInTheCaseList.Contains(new LeafPairKey(leaf.Name, clrType, ConversionDirection.Write)));

    /// <summary>
    /// The samples of a pair that no differential case reaches give the pinned bytes: the whole column, and a slice from
    /// row 1. <see cref="Write_PinnedLeafPairs_AreThePairsNotInTheCaseList"/> keeps the list complete.
    /// </summary>
    [TestCase("Enum('p' = 1, 'q' = 2)", typeof(sbyte), "010202", "0202")]
    [TestCase("Enum('p' = 1, 'q' = 2)", typeof(string), "01020201", "020201")]
    [TestCase("Enum('big' = 1000, 'small' = -1)", typeof(short), "E803FFFFE803", "FFFFE803")]
    [TestCase("Enum('big' = 1000, 'small' = -1)", typeof(string), "E803FFFFFFFFE803", "FFFFFFFFE803")]
    [TestCase(
        "Decimal256(20)",
        typeof(ClickHouseTcpDecimal),
        "0000000000000000000000000000000000000000000000000000000000000000874B9F2CA8F258F1FD1E66000000000000000000000000000000000000000000FFFFEF9CD2A13894FAFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "874B9F2CA8F258F1FD1E66000000000000000000000000000000000000000000FFFFEF9CD2A13894FAFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")]
    [TestCase("IntervalNanosecond", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalMicrosecond", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalMillisecond", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalMinute", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalHour", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalWeek", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalMonth", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalQuarter", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    [TestCase("IntervalYear", typeof(long), IntervalSamples, IntervalSamplesFromRow1)]
    public Task Write_LeafPairNotInTheCaseList_GivesThePinnedBytes(string type, Type clrType, string whole, string fromRow1)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafWriterTests), nameof(AssertWritesThePinnedBytesAsync), new[] { clrType }, type, whole, fromRow1);

    [Test]
    public void Write_PinnedLeafPairs_AreThePairsNotInTheCaseList()
    {
        string[] pinned = typeof(LeafWriterTests).GetMethod(nameof(Write_LeafPairNotInTheCaseList_GivesThePinnedBytes))
            .GetCustomAttributes(typeof(TestCaseAttribute), inherit: false)
            .Cast<TestCaseAttribute>()
            .Select(testCase => $"{testCase.Arguments[0]} {((Type)testCase.Arguments[1]).Name}")
            .ToArray();
        string[] listed = WritePairsNotInTheCaseList().Select(testCase => $"{testCase.Arguments[0]} {((Type)testCase.Arguments[1]).Name}").ToArray();

        Assert.That(pinned, Is.EquivalentTo(listed));
    }

    /// <summary>Segments (the row arrays of an <c>Array</c>) give the bytes of the same values in one span.</summary>
    [TestCaseSource(nameof(WritePairs))]
    public Task Write_Segments_GiveTheBytesOfOneSpan(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafWriterTests), nameof(AssertSegmentsWriteLikeOneSpanAsync), new[] { clrType }, type);

    /// <summary>
    /// Each kind of leaf writer under marks (every third position from the second, as the values of a <c>Nullable</c>
    /// column): the prefix, the null map, then the values with the placeholder of the leaf at each marked position. The
    /// marked positions hold default values, which the leaf must not read: default(DateTimeOffset) is outside the
    /// DateTime range, and a String refuses a null value. The differential cases of <c>Nullable(X)</c> cover the other
    /// leaves.
    /// </summary>
    [TestCase("DateTime", typeof(DateTimeOffset), "0001000000000000000000FFFFFFFF")]
    [TestCase("String", typeof(byte[]), "000100000100000968C3A96C6C6FE29C9303FF006100")]
    [TestCase("String", typeof(string), "0001000000000968C3A96C6C6FE29C9303610062")]
    [TestCase("FixedString(4)", typeof(byte[]), "000100000000000000000000FF00610061C3A92E")]
    [TestCase("JSON", typeof(string), "0100000000000000000100027B7D027B7D097B2262223A2278227D")]
    public Task Write_MarkedPositions_GiveThePinnedBytes(string type, Type clrType, string expected)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafWriterTests), nameof(AssertMarksGiveThePinnedBytesAsync), new[] { clrType }, type, expected);

    /// <summary>
    /// A value that the leaf cannot store fails with the pinned error of its case (<see cref="RefusedErrors"/>): exception
    /// type, parameter name and message.
    /// </summary>
    [TestCaseSource(nameof(RefusedValuesWithErrors))]
    [SetCulture("")]
    public Task Write_ValueThatTheLeafCannotStore_FailsWithThePinnedError(string type, Array values, int start, string exception, string parameter, string message)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(LeafWriterTests),
            nameof(AssertFailsWithAsync),
            new[] { values.GetType().GetElementType() },
            type,
            values,
            start,
            exception,
            parameter,
            message);

    /// <summary>
    /// In a segmented source each segment is the array of one row, as an <c>Array</c> writes its rows. A refusal names
    /// the element of the row for <c>FixedString</c>, and the flat row for <c>String</c>.
    /// </summary>
    [TestCase(
        "FixedString(2)",
        "ArgumentException",
        "value",
        "A FixedString(2) value at element 1 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')")]
    [TestCase("String", "ArgumentException", "column", "A String column cannot hold a null value (at row 2); wrap the type in Nullable to write nulls. (Parameter 'column')")]
    public async Task Write_RefusedValueInASegment_NamesThePositionOfTheValue(string type, string exception, string parameter, string message)
    {
        byte[][][] rows = { new[] { "ab"u8.ToArray() }, new[] { "cd"u8.ToArray(), type == "String" ? null : "e"u8.ToArray() } };
        ColumnWriter<byte[]> writer = Derivation.Writer<byte[]>(type, ConverterHarness.Context);

        Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteSegmentsAsync(writer, rows));

        ConverterHarness.AssertFailure(actual, exception, parameter, message, "segments");
    }

    /// <summary>
    /// Under marks, the values of a segmented source come from a <c>Nullable</c> child of an <c>Array</c>, so a refusal
    /// names the flat row.
    /// </summary>
    [Test]
    public async Task Write_RefusedValueInMarkedSegments_NamesTheFlatRow()
    {
        byte[][][] rows = { new[] { "ab"u8.ToArray(), null }, new[] { "c"u8.ToArray() } };
        byte[] marks = { 0, 1, 0 };
        ColumnWriter<byte[]> writer = Derivation.Writer<byte[]>("FixedString(2)", ConverterHarness.Context);

        Exception actual = await ConverterHarness.CatchAsync(
            () => CodecTestHarness.WriteAsync(w => writer.Write(w, ValueSource<byte[]>.OfSegments(rows).WithAbsent(marks), null)));

        ConverterHarness.AssertFailure(
            actual,
            "ArgumentException",
            "value",
            "A FixedString(2) value at row 2 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')",
            "marked segments");
    }

    /// <summary>
    /// The leaf converts in chunks of 4,096 bytes. Values, marks and a refusal past the first chunk give the bytes and
    /// the position of each value: a <c>DateTime</c> is its Unix seconds as a little-endian <c>UInt32</c>, and a marked
    /// position is the placeholder 0.
    /// </summary>
    [Test]
    public async Task Write_MoreValuesThanOneChunk_GivesTheBytesAndThePositionOfEachValue()
    {
        DateTimeOffset[] instants = Enumerable.Range(0, 2_500).Select(i => DateTimeOffset.FromUnixTimeSeconds(i * 1_000L)).ToArray();
        ColumnWriter<DateTimeOffset> writer = Derivation.Writer<DateTimeOffset>("DateTime", ConverterHarness.Context);
        byte[] fromRow7 = await ConverterHarness.WriteNewAsync(writer, instants, 7, instants.Length - 7);

        byte[] marks = Marks(instants.Length);
        byte[] marked = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteBytes(marks);
            writer.Write(w, ValueSource<DateTimeOffset>.Of(instants).WithAbsent(marks), null);
        });

        decimal[] amounts = Enumerable.Range(0, 1_500).Select(i => i == 1_300 ? 10_000_000m : i / 4m).ToArray();
        ColumnWriter<decimal> decimals = Derivation.Writer<decimal>("Decimal(9, 2)", ConverterHarness.Context);
        Exception refusal = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteNewAsync(decimals, amounts, 5, amounts.Length - 5));

        Assert.Multiple(() =>
        {
            Assert.That(fromRow7, Is.EqualTo(Seconds(Enumerable.Range(7, instants.Length - 7).Select(i => (uint)(i * 1_000)))), "slice from row 7");
            Assert.That(marked, Is.EqualTo(marks.Concat(Seconds(Enumerable.Range(0, instants.Length).Select(i => marks[i] != 0 ? 0u : (uint)(i * 1_000)))).ToArray()), "marks");
            ConverterHarness.AssertFailure(refusal, "OverflowException", null, "Value at index 1295 exceeds the declared precision 9 of decimal type 'Decimal(9, 2)'.", "refusal past the first chunk");
        });
    }

    /// <summary>
    /// <c>FixedString(N)</c> from text writes the UTF-8 bytes padded with zero bytes to N, for a whole column, a slice and
    /// marked positions (N zero bytes).
    /// </summary>
    [Test]
    public async Task Write_FixedStringFromText_GivesTheBytesOfTheTextPaddedWithZeros()
    {
        byte[][] padded = FixedStringTexts.Select(text => Padded(text, 4)).ToArray();
        ColumnWriter<string> writer = Derivation.Writer<string>("FixedString(4)", ConverterHarness.Context);
        byte[] marks = Marks(FixedStringTexts.Length);
        byte[] marked = marks.Concat(padded.SelectMany((value, i) => marks[i] != 0 ? new byte[4] : value)).ToArray();

        byte[] whole = await ConverterHarness.WriteNewAsync(writer, FixedStringTexts, 0, FixedStringTexts.Length);
        byte[] slice = await ConverterHarness.WriteNewAsync(writer, FixedStringTexts, 2, FixedStringTexts.Length - 2);

        Assert.Multiple(() =>
        {
            Assert.That(whole, Is.EqualTo(padded.SelectMany(value => value).ToArray()), "whole column");
            Assert.That(slice, Is.EqualTo(padded.Skip(2).SelectMany(value => value).ToArray()), "slice from row 2");
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
    /// the marks, then each value padded to N or N zero bytes at a marked position, and its <c>WritePlaceholder</c> gives
    /// N zero bytes.
    /// </summary>
    [Test]
    public async Task Write_WideFixedStringUnderMarks_GivesTheMarksThenThePaddedValues()
    {
        const int size = 10_000;
        const string type = "FixedString(10000)";
        string[] texts = { "a", "b", new string('x', size), "c", "é" };
        byte[][] padded = texts.Select(text => Padded(text, size)).ToArray();
        byte[] marks = Marks(texts.Length);
        byte[] expected = marks.Concat(padded.SelectMany((value, i) => marks[i] != 0 ? new byte[size] : value)).ToArray();
        var bytesLeaf = (BytesLeafWriter<byte[]>)Derivation.Writer<byte[]>(type, ConverterHarness.Context);
        byte[] placeholder = await CodecTestHarness.WriteAsync(bytesLeaf.WritePlaceholder);

        await AssertMarkedWriteAsync(type, padded.Select((value, i) => marks[i] != 0 ? null : value).ToArray(), marks, expected);
        await AssertMarkedWriteAsync(type, texts.Select((value, i) => marks[i] != 0 ? null : value).ToArray(), marks, expected);
        Assert.That(placeholder, Is.EqualTo(new byte[size]));
    }

    /// <summary>
    /// A wide <c>FixedString</c> writer rents its N-byte buffer only for a value: a write of no values (an empty span,
    /// or segments that are all empty) or of marked positions only allocates little. The widths are in pool sizes that
    /// no other case rents (the marked case writes into a buffer of 2N), so a buffer that one case gives back to the
    /// pool does not hide a rent of another case.
    /// </summary>
    [TestCase(6_000_000, "empty span")]
    [TestCase(3_000_000, "empty segments")]
    [TestCase(24_000_000, "marked positions only")]
    public void Write_WideFixedStringWithNoValueToEncode_AllocatesLittle(int size, string source)
    {
        string type = $"FixedString({size})";
        var text = Derivation.Writer<string>(type, ConverterHarness.Context);
        var bytes = Derivation.Writer<byte[]>(type, ConverterHarness.Context);
        string[][] emptySegments = { Array.Empty<string>(), Array.Empty<string>() };
        byte[][][] emptyByteSegments = { Array.Empty<byte[]>() };
        byte[] marks = { 1, 1 };
        string[] absentTexts = new string[2];
        byte[][] absentBytes = new byte[2][];

        // For marked positions, a buffer that already holds two placeholders, so the measured writes do not grow it.
        int bufferSize = source == "marked positions only" ? (2 * size) + 1024 : 65536;
        using var output = new ClickHouse.Driver.Tcp.Protocol.ClickHouseBinaryWriter(System.IO.Stream.Null, bufferSize, writesToTransport: false);
        long before = GC.GetAllocatedBytesForCurrentThread();
        switch (source)
        {
            case "empty span":
                text.Write(output, ValueSource<string>.Of(ReadOnlySpan<string>.Empty), null);
                bytes.Write(output, ValueSource<byte[]>.Of(ReadOnlySpan<byte[]>.Empty), null);
                break;
            case "empty segments":
                text.Write(output, ValueSource<string>.OfSegments(emptySegments), null);
                bytes.Write(output, ValueSource<byte[]>.OfSegments(emptyByteSegments), null);
                break;
            default:
                text.Write(output, ValueSource<string>.Of(absentTexts).WithAbsent(marks), null);
                output.Reset();
                bytes.Write(output, ValueSource<byte[]>.Of(absentBytes).WithAbsent(marks), null);
                break;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.That(allocated, Is.LessThan(64 * 1024), $"bytes allocated to write {source} as {type}");
    }

    /// <summary>A UUID is two little-endian 64-bit halves on the wire; the scalar conversion gives those bytes.</summary>
    [TestCase("00000000-0000-0000-0000-000000000000", "00000000000000000000000000000000")]
    [TestCase("00112233-4455-6677-8899-aabbccddeeff", "7766554433221100FFEEDDCCBBAA9988")]
    [TestCase("ffeeddcc-bbaa-9988-7766-554433221100", "8899AABBCCDDEEFF0011223344556677")]
    [TestCase("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0", "78695A4B3C2D1E0FF0E1D2C3B4A59687")]
    [TestCase("ffffffff-ffff-ffff-ffff-ffffffffffff", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")]
    public void ConvertScalar_Uuid_GivesThePinnedWireBytes(string uuid, string expected)
    {
        var value = new Guid(uuid);
        UInt128 scalar = UuidBytes.ConvertScalar(value);
        UInt128 converted = default(UuidBytes).Convert(value, 0);

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(System.Runtime.InteropServices.MemoryMarshal.AsBytes(new[] { scalar }.AsSpan())), Is.EqualTo(expected), "scalar");
            Assert.That(converted, Is.EqualTo(scalar), "the conversion of this runtime");
        });
    }

    /// <summary>
    /// The canonical value of a fixed-width leaf is its wire value, so its bytes in memory are the wire bytes: of the
    /// samples of the leaf, and of the sample at row 1 alone. The dictionary of a LowCardinality write relies on this.
    /// </summary>
    [TestCase("UUID", "000000000000000000000000000000007766554433221100FFEEDDCCBBAA9988FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "7766554433221100FFEEDDCCBBAA9988")]
    [TestCase(
        "IPv6",
        "000000000000000000000000000000000000000000000000000000000000000120010DB800000000000000000000000100000000000000000000FFFF0102030420010DB885A308D313198A2E03707348",
        "00000000000000000000000000000001")]
    [TestCase("IPv4", "000000000100007F0101A8C0FFFFFFFF", "0100007F")]
    [TestCase("Date", "0000194DFFFF", "194D")]
    [TestCase(
        "Decimal(38, 10)",
        "00000000000000000000000000000000874B9F2CA8F258F1FD1E660000000000FF1BF4ABFDFFFFFFFFFFFFFFFFFFFFFF",
        "874B9F2CA8F258F1FD1E660000000000")]
    [TestCase(
        "Float64",
        "000000000000000000000000000000803B255FB8836EBBD4FFFFFFFFFFFFEFFFFFFFFFFFFFFFEF7F000000000000F8FF010000000000F87F020000000000F8FF000000000000F07F0100000000000000",
        "0000000000000080")]
    public Task Encode_CanonicalValues_GiveThePinnedWireBytes(string type, string all, string row1)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(LeafWriterTests),
            nameof(AssertCanonicalEncodesThePinnedBytesAsync),
            new[] { ConverterHarness.Codec(type).ElementType, CanonicalType(type) },
            type,
            all,
            row1);

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
        yield return Case("DateTime", new[] { new DateTime(2200, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        yield return Case("Time64(3)", new[] { new TimeSpan(1000, 0, 0) });
    }

    private const string IntervalSamples = "0000000000000080FBFFFFFFFFFFFFFF0000000000000000FFFFFFFFFFFFFF7F";

    private const string IntervalSamplesFromRow1 = "FBFFFFFFFFFFFFFF0000000000000000FFFFFFFFFFFFFF7F";

    private static async Task AssertWritesThePinnedBytesAsync<T>(string type, string whole, string fromRow1)
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        Assert.That(values, Has.Length.GreaterThanOrEqualTo(3), "a sample needs a slice that starts after row 0");
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);

        byte[] newWhole = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);
        byte[] newSlice = await ConverterHarness.WriteNewAsync(writer, values, 1, values.Length - 1);

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(newWhole), Is.EqualTo(whole), "whole column");
            Assert.That(Convert.ToHexString(newSlice), Is.EqualTo(fromRow1), "slice from row 1");
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

    // The values of the samples, with default values at the marked positions.
    private static Task AssertMarksGiveThePinnedBytesAsync<T>(string type, string expected)
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        byte[] marks = Marks(values.Length);
        T[] present = values.Select((value, i) => marks[i] != 0 ? default : value).ToArray();
        return AssertMarkedWriteAsync(type, present, marks, Convert.FromHexString(expected));
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

    private static IEnumerable<TestCaseData> RefusedValuesWithErrors() => ConverterHarness.WithErrors(RefusedValues(), RefusedErrors);

    private static async Task AssertFailsWithAsync<T>(string type, T[] values, int start, string exception, string parameter, string message)
    {
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);
        Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteNewAsync(writer, values, start, values.Length - start));
        ConverterHarness.AssertFailure(actual, exception, parameter, message, "write");
    }

    private static async Task AssertCanonicalEncodesThePinnedBytesAsync<T, TCanon>(string type, string all, string row1)
        where TCanon : unmanaged, IEquatable<TCanon>
    {
        var values = (T[])LeafSamples.Writable(type, typeof(T));
        var writer = (FixedLeafWriter<T, TCanon>)Derivation.Writer<T>(type, ConverterHarness.Context);
        var canonical = new TCanon[values.Length];
        writer.ToCanonical(values, canonical, 0);

        byte[] encoded = await CodecTestHarness.WriteAsync(w => FixedLeafWriter<T, TCanon>.Encode(w, canonical));
        byte[] one = await CodecTestHarness.WriteAsync(w => FixedLeafWriter<T, TCanon>.Encode(w, new[] { writer.ToCanonical(values[1], 1) }));

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(encoded), Is.EqualTo(all), "bulk");
            Assert.That(Convert.ToHexString(one), Is.EqualTo(row1), "one value");
        });
    }

    private static IEnumerable<TestCaseData> Pairs(Func<Leaf, Type, bool> include)
    {
        foreach (string type in LeafSamples.Types)
        {
            IColumnCodec codec = ConverterHarness.Codec(type);
            Leaf leaf = LeafTableTests.LeafOf(type);
            foreach (Type clrType in leaf.WriteTypes(codec))
            {
                if (include(leaf, clrType))
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

    // DateTime values on the wire: little-endian UInt32 seconds.
    private static byte[] Seconds(IEnumerable<uint> seconds) => seconds.SelectMany(BitConverter.GetBytes).ToArray();
}
