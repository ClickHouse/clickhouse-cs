using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Format;

/// <summary>
/// A column that a query read, inserted into a column of another type. The codec writes it from its storage only when
/// the parameters that give its stored values their meaning are the same: the scale of a <c>DateTime64</c> or a
/// <c>Time64</c>, the members of an <c>Enum</c>. Else the insert converts the values through their meaning (a time, a
/// duration, a label), or refuses the column when no CLR type holds them without a loss.
/// </summary>
[TestFixture]
public class DecodedColumnConversionTests
{
    private static readonly ResolveContext Utc = new() { ServerTimezone = "UTC" };

    // Three rows of Tuple(DateTime64(3, 'UTC'), String): the counts 1, 2 and 3, and the strings FF, empty and C3 28 (two
    // byte sequences that are not UTF-8).
    private const string TupleOfStringsBytes = "010000000000000002000000000000000300000000000000" + "01FF" + "00" + "02C328";

    private static readonly DateTimeOffset[] Instants =
    {
        new(2024, 1, 2, 3, 4, 5, 678, TimeSpan.Zero),
        DateTimeOffset.UnixEpoch,
        new(1969, 12, 31, 23, 59, 59, 999, TimeSpan.Zero),
    };

    [Test]
    public void CanWrite_DecodedDateTime64ColumnOfAnotherScale_IsFalse()
    {
        using IColumn decoded = DecodedColumns.Of("c", "DateTime64(3, 'UTC')", Instants);

        Assert.Multiple(() =>
        {
            Assert.That(Codec("DateTime64(3, 'UTC')").CanWrite(decoded), Is.True);
            Assert.That(Codec("DateTime64(3, 'Asia/Tokyo')").CanWrite(decoded), Is.True, "a count is an instant in every timezone");
            Assert.That(Codec("DateTime64(6, 'UTC')").CanWrite(decoded), Is.False);
            Assert.That(Codec("DateTime64(0, 'UTC')").CanWrite(decoded), Is.False);
        });
    }

    [Test]
    public void CanWrite_DecodedTime64ColumnOfAnotherScale_IsFalse()
    {
        using IColumn decoded = DecodedColumns.Of("c", "Time64(3)", TimeSpan.FromMilliseconds(3_723_456));

        Assert.Multiple(() =>
        {
            Assert.That(Codec("Time64(3)").CanWrite(decoded), Is.True);
            Assert.That(Codec("Time64(6)").CanWrite(decoded), Is.False);
        });
    }

    [Test]
    public void CanWrite_DecodedEnumColumnOfOtherMembers_IsFalse()
    {
        using IColumn decoded = DecodedColumns.Of("c", "Enum8('a' = 1, 'b' = 2)", "a", "b");

        Assert.Multiple(() =>
        {
            Assert.That(Codec("Enum8('a' = 1, 'b' = 2)").CanWrite(decoded), Is.True);
            Assert.That(Codec("Enum8('b' = 2, 'a' = 1)").CanWrite(decoded), Is.True, "the same members in another order");
            Assert.That(Codec("Enum8('a' = 2, 'b' = 1)").CanWrite(decoded), Is.False, "the ordinals mean other labels");
            Assert.That(Codec("Enum8('a' = 1, 'b' = 2, 'c' = 3)").CanWrite(decoded), Is.False, "another member");
            Assert.That(Codec("Enum16('a' = 1, 'b' = 2)").CanWrite(decoded), Is.False, "another width");
        });
    }

    /// <summary>A composite codec asks the codec of each child, so a child of other parameters makes it refuse the column.</summary>
    [TestCaseSource(nameof(CompositeCases))]
    public void CanWrite_DecodedCompositeWithAChildOfOtherParameters_IsFalse(string source, string target, Array values)
    {
        using IColumn decoded = Decode(source, values);

        Assert.Multiple(() =>
        {
            Assert.That(Codec(source).CanWrite(decoded), Is.True, "the codec of the type that the column was read as");
            Assert.That(Codec(target).CanWrite(decoded), Is.False);
        });
    }

    [TestCase("DateTime64(3)", "DateTime64(6)", true)]
    [TestCase("DateTime64(3, 'UTC')", "DateTime64(3, 'Asia/Tokyo')", false)]
    [TestCase("Time64(3)", "Time64(6)", true)]
    [TestCase("Enum8('a' = 1, 'b' = 2)", "Enum8('b' = 2, 'a' = 1)", false)]
    [TestCase("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 2, 'b' = 1)", true)]
    [TestCase("Enum8('a' = 1)", "Enum16('a' = 1)", true)]
    [TestCase("Nullable(DateTime64(3))", "DateTime64(6)", true)]
    [TestCase("DateTime64(3)", "Nullable(DateTime64(6))", true)]
    [TestCase("DateTime64(3)", "Nullable(DateTime64(3))", false)]
    [TestCase("LowCardinality(Nullable(Enum8('a' = 1)))", "LowCardinality(Nullable(Enum8('a' = 2)))", true)]
    [TestCase("Array(DateTime64(3))", "Array(DateTime64(3))", false)]
    [TestCase("Array(DateTime64(3))", "Array(DateTime64(6))", true)]
    [TestCase("Map(String, Time64(3))", "Map(String, Time64(6))", true)]
    [TestCase("Tuple(a DateTime64(3), b String)", "Tuple(a DateTime64(6), b String)", true)]
    [TestCase("Variant(DateTime64(3), String)", "Variant(String, DateTime64(3))", false)]
    [TestCase("Variant(DateTime64(3), String)", "Variant(DateTime64(6), String)", true)]
    [TestCase("Variant(Array(DateTime64(3)), String)", "Variant(String, Array(DateTime64(6)))", true)]
    [TestCase("Decimal(9, 2)", "Decimal(18, 4)", false)]
    [TestCase("DateTime('UTC')", "DateTime('Asia/Tokyo')", false)]
    [TestCase("FixedString(2)", "FixedString(4)", false)]
    [TestCase("DateTime64(3)", "Int64", false)]
    [TestCase("Enum8('a' = 1)", "Int8", false)]
    public void ChangesMeaning_SourceAndTargetTypes_IsWhetherAValueMeansAnotherValue(string source, string target, bool expected)
        => Assert.That(ConverterDerivation.Default.ChangesMeaning(source, target), Is.EqualTo(expected));

    /// <summary>
    /// The insert converts the values through their meaning: the target reads back the instants, durations and labels of
    /// the source, for all rows and for a slice from row 1.
    /// </summary>
    [TestCaseSource(nameof(ConvertedCases))]
    public Task Write_DecodedColumnOfARelatedType_WritesTheMeaningOfTheValues(string source, string target, Array values)
        => (Task)ConverterHarness.InvokeGeneric(typeof(DecodedColumnConversionTests), nameof(AssertConvertsAsync), new[] { values.GetType().GetElementType() }, source, target, values);

    /// <summary>
    /// The counts of a <c>DateTime64(3)</c> are milliseconds, and the counts of a <c>DateTime64(6)</c> microseconds: the
    /// insert multiplies them by 1000.
    /// </summary>
    [Test]
    public async Task Write_DecodedDateTime64IntoAFinerScale_WritesTheCountsOfTheFinerScale()
    {
        using IColumn decoded = DecodedColumns.Of("c", "DateTime64(3, 'UTC')", Instants);

        byte[] bytes = await WriteAsync(decoded, "DateTime64(6, 'UTC')", 0, decoded.RowCount);

        Assert.That(
            Convert.ToHexString(bytes),
            Is.EqualTo(Convert.ToHexString(Instants.SelectMany(instant => BitConverter.GetBytes(instant.ToUnixTimeMilliseconds() * 1000)).ToArray())));
    }

    /// <summary>An Enum is written by label: the ordinals of the target for the labels of the source.</summary>
    [Test]
    public async Task Write_DecodedEnumIntoOtherOrdinals_WritesTheOrdinalsOfTheLabels()
    {
        using IColumn decoded = DecodedColumns.Of("c", "Enum8('a' = 1, 'b' = 2)", "a", "b", "a");

        byte[] bytes = await WriteAsync(decoded, "Enum8('a' = 2, 'b' = 1)", 0, decoded.RowCount);

        Assert.That(Convert.ToHexString(bytes), Is.EqualTo("020102"));
    }

    /// <summary>An array that CreateArray builds over a decoded inner column is converted as an array of that column's type.</summary>
    [Test]
    public async Task Write_ArrayOverADecodedColumnOfAnotherScale_WritesTheMeaningOfTheValues()
    {
        IColumn<long> inner = (IColumn<long>)DecodedColumns.Of("c", "DateTime64(3, 'UTC')", Instants);
        IArrayColumn<long> array = ClickHouseTcpColumn.CreateArray("c", inner, new[] { 0, 2, 2, 3 });
        const string target = "Array(DateTime64(6, 'UTC'))";

        byte[] bytes = await WriteAsync(array, target, 0, array.RowCount, prefix: true);
        using IColumn read = await ConverterHarness.ReadBackAsync(target, bytes, array.RowCount);

        Assert.That(
            ConverterHarness.ReadAs<DateTimeOffset[]>(read, 0, read.RowCount),
            Is.EqualTo(new[] { Instants[..2], Array.Empty<DateTimeOffset>(), Instants[2..] }));
    }

    /// <summary>
    /// A codec of the same parameters writes the column from its storage: a <c>DateTime64</c> into another timezone keeps
    /// its counts, which are instants.
    /// </summary>
    [Test]
    public async Task Write_DecodedDateTime64IntoAnotherTimezone_WritesTheStoredCounts()
    {
        using IColumn decoded = DecodedColumns.Of("c", "DateTime64(3, 'UTC')", Instants);
        byte[] stored = await CodecTestHarness.WriteStoredAsync(Codec("DateTime64(3, 'UTC')"), decoded, 0, decoded.RowCount);

        byte[] bytes = await WriteAsync(decoded, "DateTime64(3, 'Asia/Tokyo')", 0, decoded.RowCount);

        Assert.That(bytes, Is.EqualTo(stored));
    }

    /// <summary>
    /// A <c>Decimal</c> column stores CLR values, which the codec of another precision and scale encodes at its own scale;
    /// a <c>DateTime</c> column stores instants. Neither changes meaning, so the codec writes them.
    /// </summary>
    [Test]
    public async Task Write_DecodedDecimalOrDateTimeIntoOtherParameters_IsWrittenByTheCodec()
    {
        using IColumn decimals = DecodedColumns.Of("c", "Decimal(9, 2)", 1.23m, -4.5m);
        using IColumn instants = DecodedColumns.Of("c", "DateTime('UTC')", Instants[0].AddMilliseconds(-678), Instants[1]);

        byte[] decimalBytes = await WriteAsync(decimals, "Decimal(18, 4)", 0, decimals.RowCount);
        using IColumn decimalsRead = await ConverterHarness.ReadBackAsync("Decimal(18, 4)", decimalBytes, decimals.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(Codec("Decimal(18, 4)").CanWrite(decimals), Is.True);
            Assert.That(ConverterHarness.ReadAs<decimal>(decimalsRead, 0, 2), Is.EqualTo(new[] { 1.23m, -4.5m }));
            Assert.That(Codec("DateTime('Asia/Tokyo')").CanWrite(instants), Is.True);
        });
    }

    /// <summary>
    /// The width of a <c>FixedString</c>, the dimension of a <c>QBit</c> and the range of a <c>Date</c> do not change the
    /// meaning of a value, so the converter writes the values as they are: a value of another width or dimension is
    /// refused, and a <c>Date32</c> value that a <c>Date</c> can hold is written.
    /// </summary>
    [Test]
    public async Task Write_DecodedColumnOfAnotherWidthDimensionOrRange_WritesTheValuesOrRefusesThem()
    {
        using IColumn text = DecodedColumns.Of("c", "FixedString(2)", new[] { "ab"u8.ToArray() });
        using IColumn dictionary = DecodedColumns.Of("c", "LowCardinality(FixedString(2))", new[] { "ab"u8.ToArray() });
        using IColumn vectors = DecodedColumns.Of("c", "QBit(Float32, 2)", new[] { new[] { 1f, 2f } });
        using IColumn days = DecodedColumns.Of("c", "Date32", new DateOnly(2024, 1, 2));

        Exception width = ConverterHarness.Catch(() => WriteAsync(text, "FixedString(4)", 0, 1).GetAwaiter().GetResult());
        Exception dictionaryWidth = ConverterHarness.Catch(() => WriteAsync(dictionary, "LowCardinality(FixedString(4))", 0, 1).GetAwaiter().GetResult());
        Exception dimension = ConverterHarness.Catch(() => WriteAsync(vectors, "QBit(Float32, 4)", 0, 1).GetAwaiter().GetResult());
        byte[] date = await WriteAsync(days, "Date", 0, 1);

        Assert.Multiple(() =>
        {
            Assert.That(width?.Message, Does.StartWith("A FixedString(4) value at row 0 is 2 bytes; every value must be exactly 4 bytes."));
            Assert.That(dictionaryWidth?.Message, Does.StartWith("A FixedString(4) value at row 1 is 2 bytes; every value must be exactly 4 bytes."), "named by its dictionary slot");
            Assert.That(dimension?.Message, Does.StartWith("A QBit(Float32, 4) vector at row 0 has 2 element(s); every vector must have exactly 4."));
            Assert.That(Convert.ToHexString(date), Is.EqualTo("0C4D"), "19724 days since 1970-01-01, little-endian");
        });
    }

    /// <summary>
    /// A <c>Nested</c> type is written only from a column of its own layout, so a field of another scale is refused with
    /// the reason.
    /// </summary>
    [Test]
    public async Task For_DecodedNestedWithAFieldOfAnotherScale_IsRefusedWithTheReason()
    {
        const string source = "Nested(a DateTime64(3, 'UTC'))";
        const string target = "Nested(a DateTime64(6, 'UTC'))";
        byte[] bytes = { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        using IColumn decoded = await ConverterHarness.ReadBackAsync(source, bytes, 1);

        InsertColumnWrite write = InsertColumnWrite.For(Codec(target), decoded, target, Utc, ConverterDerivation.Default, out string refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null);
            Assert.That(refusal, Does.StartWith($"The values of {source} are converted to another type only through"));
        });
    }

    /// <summary>A value that the target type cannot hold is refused with the row of the column, as for any other value.</summary>
    [Test]
    public void Write_DecodedValueThatTheTargetCannotHold_IsRefusedWithItsRow()
    {
        using IColumn enums = DecodedColumns.Of("c", "Enum8('a' = 1, 'b' = 2)", "a", "a", "b");
        using IColumn instants = DecodedColumns.Of("c", "DateTime64(6, 'UTC')", Instants[1], Instants[1].AddTicks(10));

        Exception label = ConverterHarness.Catch(() => WriteAsync(enums, "Enum8('a' = 1)", 1, 2).GetAwaiter().GetResult());
        Exception precision = ConverterHarness.Catch(() => WriteAsync(instants, "DateTime64(3, 'UTC')", 0, 2).GetAwaiter().GetResult());

        Assert.Multiple(() =>
        {
            ConverterHarness.AssertFailure(label, "ArgumentException", "label", "'b' is not a label of 'Enum8('a' = 1)'. Its labels are: 'a'. (Parameter 'label')", "label");
            ConverterHarness.AssertFailure(
                precision,
                "ArgumentException",
                "value",
                "1970-01-01T00:00:00.0000010+00:00 cannot be written to DateTime64(3, 'UTC') (scale 3) without losing precision. (Parameter 'value')",
                "precision");
        });
    }

    /// <summary>
    /// No CLR type holds the counts of a scale above 7, and a Variant places its values by their CLR type, so the insert
    /// refuses these columns and says why.
    /// </summary>
    [TestCase(
        "DateTime64(9, 'UTC')",
        "DateTime64(6, 'UTC')",
        "A value of DateTime64(9, 'UTC') is a count at scale 9, finer than the 100 ns ticks of the System.DateTimeOffset that converts it, so a column read as DateTime64(9, 'UTC') is written only into a column of the same scale.")]
    [TestCase(
        "Variant(DateTime64(3, 'UTC'), String)",
        "Variant(DateTime64(6, 'UTC'), String)",
        "The values of Variant(DateTime64(3, 'UTC'), String) are converted to another type only through Nullable, LowCardinality, Array, Map and Tuple, so a column read as Variant(DateTime64(3, 'UTC'), String) is written only into a column whose DateTime64, Time64 and Enum types are the same.")]
    public void For_DecodedColumnThatNoClrTypeConverts_IsRefusedWithTheReason(string source, string target, string reason)
    {
        using IColumn decoded = source.StartsWith("Variant", StringComparison.Ordinal)
            ? DecodedColumns.Of<object>("c", source, "a", null)
            : DecodedColumns.Of("c", source, Instants);

        InsertColumnWrite write = InsertColumnWrite.For(Codec(target), decoded, target, Utc, ConverterDerivation.Default, out string refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null);
            Assert.That(refusal, Is.EqualTo(reason));
        });
    }

    /// <summary>
    /// The insert converts only the parts whose values mean other values, and writes every other part from its storage. A
    /// String that is not UTF-8, a FixedString, the bits of a NaN and of -0, a DateTime64 of the same scale, a Decimal, a
    /// UUID, an IPv6 address mapped from IPv4, an Enum of the same definition and a LowCardinality dictionary keep their
    /// bytes, in a Tuple, a Map, an Array, a Nullable Tuple and a SimpleAggregateFunction. A part of another type whose
    /// values keep their meaning goes through the converter tree of its CLR type. All rows, and the rows from row 1.
    /// </summary>
    [TestCaseSource(nameof(PartCases))]
    public async Task Write_DecodedCompositeOfAnotherMeaning_WritesTheOtherPartsFromTheirStorage(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
    {
        using IColumn decoded = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString(sourceBytes), rows);

        byte[] all = await WriteAsync(decoded, target, 0, rows, prefix: true);
        byte[] fromRow1 = await WriteAsync(decoded, target, 1, rows - 1, prefix: true);

        Assert.Multiple(() =>
        {
            Assert.That(Codec(target).CanWrite(decoded), Is.False, "the codec of the target does not write the column from its storage");
            Assert.That(Convert.ToHexString(all), Is.EqualTo(expected), "all rows");
            Assert.That(Convert.ToHexString(fromRow1), Is.EqualTo(expectedFromRow1), "rows from row 1");
        });
    }

    /// <summary>
    /// An array that CreateArray builds over a decoded Tuple column of another scale: the offsets of the rows, the converted
    /// counts, and the stored bytes of the String elements.
    /// </summary>
    [Test]
    public async Task Write_ArrayOverADecodedTupleOfAnotherScale_WritesTheOtherPartsFromTheirStorage()
    {
        const string source = "Tuple(DateTime64(3, 'UTC'), String)";
        const string target = "Array(Tuple(DateTime64(6, 'UTC'), String))";
        using IColumn inner = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString(TupleOfStringsBytes), 3);
        IArrayColumn<(long, string)> array = ClickHouseTcpColumn.CreateArray("c", (IColumn<(long, string)>)inner, new[] { 0, 1, 1, 3 });

        byte[] all = await WriteAsync(array, target, 0, 3, prefix: true);
        byte[] fromRow1 = await WriteAsync(array, target, 1, 2, prefix: true);

        Assert.Multiple(() =>
        {
            Assert.That(
                Convert.ToHexString(all),
                Is.EqualTo("010000000000000001000000000000000300000000000000E803000000000000D007000000000000B80B00000000000001FF0002C328"),
                "all rows");
            Assert.That(
                Convert.ToHexString(fromRow1),
                Is.EqualTo("00000000000000000200000000000000D007000000000000B80B0000000000000002C328"),
                "rows from row 1");
        });
    }

    /// <summary>
    /// A Tuple into a Nullable Tuple has no NULL; a Nullable Tuple into a Tuple refuses a row that holds no value, with its
    /// row, and writes the rows that hold one.
    /// </summary>
    [Test]
    public async Task Write_DecodedTupleIntoANullableTupleOrBack_WritesTheNullMapOrRefusesANull()
    {
        using IColumn tuple = await ConverterHarness.ReadBackAsync("Tuple(DateTime64(3, 'UTC'), String)", Convert.FromHexString(TupleOfStringsBytes), 3);
        using IColumn nullable = await ConverterHarness.ReadBackAsync(
            "Nullable(Tuple(DateTime64(3, 'UTC'), String))",
            Convert.FromHexString("010000" + TupleOfStringsBytes),
            3);

        byte[] intoNullable = await WriteAsync(tuple, "Nullable(Tuple(DateTime64(6, 'UTC'), String))", 1, 2, prefix: true);
        byte[] fromRow1 = await WriteAsync(nullable, "Tuple(DateTime64(6, 'UTC'), String)", 1, 2, prefix: true);
        Exception refused = ConverterHarness.Catch(() => WriteAsync(nullable, "Tuple(DateTime64(6, 'UTC'), String)", 0, 3).GetAwaiter().GetResult());

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(intoNullable), Is.EqualTo("0000" + "D007000000000000B80B000000000000" + "0002C328"));
            Assert.That(Convert.ToHexString(fromRow1), Is.EqualTo("D007000000000000B80B000000000000" + "0002C328"));
            ConverterHarness.AssertFailure(
                refused,
                "InvalidOperationException",
                null,
                "Column 'c' (Tuple(DateTime64(6, 'UTC'), String)) is null at row 0 of the insert, but it cannot hold null. Make the column Nullable(...), or leave out the rows with no value.",
                "the NULL at row 0");
        });
    }

    /// <summary>
    /// A part that no CLR type converts, and a part of another type that its CLR type does not write, refuse the whole
    /// column, with the reason of the part.
    /// </summary>
    [TestCase(
        "Tuple(DateTime64(9, 'UTC'), String)",
        "Tuple(DateTime64(6, 'UTC'), String)",
        "010000000000000002000000000000000300000000000000" + "01FF0002C328",
        "A value of DateTime64(9, 'UTC') is a count at scale 9, finer than the 100 ns ticks of the System.DateTimeOffset that converts it, so a column read as DateTime64(9, 'UTC') is written only into a column of the same scale.")]
    [TestCase(
        "Tuple(DateTime64(3, 'UTC'), Int32)",
        "Tuple(DateTime64(6, 'UTC'), Int64)",
        "010000000000000002000000000000000300000000000000" + "07000000FFFFFFFFFFFFFF7F",
        "'Int64' cannot be written from System.Int32. It is written from: System.Int64.")]
    public async Task For_DecodedTupleWithAPartThatCannotBeWritten_IsRefusedWithTheReasonOfThePart(string source, string target, string sourceBytes, string reason)
    {
        using IColumn decoded = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString(sourceBytes), 3);

        InsertColumnWrite write = InsertColumnWrite.For(Codec(target), decoded, target, Utc, ConverterDerivation.Default, out string refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null);
            Assert.That(refusal, Is.EqualTo(reason));
        });
    }

    public static IEnumerable<TestCaseData> PartCases()
    {
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), String)",
            "Tuple(DateTime64(6, 'UTC'), String)",
            3,
            "01000000000000000200000000000000030000000000000001FF0002C328",
            "E803000000000000D007000000000000B80B00000000000001FF0002C328",
            "D007000000000000B80B0000000000000002C328");
        yield return Bytes(
            "Tuple(String, Time64(3))",
            "Tuple(String, Time64(6))",
            3,
            "01FF0002C328010000000000000002000000000000000300000000000000",
            "01FF0002C328E803000000000000D007000000000000B80B000000000000",
            "0002C328D007000000000000B80B000000000000");
        yield return Bytes(
            "Map(String, DateTime64(3, 'UTC'))",
            "Map(String, DateTime64(6, 'UTC'))",
            3,
            "01000000000000000100000000000000030000000000000001FF0002C328010000000000000002000000000000000300000000000000",
            "01000000000000000100000000000000030000000000000001FF0002C328E803000000000000D007000000000000B80B000000000000",
            "000000000000000002000000000000000002C328D007000000000000B80B000000000000");
        yield return Bytes(
            "Map(Enum8('a' = 1, 'b' = 2), String)",
            "Map(Enum8('a' = 2, 'b' = 1), String)",
            3,
            "01000000000000000100000000000000030000000000000001020101FF0002C328",
            "01000000000000000100000000000000030000000000000002010201FF0002C328",
            "0000000000000000020000000000000001020002C328");
        yield return Bytes(
            "Array(Tuple(DateTime64(3, 'UTC'), String))",
            "Array(Tuple(DateTime64(6, 'UTC'), String))",
            3,
            "01000000000000000100000000000000030000000000000001000000000000000200000000000000030000000000000001FF0002C328",
            "010000000000000001000000000000000300000000000000E803000000000000D007000000000000B80B00000000000001FF0002C328",
            "00000000000000000200000000000000D007000000000000B80B0000000000000002C328");
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), FixedString(2))",
            "Tuple(DateTime64(6, 'UTC'), FixedString(2))",
            3,
            "010000000000000002000000000000000300000000000000FF00C3288081",
            "E803000000000000D007000000000000B80B000000000000FF00C3288081",
            "D007000000000000B80B000000000000C3288081");
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), Float64)",
            "Tuple(DateTime64(6, 'UTC'), Float64)",
            3,
            "010000000000000002000000000000000300000000000000010000000000F47F0000000000000080230100000000F8FF",
            "E803000000000000D007000000000000B80B000000000000010000000000F47F0000000000000080230100000000F8FF",
            "D007000000000000B80B0000000000000000000000000080230100000000F8FF");
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), DateTime64(9, 'UTC'))",
            "Tuple(DateTime64(6, 'UTC'), DateTime64(9, 'UTC'))",
            3,
            "01000000000000000200000000000000030000000000000015CD853DFE9C971700000000000000000100000000000000",
            "E803000000000000D007000000000000B80B00000000000015CD853DFE9C971700000000000000000100000000000000",
            "D007000000000000B80B00000000000000000000000000000100000000000000");
        yield return Bytes(
            "Tuple(Enum8('a' = 1, 'b' = 2), Decimal(38, 10), UUID, IPv6, Enum8('x' = 1, 'y' = 2))",
            "Tuple(Enum8('a' = 2, 'b' = 1), Decimal(38, 10), UUID, IPv6, Enum8('x' = 1, 'y' = 2))",
            2,
            "0102D20A1FEB8CA954AB0000000000000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00112233445566778899AABBCCDDEEFFFFEEDDCCBBAA9988776655443322110000000000000000000000FFFF0102030420010DB80000000000000000000000010102",
            "0201D20A1FEB8CA954AB0000000000000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00112233445566778899AABBCCDDEEFFFFEEDDCCBBAA9988776655443322110000000000000000000000FFFF0102030420010DB80000000000000000000000010102",
            "01FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEEDDCCBBAA9988776655443322110020010DB800000000000000000000000102");
        yield return Bytes(
            "Nullable(Tuple(Enum8('a' = 1, 'b' = 2), String))",
            "Nullable(Tuple(Enum8('b' = 1), String))",
            3,
            "01000001020201FF0002C328",
            "01000001010101FF0002C328",
            "000001010002C328");
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), Decimal(9, 2))",
            "Tuple(DateTime64(6, 'UTC'), Decimal(18, 4))",
            3,
            "0100000000000000020000000000000003000000000000007B0000003EFEFFFF00000000",
            "E803000000000000D007000000000000B80B0000000000000C300000000000003850FFFFFFFFFFFF0000000000000000",
            "D007000000000000B80B0000000000003850FFFFFFFFFFFF0000000000000000");
        yield return Bytes(
            "SimpleAggregateFunction(anyLast, Tuple(DateTime64(3, 'UTC'), String))",
            "SimpleAggregateFunction(anyLast, Tuple(DateTime64(6, 'UTC'), String))",
            3,
            "01000000000000000200000000000000030000000000000001FF0002C328",
            "E803000000000000D007000000000000B80B00000000000001FF0002C328",
            "D007000000000000B80B0000000000000002C328");
        yield return Bytes(
            "Tuple(Nullable(DateTime64(3, 'UTC')), String)",
            "Tuple(Nullable(DateTime64(6, 'UTC')), String)",
            3,
            "00010001000000000000000000000000000000030000000000000001FF0002C328",
            "000100E8030000000000000000000000000000B80B00000000000001FF0002C328",
            "01000000000000000000B80B0000000000000002C328");
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), LowCardinality(String))",
            "Tuple(DateTime64(6, 'UTC'), LowCardinality(String))",
            3,
            "0100000000000000010000000000000002000000000000000300000000000000000600000000000003000000000000000001FF02C3280300000000000000010002",
            "0100000000000000E803000000000000D007000000000000B80B000000000000000600000000000003000000000000000001FF02C3280300000000000000010002",
            "0100000000000000D007000000000000B80B000000000000000600000000000003000000000000000001FF02C32802000000000000000002");
    }

    public static IEnumerable<TestCaseData> CompositeCases()
    {
        yield return Case("Nullable(DateTime64(3, 'UTC'))", "Nullable(DateTime64(6, 'UTC'))", new DateTimeOffset?[] { Instants[0], null });
        yield return Case("Array(Time64(3))", "Array(Time64(6))", new[] { new[] { TimeSpan.FromSeconds(1) }, Array.Empty<TimeSpan>() });
        yield return Case("LowCardinality(Nullable(Enum8('a' = 1, 'b' = 2)))", "LowCardinality(Nullable(Enum8('a' = 2, 'b' = 1)))", new[] { "a", null, "b" });
        yield return Case("Map(String, Enum8('a' = 1, 'b' = 2))", "Map(String, Enum8('a' = 2, 'b' = 1))", new[] { new[] { new KeyValuePair<string, string>("k", "b") } });
        yield return Case("Tuple(DateTime64(3, 'UTC'), String)", "Tuple(DateTime64(6, 'UTC'), String)", new[] { (Instants[0], "x") });
        yield return Case("Variant(DateTime64(3, 'UTC'), String)", "Variant(DateTime64(6, 'UTC'), String)", new object[] { "a", null });
    }

    public static IEnumerable<TestCaseData> ConvertedCases()
    {
        yield return Case("DateTime64(3, 'UTC')", "DateTime64(6, 'UTC')", Instants);
        yield return Case("DateTime64(6, 'UTC')", "DateTime64(3, 'Asia/Tokyo')", Instants);
        yield return Case("DateTime64(3, 'UTC')", "DateTime64(9, 'UTC')", Instants);
        yield return Case("Time64(3)", "Time64(6)", new[] { TimeSpan.FromMilliseconds(3_723_456), TimeSpan.Zero, -TimeSpan.FromMilliseconds(1) });
        yield return Case("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 2, 'b' = 1)", new[] { "a", "b", "a" });
        yield return Case("Enum8('a' = 1, 'b' = 2)", "Enum16('b' = 1000, 'a' = -1000)", new[] { "a", "b", "b" });
        yield return Case("Nullable(DateTime64(3, 'UTC'))", "Nullable(DateTime64(6, 'UTC'))", new DateTimeOffset?[] { Instants[0], null, Instants[2] });
        yield return Case("Nullable(Enum8('a' = 1, 'b' = 2))", "Nullable(Enum8('b' = 1, 'a' = 2))", new[] { "a", null, "b" });
        yield return Case("LowCardinality(Nullable(Enum8('a' = 1, 'b' = 2)))", "LowCardinality(Nullable(Enum8('a' = 2, 'b' = 1)))", new[] { "a", null, "b", "a" });
        yield return Case("Array(DateTime64(3, 'UTC'))", "Array(DateTime64(6, 'UTC'))", new[] { Instants[..2], Array.Empty<DateTimeOffset>(), Instants[2..] });
        yield return Case("Map(String, Time64(3))", "Map(String, Time64(6))", new[] { new[] { new KeyValuePair<string, TimeSpan>("k", TimeSpan.FromSeconds(1)) }, Array.Empty<KeyValuePair<string, TimeSpan>>(), new[] { new KeyValuePair<string, TimeSpan>("m", TimeSpan.Zero) } });
        yield return Case(
            "Tuple(DateTime64(3, 'UTC'), Enum8('a' = 1, 'b' = 2))",
            "Tuple(DateTime64(6, 'UTC'), Enum8('b' = 1, 'a' = 2))",
            new[] { (Instants[0], "a"), (Instants[1], "b"), (Instants[2], "b") });
    }

    private static TestCaseData Case<T>(string source, string target, T[] values) => new TestCaseData(source, target, values).SetArgDisplayNames(source, target);

    private static TestCaseData Bytes(string source, string target, int rows, string sourceBytes, string expected, string expectedFromRow1)
        => new TestCaseData(source, target, rows, sourceBytes, expected, expectedFromRow1).SetArgDisplayNames(source, target);

    private static IColumnCodec Codec(string type) => ColumnCodecRegistry.Default.Resolve(type, Utc);

    private static IColumn Decode(string type, Array values)
        => (IColumn)ConverterHarness.InvokeGeneric(typeof(DecodedColumns), nameof(DecodedColumns.Of), new[] { values.GetType().GetElementType() }, "c", type, values);

    private static async Task AssertConvertsAsync<T>(string source, string target, T[] values)
    {
        using IColumn decoded = DecodedColumns.Of("c", source, values);
        Assert.That(Codec(target).CanWrite(decoded), Is.False, "the codec of the target does not write the column from its storage");

        foreach (int start in new[] { 0, 1 })
        {
            int length = values.Length - start;
            byte[] bytes = await WriteAsync(decoded, target, start, length, prefix: true);
            using IColumn read = await ConverterHarness.ReadBackAsync(target, bytes, length);
            Assert.That(ConverterHarness.ReadAs<T>(read, 0, length), Is.EqualTo(values[start..]), $"{source} into {target}, rows [{start}, {values.Length})");
        }
    }

    // The bytes that an insert writes for rows [start, start + length) of the column into a column of the type.
    private static Task<byte[]> WriteAsync(IColumn column, string type, int start, int length, bool prefix = false)
        => CodecTestHarness.WriteAsync(w => CodecTestHarness.WriteRows(w, Codec(type), column, type, start, length, Utc, prefix));
}
