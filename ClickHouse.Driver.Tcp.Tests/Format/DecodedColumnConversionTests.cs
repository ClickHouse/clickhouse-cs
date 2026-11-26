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
    public void WritesFromStorage_DecodedDateTime64ColumnOfAnotherScale_IsFalse()
    {
        using IColumn decoded = DecodedColumns.Of("c", "DateTime64(3, 'UTC')", Instants);

        Assert.Multiple(() =>
        {
            Assert.That(Codec("DateTime64(3, 'UTC')").WritesFromStorage(decoded), Is.True);
            Assert.That(Codec("DateTime64(3, 'Asia/Tokyo')").WritesFromStorage(decoded), Is.True, "a count is an instant in every timezone");
            Assert.That(Codec("DateTime64(6, 'UTC')").WritesFromStorage(decoded), Is.False);
            Assert.That(Codec("DateTime64(0, 'UTC')").WritesFromStorage(decoded), Is.False);
        });
    }

    [Test]
    public void WritesFromStorage_DecodedTime64ColumnOfAnotherScale_IsFalse()
    {
        using IColumn decoded = DecodedColumns.Of("c", "Time64(3)", TimeSpan.FromMilliseconds(3_723_456));

        Assert.Multiple(() =>
        {
            Assert.That(Codec("Time64(3)").WritesFromStorage(decoded), Is.True);
            Assert.That(Codec("Time64(6)").WritesFromStorage(decoded), Is.False);
        });
    }

    [Test]
    public void WritesFromStorage_DecodedEnumColumnOfOtherMembers_IsFalse()
    {
        using IColumn decoded = DecodedColumns.Of("c", "Enum8('a' = 1, 'b' = 2)", "a", "b");

        Assert.Multiple(() =>
        {
            Assert.That(Codec("Enum8('a' = 1, 'b' = 2)").WritesFromStorage(decoded), Is.True);
            Assert.That(Codec("Enum8('b' = 2, 'a' = 1)").WritesFromStorage(decoded), Is.True, "the same members in another order");
            Assert.That(Codec("Enum8('a' = 2, 'b' = 1)").WritesFromStorage(decoded), Is.False, "the ordinals mean other labels");
            Assert.That(Codec("Enum8('a' = 1, 'b' = 2, 'c' = 3)").WritesFromStorage(decoded), Is.False, "another member");
            Assert.That(Codec("Enum16('a' = 1, 'b' = 2)").WritesFromStorage(decoded), Is.False, "another width");
        });
    }

    /// <summary>A composite codec asks the codec of each child, so a child of other parameters makes it refuse the column.</summary>
    [TestCaseSource(nameof(CompositeCases))]
    public void WritesFromStorage_DecodedCompositeWithAChildOfOtherParameters_IsFalse(string source, string target, Array values)
    {
        using IColumn decoded = Decode(source, values);

        Assert.Multiple(() =>
        {
            Assert.That(Codec(source).WritesFromStorage(decoded), Is.True, "the codec of the type that the column was read as");
            Assert.That(Codec(target).WritesFromStorage(decoded), Is.False);
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
            Assert.That(Codec("Decimal(18, 4)").WritesFromStorage(decimals), Is.True);
            Assert.That(ConverterHarness.ReadAs<decimal>(decimalsRead, 0, 2), Is.EqualTo(new[] { 1.23m, -4.5m }));
            Assert.That(Codec("DateTime('Asia/Tokyo')").WritesFromStorage(instants), Is.True);
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
    /// No CLR type holds the counts of a scale above 7, and a Variant is written into a Variant only when the target has
    /// each of its alternatives, so the insert refuses these columns and says why.
    /// </summary>
    [TestCase(
        "DateTime64(9, 'UTC')",
        "DateTime64(6, 'UTC')",
        "A value of DateTime64(9, 'UTC') is a count at scale 9, finer than the 100 ns ticks of the System.DateTimeOffset that converts it, so a column read as DateTime64(9, 'UTC') is written only into a column of the same scale.")]
    [TestCase(
        "Variant(DateTime64(3, 'UTC'), String)",
        "Variant(DateTime64(6, 'UTC'), String)",
        "A column read as Variant(DateTime64(3, 'UTC'), String) is written into Variant(DateTime64(6, 'UTC'), String) only when each of its alternatives is an alternative of Variant(DateTime64(6, 'UTC'), String), and Variant(DateTime64(6, 'UTC'), String) has no alternative DateTime64(3, 'UTC'). A CAST of the column to Variant(DateTime64(6, 'UTC'), String) in the query converts those values.")]
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
    public Task Write_DecodedCompositeOfAnotherMeaning_WritesTheOtherPartsFromTheirStorage(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1);

    /// <summary>
    /// A <c>String</c> that a query read, into another type of a string (<c>LowCardinality</c>, <c>Nullable</c>,
    /// <c>FixedString</c>, also in <c>Array</c>, <c>Map</c> and <c>Tuple</c>), is written from its bytes, so bytes that
    /// are not UTF-8 keep their value. A <c>LowCardinality</c>, <c>Nullable</c> or <c>FixedString</c> column that a query
    /// read, into another type of a string, is written from its bytes too. All rows, and the rows from row 1.
    /// </summary>
    [TestCaseSource(nameof(StringShapeCases))]
    public Task Write_DecodedStringIntoAnotherTypeOfAString_WritesItsBytes(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1);

    /// <summary>
    /// A value that an enclosing NULL hides is not read, converted or checked: each part of another type writes the
    /// placeholder of its target there, and a part of the same type keeps its stored bytes. The hidden values here are a
    /// <c>Decimal</c> out of the range of the target, an enum ordinal that the target does not declare and one that the
    /// source does not declare, a <c>DateTime64</c> finer than the target scale, and strings of another width than the
    /// target <c>FixedString</c>. Also a <c>Nullable</c> of a leaf, and the elements of an <c>Array</c> and a <c>Map</c>
    /// under a NULL row and in an <c>Array(Nullable(Tuple))</c>. All rows, and the rows from row 1; in the last cases the
    /// hidden row is row 1, so the rows from row 1 start with it.
    /// </summary>
    [TestCaseSource(nameof(HiddenValueCases))]
    public Task Write_DecodedColumnWithAHiddenValue_WritesThePlaceholderOfTheTarget(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1, storageOfTheTarget: null);

    /// <summary>
    /// A column that a query read, with a <c>String</c> part next to a part of another type that keeps the meaning of its
    /// values (an <c>Enum8</c> into an <c>Int8</c>, an <c>Int32</c> into a <c>Nullable(Int32)</c>), also in a <c>Map</c>,
    /// an <c>Array</c> and a nested <c>Tuple</c>, and a <c>Tuple</c> into a <c>Nullable(Tuple)</c>: the <c>String</c> part
    /// keeps its bytes, also bytes that are not UTF-8. All rows, and the rows from row 1.
    /// </summary>
    [TestCaseSource(nameof(SiblingCases))]
    public Task Write_DecodedCompositeWithAPartOfAnotherType_WritesTheStringBytes(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1);

    /// <summary>
    /// A <c>Nullable(Tuple)</c> with a NULL row and one with no NULL row, with the same values in rows 1 and 2: the insert
    /// writes the same bytes for those rows, and the <c>String</c> part keeps its bytes in both.
    /// </summary>
    [Test]
    public async Task Write_DecodedNullableTupleWithAndWithoutANullRow_WritesTheSameBytesForTheSameRows()
    {
        const string source = "Nullable(Tuple(String, Enum8('a' = 1, 'b' = 2)))";
        const string target = "Nullable(Tuple(String, Int8))";
        const string values = "01FF0002C328" + "010201";
        using IColumn withNull = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString("010000" + values), 3);
        using IColumn withoutNull = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString("000000" + values), 3);

        byte[] fromWithNull = await WriteAsync(withNull, target, 1, 2, prefix: true);
        byte[] fromWithoutNull = await WriteAsync(withoutNull, target, 1, 2, prefix: true);

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(fromWithoutNull), Is.EqualTo(Convert.ToHexString(fromWithNull)));
            Assert.That(Convert.ToHexString(fromWithNull), Is.EqualTo("0000" + "0002C328" + "0201"));
        });
    }

    /// <summary>
    /// A <c>String</c> that a query read, into a <c>Variant</c> whose <c>String</c> or <c>LowCardinality(String)</c>
    /// alternative takes both the text and the bytes of a value: through its bytes, so bytes that are not UTF-8 keep their
    /// value, a NULL is the NULL of the <c>Variant</c>, and a value that an enclosing NULL hides is not read. All rows, and
    /// the rows from row 1.
    /// </summary>
    [TestCaseSource(nameof(VariantCases))]
    public Task Write_DecodedStringIntoAVariant_WritesItsBytesOnlyIntoAStringAlternativeForBoth(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1, storageOfTheTarget: null);

    /// <summary>
    /// A <c>String</c> that a query read, into a part that only the converter tree of its CLR type writes (an
    /// <c>Array(UInt8)</c> alternative for the bytes, a <c>FixedString(N)</c> alternative for the text, a
    /// <c>Dynamic</c>): the tree would read the String as text, so the insert refuses the column with the reason.
    /// </summary>
    [TestCase("String", "Variant(Array(UInt8), String)", "02616200" + "0178")]
    [TestCase("String", "Variant(FixedString(2), Int64)", "026162" + "026364" + "026566")]
    [TestCase("String", "Dynamic", "026162" + "026364" + "026566")]
    [TestCase("Tuple(String, Int32)", "Tuple(Dynamic, Int32)", "026162" + "026364" + "026566" + "010000000200000003000000")]
    public async Task For_DecodedStringIntoAPartThatTakesItsText_IsRefusedWithTheReason(string source, string target, string sourceBytes)
    {
        using IColumn decoded = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString(sourceBytes), 3);
        string part = source.StartsWith("Tuple", StringComparison.Ordinal) ? "String" : source;
        string partTarget = target.StartsWith("Tuple", StringComparison.Ordinal) ? "Dynamic" : target;

        InsertColumnWrite write = InsertColumnWrite.For(Codec(target), decoded, target, Utc, ConverterDerivation.Default, out string refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null);
            Assert.That(refusal, Is.EqualTo(
                $"A part read as {part} is written into {partTarget} only through the text of its String values, but the insert writes a String that a query read only with its bytes. " +
                $"Convert the values with a CAST to {partTarget} in the query, or insert a column of string values that you build from them with ClickHouseTcpColumn.Create."));
        });
    }

    /// <summary>
    /// A <c>Nullable</c> part that a query read, into a <c>Variant</c> or a <c>Dynamic</c>, which holds NULL itself: a NULL
    /// row is the NULL of the target, and the value under it is not written. All rows, and the rows from row 1.
    /// </summary>
    [TestCaseSource(nameof(NullOfTargetCases))]
    public Task Write_DecodedNullableIntoATargetThatHoldsNull_WritesTheNullOfTheTarget(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1);

    /// <summary>
    /// A <c>Variant</c> that a query read, into a <c>Variant</c> that has each of its alternatives and others: the
    /// discriminators go through the map of the alternatives, the NULL marker stays, each alternative is written from its
    /// storage, and an alternative that only the target has writes its prefix and no row. All rows, and the rows from row 1.
    /// </summary>
    [TestCaseSource(nameof(VariantIntoVariantCases))]
    public Task Write_DecodedVariantIntoAVariantWithItsAlternatives_WritesEachAlternativeFromItsStorage(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1)
        => AssertWritesAsync(source, target, rows, sourceBytes, expected, expectedFromRow1);

    /// <summary>
    /// A <c>FixedString</c> takes the bytes of a <c>String</c> that a query read only when each value has its width, as
    /// for any <c>byte[]</c>. A column of <c>string</c> values that a caller built goes by the text rules, which pad a
    /// shorter value with zero bytes.
    /// </summary>
    [Test]
    public async Task Write_StringIntoAWiderFixedString_RefusesReadBytesAndPadsTheTextOfACaller()
    {
        using IColumn decoded = await ConverterHarness.ReadBackAsync("String", Convert.FromHexString("026162"), 1);
        using var text = new ArrayColumn<string>("c", "String", new[] { "ab" });

        Exception refused = ConverterHarness.Catch(() => WriteAsync(decoded, "FixedString(3)", 0, 1).GetAwaiter().GetResult());
        byte[] padded = await WriteAsync(text, "FixedString(3)", 0, 1);

        Assert.Multiple(() =>
        {
            Assert.That(refused?.Message, Does.StartWith("A FixedString(3) value at row 0 is 2 bytes; every value must be exactly 3 bytes."));
            Assert.That(Convert.ToHexString(padded), Is.EqualTo("616200"));
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

    public static IEnumerable<TestCaseData> HiddenValueCases()
    {
        yield return Bytes(
            "Nullable(Decimal(18, 2))",
            "Nullable(Decimal(9, 2))",
            3,
            "010000141A99BE1C0000007B000000000000003EFEFFFFFFFFFFFF",
            "010000000000007B0000003EFEFFFF",
            "00007B0000003EFEFFFF");
        yield return Bytes(
            "Nullable(Tuple(DateTime64(3, 'UTC'), Decimal(18, 2)))",
            "Nullable(Tuple(DateTime64(6, 'UTC'), Decimal(9, 2)))",
            3,
            "010000010000000000000002000000000000000300000000000000141A99BE1C0000007B000000000000003EFEFFFFFFFFFFFF",
            "0100000000000000000000D007000000000000B80B000000000000000000007B0000003EFEFFFF",
            "0000D007000000000000B80B0000000000007B0000003EFEFFFF");
        yield return Bytes(
            "Nullable(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3), String))",
            "Nullable(Tuple(Enum8('a' = 1, 'b' = 2), String))",
            3,
            "01000003010201FF0002C328",
            "01000001010201FF0002C328",
            "000001020002C328");
        yield return Bytes(
            "Nullable(Tuple(Enum8('a' = 1, 'b' = 2), String))",
            "Nullable(Tuple(Enum8('b' = 1, 'a' = 2), String))",
            3,
            "01000007010201FF0002C328",
            "01000001020101FF0002C328",
            "000002010002C328");
        yield return Bytes(
            "Nullable(Tuple(DateTime64(6, 'UTC'), String))",
            "Nullable(Tuple(DateTime64(3, 'UTC'), String))",
            3,
            "01000041420F0000000000D007000000000000B80B00000000000001FF0002C328",
            "01000000000000000000000200000000000000030000000000000001FF0002C328",
            "0000020000000000000003000000000000000002C328");
        yield return Bytes(
            "Nullable(Tuple(String, DateTime64(3, 'UTC')))",
            "Nullable(Tuple(FixedString(2), DateTime64(6, 'UTC')))",
            3,
            "0100000361626302C328026162010000000000000002000000000000000300000000000000",
            "0100000000C32861620000000000000000D007000000000000B80B000000000000",
            "0000C3286162D007000000000000B80B000000000000");
        yield return Bytes(
            "Nullable(Tuple(LowCardinality(Nullable(String)), DateTime64(3, 'UTC')))",
            "Nullable(Tuple(LowCardinality(Nullable(FixedString(2))), DateTime64(6, 'UTC')))",
            3,
            "01000000000000000100000006000000000000050000000000000000000361626302C3280261620300000000000000020304010000000000000002000000000000000300000000000000",
            "01000000000000000100000006000000000000040000000000000000000000C328616203000000000000000002030000000000000000D007000000000000B80B000000000000",
            "010000000000000000000006000000000000040000000000000000000000C328616202000000000000000203D007000000000000B80B000000000000");
        yield return Bytes(
            "Nullable(Tuple(Array(Decimal(18, 2)), DateTime64(3, 'UTC')))",
            "Nullable(Tuple(Array(Decimal(9, 2)), DateTime64(6, 'UTC')))",
            3,
            "010000010000000000000001000000000000000300000000000000141A99BE1C0000007B000000000000003EFEFFFFFFFFFFFF010000000000000002000000000000000300000000000000",
            "010000010000000000000001000000000000000300000000000000000000007B0000003EFEFFFF0000000000000000D007000000000000B80B000000000000",
            "0000000000000000000002000000000000007B0000003EFEFFFFD007000000000000B80B000000000000");
        yield return Bytes(
            "Nullable(Tuple(Map(String, Decimal(18, 2)), DateTime64(3, 'UTC')))",
            "Nullable(Tuple(Map(String, Decimal(9, 2)), DateTime64(6, 'UTC')))",
            3,
            "01000001000000000000000100000000000000030000000000000001FF016B02C328141A99BE1C0000007B000000000000003EFEFFFFFFFFFFFF010000000000000002000000000000000300000000000000",
            "01000001000000000000000100000000000000030000000000000001FF016B02C328000000007B0000003EFEFFFF0000000000000000D007000000000000B80B000000000000",
            "000000000000000000000200000000000000016B02C3287B0000003EFEFFFFD007000000000000B80B000000000000");
        yield return Bytes(
            "Array(Nullable(Tuple(DateTime64(3, 'UTC'), Decimal(18, 2))))",
            "Array(Nullable(Tuple(DateTime64(6, 'UTC'), Decimal(9, 2))))",
            3,
            "010000000000000001000000000000000300000000000000010000010000000000000002000000000000000300000000000000141A99BE1C0000007B000000000000003EFEFFFFFFFFFFFF",
            "0100000000000000010000000000000003000000000000000100000000000000000000D007000000000000B80B000000000000000000007B0000003EFEFFFF",
            "000000000000000002000000000000000000D007000000000000B80B0000000000007B0000003EFEFFFF");

        // The hidden row is row 1, so the write of the rows from row 1 starts at a hidden row too.
        yield return Bytes(
            "Nullable(Decimal(18, 2))",
            "Nullable(Decimal(9, 2))",
            3,
            "0001007B00000000000000141A99BE1C0000003EFEFFFFFFFFFFFF",
            "0001007B000000000000003EFEFFFF",
            "0100000000003EFEFFFF");
        yield return Bytes(
            "Nullable(Tuple(Array(Decimal(18, 2)), DateTime64(3, 'UTC')))",
            "Nullable(Tuple(Array(Decimal(9, 2)), DateTime64(6, 'UTC')))",
            3,
            "0001000100000000000000020000000000000003000000000000007B00000000000000141A99BE1C0000003EFEFFFFFFFFFFFF010000000000000002000000000000000300000000000000",
            "0001000100000000000000020000000000000003000000000000007B000000000000003EFEFFFFE8030000000000000000000000000000B80B000000000000",
            "010001000000000000000200000000000000000000003EFEFFFF0000000000000000B80B000000000000");
        yield return Bytes(
            "Nullable(Tuple(Map(String, Decimal(18, 2)), DateTime64(3, 'UTC')))",
            "Nullable(Tuple(Map(String, Decimal(9, 2)), DateTime64(6, 'UTC')))",
            3,
            "0001000100000000000000020000000000000003000000000000000161016B01637B00000000000000141A99BE1C0000003EFEFFFFFFFFFFFF010000000000000002000000000000000300000000000000",
            "0001000100000000000000020000000000000003000000000000000161016B01637B000000000000003EFEFFFFE8030000000000000000000000000000B80B000000000000",
            "010001000000000000000200000000000000016B0163000000003EFEFFFF0000000000000000B80B000000000000");
    }

    public static IEnumerable<TestCaseData> SiblingCases()
    {
        yield return Bytes(
            "Tuple(String, Enum8('a' = 1, 'b' = 2))",
            "Tuple(String, Int8)",
            3,
            "01FF0002C328" + "010201",
            "01FF0002C328" + "010201",
            "0002C328" + "0201");
        yield return Bytes(
            "Tuple(String, Int32)",
            "Tuple(String, Nullable(Int32))",
            3,
            "01FF0002C328" + "07000000FFFFFFFF05000000",
            "01FF0002C328" + "000000" + "07000000FFFFFFFF05000000",
            "0002C328" + "0000" + "FFFFFFFF05000000");
        yield return Bytes(
            "Map(String, Enum8('a' = 1, 'b' = 2))",
            "Map(String, Int8)",
            3,
            "010000000000000001000000000000000300000000000000" + "01FF0002C328" + "010201",
            "010000000000000001000000000000000300000000000000" + "01FF0002C328" + "010201",
            "00000000000000000200000000000000" + "0002C328" + "0201");
        yield return Bytes(
            "Array(Tuple(String, Enum8('a' = 1, 'b' = 2)))",
            "Array(Tuple(String, Int8))",
            3,
            "010000000000000001000000000000000300000000000000" + "01FF0002C328" + "010201",
            "010000000000000001000000000000000300000000000000" + "01FF0002C328" + "010201",
            "00000000000000000200000000000000" + "0002C328" + "0201");
        yield return Bytes(
            "Tuple(LowCardinality(String), Tuple(String, Enum8('a' = 1, 'b' = 2)))",
            "Tuple(String, Tuple(String, Int8))",
            3,
            "0100000000000000" + "000600000000000003000000000000000001FF02C3280300000000000000010002" + "0180" + "00" + "01C0" + "010201",
            "01FF0002C328" + "0180" + "00" + "01C0" + "010201",
            "0002C328" + "0001C0" + "0201");
        yield return Bytes(
            "Tuple(String)",
            "Nullable(Tuple(String))",
            3,
            "01FF0002C328",
            "000000" + "01FF0002C328",
            "0000" + "0002C328");
    }

    public static IEnumerable<TestCaseData> VariantCases()
    {
        yield return Bytes("String", "Variant(String, Int64)", 3, "01FF0002C328", "0000000000000000" + "000000" + "01FF0002C328", "0000000000000000" + "0000" + "0002C328");
        yield return Bytes("Nullable(String)", "Variant(String, Int64)", 3, "000100" + "01FF0141" + "02C328", "0000000000000000" + "00FF00" + "01FF02C328", "0000000000000000" + "FF00" + "02C328");
        yield return Bytes(
            "Nullable(Tuple(String))",
            "Nullable(Tuple(Variant(String, Int64)))",
            3,
            "010000" + "0141" + "01FF" + "02C328",
            "0000000000000000" + "010000" + "FF0000" + "01FF02C328",
            "0000000000000000" + "0000" + "0000" + "01FF02C328");
        yield return Bytes(
            "String",
            "Variant(LowCardinality(String), UInt64)",
            3,
            "01FF0002C328",
            "0000000000000000" + "0100000000000000" + "000000" + "000600000000000003000000000000000001FF02C3280300000000000000010002",
            "0000000000000000" + "0100000000000000" + "0000" + "0006000000000000020000000000000000" + "02C328" + "02000000000000000001");
    }

    // Four rows of Variant(Int32, String): the String 41 FF 42, the Int32 7, NULL, and the String C3 28.
    private const string VariantRows = "0000000000000000" + "0100FF01" + "07000000" + "0341FF42" + "02C328";

    public static IEnumerable<TestCaseData> VariantIntoVariantCases()
    {
        yield return Bytes(
            "Variant(Int32, String)",
            "Variant(Float64, Int32, String)",
            4,
            VariantRows,
            "0000000000000000" + "0201FF02" + "07000000" + "0341FF42" + "02C328",
            "0000000000000000" + "01FF02" + "07000000" + "02C328");
        yield return Bytes(
            "Variant(Int32, String)",
            "Variant(Int32, String, UInt64)",
            4,
            VariantRows,
            "0000000000000000" + "0100FF01" + "07000000" + "0341FF42" + "02C328",
            "0000000000000000" + "00FF01" + "07000000" + "02C328");
        yield return Bytes(
            "Variant(Int32, String)",
            "Variant(Int32, LowCardinality(String), String)",
            4,
            VariantRows,
            "0000000000000000" + "0100000000000000" + "0200FF02" + "07000000" + "0341FF42" + "02C328",
            "0000000000000000" + "0100000000000000" + "00FF02" + "07000000" + "02C328");
        yield return Bytes(
            "Array(Variant(Int32, String))",
            "Array(Variant(Float64, Int32, String))",
            3,
            "0000000000000000" + "020000000000000002000000000000000300000000000000" + "010001" + "07000000" + "0341FF42" + "02C328",
            "0000000000000000" + "020000000000000002000000000000000300000000000000" + "020102" + "07000000" + "0341FF42" + "02C328",
            "0000000000000000" + "00000000000000000100000000000000" + "02" + "02C328");
    }

    public static IEnumerable<TestCaseData> NullOfTargetCases()
    {
        // Rows 7, NULL (over the stored value 42) and -1.
        const string Ints = "000100" + "070000002A000000FFFFFFFF";
        yield return Bytes("Nullable(Int32)", "Variant(Int32, String)", 3, Ints, "0000000000000000" + "00FF00" + "07000000FFFFFFFF", "0000000000000000" + "FF00" + "FFFFFFFF");
        // A flattened Dynamic: the version, the one type Int32, then an index for each row (1, the number of types, for NULL).
        yield return Bytes("Nullable(Int32)", "Dynamic", 3, Ints, "0300000000000000" + "0105496E743332" + "000100" + "07000000FFFFFFFF", "0300000000000000" + "0105496E743332" + "0100" + "FFFFFFFF");
        yield return Bytes("Tuple(Nullable(Int32), Int8)", "Tuple(Variant(Int32, String), Int8)", 3, Ints + "010203", "0000000000000000" + "00FF00" + "07000000FFFFFFFF" + "010203", "0000000000000000" + "FF00" + "FFFFFFFF" + "0203");
    }

    public static IEnumerable<TestCaseData> StringShapeCases()
    {
        yield return Bytes(
            "String",
            "LowCardinality(String)",
            3,
            "01FF0002C328",
            "0100000000000000000600000000000003000000000000000001FF02C3280300000000000000010002",
            "0100000000000000000600000000000002000000000000000002C32802000000000000000001");
        yield return Bytes(
            "String",
            "LowCardinality(Nullable(String))",
            3,
            "01FF0002C328",
            "010000000000000000060000000000000400000000000000000001FF02C3280300000000000000020103",
            "010000000000000000060000000000000300000000000000000002C32802000000000000000102");
        yield return Bytes(
            "String",
            "Nullable(String)",
            3,
            "01FF0002C328",
            "00000001FF0002C328",
            "00000002C328");
        yield return Bytes(
            "String",
            "FixedString(2)",
            3,
            "02FF0002C328026162",
            "FF00C3286162",
            "C3286162");
        yield return Bytes(
            "String",
            "Nullable(FixedString(2))",
            3,
            "02FF0002C328026162",
            "000000FF00C3286162",
            "0000C3286162");
        yield return Bytes(
            "String",
            "LowCardinality(FixedString(2))",
            3,
            "02FF0002C328026162",
            "0100000000000000000600000000000004000000000000000000FF00C32861620300000000000000010203",
            "0100000000000000000600000000000003000000000000000000C328616202000000000000000102");
        yield return Bytes(
            "Nullable(String)",
            "LowCardinality(Nullable(String))",
            3,
            "00010001FF0002C328",
            "010000000000000000060000000000000400000000000000000001FF02C3280300000000000000020003",
            "010000000000000000060000000000000300000000000000000002C32802000000000000000002");
        yield return Bytes(
            "LowCardinality(String)",
            "String",
            3,
            "0100000000000000000600000000000003000000000000000001FF02C3280300000000000000010002",
            "01FF0002C328",
            "0002C328");
        yield return Bytes(
            "LowCardinality(Nullable(String))",
            "Nullable(String)",
            3,
            "010000000000000000060000000000000400000000000000000001FF02C3280300000000000000020003",
            "00010001FF0002C328",
            "01000002C328");
        yield return Bytes(
            "FixedString(2)",
            "String",
            3,
            "FF00C3286162",
            "02FF0002C328026162",
            "02C328026162");
        yield return Bytes(
            "Array(String)",
            "Array(LowCardinality(String))",
            3,
            "01000000000000000100000000000000030000000000000001FF0002C328",
            "0100000000000000010000000000000001000000000000000300000000000000000600000000000003000000000000000001FF02C3280300000000000000010002",
            "010000000000000000000000000000000200000000000000000600000000000002000000000000000002C32802000000000000000001");
        yield return Bytes(
            "Map(String, String)",
            "Map(LowCardinality(String), Nullable(String))",
            3,
            "01000000000000000100000000000000030000000000000001FF0002C32802C32801FF00",
            "0100000000000000010000000000000001000000000000000300000000000000000600000000000003000000000000000001FF02C328030000000000000001000200000002C32801FF00",
            "010000000000000000000000000000000200000000000000000600000000000002000000000000000002C32802000000000000000001000001FF00");
        yield return Bytes(
            "Tuple(String, Int32)",
            "Tuple(FixedString(2), Int32)",
            3,
            "02FF0002C32802616207000000FFFFFFFF05000000",
            "FF00C328616207000000FFFFFFFF05000000",
            "C3286162FFFFFFFF05000000");
        yield return Bytes(
            "Array(Nullable(String))",
            "Array(LowCardinality(Nullable(String)))",
            3,
            "01000000000000000100000000000000030000000000000000010001FF0002C328",
            "010000000000000001000000000000000100000000000000030000000000000000060000000000000400000000000000000001FF02C3280300000000000000020003",
            "01000000000000000000000000000000020000000000000000060000000000000300000000000000000002C32802000000000000000002");
        yield return Bytes(
            "Tuple(DateTime64(3, 'UTC'), String)",
            "Tuple(DateTime64(6, 'UTC'), LowCardinality(String))",
            3,
            "01000000000000000200000000000000030000000000000001FF0002C328",
            "0100000000000000E803000000000000D007000000000000B80B000000000000000600000000000003000000000000000001FF02C3280300000000000000010002",
            "0100000000000000D007000000000000B80B000000000000000600000000000002000000000000000002C32802000000000000000001");
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
        Assert.That(Codec(target).WritesFromStorage(decoded), Is.False, "the codec of the target does not write the column from its storage");

        foreach (int start in new[] { 0, 1 })
        {
            int length = values.Length - start;
            byte[] bytes = await WriteAsync(decoded, target, start, length, prefix: true);
            using IColumn read = await ConverterHarness.ReadBackAsync(target, bytes, length);
            Assert.That(ConverterHarness.ReadAs<T>(read, 0, length), Is.EqualTo(values[start..]), $"{source} into {target}, rows [{start}, {values.Length})");
        }
    }

    // A column decoded from the source bytes, written into the target for all rows and from row 1.
    // storageOfTheTarget: whether the codec of the target writes the column from its storage, or null to leave that open.
    private static async Task AssertWritesAsync(
        string source,
        string target,
        int rows,
        string sourceBytes,
        string expected,
        string expectedFromRow1,
        bool? storageOfTheTarget = false)
    {
        using IColumn decoded = await ConverterHarness.ReadBackAsync(source, Convert.FromHexString(sourceBytes), rows);

        byte[] all = await WriteAsync(decoded, target, 0, rows, prefix: true);
        byte[] fromRow1 = await WriteAsync(decoded, target, 1, rows - 1, prefix: true);

        Assert.Multiple(() =>
        {
            if (storageOfTheTarget is bool storage)
            {
                Assert.That(Codec(target).WritesFromStorage(decoded), Is.EqualTo(storage), "whether the codec of the target writes the column from its storage");
            }

            Assert.That(Convert.ToHexString(all), Is.EqualTo(expected), "all rows");
            Assert.That(Convert.ToHexString(fromRow1), Is.EqualTo(expectedFromRow1), "rows from row 1");
        });
    }

    // The bytes that an insert writes for rows [start, start + length) of the column into a column of the type.
    private static Task<byte[]> WriteAsync(IColumn column, string type, int start, int length, bool prefix = false)
        => CodecTestHarness.WriteAsync(w => CodecTestHarness.WriteRows(w, Codec(type), column, type, start, length, Utc, prefix));
}
