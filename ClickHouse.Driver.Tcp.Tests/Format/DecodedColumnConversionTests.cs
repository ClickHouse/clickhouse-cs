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
