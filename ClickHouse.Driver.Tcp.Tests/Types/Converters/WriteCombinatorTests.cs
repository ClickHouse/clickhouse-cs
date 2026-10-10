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
/// The write combinators, for what the differential tests (<see cref="WriteConverterRegistration"/>) do not reach: the
/// column types that no differential case has, each written for a whole column and for a slice that starts after
/// earlier values and read back through the codec of the type; refused values (type, message and parameter name); the
/// state prefixes of zero rows; the bytes of LowCardinality dictionaries.
/// </summary>
[TestFixture]
public class WriteCombinatorTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    // The error of each case of RefusedValues, in order: the exception type, the parameter name and the message. The
    // messages format values with the invariant culture.
    private static readonly (string Exception, string Parameter, string Message)[] RefusedErrors =
    {
        ("ArgumentException", "column", "Array column 'c' has a null value at row 2; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements. (Parameter 'column')"), // 0 Array(Int32)
        ("ArgumentException", "column", "Array column 'c' has a null value at row 1; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements. (Parameter 'column')"), // 1 Array(Array(Int32))
        ("ArgumentException", "column", "Map column 'c' has a null value at row 2; Map(K, V) rows are non-nullable. Use Array.Empty<KeyValuePair<K, V>>() for an empty row, or Map(K, Nullable(V)) to carry null values. (Parameter 'column')"), // 2 Map(String, Int32)
        ("ArgumentException", "column", "Map column 'c' has a null value at row 1; Map(K, V) rows are non-nullable. Use Array.Empty<KeyValuePair<K, V>>() for an empty row, or Map(K, Nullable(V)) to carry null values. (Parameter 'column')"), // 3 Array(Map(String, Int32))
        ("ArgumentException", "column", "Array column 'c' has a null value at row 2; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements. (Parameter 'column')"), // 4 Tuple(Int32, Array(String))
        ("ArgumentException", "column", "Array column 'c' has a null value at row 0; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements. (Parameter 'column')"), // 5 Map(String, Array(Int32))
        ("ArgumentNullException", "value", "Value cannot be null. (Parameter 'value')"), // 6 Map(String, Int32)
        ("ArgumentException", "value", "A FixedString(2) value at row 2 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 7 Nullable(FixedString(2))
        ("ArgumentException", "value", "A FixedString(2) value at element 1 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 8 Array(FixedString(2))
        ("ArgumentException", "value", "A FixedString(2) value at row 1 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 9 Array(Nullable(FixedString(2)))
        ("ArgumentException", "value", "A FixedString(2) value at row 2 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 10 Tuple(Int32, FixedString(2))
        ("OverflowException", null, "Value at index 2 exceeds the declared precision 3 of decimal type 'Decimal(3, 2)'."), // 11 Nullable(Decimal(3, 2))
        ("OverflowException", null, "Value at index 1 exceeds the declared precision 3 of decimal type 'Decimal(3, 2)'."), // 12 Array(Decimal(3, 2))
        ("ArgumentException", null, "Variant 'Variant(String, UInt64)' has no alternative for a value of CLR type 'System.Double'. Supported CLR types: System.String, System.UInt64."), // 13 Variant(String, UInt64)
        ("ArgumentException", null, "Variant 'Variant(IPv4, IPv6)' has no alternative for a value of CLR type 'System.String'. Supported CLR types: System.Net.IPAddress."), // 14 Variant(IPv4, IPv6)
        ("ArgumentException", null, "Variant 'Variant(Ring, LineString)' cannot place a value of CLR type 'System.ValueTuple`2[System.Double,System.Double][]': the alternatives 'Ring', 'LineString' all surface that type, and the value does not say which of them is meant."), // 15 Variant(Ring, LineString)
        ("NotSupportedException", null, "No ClickHouse type is inferred for a Dynamic value of CLR type 'System.Object'. Supported: the fixed-width scalars, String, UUID, Date, IP addresses, decimals, date-times, and arrays/maps/tuples of them."), // 16 Dynamic
        ("ArgumentException", "label", "'b' is not a label of 'Enum8('a' = 1)'. Its labels are: 'a'. (Parameter 'label')"), // 17 Nullable(Enum8('a' = 1))
        ("ArgumentNullException", "value", "Value cannot be null. (Parameter 'value')"), // 18 Array(String)
        ("ArgumentException", "value", "An IPv4 column requires IPv4 addresses; got '::1'. (Parameter 'value')"), // 19 Nullable(IPv4)
        ("ArgumentException", "column", "Array column 'c' has a null value at row 2; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements. (Parameter 'column')"), // 20 Nullable(Tuple(Int32, Array(String)))
        ("ArgumentException", "value", "A FixedString(2) value at row 2 is 1 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 21 LowCardinality(FixedString(2))
        ("ArgumentException", "value", "A FixedString(2) column cannot hold a null value (at row 2); wrap the type in Nullable to write nulls. (Parameter 'value')"), // 22 LowCardinality(FixedString(2))
        ("ArgumentException", "value", "A FixedString(2) value at row 3 is 3 bytes; every value must be exactly 2 bytes. Resize it to 2 bytes before writing it \u2014 the write path will not pad or truncate, since doing so would silently alter the data. (Parameter 'value')"), // 23 LowCardinality(Nullable(FixedString(2)))
        ("OverflowException", null, "Value at index 3 exceeds the declared precision 3 of decimal type 'Decimal(3, 2)'."), // 24 LowCardinality(Decimal(3, 2))
        ("OverflowException", null, "Value at index 2 exceeds the declared precision 3 of decimal type 'Decimal(3, 2)'."), // 25 LowCardinality(Nullable(Decimal(3, 2)))
        ("ArgumentException", "label", "'b' is not a label of 'Enum8('a' = 1)'. Its labels are: 'a'. (Parameter 'label')"), // 26 LowCardinality(Enum8('a' = 1))
        ("ArgumentOutOfRangeException", "utc", "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC). (Parameter 'utc')\nActual value was 01/01/1969 00:00:00."), // 27 LowCardinality(DateTime('UTC'))
    };

    /// <summary>
    /// Column types that no differential case has (wide and nested tuples, <c>Nullable(Tuple(...))</c>, <c>Array(Map)</c>,
    /// LowCardinality of other leaves, <c>Map(String, Variant)</c>, ...), with at least four rows, so a slice from row 2
    /// starts after earlier values and every child offset of the slice is above 0.
    /// </summary>
    public static IEnumerable<TestCaseData> Columns()
    {
        yield return Column("Nullable(FixedString(3))", new[] { new byte[] { 1, 2, 3 }, null, new byte[] { 4, 5, 6 }, null });
        yield return Column("Nullable(Enum8('a' = 1, 'b' = 2))", new[] { "a", null, "b", "a" });
        yield return Column("Array(FixedString(2))", new[] { new[] { new byte[] { 1, 2 } }, Array.Empty<byte[]>(), new[] { new byte[] { 3, 4 }, new byte[] { 5, 6 } }, new[] { new byte[] { 7, 8 } } });
        yield return Column("Array(Array(Nullable(String)))", new[] { new[] { new[] { "a", null } }, new[] { Array.Empty<string>(), new string[] { null } }, Array.Empty<string[]>(), new[] { new[] { "b" } } });
        yield return Column("Array(Map(String, Int32))", new[] { new[] { Map(("a", 1)) }, new[] { Map(), Map(("b", 2), ("c", 3)) }, Array.Empty<KeyValuePair<string, int>[]>(), new[] { Map(("d", 4)) } });
        yield return Column("LowCardinality(FixedString(2))", new[] { new byte[] { 1, 2 }, new byte[] { 0, 0 }, new byte[] { 1, 2 }, new byte[] { 3, 4 } });
        yield return Column("LowCardinality(Nullable(FixedString(2)))", new[] { new byte[] { 1, 2 }, null, new byte[] { 1, 2 }, new byte[] { 0, 0 } });
        yield return Column("LowCardinality(Int32)", new[] { 5, 0, 5, -1, 7 });
        yield return Column("LowCardinality(Nullable(Int32))", new int?[] { 5, null, 5, 0, null });
        yield return Column("LowCardinality(Float64)", new[] { 0.0, -0.0, double.NaN, 1.5, -0.0 });
        yield return Column("LowCardinality(Nullable(Enum8('a' = 1, 'b' = 2)))", new[] { "b", null, "a", "b" });
        yield return Column("LowCardinality(Nullable(IPv4))", new[] { IPAddress.Parse("1.2.3.4"), null, IPAddress.Parse("1.2.3.4"), IPAddress.Parse("0.0.0.0") });
        yield return Column("LowCardinality(Decimal(9, 2))", new[] { 1.25m, 1.250m, -2m, 0m });
        yield return Column("LowCardinality(Date)", new[] { new DateOnly(2024, 1, 1), new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 1), new DateOnly(2000, 2, 29) });
        yield return Column("Map(String, Nullable(String))", new[] { Map(("a", "x"), ("b", (string)null)), Map<string, string>(), Map(("c", (string)null)), Map(("d", "y")) });
        yield return Column("Map(LowCardinality(String), Array(Int32))", new[] { Map(("a", new[] { 1 })), Map(("a", Array.Empty<int>()), ("b", new[] { 2, 3 })), Map<string, int[]>(), Map(("c", new[] { 4 })) });
        yield return Column("Tuple(UInt8, String, Float64)", new[] { ((byte)1, "a", 1.5), ((byte)2, "b", -0.0), ((byte)3, "c", 2.0), ((byte)4, "d", 3.0) });
        yield return Column("Tuple(UInt8, Int8, String, Nullable(Int32))", new[] { ((byte)1, (sbyte)-1, "a", (int?)1), ((byte)2, (sbyte)2, "b", (int?)null), ((byte)3, (sbyte)3, "c", (int?)3), ((byte)4, (sbyte)4, "d", (int?)null) });
        yield return Column("Tuple(UInt8, Int8, UInt16, Int16, String)", Enumerable.Range(1, 4).Select(i => ((byte)i, (sbyte)-i, (ushort)i, (short)-i, i.ToString(System.Globalization.CultureInfo.InvariantCulture))).ToArray());
        yield return Column("Tuple(UInt8, Int8, UInt16, Int16, UInt32, Array(String))", Enumerable.Range(1, 4).Select(i => ((byte)i, (sbyte)-i, (ushort)i, (short)-i, (uint)i, new[] { "x", "y" }.Take(i % 3).ToArray())).ToArray());
        yield return Column("Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, LowCardinality(String))", Enumerable.Range(1, 4).Select(i => ((byte)i, (sbyte)-i, (ushort)i, (short)-i, (uint)i, -i, i % 2 == 0 ? "a" : "b")).ToArray());
        yield return Column("Tuple(a Int32, b Array(String))", new[] { (1, new[] { "a" }), (2, Array.Empty<string>()), (3, new[] { "b", "c" }), (4, new[] { "d" }) });
        yield return Column("Nullable(Tuple(Int32, String))", new (int, string)?[] { (1, "a"), null, (3, "c"), null, (5, string.Empty) });
        yield return Column(
            "Nullable(Tuple(Int32, LowCardinality(String), Array(Int32), Nullable(String), Map(String, Int32), FixedString(2)))",
            new (int, string, int[], string, KeyValuePair<string, int>[], byte[])?[]
            {
                (1, "a", new[] { 1 }, "x", Map(("k", 1)), new byte[] { 1, 2 }),
                null,
                (3, "b", Array.Empty<int>(), null, Map<string, int>(), new byte[] { 3, 4 }),
                null,
                (5, "a", new[] { 5, 6 }, "y", Map(("l", 2)), new byte[] { 5, 6 }),
            });
        yield return Column("Nullable(Tuple(Int32, LowCardinality(Nullable(String))))", new (int, string)?[] { (1, "a"), null, (3, null), (4, "a") });
        yield return Column("Variant(Array(String), LowCardinality(String), UInt8)", new object[] { new[] { "a" }, "b", (byte)1, null, "b", Array.Empty<string>() });
        yield return Column("Map(String, Variant(String, UInt64))", new[] { Map(("a", (object)"x")), Map<string, object>(), Map(("b", (object)1UL), ("c", (object)null)), Map(("d", (object)"y")) });
        yield return Column("Array(QBit(Float64, 2))", new[] { new[] { new[] { 1.0, 2.0 } }, Array.Empty<double[]>(), new[] { new[] { 3.0, 4.0 }, new[] { 5.0, 6.0 } }, new[] { new[] { 7.0, 8.0 } } });
        yield return Column("SimpleAggregateFunction(anyLast, Array(Nullable(Int32)))", new[] { new int?[] { 1, null }, Array.Empty<int?>(), new int?[] { 3 }, new int?[] { null } });
    }

    /// <summary>
    /// A whole column and a slice from row 2 give bytes that the codec of the type reads back as the values of the rows,
    /// with no bytes left over.
    /// </summary>
    [TestCaseSource(nameof(Columns))]
    public Task Write_Column_GivesBytesThatReadBackAsTheValues(string type, Array values)
        => (Task)ConverterHarness.InvokeGeneric(typeof(WriteCombinatorTests), nameof(AssertWritesTheValuesAsync), new[] { values.GetType().GetElementType() }, type, values);

    /// <summary>Zero values: the state prefix of the type (empty when the type has none), and no body.</summary>
    [TestCase("Array(Int32)", typeof(int[]), "")]
    [TestCase("QBit(Float32, 4)", typeof(float[]), "")]
    [TestCase("LowCardinality(String)", typeof(string), "0100000000000000")]
    [TestCase("LowCardinality(Nullable(Int32))", typeof(int?), "0100000000000000")]
    [TestCase("Array(LowCardinality(String))", typeof(string[]), "0100000000000000")]
    [TestCase("Nullable(Tuple(Int32, LowCardinality(Nullable(String))))", typeof((int, string)?), "0100000000000000")]
    [TestCase("Variant(String, UInt64)", typeof(object), "0000000000000000")]
    [TestCase("Variant(Array(String), LowCardinality(String), UInt8)", typeof(object), "00000000000000000100000000000000")]
    [TestCase("Map(String, Variant(String, UInt64))", typeof(KeyValuePair<string, object>[]), "0000000000000000")]
    [TestCase("Dynamic", typeof(object), "030000000000000000")]
    [TestCase("Array(Dynamic)", typeof(object[]), "030000000000000000")]
    public Task Write_ZeroValues_GivesTheStatePrefixOnly(string type, Type clrType, string expected)
        => (Task)ConverterHarness.InvokeGeneric(typeof(WriteCombinatorTests), nameof(AssertWritesZeroValuesAsync), new[] { clrType }, type, expected);

    /// <summary>
    /// A value that the type cannot store fails with the pinned error of its case (<see cref="RefusedErrors"/>): exception
    /// type, parameter name and message.
    /// </summary>
    [TestCaseSource(nameof(RefusedValuesWithErrors))]
    [SetCulture("")]
    public Task Write_ValueThatTheTypeCannotStore_FailsWithThePinnedError(string type, Array values, int start, string exception, string parameter, string message)
        => (Task)ConverterHarness.InvokeGeneric(typeof(WriteCombinatorTests), nameof(AssertFailsWithAsync), new[] { values.GetType().GetElementType() }, type, values, start, exception, parameter, message);

    public static IEnumerable<TestCaseData> RefusedValuesWithErrors() => ConverterHarness.WithErrors(RefusedValues(), RefusedErrors);

    public static IEnumerable<TestCaseData> RefusedValues()
    {
        yield return Refused("Array(Int32)", new[] { new[] { 1 }, new[] { 2 }, null, new[] { 3 } }, 1);
        yield return Refused("Array(Array(Int32))", new[] { new[] { new[] { 1 } }, new[] { new[] { 2 }, null } }, 1);
        yield return Refused("Map(String, Int32)", new[] { Map(("a", 1)), Map(), null }, 1);
        yield return Refused("Array(Map(String, Int32))", new[] { new[] { Map(("a", 1)) }, new[] { Map(), null } }, 1);
        yield return Refused("Tuple(Int32, Array(String))", new[] { (1, new[] { "a" }), (2, Array.Empty<string>()), (3, (string[])null) }, 1);
        yield return Refused("Map(String, Array(Int32))", new[] { Map(("a", new[] { 1 })), Map(("b", (int[])null)) }, 1);
        yield return Refused("Map(String, Int32)", new[] { Map(("a", 1)), Map(((string)null, 2)) }, 1);
        yield return Refused("Nullable(FixedString(2))", new[] { new byte[] { 1, 2 }, null, new byte[] { 1 } }, 1);
        yield return Refused("Array(FixedString(2))", new[] { new[] { new byte[] { 1, 2 } }, new[] { new byte[] { 1, 2 }, new byte[] { 3 } } }, 1);
        yield return Refused("Array(Nullable(FixedString(2)))", new[] { new[] { new byte[] { 1, 2 } }, new[] { null, new byte[] { 3 } } }, 1);
        yield return Refused("Tuple(Int32, FixedString(2))", new[] { (1, new byte[] { 1, 2 }), (2, new byte[] { 3, 4 }), (3, new byte[] { 5 }) }, 1);
        yield return Refused("Nullable(Decimal(3, 2))", new decimal?[] { 1m, null, 2m, 100m }, 1);
        yield return Refused("Array(Decimal(3, 2))", new[] { new[] { 1m }, new[] { 2m, 100m } }, 1);
        yield return Refused("Variant(String, UInt64)", new object[] { "a", 1UL, 1.5 }, 1);
        yield return Refused("Variant(IPv4, IPv6)", new object[] { IPAddress.Parse("1.2.3.4"), IPAddress.Parse("::1"), "x" }, 1);
        yield return Refused("Variant(Ring, LineString)", new object[] { new[] { (1.0, 2.0) } }, 0);
        yield return Refused("Dynamic", new object[] { 1, "a", new object() }, 1);
        yield return Refused("Nullable(Enum8('a' = 1))", new[] { "a", null, "b" }, 1);
        yield return Refused("Array(String)", new[] { new[] { "a" }, new[] { (string)null } }, 1);
        yield return Refused("Nullable(IPv4)", new[] { IPAddress.Parse("1.2.3.4"), null, IPAddress.Parse("::1") }, 1);
        yield return Refused("Nullable(Tuple(Int32, Array(String)))", new (int, string[])?[] { (1, new[] { "a" }), null, (3, null) }, 1);

        // A LowCardinality dictionary names a refused value by its slot in the dictionary.
        yield return Refused("LowCardinality(FixedString(2))", new[] { new byte[] { 1, 2 }, new byte[] { 1, 2 }, new byte[] { 3 } }, 1);
        yield return Refused("LowCardinality(FixedString(2))", new[] { new byte[] { 1, 2 }, new byte[] { 3, 4 }, null }, 1);
        yield return Refused("LowCardinality(Nullable(FixedString(2)))", new[] { new byte[] { 1, 2 }, null, new byte[] { 3, 4, 5 } }, 0);
        yield return Refused("LowCardinality(Decimal(3, 2))", new[] { 1m, 1m, 2m, 100m }, 1);
        yield return Refused("LowCardinality(Nullable(Decimal(3, 2)))", new decimal?[] { 1m, null, 100m }, 1);
        yield return Refused("LowCardinality(Enum8('a' = 1))", new[] { "a", "a", "b" }, 1);
        yield return Refused("LowCardinality(DateTime('UTC'))", new[] { DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddYears(-1) }, 0);
    }

    /// <summary>
    /// CLR types that have no row shape of the column type, or a child that the child type is not written from: a
    /// composite is written from a one-dimension zero-based array of a type that its child is written from, an array of
    /// <c>KeyValuePair</c>, or a <c>ValueTuple</c> of its arity. A <c>Nested</c> type is written only from a column of its
    /// own layout, and <c>Nothing</c> from no type.
    /// </summary>
    [TestCase("Array(DateTime('UTC'))", typeof(Guid[]))]
    [TestCase("Array(DateTime('UTC'))", typeof(DateTime[,]))]
    [TestCase("Array(DateTime('UTC'))", typeof(DateTime))]
    [TestCase("Array(String)", typeof(DateTime[]))]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, Guid>[]))]
    [TestCase("Map(String, DateTime('UTC'))", typeof(Tuple<string, DateTime>[]))]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<DateTime>))]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(Tuple<DateTime, string>))]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof((Guid, string)))]
    [TestCase("Nested(a UInt8)", typeof(byte[]))]
    [TestCase("Nested(a UInt8)", typeof(object))]
    [TestCase("Nothing", typeof(object))]
    public void CanWrite_TypeGivenAShapeItDoesNotHave_IsFalse(string type, Type source)
        => Assert.That(ClickHouseTcpTypes.CanWrite(type, source), Is.False);

    /// <summary>An array type that need not start at index zero is not the row shape of an <c>Array</c> or a <c>Map</c>.</summary>
    [Test]
    public void CanWrite_ArrayThatNeedNotStartAtZero_IsFalse()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseTcpTypes.CanWrite("Array(DateTime('UTC'))", typeof(DateTime).MakeArrayType(1)), Is.False);
            Assert.That(ClickHouseTcpTypes.CanWrite("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, DateTime>).MakeArrayType(1)), Is.False);
        });
    }

    /// <summary>
    /// A null string in a dictionary without NULL: the leaf refuses it with the parameter name <c>value</c>, as the
    /// <c>String</c> write of a null does.
    /// </summary>
    [Test]
    public void Write_NullStringInLowCardinalityWithoutNull_FailsWithTheMessageOfTheStringWrite()
    {
        ColumnWriter<string> writer = Derivation.Writer<string>("LowCardinality(String)", ConverterHarness.Context);
        Exception stringWrite = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(Derivation.Writer<string>("String", ConverterHarness.Context), new[] { "a", null }, 0, 2).GetAwaiter().GetResult());
        Exception lowCardinality = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(writer, new[] { "a", null }, 0, 2).GetAwaiter().GetResult());

        Assert.Multiple(() =>
        {
            ConverterHarness.AssertSameFailure(stringWrite, lowCardinality, "LowCardinality(String) against String");
            Assert.That(lowCardinality, Is.TypeOf<ArgumentNullException>());
            Assert.That(((ArgumentException)lowCardinality).ParamName, Is.EqualTo("value"));
        });
    }

    /// <summary>
    /// A dictionary of more entries than one key byte, or two, addresses: the keys are written at the width that the
    /// dictionary size needs (code 1 for two bytes, 2 for four bytes in the metadata word), and the codec reads the slice
    /// back as its values.
    /// </summary>
    [TestCase(300, "0106000000000000")]
    [TestCase(70000, "0206000000000000")]
    public async Task Write_LowCardinalityWithManyEntries_WritesTheKeysAtTheWidthOfTheDictionary(int distinct, string metadata)
    {
        // Every seventh number is NULL, so the numbers take more rows to reach the same number of distinct values.
        string[] values = Enumerable.Range(0, distinct + 10).Select(i => (i % distinct).ToString(System.Globalization.CultureInfo.InvariantCulture)).ToArray();
        int?[] numbers = Enumerable.Range(0, (distinct * 7 / 6) + 10).Select(i => i % 7 == 0 ? (int?)null : i).ToArray();

        byte[] strings = await ConverterHarness.WriteNewAsync(Derivation.Writer<string>("LowCardinality(String)", ConverterHarness.Context), values, 3, values.Length - 3);
        byte[] nullable = await ConverterHarness.WriteNewAsync(Derivation.Writer<int?>("LowCardinality(Nullable(Int32))", ConverterHarness.Context), numbers, 3, numbers.Length - 3);
        using IColumn stringsRead = await ConverterHarness.ReadBackAsync("LowCardinality(String)", strings, values.Length - 3);
        using IColumn nullableRead = await ConverterHarness.ReadBackAsync("LowCardinality(Nullable(Int32))", nullable, numbers.Length - 3);

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(strings, 8, 8), Is.EqualTo(metadata), "LowCardinality(String): the metadata word after the version");
            Assert.That(Convert.ToHexString(nullable, 8, 8), Is.EqualTo(metadata), "LowCardinality(Nullable(Int32)): the metadata word after the version");
            Assert.That(ConverterHarness.ReadAs<string>(stringsRead, 0, values.Length - 3), Is.EqualTo(values[3..]));
            Assert.That(ConverterHarness.ReadAs<int?>(nullableRead, 0, numbers.Length - 3), Is.EqualTo(numbers[3..]));
        });
    }

    /// <summary>
    /// <c>LowCardinality(String)</c> from <see cref="T:byte[]"/> (ClickHouse/integrations#792): the dictionary interns the
    /// bytes, so bytes that UTF-8 cannot spell stay as they are, and bytes that are the UTF-8 of a text write as that text
    /// does.
    /// </summary>
    [TestCase("LowCardinality(String)")]
    [TestCase("LowCardinality(Nullable(String))")]
    public async Task Write_LowCardinalityStringFromBytes_InternsTheBytes(string type)
    {
        bool nullable = type.Contains("Nullable", StringComparison.Ordinal);
        ColumnWriter<byte[]> writer = Derivation.Writer<byte[]>(type, ConverterHarness.Context);
        byte[][] values = { new byte[] { 0x61 }, new byte[] { 0xFF, 0xFE }, new byte[] { 0x61 }, nullable ? null : Array.Empty<byte>(), new byte[] { 0xFF, 0xFE } };
        byte[] actual = await ConverterHarness.WriteNewAsync(writer, values, 1, values.Length - 1);

        // Prefix: version 1. Body: flags with key width 1 byte, the dictionary (the reserved slots, FF FE, "a"), the key
        // count, the keys of rows 1 to 4.
        string reserved = nullable ? "0000" : "00";
        int ff = nullable ? 2 : 1;
        string expected = "0100000000000000" + "0006000000000000" + $"{ff + 2:X2}00000000000000" + reserved + "02FFFE" + "0161"
            + "0400000000000000" + $"{ff:X2}{ff + 1:X2}{(nullable ? 0 : ff - 1):X2}{ff:X2}";
        Assert.That(Convert.ToHexString(actual), Is.EqualTo(expected));

        // Bytes that are the UTF-8 of a text write as that text does.
        byte[][] utf8 = { new byte[] { 0x61 }, Encoding.UTF8.GetBytes("é"), nullable ? null : new byte[] { 0x61 } };
        string[] same = { "a", "é", nullable ? null : "a" };
        Assert.That(
            Convert.ToHexString(await ConverterHarness.WriteNewAsync(writer, utf8, 0, utf8.Length)),
            Is.EqualTo(Convert.ToHexString(await ConverterHarness.WriteNewAsync(Derivation.Writer<string>(type, ConverterHarness.Context), same, 0, same.Length))));
    }

    /// <summary>
    /// <c>LowCardinality(FixedString(N))</c> from <see cref="string"/>: the UTF-8 of the text, zero bytes up to N, so the
    /// bytes are those of the write of the padded bytes.
    /// </summary>
    [TestCase("LowCardinality(FixedString(4))")]
    [TestCase("LowCardinality(Nullable(FixedString(4)))")]
    public async Task Write_LowCardinalityFixedStringFromText_WritesThePaddedBytes(string type)
    {
        bool nullable = type.Contains("Nullable", StringComparison.Ordinal);
        ColumnWriter<string> writer = Derivation.Writer<string>(type, ConverterHarness.Context);
        string[] texts = { "ab", "abcd", "é", "ab", nullable ? null : string.Empty };
        byte[][] padded = Array.ConvertAll(texts, text => text is null ? null : Encoding.UTF8.GetBytes(text).Concat(new byte[4]).Take(4).ToArray());

        ColumnWriter<byte[]> bytes = Derivation.Writer<byte[]>(type, ConverterHarness.Context);
        foreach (int start in new[] { 0, 2 })
        {
            byte[] expected = await ConverterHarness.WriteNewAsync(bytes, padded, start, texts.Length - start);
            byte[] actual = await ConverterHarness.WriteNewAsync(writer, texts, start, texts.Length - start);
            Assert.That(Convert.ToHexString(actual), Is.EqualTo(Convert.ToHexString(expected)), $"rows [{start}, {texts.Length})");
        }
    }

    /// <summary>A text of more than N UTF-8 bytes is refused, named by its slot in the dictionary.</summary>
    [Test]
    public void Write_LowCardinalityFixedStringFromTooLongText_IsRefusedAtItsSlot()
    {
        ColumnWriter<string> writer = Derivation.Writer<string>("LowCardinality(FixedString(2))", ConverterHarness.Context);

        Exception failure = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(writer, new[] { "a", "a", "abc" }, 0, 3).GetAwaiter().GetResult());

        Assert.That(failure, Is.TypeOf<ArgumentException>());
        Assert.That(
            failure.Message,
            Does.StartWith("A FixedString(2) value at row 2 is 3 bytes in UTF-8; a text value can have at most 2 bytes."),
            "slot 0 is the placeholder, slot 1 is \"a\", so the text would take slot 2");
    }

    /// <summary>Two lone surrogates have the same UTF-8 bytes (EF BF BD), so they share one dictionary entry.</summary>
    [Test]
    public async Task Write_LowCardinalityStringWithTwoLoneSurrogates_GivesThemOneEntry()
    {
        string[] values = { "\uD800", "\uDBFF", "\uD800" };
        ColumnWriter<string> writer = Derivation.Writer<string>("LowCardinality(String)", ConverterHarness.Context);

        byte[] actual = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);

        Assert.That(
            Convert.ToHexString(actual),
            Is.EqualTo("0100000000000000" + "0006000000000000" + "0200000000000000" + "00" + "03EFBFBD" + "0300000000000000" + "010101"));
    }

    private static TestCaseData Column<T>(string type, T[] values) => new TestCaseData(type, values).SetArgDisplayNames(type);

    private static TestCaseData Refused<T>(string type, T[] values, int start) => new TestCaseData(type, values, start).SetArgDisplayNames(type, $"start {start}");

    private static KeyValuePair<TKey, TValue>[] Map<TKey, TValue>(params (TKey Key, TValue Value)[] pairs)
        => Array.ConvertAll(pairs, pair => new KeyValuePair<TKey, TValue>(pair.Key, pair.Value));

    private static KeyValuePair<string, int>[] Map() => Array.Empty<KeyValuePair<string, int>>();

    private static async Task AssertWritesTheValuesAsync<T>(string type, T[] values)
    {
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);
        foreach (int start in new[] { 0, 2 })
        {
            int length = values.Length - start;
            byte[] bytes = await ConverterHarness.WriteNewAsync(writer, values, start, length);
            using IColumn read = await ConverterHarness.ReadBackAsync(type, bytes, length);
            Assert.That(ConverterHarness.ReadAs<T>(read, 0, length), Is.EqualTo(values[start..]), $"{type}, rows [{start}, {values.Length})");
        }
    }

    private static async Task AssertWritesZeroValuesAsync<T>(string type, string expected)
    {
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);
        byte[] actual = await ConverterHarness.WriteNewAsync(writer, Array.Empty<T>(), 0, 0);
        Assert.That(Convert.ToHexString(actual), Is.EqualTo(expected), type);
    }

    private static async Task AssertFailsWithAsync<T>(string type, T[] values, int start, string exception, string parameter, string message)
    {
        ColumnWriter<T> writer = Derivation.Writer<T>(type, ConverterHarness.Context);
        Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteNewAsync(writer, values, start, values.Length - start));
        ConverterHarness.AssertFailure(actual, exception, parameter, message, $"{type}, rows [{start}, {values.Length})");
    }

}
