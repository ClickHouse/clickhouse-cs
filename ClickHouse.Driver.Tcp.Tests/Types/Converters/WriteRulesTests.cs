using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The write rules of D6 (<see cref="WriteRules"/>), for a matrix of column types and CLR source types that the
/// differential case list does not have (enums, casts, nullable sources and targets, the array casts that read elements
/// as another type): the two gather tiers of the POCO write plan give the same outcome, and the columnar insert, the POCO
/// write plan, the untyped rows and <c>ClickHouseTcpTypes.CanWrite</c> accept the same CLR types.
/// </summary>
[TestFixture]
public class WriteRulesTests
{
    internal static readonly string[] ColumnTypes =
    {
        "Int8", "UInt8", "Int16", "Int32", "UInt32", "Int64", "Bool", "Float64", "Enum8('a' = 1, 'b' = 2, 'c' = 3)",
        "Enum16('a' = 1, 'b' = 2, 'c' = 3)", "String", "FixedString(2)", "Date", "DateTime('UTC')", "DateTime64(3, 'UTC')", "Time",
        "UUID", "IPv4",
        "Nullable(Int8)", "Nullable(Int32)", "Nullable(UInt32)", "Nullable(String)", "Nullable(Enum8('a' = 1, 'b' = 2, 'c' = 3))",
        "Nullable(DateTime('UTC'))", "Nullable(Tuple(Int32, String))", "Nullable(Tuple(DateTime('UTC'), String))",
        "LowCardinality(String)", "LowCardinality(Int32)", "LowCardinality(Nullable(String))", "LowCardinality(Nullable(Int32))",
        "LowCardinality(DateTime('UTC'))", "LowCardinality(Nullable(DateTime('UTC')))", "LowCardinality(FixedString(2))",
        "Array(Int8)", "Array(UInt8)", "Array(Int32)", "Array(UInt32)", "Array(String)", "Array(Array(Int32))",
        "Array(Nullable(Int32))", "Array(DateTime('UTC'))", "Array(IPv4)",
        "Map(String, Int32)", "Tuple(Int32, String)", "Tuple(DateTime('UTC'), String)", "Variant(Int32, String)", "Dynamic", "Point",
        "SimpleAggregateFunction(anyLast, Nullable(Int32))", "SimpleAggregateFunction(sum, Int32)", "Nested(a Int32)",
    };

    internal static readonly Type[] Sources =
    {
        typeof(object), typeof(string), typeof(byte[]), typeof(sbyte[]), typeof(SByteEnum[]), typeof(ByteEnum[]),
        typeof(int), typeof(int?), typeof(uint), typeof(uint?), typeof(sbyte), typeof(sbyte?), typeof(byte), typeof(short), typeof(long),
        typeof(long?), typeof(double), typeof(bool),
        typeof(SByteEnum), typeof(SByteEnum?), typeof(ByteEnum), typeof(IntEnum), typeof(IntEnum?), typeof(UIntEnum), typeof(UIntEnum?),
        typeof(LongEnum),
        typeof(int[]), typeof(uint[]), typeof(IntEnum[]), typeof(UIntEnum[]), typeof(int[][]), typeof(uint[][]), typeof(object[]),
        typeof(string[]), typeof(int?[]), typeof(DateTime), typeof(DateTime?), typeof(DateTimeOffset), typeof(DateTimeOffset?),
        typeof(TimeSpan), typeof(TimeSpan?), typeof(DateOnly?), typeof(Guid?), typeof(IPAddress), typeof(Address), typeof(Address[]),
        typeof((int, string)), typeof((int, string)?), typeof((DateTime, string)), typeof((DateTime, string)?),
        typeof(KeyValuePair<string, int>[]), typeof(KeyValuePair<string, DateTime>[]), typeof((double, double)), typeof((double, double)?),
    };

    private static readonly MethodInfo RunMethod = typeof(WriteRulesTests).GetMethod(nameof(Run), BindingFlags.NonPublic | BindingFlags.Static);

    // The client's POCO insert in each gather tier.
    private static readonly WriteArm[] PocoArms = { ClientArms.PocoWrite, new RowWriteArms.ClientPocoArm("Client.PocoWrite: Delegate", PocoGatherTier.Delegate) };

    /// <summary>
    /// The CLR types of <see cref="Sources"/> that each column type of <see cref="ColumnTypes"/> is written from: its own
    /// writes and the write rules of D6. A change of the rules or of the leaf table changes this table; the failure message
    /// prints the table that the derivation gives.
    /// </summary>
    private static readonly Dictionary<string, string> AcceptedSources = new()
    {
        ["Int8"] = "sbyte, sbyte?, SByteEnum, SByteEnum?",
        ["UInt8"] = "byte, ByteEnum",
        ["Int16"] = "short",
        ["Int32"] = "int, int?, IntEnum, IntEnum?",
        ["UInt32"] = "uint, uint?, UIntEnum, UIntEnum?",
        ["Int64"] = "long, long?, LongEnum",
        ["Bool"] = "bool",
        ["Float64"] = "double",
        ["Enum8('a' = 1, 'b' = 2, 'c' = 3)"] = "string, sbyte, sbyte?, SByteEnum, SByteEnum?",
        ["Enum16('a' = 1, 'b' = 2, 'c' = 3)"] = "string, short",
        ["String"] = "string, byte[], ByteEnum[]",
        ["FixedString(2)"] = "string, byte[], ByteEnum[]",
        ["Date"] = "DateOnly?",
        ["DateTime('UTC')"] = "uint, uint?, UIntEnum, UIntEnum?, DateTime, DateTime?, DateTimeOffset, DateTimeOffset?",
        ["DateTime64(3, 'UTC')"] = "long, long?, LongEnum, DateTime, DateTime?, DateTimeOffset, DateTimeOffset?",
        ["Time"] = "int, int?, IntEnum, IntEnum?, TimeSpan, TimeSpan?",
        ["UUID"] = "Guid?",
        ["IPv4"] = "IPAddress, Address",
        ["Nullable(Int8)"] = "sbyte, sbyte?, SByteEnum, SByteEnum?",
        ["Nullable(Int32)"] = "int, int?, IntEnum, IntEnum?",
        ["Nullable(UInt32)"] = "uint, uint?, UIntEnum, UIntEnum?",
        ["Nullable(String)"] = "string, byte[], ByteEnum[]",
        ["Nullable(Enum8('a' = 1, 'b' = 2, 'c' = 3))"] = "string, sbyte, sbyte?, SByteEnum, SByteEnum?",
        ["Nullable(DateTime('UTC'))"] = "uint, uint?, UIntEnum, UIntEnum?, DateTime, DateTime?, DateTimeOffset, DateTimeOffset?",
        ["Nullable(Tuple(Int32, String))"] = "(int, string), (int, string)?",
        ["Nullable(Tuple(DateTime('UTC'), String))"] = "(DateTime, string), (DateTime, string)?",
        ["LowCardinality(String)"] = "string, byte[], ByteEnum[]",
        ["LowCardinality(Int32)"] = "int, int?, IntEnum, IntEnum?",
        ["LowCardinality(Nullable(String))"] = "string, byte[], ByteEnum[]",
        ["LowCardinality(Nullable(Int32))"] = "int, int?, IntEnum, IntEnum?",
        ["LowCardinality(DateTime('UTC'))"] = "uint, uint?, UIntEnum, UIntEnum?, DateTime, DateTime?, DateTimeOffset, DateTimeOffset?",
        ["LowCardinality(Nullable(DateTime('UTC')))"] = "uint, uint?, UIntEnum, UIntEnum?, DateTime, DateTime?, DateTimeOffset, DateTimeOffset?",
        ["LowCardinality(FixedString(2))"] = "string, byte[], ByteEnum[]",
        ["Array(Int8)"] = "sbyte[], SByteEnum[]",
        ["Array(UInt8)"] = "byte[], ByteEnum[]",
        ["Array(Int32)"] = "int[], IntEnum[]",
        ["Array(UInt32)"] = "uint[], UIntEnum[]",
        ["Array(String)"] = "string[]",
        ["Array(Array(Int32))"] = "int[][]",
        ["Array(Nullable(Int32))"] = "int?[]",
        ["Array(DateTime('UTC'))"] = "uint[], UIntEnum[]",
        ["Array(IPv4)"] = "Address[]",
        ["Map(String, Int32)"] = "KeyValuePair<string, int>[]",
        ["Tuple(Int32, String)"] = "(int, string), (int, string)?",
        ["Tuple(DateTime('UTC'), String)"] = "(DateTime, string), (DateTime, string)?",
        ["Variant(Int32, String)"] =
            "object, string, byte[], sbyte[], SByteEnum[], ByteEnum[], int, int?, uint, uint?, sbyte," +
            " sbyte?, byte, short, long, long?, double, bool, SByteEnum, SByteEnum?, ByteEnum, IntEnum," +
            " IntEnum?, UIntEnum, UIntEnum?, LongEnum, int[], uint[], IntEnum[], UIntEnum[], int[][]," +
            " uint[][], object[], string[], int?[], DateTime, DateTime?, DateTimeOffset," +
            " DateTimeOffset?, TimeSpan, TimeSpan?, DateOnly?, Guid?, IPAddress, Address, Address[]," +
            " (int, string), (int, string)?, (DateTime, string), (DateTime, string)?," +
            " KeyValuePair<string, int>[], KeyValuePair<string, DateTime>[], (double, double), (double," +
            " double)?",
        ["Dynamic"] =
            "object, string, byte[], sbyte[], SByteEnum[], ByteEnum[], int, int?, uint, uint?, sbyte," +
            " sbyte?, byte, short, long, long?, double, bool, SByteEnum, SByteEnum?, ByteEnum, IntEnum," +
            " IntEnum?, UIntEnum, UIntEnum?, LongEnum, int[], uint[], IntEnum[], UIntEnum[], int[][]," +
            " uint[][], object[], string[], int?[], DateTime, DateTime?, DateTimeOffset," +
            " DateTimeOffset?, TimeSpan, TimeSpan?, DateOnly?, Guid?, IPAddress, Address, Address[]," +
            " (int, string), (int, string)?, (DateTime, string), (DateTime, string)?," +
            " KeyValuePair<string, int>[], KeyValuePair<string, DateTime>[], (double, double), (double," +
            " double)?",
        ["Point"] = "(double, double), (double, double)?",
        ["SimpleAggregateFunction(anyLast, Nullable(Int32))"] = "int, int?, IntEnum, IntEnum?",
        ["SimpleAggregateFunction(sum, Int32)"] = "int, int?, IntEnum, IntEnum?",
        ["Nested(a Int32)"] = "",
    };

    internal enum SByteEnum : sbyte
    {
        A = 1,
    }

    internal enum ByteEnum : byte
    {
        A = 1,
    }

    internal enum IntEnum
    {
        A = 1,
    }

    internal enum UIntEnum : uint
    {
        A = 1,
    }

    internal enum LongEnum : long
    {
        A = 1,
    }

    private static IEnumerable<string> Types() => ColumnTypes;

    [Test]
    public void Derive_EachColumnType_IsWrittenFromTheListedSources()
    {
        Dictionary<string, string> actual = ColumnTypes.ToDictionary(
            type => type,
            type => string.Join(", ", Sources.Where(source => ConverterDerivation.Default.Derive(type, ConverterHarness.Context, source, ConversionDirection.Write).Succeeded).Select(TypeNames.Of)));

        Assert.That(actual, Is.EquivalentTo(AcceptedSources), "The table is:" + Environment.NewLine + string.Join(Environment.NewLine, actual.Select(entry => $"[\"{entry.Key}\"] = \"{entry.Value}\",")));
    }

    /// <summary>
    /// For each source, the gather with no compiled code writes the bytes of the compiled gather, or fails with the same
    /// exception and text (a NULL that the column cannot hold, a value that the column type refuses), or refuses alike,
    /// for all rows and from row 1, with and without a NULL at row 1.
    /// </summary>
    [TestCaseSource(nameof(Types))]
    public void PocoWrite_EachSource_GivesTheSameOutcomeInBothGatherTiers(string columnType)
    {
        var differences = new List<string>();
        foreach (Type source in Sources)
        {
            foreach (bool withNull in new[] { false, true })
            {
                foreach (int start in new[] { 0, 1 })
                {
                    Array values = Samples(source, withNull);
                    Outcome compiled = WriteOutcome(PocoArms[0], source, values, columnType, start);
                    Outcome viaDelegates = WriteOutcome(PocoArms[1], source, values, columnType, start);
                    if (Differential.Outcome.Difference(compiled, viaDelegates) is string difference)
                    {
                        differences.Add($"{TypeNames.Of(source)}{(withNull ? " with a NULL" : string.Empty)} from row {start}: {PocoArms[1].Name} {viaDelegates}; {PocoArms[0].Name} {compiled}: {difference}");
                    }
                }
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    /// <summary>
    /// Where the derivation writes a <c>T?</c> source through a <see cref="NonNullWriter{T}"/>, the POCO write plan gathers
    /// the property into a column of <c>T</c>, which the insert writes through the derivation of <c>T</c>. That tree has
    /// the type of the tree inside the null check.
    /// </summary>
    [TestCaseSource(nameof(Types))]
    public void Derive_NullableSourceThatCannotBeNull_TheValueTypeGivesTheTreeInsideTheNullCheck(string columnType)
    {
        var differences = new List<string>();
        foreach (Type source in Sources.Where(source => Nullable.GetUnderlyingType(source) is not null))
        {
            object writer = ConverterDerivation.Default.Derive(columnType, ConverterHarness.Context, source, ConversionDirection.Write).Converter;
            if (writer is null || !writer.GetType().IsGenericType || writer.GetType().GetGenericTypeDefinition() != typeof(NonNullWriter<>))
            {
                continue;
            }

            object inner = writer.GetType().GetProperty(nameof(NonNullWriter<int>.Inner)).GetValue(writer);
            Derivation values = ConverterDerivation.Default.Derive(columnType, ConverterHarness.Context, Nullable.GetUnderlyingType(source), ConversionDirection.Write);
            if (!values.Succeeded || values.Converter.GetType() != inner.GetType())
            {
                differences.Add($"{TypeNames.Of(source)}: the tree inside the null check is {inner.GetType()}, the value type gives {(values.Succeeded ? values.Converter.GetType() : values.Refusal)}");
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    /// <summary>
    /// One set of rules for every write tier: for each source, the columnar insert accepts it exactly when the POCO write
    /// plan does and <c>ClickHouseTcpTypes.CanWrite</c> says true, and with no NULL both write the same bytes. With a NULL
    /// that the column cannot hold, both fail: each tier names the row in its own words.
    /// </summary>
    [TestCaseSource(nameof(Types))]
    public void Write_EachSource_TheColumnarInsertAgreesWithThePocoWritePlanAndCanWrite(string columnType)
    {
        var differences = new List<string>();
        foreach (Type source in Sources)
        {
            bool canWrite = ClickHouseTcpTypes.CanWrite(columnType, source);
            foreach (bool withNull in new[] { false, true })
            {
                Array values = Samples(source, withNull);
                Outcome poco = WriteOutcome(ClientArms.PocoWrite, source, values, columnType, start: 0);
                Outcome columnar = WriteOutcome(ClientArms.Write, source, values, columnType, start: 0);
                bool pocoAccepts = poco.Kind != OutcomeKind.Refused;
                bool columnarAccepts = columnar.Kind != OutcomeKind.Refused;
                if (pocoAccepts != canWrite || columnarAccepts != canWrite)
                {
                    differences.Add($"{TypeNames.Of(source)}: CanWrite {canWrite}, POCO {poco}, columnar {columnar}");
                }
                else if (!withNull && Differential.Outcome.Difference(poco, columnar) is string difference && poco.Kind == OutcomeKind.Bytes)
                {
                    differences.Add($"{TypeNames.Of(source)}: columnar {columnar}; POCO {poco}: {difference}");
                }
                else if (poco.Kind == OutcomeKind.Failed != (columnar.Kind == OutcomeKind.Failed))
                {
                    differences.Add($"{TypeNames.Of(source)}{(withNull ? " with a NULL" : string.Empty)}: columnar {columnar}; POCO {poco}");
                }
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    /// <summary>
    /// For the boxed values of each source, the untyped insert accepts the values exactly when
    /// <c>ClickHouseTcpTypes.CanWrite</c> says true for the CLR type of the values.
    /// </summary>
    [TestCaseSource(nameof(Types))]
    public void UntypedWrite_EachSource_AcceptsTheValuesExactlyWhenCanWriteSaysTrue(string columnType)
    {
        var differences = new List<string>();
        foreach (Type source in Sources)
        {
            foreach (bool withNull in new[] { false, true })
            {
                // The untyped insert takes the CLR type of the first value that is not null.
                Array values = Samples(source, withNull);
                Type present = values.Cast<object>().First(value => value is not null).GetType();
                bool canWrite = ClickHouseTcpTypes.CanWrite(columnType, present);
                foreach (int start in new[] { 0, 1 })
                {
                    Outcome now = WriteOutcome(ClientArms.UntypedWrite, source, values, columnType, start);
                    if (now.Kind == OutcomeKind.Refused == canWrite)
                    {
                        differences.Add($"{TypeNames.Of(source)}: CanWrite({TypeNames.Of(present)}) {canWrite}, untyped {now}");
                    }
                }
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    /// <summary>
    /// The cast rule of the writes takes its types from the cast rule of the reads (<see cref="ReadRules.IsAssignable"/>),
    /// so it gives no array cast that gives the elements another meaning (<see cref="ReadRules.ReinterpretsElements"/>).
    /// For the array sources of the matrix, the array casts that read value-type elements as another type are an enum
    /// array written as an array of its underlying type.
    /// </summary>
    [Test]
    public void CastTargets_ArraySources_KeepOnlyAnEnumArrayAsAnArrayOfItsUnderlyingType()
    {
        Type[] arrays = Sources.Where(source => source.IsArray).ToArray();
        (Type Source, Type Target)[] casts = arrays.SelectMany(source => WriteRules.CastTargets(source).Select(target => (source, target))).ToArray();

        string[] elementCasts = casts
            .Where(cast => cast.Target.IsArray && cast.Source.GetElementType().IsValueType && cast.Target.GetElementType() != cast.Source.GetElementType())
            .Select(cast => $"{TypeNames.Of(cast.Source)} as {TypeNames.Of(cast.Target)}")
            .OrderBy(text => text, StringComparer.Ordinal)
            .ToArray();

        string[] expected = { "ByteEnum[] as byte[]", "IntEnum[] as int[]", "SByteEnum[] as sbyte[]", "UIntEnum[] as uint[]" };

        Assert.Multiple(() =>
        {
            Assert.That(casts.Where(cast => !ReadRules.IsAssignable(cast.Source, cast.Target)), Is.Empty);
            Assert.That(casts.Where(cast => ReadRules.ReinterpretsElements(cast.Source, cast.Target)), Is.Empty);
            Assert.That(elementCasts, Is.EqualTo(expected), "Listed:" + Environment.NewLine + string.Join(Environment.NewLine, elementCasts.Select(r => $"\"{r}\",")));
        });
    }

    /// <summary>
    /// An untyped column whose first value is written through a cast takes later values of the type that it is written
    /// as, also of the source type, and writes the bytes of the columnar insert of the values as that type.
    /// </summary>
    [TestCase("IPv4", typeof(IPAddress))]
    [TestCase("Array(Int32)", typeof(int[]))]
    [TestCase("String", typeof(byte[]))]
    public void UntypedWrite_FirstValueWrittenThroughACast_TakesLaterValuesOfTheTypeThatItIsWrittenAs(string columnType, Type writtenAs)
    {
        object[] values = columnType switch
        {
            "IPv4" => new object[] { new Address(1), IPAddress.Parse("1.2.3.4"), new Address(3) },
            "Array(Int32)" => new object[] { new[] { (IntEnum)(-1) }, new[] { -1, 2 }, new[] { IntEnum.A } },
            _ => new object[] { new[] { (ByteEnum)0xFF }, new byte[] { 0x41, 0x42 }, new[] { ByteEnum.A } },
        };

        // The CLR cast puts each value into an array of the type that the column is written as.
        Array typed = Array.CreateInstance(writtenAs, values.Length);
        Array.Copy(values, typed, values.Length);
        Outcome columnar = WriteOutcome(ClientArms.Write, writtenAs, typed, columnType, start: 0);
        Outcome untyped = WriteOutcome(ClientArms.UntypedWrite, typeof(object), values, columnType, start: 0);

        Assert.Multiple(() =>
        {
            Assert.That(untyped.Kind, Is.EqualTo(OutcomeKind.Bytes), untyped.ToString());
            Assert.That(Differential.Outcome.Difference(columnar, untyped), Is.Null, $"untyped {untyped}; columnar {columnar}");
        });
    }

    /// <summary>
    /// A <c>uint[]</c> is not written into <c>Array(Int32)</c>: the cast would store 3000000000 as -1294967296. Each write
    /// tier refuses it before it writes a value.
    /// </summary>
    [Test]
    public void Write_UInt32ArrayIntoAnInt32Array_IsRefusedInEveryTier()
    {
        Array values = new[] { new[] { 3_000_000_000u }, Array.Empty<uint>(), new[] { 7u } };

        Assert.Multiple(() =>
        {
            foreach (WriteArm arm in new[] { ClientArms.Write, ClientArms.PocoWrite, ClientArms.UntypedWrite })
            {
                Outcome outcome = WriteOutcome(arm, typeof(uint[]), values, "Array(Int32)", start: 0);
                Assert.That(outcome.Kind, Is.EqualTo(OutcomeKind.Refused), $"{arm.Name}: {outcome}");
            }

            Assert.That(ClickHouseTcpTypes.CanWrite("Array(Int32)", typeof(uint[])), Is.False);
        });
    }

    // The values of a source type: three values that most column types of the matrix take, with a NULL at row 1 when
    // asked and the type holds one.
    internal static Array Samples(Type source, bool withNull)
    {
        Array values = Array.CreateInstance(source, 3);
        for (int i = 0; i < 3; i++)
        {
            values.SetValue(withNull && i == 1 && (!source.IsValueType || Nullable.GetUnderlyingType(source) is not null) ? null : Sample(source, i + 1), i);
        }

        return values;
    }

    private static object Sample(Type type, int n)
    {
        Type value = Nullable.GetUnderlyingType(type) ?? type;
        if (value.IsEnum)
        {
            return Enum.ToObject(value, n);
        }

        if (value.IsArray)
        {
            Type element = value.GetElementType();
            Array array = Array.CreateInstance(element, element == typeof(byte) || element == typeof(sbyte) || element.IsEnum && Enum.GetUnderlyingType(element).Name.EndsWith("Byte", StringComparison.Ordinal) ? 2 : n - 1);
            for (int i = 0; i < array.Length; i++)
            {
                array.SetValue(Sample(element, n + i), i);
            }

            return array;
        }

        return value switch
        {
            _ when value == typeof(object) => n % 2 == 0 ? "b" : (object)n,
            _ when value == typeof(string) => ((char)('a' + n - 1)).ToString(),
            _ when value == typeof(int) => n,
            _ when value == typeof(uint) => (uint)n,
            _ when value == typeof(sbyte) => (sbyte)n,
            _ when value == typeof(byte) => (byte)n,
            _ when value == typeof(short) => (short)n,
            _ when value == typeof(long) => (long)n,
            _ when value == typeof(double) => n + 0.5,
            _ when value == typeof(bool) => n % 2 == 1,
            _ when value == typeof(DateTime) => DateTime.UnixEpoch.AddSeconds(n),
            _ when value == typeof(DateTimeOffset) => DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + n),
            _ when value == typeof(TimeSpan) => TimeSpan.FromSeconds(n),
            _ when value == typeof(DateOnly) => new DateOnly(2020, 1, n),
            _ when value == typeof(Guid) => new Guid(n, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            _ when value == typeof(IPAddress) => new IPAddress(n),
            _ when value == typeof(Address) => new Address(n),
            _ when value == typeof((int, string)) => (n, "x"),
            _ when value == typeof((DateTime, string)) => (DateTime.UnixEpoch.AddSeconds(n), "x"),
            _ when value == typeof((double, double)) => ((double)n, 2.0),
            _ when value == typeof(KeyValuePair<string, int>) => new KeyValuePair<string, int>("k", n),
            _ when value == typeof(KeyValuePair<string, DateTime>) => new KeyValuePair<string, DateTime>("k", DateTime.UnixEpoch.AddSeconds(n)),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "The matrix has no sample of this type."),
        };
    }

    private static Outcome WriteOutcome(WriteArm arm, Type source, Array values, string columnType, int start)
        => (Outcome)RowWriteArms.Invoke(RunMethod, source, arm, values, columnType, start);

    private static Outcome Run<T>(WriteArm arm, T[] values, string columnType, int start)
    {
        using var column = new ArrayColumn<T>("value", columnType, values);
        SliceWriter write;
        try
        {
            write = arm.Bind(column, columnType, DifferentialEngine.Context);
        }
        catch (Exception e)
        {
            return Differential.Outcome.Refusal(e);
        }

        try
        {
            using var stream = new MemoryStream();
            using (var writer = new ClickHouseBinaryWriter(stream))
            {
                write(writer, start, values.Length - start);
                writer.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
            }

            return Differential.Outcome.OfBytes(stream.ToArray());
        }
        catch (Exception e)
        {
            return Differential.Outcome.Failure(e);
        }
    }

    /// <summary>A subclass, for the cast to a base class.</summary>
    internal sealed class Address : IPAddress
    {
        public Address(long address)
            : base(address)
        {
        }
    }
}
