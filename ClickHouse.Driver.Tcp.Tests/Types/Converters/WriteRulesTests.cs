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
/// The write rules of D6 (<see cref="WriteRules"/>): the derivation accepts every CLR type that the POCO write plan
/// accepted, and the writes give the same bytes and the same failures, for a matrix of column types and CLR source types
/// that the differential case list does not have (enums, casts, nullable sources and targets, the array casts that read
/// elements as another type). The columnar insert and the untyped rows take the same rules.
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

    /// <summary>
    /// The CLR types that the derivation writes a column type from and the old POCO write plan refused (SPEC D7, the
    /// coordinator's ruling on the D6 write rules).
    /// </summary>
    internal static readonly (string ColumnType, Type Source, string Reason)[] Additions =
    {
        ("FixedString(2)", typeof(string), FixedStringFromText),
        ("LowCardinality(FixedString(2))", typeof(string), FixedStringFromText),
        ("LowCardinality(String)", typeof(byte[]), Issue792),
        ("LowCardinality(String)", typeof(sbyte[]), Issue792Cast),
        ("LowCardinality(String)", typeof(SByteEnum[]), Issue792Cast),
        ("LowCardinality(String)", typeof(ByteEnum[]), Issue792Cast),
        ("LowCardinality(Nullable(String))", typeof(byte[]), Issue792),
        ("LowCardinality(Nullable(String))", typeof(sbyte[]), Issue792Cast),
        ("LowCardinality(Nullable(String))", typeof(SByteEnum[]), Issue792Cast),
        ("LowCardinality(Nullable(String))", typeof(ByteEnum[]), Issue792Cast),
        ("LowCardinality(DateTime('UTC'))", typeof(DateTime?), NullableRules),
        ("LowCardinality(DateTime('UTC'))", typeof(DateTimeOffset?), NullableRules),
        ("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTime), NullableRules),
        ("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTimeOffset), NullableRules),
        ("Nullable(Tuple(DateTime('UTC'), String))", typeof((DateTime, string)), NullableRules),
        ("Tuple(DateTime('UTC'), String)", typeof((DateTime, string)?), NullableRules),
    };

    /// <summary>
    /// The writes that the old row inserts accepted and then failed, and that the converter layer writes (D7): a Variant
    /// value whose CLR type is the canonical type of no alternative goes to the alternative that is written from it.
    /// </summary>
    internal static readonly (string ColumnType, Type Source, string Reason)[] OutcomeChanges =
    {
        ("Variant(Int32, String)", typeof(byte[]), VariantPlacement),
    };

    private const string VariantPlacement = "A Variant value of no alternative's canonical type goes to the alternative that is written from it (D7).";

    private const string FixedStringFromText = "FixedString(N) is written from string (D7).";
    private const string Issue792 = "LowCardinality(String) is written from byte[] (ClickHouse/integrations#792).";
    private const string Issue792Cast = "The cast rule writes an array that the CLR reads as byte[] through LowCardinality(String) from byte[].";
    private const string NullableRules = "The nullable rules apply to every CLR type that the type is written from, not only to the codec's preferred types.";

    private static readonly MethodInfo RunMethod = typeof(WriteRulesTests).GetMethod(nameof(Run), BindingFlags.NonPublic | BindingFlags.Static);

    // The client's POCO insert in each gather tier.
    private static readonly WriteArm[] PocoArms = { ClientArms.PocoWrite, new RowWriteArms.ClientPocoArm("Client.PocoWrite: Delegate", PocoGatherTier.Delegate) };

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

    [TestCaseSource(nameof(Types))]
    public void Derive_EachSource_AcceptsWhatThePocoWritePlanAcceptedAndTheListedAdditions(string columnType)
    {
        IColumnCodec codec = ConverterHarness.Codec(columnType);
        var differences = new List<string>();
        foreach (Type source in Sources)
        {
            bool old = LegacyPocoWriteConversion.TryChooseWriteType(codec, source, out _);
            bool derived = ConverterDerivation.Default.Derive(columnType, ConverterHarness.Context, source, ConversionDirection.Write).Succeeded;
            bool added = Additions.Any(addition => addition.ColumnType == columnType && addition.Source == source);
            if (derived != (old || added) || (old && added))
            {
                differences.Add($"{TypeNames.Of(source)}: derived {derived}, old {old}, listed as an addition {added}");
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    [Test]
    public void Additions_EveryEntry_IsATypeAndSourceOfTheMatrix()
    {
        foreach ((string type, Type source, _) in Additions)
        {
            Assert.That(ColumnTypes, Does.Contain(type));
            Assert.That(Sources, Does.Contain(source));
        }
    }

    /// <summary>
    /// For each source that the old POCO write plan accepted, the plan of today writes the same bytes, or fails with the
    /// same exception and text (a NULL that the column cannot hold, a value that the column type refuses), in both gather
    /// tiers, for all rows and from row 1, with and without a NULL at row 1.
    /// </summary>
    [TestCaseSource(nameof(Types))]
    public void PocoWrite_EachSourceThatTheOldPlanAccepted_GivesTheOldOutcome(string columnType)
    {
        IColumnCodec codec = ConverterHarness.Codec(columnType);
        var differences = new List<string>();
        foreach (Type source in Sources.Where(source => LegacyPocoWriteConversion.TryChooseWriteType(codec, source, out _)))
        {
            foreach (bool withNull in new[] { false, true })
            {
                foreach (int start in new[] { 0, 1 })
                {
                    Array values = Samples(source, withNull);
                    Outcome old = WriteOutcome(ReferenceArms.PocoWrite, source, values, columnType, start);
                    foreach (WriteArm arm in PocoArms)
                    {
                        Outcome now = WriteOutcome(arm, source, values, columnType, start);
                        string difference = IsOutcomeChange(columnType, source)
                            ? old.Kind == OutcomeKind.Failed && now.Kind == OutcomeKind.Bytes ? null : "a listed change, which the old plan fails and the plan of today writes"
                            : Differential.Outcome.Difference(old, now);
                        if (difference is not null)
                        {
                            differences.Add($"{arm.Name}: {TypeNames.Of(source)}{(withNull ? " with a NULL" : string.Empty)} from row {start}: {now}; old {old}: {difference}");
                        }
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
    /// For the boxed values of each source, the untyped insert of today gives the outcome of the old one wherever the old
    /// one accepted the values, and accepts the values exactly when <c>ClickHouseTcpTypes.CanWrite</c> says true for the
    /// CLR type of the values.
    /// </summary>
    [TestCaseSource(nameof(Types))]
    public void UntypedWrite_EachSource_GivesTheOldOutcomeAndTheAnswerOfCanWrite(string columnType)
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
                    Outcome old = WriteOutcome(ReferenceArms.UntypedWrite, source, values, columnType, start);
                    Outcome now = WriteOutcome(ClientArms.UntypedWrite, source, values, columnType, start);
                    string difference = IsOutcomeChange(columnType, source)
                        ? old.Kind == OutcomeKind.Failed && now.Kind == OutcomeKind.Bytes ? null : "a listed change, which the old insert fails and the insert of today writes"
                        : old.Kind != OutcomeKind.Refused ? Differential.Outcome.Difference(old, now) : null;
                    if (difference is not null)
                    {
                        differences.Add($"{TypeNames.Of(source)}{(withNull ? " with a NULL" : string.Empty)} from row {start}: {now}; old {old}: {difference}");
                    }

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
    /// The cast rule of the writes takes its types from the cast rule of the reads (<see cref="ReadRules.IsAssignable"/>).
    /// This lists the array casts among them that read elements as another type (<see cref="ReadRules.ReinterpretsElements"/>),
    /// for the array sources of the matrix: they are the write side of the open question on those casts.
    /// </summary>
    [Test]
    public void CastTargets_ArraySources_AreReadRuleCastsAndTheElementReinterpretationsAreListed()
    {
        Type[] arrays = Sources.Where(source => source.IsArray).ToArray();
        Assert.That(arrays.SelectMany(source => WriteRules.CastTargets(source).Select(target => (source, target))).Where(pair => !ReadRules.IsAssignable(pair.source, pair.target)), Is.Empty);

        string[] reinterpreted = arrays
            .SelectMany(source => WriteRules.CastTargets(source).Where(target => ReadRules.ReinterpretsElements(source, target)).Select(target => $"{TypeNames.Of(source)} as {TypeNames.Of(target)}"))
            .OrderBy(text => text, StringComparer.Ordinal)
            .ToArray();

        string[] expected =
        {
            "ByteEnum[] as byte[]", "ByteEnum[] as sbyte[]", "IntEnum[] as int[]", "IntEnum[] as uint[]", "SByteEnum[] as byte[]",
            "SByteEnum[] as sbyte[]", "UIntEnum[] as int[]", "UIntEnum[] as uint[]", "byte[] as sbyte[]", "int[] as uint[]",
            "int[][] as uint[][]", "sbyte[] as byte[]", "uint[] as int[]", "uint[][] as int[][]",
        };

        Assert.That(reinterpreted, Is.EqualTo(expected), "Listed:" + Environment.NewLine + string.Join(Environment.NewLine, reinterpreted.Select(r => $"\"{r}\",")));
    }

    /// <summary>
    /// An untyped column whose first value is written through a cast takes later values of the type that it is written
    /// as, also of the source type: the untyped insert of today writes them as the old one did.
    /// </summary>
    [TestCase("IPv4")]
    [TestCase("Array(Int32)")]
    [TestCase("String")]
    public void UntypedWrite_FirstValueWrittenThroughACast_TakesLaterValuesOfTheTypeThatItIsWrittenAs(string columnType)
    {
        object[] values = columnType switch
        {
            "IPv4" => new object[] { new Address(1), IPAddress.Parse("1.2.3.4"), new Address(3) },
            "Array(Int32)" => new object[] { new[] { 3_000_000_000u }, new[] { -1, 2 }, new[] { 7u } },
            _ => new object[] { new sbyte[] { -1 }, new byte[] { 0x41, 0x42 }, new sbyte[] { 1 } },
        };

        Outcome old = WriteOutcome(ReferenceArms.UntypedWrite, typeof(object), values, columnType, start: 0);
        Outcome now = WriteOutcome(ClientArms.UntypedWrite, typeof(object), values, columnType, start: 0);

        Assert.Multiple(() =>
        {
            Assert.That(now.Kind, Is.EqualTo(OutcomeKind.Bytes), now.ToString());
            Assert.That(Differential.Outcome.Difference(old, now), Is.Null, $"today {now}; old {old}");
        });
    }

    /// <summary>The reinterpretation keeps the bits of each element, as the old POCO write plan wrote them.</summary>
    [Test]
    public void PocoWrite_UInt32ArrayIntoAnInt32Array_WritesTheBitsOfEachElement()
    {
        Array values = new[] { new[] { 3_000_000_000u }, Array.Empty<uint>(), new[] { 7u } };

        Outcome old = WriteOutcome(ReferenceArms.PocoWrite, typeof(uint[]), values, "Array(Int32)", start: 0);
        Outcome now = WriteOutcome(ClientArms.PocoWrite, typeof(uint[]), values, "Array(Int32)", start: 0);

        Assert.Multiple(() =>
        {
            Assert.That(now.Kind, Is.EqualTo(OutcomeKind.Bytes), now.ToString());
            Assert.That(Differential.Outcome.Difference(old, now), Is.Null);
            Assert.That(now.Bytes[^8..], Is.EqualTo(new byte[] { 0x00, 0x5E, 0xD0, 0xB2, 7, 0, 0, 0 }), "3000000000 is the Int32 -1294967296");
        });
    }

    private static bool IsOutcomeChange(string columnType, Type source)
        => OutcomeChanges.Any(change => change.ColumnType == columnType && change.Source == source);

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
