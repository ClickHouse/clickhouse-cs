using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Pins the leaf table: the counts of its pairs, every registered name as a leaf or a composite, and the read pairs and
/// the write pairs of each leaf.
/// </summary>
[TestFixture]
public class LeafTableTests
{
    // Every name that the registry knows and that is not a leaf: the composites, the aliases over them, and the types
    // that the registry refuses.
    private static readonly string[] CompositeNames =
    {
        "Nullable", "Array", "Tuple", "Map", "LowCardinality", "Nested", "Variant", "Dynamic", "QBit",
        "Point", "Ring", "LineString", "Polygon", "MultiLineString", "MultiPolygon", "Geometry",
        "SimpleAggregateFunction", "AggregateFunction",
    };

    /// <summary>The leaf of a type string, as the derivation finds it.</summary>
    internal static Leaf LeafOf(string type)
    {
        string name = TypeParser.Parse(type).Name;
        Assert.That(ColumnCodecRegistry.Default.TryCanonicalName(name, out string canonical), Is.True, $"'{name}' is not a registered name");
        Assert.That(LeafTable.TryGet(canonical, out Leaf leaf), Is.True, $"'{canonical}' is not in the leaf table");
        return leaf;
    }

    [Test]
    public void All_EveryRegisteredName_IsALeafOrAComposite()
    {
        var byName = (System.Collections.IDictionary)typeof(ColumnCodecRegistry)
            .GetField("byName", BindingFlags.NonPublic | BindingFlags.Instance)
            .GetValue(ColumnCodecRegistry.Default);
        string[] registered = byName.Keys.Cast<string>().ToArray();
        string[] leaves = LeafTable.All.Select(leaf => leaf.Name).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(registered.Except(leaves).Except(CompositeNames), Is.Empty, "registered names with no leaf entry");
            Assert.That(leaves.Except(registered), Is.Empty, "leaf entries with no registered codec");
            Assert.That(leaves.Intersect(CompositeNames), Is.Empty, "names that are both a leaf and a composite");
        });
    }

    [Test]
    public void SampleTypes_CoverEveryLeaf()
    {
        string[] covered = LeafSamples.Types.Select(type => LeafOf(type).Name).Distinct().ToArray();

        Assert.That(LeafTable.All.Select(leaf => leaf.Name).Except(covered), Is.Empty);
    }

    /// <summary>
    /// The exact pairs. The conversions are the pairs whose CLR type differs from the value that the decoded column
    /// stores: 12 for reads and 12 for writes (with <c>FixedString</c> from text), and 2 more of each for the bare
    /// <c>Enum</c> spelling.
    /// </summary>
    [Test]
    public void All_PairCounts_AreTheCodecPairs()
    {
        LeafPair[] reads = LeafTable.All.SelectMany(leaf => leaf.Reads).ToArray();
        LeafPair[] writes = LeafTable.All.SelectMany(leaf => leaf.Writes).ToArray();
        TestContext.Out.WriteLine(Describe());

        Assert.Multiple(() =>
        {
            Assert.That(LeafTable.All, Has.Count.EqualTo(48), "leaves");
            Assert.That(reads, Has.Length.EqualTo(64), "read pairs");
            Assert.That(writes, Has.Length.EqualTo(63), "write pairs");
            Assert.That(reads.Count(pair => pair.IsConversion), Is.EqualTo(14), "read conversions");
            Assert.That(writes.Count(pair => pair.IsConversion), Is.EqualTo(14), "write conversions");
        });
    }

    /// <summary>
    /// The read pairs of the leaf of each sample type, in table order: the type that the decoded column stores, then the
    /// conversions. They are the readings that the client offers for each leaf.
    /// </summary>
    [TestCase("UInt8", "byte")]
    [TestCase("Int8", "sbyte")]
    [TestCase("UInt16", "ushort")]
    [TestCase("Int16", "short")]
    [TestCase("UInt32", "uint")]
    [TestCase("Int32", "int")]
    [TestCase("UInt64", "ulong")]
    [TestCase("Int64", "long")]
    [TestCase("UInt128", "UInt128")]
    [TestCase("Int128", "Int128")]
    [TestCase("UInt256", "UInt256")]
    [TestCase("Int256", "Int256")]
    [TestCase("Bool", "bool")]
    [TestCase("Float32", "float")]
    [TestCase("Float64", "double")]
    [TestCase("BFloat16", "float")]
    [TestCase("String", "string, byte[]")]
    [TestCase("FixedString(4)", "byte[], string")]
    [TestCase("JSON", "string")]
    [TestCase("Date", "DateOnly")]
    [TestCase("Date32", "DateOnly")]
    [TestCase("UUID", "Guid")]
    [TestCase("IPv4", "IPAddress")]
    [TestCase("IPv6", "IPAddress")]
    [TestCase("Nothing", "object")]
    [TestCase("Time", "int, TimeSpan, TimeOnly")]
    [TestCase("Time64(0)", "long, TimeSpan, TimeOnly")]
    [TestCase("Time64(3)", "long, TimeSpan, TimeOnly")]
    [TestCase("Time64(9)", "long, TimeSpan, TimeOnly")]
    [TestCase("DateTime", "uint, DateTimeOffset, DateTime")]
    [TestCase("DateTime('UTC')", "uint, DateTimeOffset, DateTime")]
    [TestCase("DateTime('Asia/Kolkata')", "uint, DateTimeOffset, DateTime")]
    [TestCase("DateTime64(0, 'Europe/Berlin')", "long, DateTimeOffset, DateTime")]
    [TestCase("DateTime64(3)", "long, DateTimeOffset, DateTime")]
    [TestCase("DateTime64(9, 'UTC')", "long, DateTimeOffset, DateTime")]
    [TestCase("Enum8('a' = -1, 'b' = 127, 'c' = 5)", "sbyte, string")]
    [TestCase("Enum16('x' = -32768, 'y' = 32767)", "short, string")]
    [TestCase("Enum('p' = 1, 'q' = 2)", "sbyte, string")]
    [TestCase("Enum('big' = 1000, 'small' = -1)", "short, string")]
    [TestCase("Decimal(9, 2)", "decimal")]
    [TestCase("Decimal(18, 4)", "decimal")]
    [TestCase("Decimal(38, 10)", "ClickHouseTcpDecimal")]
    [TestCase("Decimal(76, 20)", "ClickHouseTcpDecimal")]
    [TestCase("Decimal32(3)", "decimal")]
    [TestCase("Decimal64(6)", "decimal")]
    [TestCase("Decimal128(10)", "ClickHouseTcpDecimal")]
    [TestCase("Decimal256(20)", "ClickHouseTcpDecimal")]
    [TestCase("IntervalNanosecond", "long")]
    [TestCase("IntervalMicrosecond", "long")]
    [TestCase("IntervalMillisecond", "long")]
    [TestCase("IntervalSecond", "long")]
    [TestCase("IntervalMinute", "long")]
    [TestCase("IntervalHour", "long")]
    [TestCase("IntervalDay", "long")]
    [TestCase("IntervalWeek", "long")]
    [TestCase("IntervalMonth", "long")]
    [TestCase("IntervalQuarter", "long")]
    [TestCase("IntervalYear", "long")]
    public void ReadTypes_SampleType_AreTheListedTypes(string type, string readTypes)
        => Assert.That(string.Join(", ", LeafOf(type).ReadTypes(ConverterHarness.Codec(type)).Select(TypeNames.Of)), Is.EqualTo(readTypes));

    /// <summary>
    /// The write pairs of the leaf of each sample type, in table order: the type that the decoded column stores, then the
    /// conversions. They are the CLR types that the client writes each leaf from, without the rules of D6, which apply to
    /// every column type (<see cref="WriteRulesTests"/>). <c>Nothing</c> is written from no type.
    /// </summary>
    [TestCase("UInt8", "byte")]
    [TestCase("Int8", "sbyte")]
    [TestCase("UInt16", "ushort")]
    [TestCase("Int16", "short")]
    [TestCase("UInt32", "uint")]
    [TestCase("Int32", "int")]
    [TestCase("UInt64", "ulong")]
    [TestCase("Int64", "long")]
    [TestCase("UInt128", "UInt128")]
    [TestCase("Int128", "Int128")]
    [TestCase("UInt256", "UInt256")]
    [TestCase("Int256", "Int256")]
    [TestCase("Bool", "bool")]
    [TestCase("Float32", "float")]
    [TestCase("Float64", "double")]
    [TestCase("BFloat16", "float")]
    [TestCase("String", "string, byte[]")]
    [TestCase("FixedString(4)", "byte[], string")]
    [TestCase("JSON", "string")]
    [TestCase("Date", "DateOnly")]
    [TestCase("Date32", "DateOnly")]
    [TestCase("UUID", "Guid")]
    [TestCase("IPv4", "IPAddress")]
    [TestCase("IPv6", "IPAddress")]
    [TestCase("Nothing", "")]
    [TestCase("Time", "int, TimeSpan, TimeOnly")]
    [TestCase("Time64(0)", "long, TimeSpan, TimeOnly")]
    [TestCase("Time64(3)", "long, TimeSpan, TimeOnly")]
    [TestCase("Time64(9)", "long, TimeSpan, TimeOnly")]
    [TestCase("DateTime", "uint, DateTimeOffset, DateTime")]
    [TestCase("DateTime('UTC')", "uint, DateTimeOffset, DateTime")]
    [TestCase("DateTime('Asia/Kolkata')", "uint, DateTimeOffset, DateTime")]
    [TestCase("DateTime64(0, 'Europe/Berlin')", "long, DateTimeOffset, DateTime")]
    [TestCase("DateTime64(3)", "long, DateTimeOffset, DateTime")]
    [TestCase("DateTime64(9, 'UTC')", "long, DateTimeOffset, DateTime")]
    [TestCase("Enum8('a' = -1, 'b' = 127, 'c' = 5)", "sbyte, string")]
    [TestCase("Enum16('x' = -32768, 'y' = 32767)", "short, string")]
    [TestCase("Enum('p' = 1, 'q' = 2)", "sbyte, string")]
    [TestCase("Enum('big' = 1000, 'small' = -1)", "short, string")]
    [TestCase("Decimal(9, 2)", "decimal")]
    [TestCase("Decimal(18, 4)", "decimal")]
    [TestCase("Decimal(38, 10)", "ClickHouseTcpDecimal")]
    [TestCase("Decimal(76, 20)", "ClickHouseTcpDecimal")]
    [TestCase("Decimal32(3)", "decimal")]
    [TestCase("Decimal64(6)", "decimal")]
    [TestCase("Decimal128(10)", "ClickHouseTcpDecimal")]
    [TestCase("Decimal256(20)", "ClickHouseTcpDecimal")]
    [TestCase("IntervalNanosecond", "long")]
    [TestCase("IntervalMicrosecond", "long")]
    [TestCase("IntervalMillisecond", "long")]
    [TestCase("IntervalSecond", "long")]
    [TestCase("IntervalMinute", "long")]
    [TestCase("IntervalHour", "long")]
    [TestCase("IntervalDay", "long")]
    [TestCase("IntervalWeek", "long")]
    [TestCase("IntervalMonth", "long")]
    [TestCase("IntervalQuarter", "long")]
    [TestCase("IntervalYear", "long")]
    public void WriteTypes_SampleType_AreTheListedTypes(string type, string writeTypes)
        => Assert.That(string.Join(", ", LeafOf(type).WriteTypes(ConverterHarness.Codec(type)).Select(TypeNames.Of)), Is.EqualTo(writeTypes));

    /// <summary>Lists each leaf with its read and write types, as a Markdown table.</summary>
    internal static string Describe()
    {
        static string Name(LeafPair pair) => (pair.IsConversion ? "*" : string.Empty) + pair.ClrType.Name;

        var lines = new List<string> { "| Leaf | Reads as | Written from |", "|---|---|---|" };
        foreach (Leaf leaf in LeafTable.All)
        {
            lines.Add($"| {leaf.Name} | {string.Join(", ", leaf.Reads.Select(Name))} | {string.Join(", ", leaf.Writes.Select(Name))} |");
        }

        return string.Join(Environment.NewLine, lines);
    }
}
