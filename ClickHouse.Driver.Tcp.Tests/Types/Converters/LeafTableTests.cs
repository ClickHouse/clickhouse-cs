using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Pins the leaf table: the counts of its pairs, every registered name as a leaf or a composite, the derivation of a leaf
/// reads as exactly the CLR types of the leaf's read pairs, and each leaf writes from exactly the CLR types that its codec
/// writes (<see cref="IColumnCodec.CanWriteElementType"/>). The one write pair that the table adds is
/// <c>FixedString</c> from <see cref="string"/> (<see cref="Additions"/>).
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

    // CLR types to ask about: every type in the table, and types that no leaf offers.
    private static readonly Type[] Candidates = LeafTable.All
        .SelectMany(leaf => leaf.Reads.Concat(leaf.Writes))
        .Select(pair => pair.ClrType)
        .Concat(new[]
        {
            typeof(int?), typeof(DateTime?), typeof(string[]), typeof(object), typeof(ValueType), typeof(Enum),
            typeof(DayOfWeek), typeof(char), typeof(Half), typeof(ReadOnlyMemory<byte>), typeof(System.Numerics.BigInteger),
        })
        .Distinct()
        .ToArray();

    public static IEnumerable<string> SampleTypes => LeafSamples.Types;

    /// <summary>The write pairs of the table that no codec writes today: <c>FixedString</c> from text.</summary>
    internal static readonly (string Leaf, Type ClrType)[] Additions = { ("FixedString", typeof(string)) };

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

    [TestCaseSource(nameof(SampleTypes))]
    public void WriteTypes_SampleType_AreTheWritableTypesOfItsCodec(string type)
    {
        IColumnCodec codec = ConverterHarness.Codec(type);
        Leaf leaf = LeafOf(type);
        Type[] writable = codec.WritableElementTypes.Where(codec.CanWriteElementType)
            .Concat(Additions.Where(addition => addition.Leaf == leaf.Name).Select(addition => addition.ClrType))
            .ToArray();

        Assert.That(leaf.WriteTypes(codec), Is.EquivalentTo(writable));
    }

    /// <summary>
    /// The derivation reads as exactly the CLR types of the leaf's read pairs, and writes from exactly the CLR types that
    /// the codec writes and the <see cref="Additions"/>. These are the leaf's own readings and writes
    /// (<see cref="ConverterDerivation.DeriveNode"/>), without the rules of D6, which apply to every column type
    /// (<see cref="ReadRulesTests"/>, <see cref="WriteRulesTests"/>).
    /// </summary>
    [TestCaseSource(nameof(SampleTypes))]
    public void Derive_EveryCandidateType_AgreesWithTheCurrentAnswers(string type)
    {
        IColumnCodec codec = ConverterHarness.Codec(type);
        Leaf leafOfType = LeafOf(type);
        string leaf = leafOfType.Name;
        Type[] readTypes = leafOfType.ReadTypes(codec).ToArray();
        TypeNode root = TypeParser.Parse(type);
        var disagreements = new List<string>();
        foreach (Type candidate in Candidates)
        {
            bool reads = ConverterDerivation.Default.DeriveNode(root, root, ConverterHarness.Context, candidate, ConversionDirection.Read).Succeeded;
            bool writes = ConverterDerivation.Default.DeriveNode(root, root, ConverterHarness.Context, candidate, ConversionDirection.Write).Succeeded;
            if (reads != readTypes.Contains(candidate))
            {
                disagreements.Add($"read as {candidate}: derived {reads}");
            }

            bool added = Additions.Contains((leaf, candidate));
            if (writes != (codec.CanWriteElementType(candidate) || added))
            {
                disagreements.Add($"write from {candidate}: derived {writes}");
            }
        }

        Assert.That(disagreements, Is.Empty);
    }

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
