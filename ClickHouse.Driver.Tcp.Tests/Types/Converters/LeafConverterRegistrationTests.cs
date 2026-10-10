using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Pins what the differential tests compare for the leaves: every pair of the leaf table is compared there with values
/// or bytes of the client's entry points, except the pairs of
/// <see cref="NotInTheCaseList"/>, which no case of the case list reaches. <see cref="LeafReaderTests"/> and
/// <see cref="LeafWriterTests"/> test those pairs themselves.
/// </summary>
[TestFixture]
public class LeafConverterRegistrationTests
{
    /// <summary>
    /// The pairs that no differential case compares: the interval units other than <c>Second</c> and <c>Day</c>,
    /// <c>Nothing</c>, the bare <c>Enum</c> spelling, and the <c>Decimal32</c>, <c>Decimal128</c> and
    /// <c>Decimal256</c> spellings. They use the same leaf classes as pairs that the cases compare
    /// (<c>Int64</c>, the enums, <c>Decimal(P, S)</c>).
    /// </summary>
    internal static readonly IReadOnlySet<LeafPairKey> NotInTheCaseList = BuildNotInTheCaseList();

    /// <summary>The compared pairs and the pairs of <see cref="NotInTheCaseList"/> together are the leaf table.</summary>
    [Test]
    public void Run_EveryCaseOfALeafType_ComparesEveryLeafPairExceptTheListedOnes()
    {
        var compared = new HashSet<LeafPairKey>();
        foreach (DifferentialCase testCase in DifferentialCases.All().Where(c => IsLeafType(c.ColumnType)))
        {
            string leaf = LeafName(testCase.ColumnType);
            foreach (FacetResult result in DifferentialEngine.ForCurrentRegistry(testCase).Facets)
            {
                Facet facet = result.Facet;
                if (facet.Tier == Tier.ReadAs && result.Baseline.All.Kind == OutcomeKind.Values)
                {
                    compared.Add(new LeafPairKey(leaf, facet.Target, ConversionDirection.Read));
                }

                if (facet.Tier == Tier.Write
                    && facet.Input.Kind != WriteInputKind.Decoded
                    && result.Baseline.All.Kind == OutcomeKind.Bytes)
                {
                    compared.Add(new LeafPairKey(leaf, facet.Input.ElementType, ConversionDirection.Write));
                }
            }
        }

        LeafPairKey[] table = LeafTable.All
            .SelectMany(leaf => leaf.Reads.Select(pair => new LeafPairKey(leaf.Name, pair.ClrType, ConversionDirection.Read))
                .Concat(leaf.Writes.Select(pair => new LeafPairKey(leaf.Name, pair.ClrType, ConversionDirection.Write))))
            .Distinct()
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(table.Except(compared).Except(NotInTheCaseList), Is.Empty, "pairs that neither the cases nor the listed pairs compare");
            Assert.That(NotInTheCaseList.Intersect(compared), Is.Empty, "listed pairs that a case now compares: remove them from the list");
        });
    }

    /// <summary>Whether the derivation of <paramref name="columnType"/> ends at a leaf.</summary>
    /// <param name="columnType">The ClickHouse type of the case.</param>
    /// <returns>Whether the type is a leaf, or a <c>SimpleAggregateFunction</c> of a leaf.</returns>
    internal static bool IsLeafType(string columnType) => LeafTable.TryGet(LeafName(columnType), out _);

    /// <summary>The leaf name of a type string, as the derivation finds it.</summary>
    internal static string LeafName(string columnType)
    {
        TypeNode node = TypeParser.Parse(columnType);
        while (Canonical(node.Name) == "SimpleAggregateFunction")
        {
            node = node.Arguments[1];
        }

        return Canonical(node.Name);
    }

    private static string Canonical(string name) => ColumnCodecRegistry.Default.TryCanonicalName(name, out string canonical) ? canonical : name;

    private static HashSet<LeafPairKey> BuildNotInTheCaseList()
    {
        var pairs = new List<LeafPairKey>();
        void Both(string leaf, Type clrType)
        {
            pairs.Add(new LeafPairKey(leaf, clrType, ConversionDirection.Read));
            pairs.Add(new LeafPairKey(leaf, clrType, ConversionDirection.Write));
        }

        foreach (string unit in new[] { "Nanosecond", "Microsecond", "Millisecond", "Minute", "Hour", "Week", "Month", "Quarter", "Year" })
        {
            Both("Interval" + unit, typeof(long));
        }

        pairs.Add(new LeafPairKey("Nothing", typeof(object), ConversionDirection.Read));
        Both("Enum", typeof(sbyte));
        Both("Enum", typeof(short));
        Both("Enum", typeof(string));
        Both("Decimal256", typeof(ClickHouseTcpDecimal));
        return pairs.ToHashSet();
    }
}

/// <summary>A (leaf, CLR type, direction) pair of the leaf table.</summary>
internal readonly record struct LeafPairKey(string Leaf, Type ClrType, ConversionDirection Direction)
{
    public override string ToString() => $"{Direction} {Leaf} {ClrType.Name}";
}
