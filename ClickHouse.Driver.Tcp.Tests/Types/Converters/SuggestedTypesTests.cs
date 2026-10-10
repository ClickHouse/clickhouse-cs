using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The CLR types that the error messages suggest (<see cref="ConverterDerivation.SuggestedTypes"/>): each one derives,
/// and a leaf suggests every CLR type of its pairs in the leaf table.
/// </summary>
[TestFixture]
public class SuggestedTypesTests
{
    private static readonly ResolveContext Context = ResolveContext.ForWrite;

    private static IEnumerable<string> LeafNames() => LeafTable.All.Select(leaf => leaf.Name).OrderBy(name => name, StringComparer.Ordinal);

    [TestCaseSource(nameof(LeafNames))]
    public void SuggestedTypes_LeafAndTheTypesAroundIt_DeriveEveryTypeAndListThePairsOfTheLeaf(string leafName)
    {
        LeafTable.TryGet(leafName, out Leaf leaf);
        var problems = new List<string>();
        foreach (string instance in Instances(leafName))
        {
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(instance, in Context);
            foreach ((string form, bool lifted) in Forms(instance))
            {
                foreach (ConversionDirection direction in Enum.GetValues<ConversionDirection>())
                {
                    IReadOnlyList<Type> suggested = ConverterDerivation.Default.SuggestedTypes(form, in Context, direction);
                    problems.AddRange(suggested
                        .Where(type => !ConverterDerivation.Default.Derive(form, in Context, type, direction).Succeeded)
                        .Select(type => $"{form} {direction}: {type} does not derive."));

                    // JSON cannot be a LowCardinality dictionary entry, so LowCardinality(JSON) is written from no type.
                    IEnumerable<Type> pairs = direction == ConversionDirection.Read ? leaf.ReadTypes(codec) : leaf.WriteTypes(codec);
                    Type[] expected = leafName == "JSON" && direction == ConversionDirection.Write && form.StartsWith("LowCardinality(", StringComparison.Ordinal)
                        ? Array.Empty<Type>()
                        : pairs.Select(type => lifted && type.IsValueType ? typeof(Nullable<>).MakeGenericType(type) : type).ToArray();
                    if (!suggested.SequenceEqual(expected))
                    {
                        problems.Add($"{form} {direction}: suggests [{string.Join(", ", suggested)}], not [{string.Join(", ", expected)}].");
                    }
                }
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    [TestCase("FixedString(4)", true, new[] { typeof(byte[]), typeof(string) })]
    [TestCase("LowCardinality(String)", true, new[] { typeof(string), typeof(byte[]) })]
    [TestCase("LowCardinality(Nullable(DateTime('UTC')))", false, new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) })]
    [TestCase("Nullable(String)", false, new[] { typeof(string), typeof(byte[]) })]
    [TestCase("SimpleAggregateFunction(sum, UInt64)", true, new[] { typeof(ulong) })]
    [TestCase("Array(DateTime('UTC'))", false, new[] { typeof(uint[]) })]
    [TestCase("Nullable(Tuple(String, UInt8))", true, new[] { typeof((string, byte)?) })]
    [TestCase("Variant(String, UInt64)", true, new[] { typeof(object) })]
    [TestCase("Nested(a UInt8)", false, new[] { typeof(object[][]) })]
    [TestCase("Nested(a UInt8)", true, new Type[0])]
    [TestCase("Nothing", true, new Type[0])]
    [TestCase("LowCardinality(JSON)", true, new Type[0])]
    public void SuggestedTypes_ColumnType_IsTheListOfTheMessages(string type, bool write, Type[] expected)
        => Assert.That(
            ConverterDerivation.Default.SuggestedTypes(type, in Context, write ? ConversionDirection.Write : ConversionDirection.Read),
            Is.EqualTo(expected));

    // The type strings of one leaf: its name, with the parameters that the leaf needs.
    private static IEnumerable<string> Instances(string leafName) => leafName switch
    {
        "FixedString" => new[] { "FixedString(4)" },
        "DateTime" => new[] { "DateTime", "DateTime('Europe/Berlin')" },
        "DateTime64" => new[] { "DateTime64(3)", "DateTime64(9, 'UTC')" },
        "Time64" => new[] { "Time64(3)" },
        "Enum8" => new[] { "Enum8('a' = 1, 'b' = 2)" },
        "Enum16" => new[] { "Enum16('a' = 1000)" },
        "Enum" => new[] { "Enum('a' = 1)", "Enum('a' = 1000)" },
        "Decimal" => new[] { "Decimal(9, 2)", "Decimal(38, 10)" },
        "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256" => new[] { $"{leafName}(2)" },
        _ => new[] { leafName },
    };

    // The leaf, and the types around it that the client resolves: (type, whether a NULL is in it).
    private static IEnumerable<(string Form, bool Lifted)> Forms(string instance)
        => new[]
            {
                (instance, false),
                ($"Nullable({instance})", true),
                ($"LowCardinality({instance})", false),
                ($"LowCardinality(Nullable({instance}))", true),
                ($"SimpleAggregateFunction(anyLast, {instance})", false),
            }
            .Where(form => Resolves(form.Item1));

    private static bool Resolves(string type)
    {
        try
        {
            ColumnCodecRegistry.Default.Resolve(type, in Context);
            return true;
        }
        catch (Exception e) when (e is FormatException or NotSupportedException)
        {
            return false;
        }
    }
}
