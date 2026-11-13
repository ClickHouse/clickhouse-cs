using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using static ClickHouse.Driver.Tcp.Tests.Poco.PocoWritePlanTests;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The gather tiers of <see cref="PocoColumnBuilderFactory"/>: which tier a runtime gets, that each tier fails on one of
/// the failing rows, also on the failure of a property getter, and that the row inserts write
/// through the converter trees (a LowCardinality dictionary of distinct encoded values). <see cref="PocoWritePlanTests"/>
/// and <see cref="PocoWritePlanDelegateTests"/> gather through each tier.
/// </summary>
[TestFixture]
public class PocoColumnBuilderFactoryTests
{
    [Test]
    public void SelectTier_NoForcedTier_PrefersTheCompiledLoopWhereverTreesCompile()
    {
        PocoGatherTier expected = RuntimeFeature.IsDynamicCodeCompiled ? PocoGatherTier.Compiled : PocoGatherTier.Delegate;

        Assert.That(PocoColumnBuilderFactory.SelectTier(null), Is.EqualTo(expected));
    }

    [Test]
    public void SelectTier_ForcedTier_IsHonored()
    {
        Assert.Multiple(() =>
        {
            foreach (PocoGatherTier tier in Enum.GetValues<PocoGatherTier>())
            {
                Assert.That(PocoColumnBuilderFactory.SelectTier(tier), Is.EqualTo(tier));
            }
        });
    }

    [Test]
    public void WritePlanFor_DifferentForcedTiers_BuildTheirOwnPlans()
    {
        Block schema = SchemaOf(Target("value", "Int32"));

        PocoWritePlan<Row<int>> compiled = new PocoTypeRegistry { ForcedGatherTier = PocoGatherTier.Compiled }.WritePlanFor<Row<int>>(schema);
        PocoWritePlan<Row<int>> viaDelegate = new PocoTypeRegistry { ForcedGatherTier = PocoGatherTier.Delegate }.WritePlanFor<Row<int>>(schema);

        Assert.That(viaDelegate, Is.Not.SameAs(compiled));
    }

    /// <summary>
    /// Two columns of which several rows fail: the getter of <c>Number</c> at row 2, the NULL of <c>Other</c> at rows 0 and
    /// 1, and in <c>Mixed</c> a NULL at row 1 and a getter that fails at row 3. Each tier fails on one of them, and a NULL
    /// failure names the property and its row.
    /// </summary>
    [TestCase("Number", "Other")]
    [TestCase("Other", "Number")]
    [TestCase("Mixed", "Number")]
    public void Gather_AGetterThatThrowsAndANull_ThrowOneOfThemInEveryTier(string first, string second)
    {
        Block schema = SchemaOf(Target(first, "Int32"), Target(second, "Int32"));
        ThrowingRow[] rows = Enumerable.Range(0, 4).Select(i => new ThrowingRow(i)).ToArray();
        (Type, string)[] failures = new[] { first, second }.SelectMany(Failures).ToArray();

        Assert.Multiple(() =>
        {
            foreach (PocoGatherTier tier in Enum.GetValues<PocoGatherTier>())
            {
                Exception failure = Gather(PocoWritePlan<ThrowingRow>.Build(PocoTypeDescriptor<ThrowingRow>.Build(), schema, tier), rows);
                ConverterHarness.AssertFailsOnOneOf(failure, tier.ToString(), failures);
            }
        });
    }

    [Test]
    public void Gather_PropertyDeclaredOnABaseClass_IsReadInEveryTier()
    {
        Block schema = SchemaOf(Target("Inherited", "String"));
        var rows = new[] { new DerivedRow { Inherited = "a" }, new DerivedRow { Inherited = "b" } };

        Assert.Multiple(() =>
        {
            foreach (PocoGatherTier tier in Enum.GetValues<PocoGatherTier>())
            {
                using var buffer = PocoRowBuffer<DerivedRow>.Create(rows, "rows", rows.Length, CancellationToken.None);
                using PocoInsertSource<DerivedRow> source = PocoWritePlan<DerivedRow>.Build(PocoTypeDescriptor<DerivedRow>.Build(), schema, tier).CreateSource(buffer, rows.Length);
                source.Gather(0, rows.Length);

                Assert.That(Insert(schema, source), Is.EqualTo(new byte[] { 1, (byte)'a', 1, (byte)'b' }), tier.ToString());
            }
        });
    }

    /// <summary>
    /// The row inserts write a LowCardinality dictionary through the converter tree, which keeps each distinct encoded
    /// value once: two lone surrogates encode to the same UTF-8 bytes (<c>EF BF BD</c>), so they share one entry.
    /// </summary>
    [Test]
    public void Insert_TwoLoneSurrogatesIntoALowCardinalityColumn_ShareOneDictionaryEntry()
    {
        Block schema = SchemaOf(Target("value", "LowCardinality(String)"));
        var rows = new[] { new Row<string> { Value = "\uD800" }, new Row<string> { Value = "\uDBFF" } };
        using var buffer = PocoRowBuffer<Row<string>>.Create(rows, "rows", rows.Length, CancellationToken.None);
        using PocoInsertSource<Row<string>> poco = PocoWritePlan<Row<string>>.Build(PocoTypeDescriptor<Row<string>>.Build(), schema).CreateSource(buffer, rows.Length);
        poco.Gather(0, rows.Length);
        object[][] untypedRows = { new object[] { "\uD800" }, new object[] { "\uDBFF" } };
        using var untypedBuffer = PocoRowBuffer<object[]>.Create(untypedRows, "rows", untypedRows.Length, CancellationToken.None);
        using PocoInsertSource<object[]> untyped = UntypedRowColumns.CreateSource(schema, untypedBuffer, untypedRows.Length);
        untyped.Gather(0, untypedRows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(DictionarySize(Insert(schema, poco)), Is.EqualTo(2), "InsertRowsAsync<T>: the placeholder and one entry");
            Assert.That(DictionarySize(Insert(schema, untyped)), Is.EqualTo(2), "InsertRowsAsync(object[]): the placeholder and one entry");
        });
    }

    // The failures of the rows of one property of ThrowingRow.
    private static (Type, string)[] Failures(string property) => property switch
    {
        "Number" => new[] { (typeof(InvalidOperationException), "The getter fails at row 2.") },
        "Other" => new[] { NullAt("Other", 0), NullAt("Other", 1) },
        "Mixed" => new[] { NullAt("Mixed", 1), (typeof(InvalidOperationException), "The getter fails at row 3.") },
        _ => throw new ArgumentOutOfRangeException(nameof(property), property, "ThrowingRow has no such property."),
    };

    private static (Type, string) NullAt(string property, int row) => (typeof(InvalidOperationException), $"Property 'ThrowingRow.{property}' is null at row {row} of the insert");

    // A LowCardinality column: the Int64 version prefix, the Int64 flags of the body, then the Int64 count of the
    // dictionary, whose slot 0 holds the placeholder of a column with no NULL.
    private static long DictionarySize(byte[] bytes) => BitConverter.ToInt64(bytes, 16);

    private static Exception Gather<T>(PocoWritePlan<T> plan, T[] rows)
        where T : class
    {
        using var buffer = PocoRowBuffer<T>.Create(rows, "rows", rows.Length, CancellationToken.None);
        using PocoInsertSource<T> source = plan.CreateSource(buffer, rows.Length);
        try
        {
            source.Gather(0, rows.Length);
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    /// <summary>
    /// A row whose <c>Number</c> getter throws at row 2, whose <c>Other</c> is null at row 0 and row 1, and whose
    /// <c>Mixed</c> is null at row 1 and throws at row 3.
    /// </summary>
    internal sealed class ThrowingRow
    {
        private readonly int row;

        public ThrowingRow(int row) => this.row = row;

        public int Number => row == 2 ? throw new InvalidOperationException($"The getter fails at row {row}.") : row;

        public int? Other => row is 0 or 1 ? null : row;

        public int? Mixed => row == 3 ? throw new InvalidOperationException($"The getter fails at row {row}.") : row == 1 ? null : row;
    }

    internal class BaseRow
    {
        public string Inherited { get; set; }
    }

    internal sealed class DerivedRow : BaseRow
    {
    }
}
