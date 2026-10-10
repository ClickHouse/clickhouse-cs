using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers the converting view of <see cref="Block.ReadAs{T}(int)"/>, which a server round-trip cannot observe: its two
/// access paths, its identity, the columns that it refuses, and the answers of <c>CanRead</c> for a composite. The
/// value and error scenarios of the conversions are cases of the differential tests (<see cref="DifferentialCases"/>).
/// </summary>
[TestFixture]
public class ColumnReadProjectionTests
{
    /// <summary>
    /// The converting view converts the whole column on its first access, through the indexer or through
    /// <c>Values</c>, so the two access paths have to agree, and a row past the end has to fail either way round.
    /// </summary>
    [Test]
    public void ReadAs_ConvertingView_AgreesBetweenTheIndexerAndValuesAndBoundsBothWays()
    {
        var ordinals = new ArrayColumn<sbyte>("state", "Enum8('a' = 1, 'b' = 2)", new sbyte[] { 1, 2 });

        IColumn<string> beforeValues = ReadAs<string>(ordinals);
        IColumn<string> afterValues = ReadAs<string>(ordinals);
        _ = afterValues.Values;

        Assert.Multiple(() =>
        {
            Assert.That(beforeValues[1], Is.EqualTo("b"), "first access through the indexer");
            Assert.That(afterValues[1], Is.EqualTo("b"), "first access through Values");
            Assert.That(beforeValues.Values.ToArray(), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(beforeValues.RowCount, Is.EqualTo(2));
            Assert.Throws<IndexOutOfRangeException>(() => _ = beforeValues[2]);
            Assert.Throws<IndexOutOfRangeException>(() => _ = afterValues[2]);
        });
    }

    /// <summary>
    /// A column built by a caller for an insert carries no type string, so there is nothing to resolve a reading
    /// from. Not reachable through a <see cref="Block"/>, whose columns all come off a header.
    /// </summary>
    [Test]
    public void ReadAs_ColumnWithNoTypeString_SaysSoRatherThanFailingToParseIt()
    {
        IColumn<int> built = ClickHouseTcpColumn.Create("v", new[] { 1, 2 });

        Assert.Multiple(() =>
        {
            Assert.That(ReadAs<int>(built), Is.SameAs(built), "the requested type is the column's own, so nothing is resolved");

            var thrown = Assert.Throws<InvalidCastException>(() => ReadAs<long>(built));
            Assert.That(thrown.Message, Does.Contain("carries no ClickHouse type").And.Contain("System.Int32"));
        });
    }

    /// <summary>
    /// Verifies that a converting view preserves source identity and does not dispose the source.
    /// </summary>
    [Test]
    public void ReadAs_ConvertingView_CarriesTheSourcesIdentityAndDisposesNothing()
    {
        var ordinals = new ArrayColumn<sbyte>("state", "Enum8('a' = 1)", new sbyte[] { 1 });

        IColumn<string> projected = ReadAs<string>(ordinals);
        projected.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(projected.Name, Is.EqualTo("state"));
            Assert.That(projected.TypeName, Is.EqualTo("Enum8('a' = 1)"));
            Assert.That(projected.GetValue(0), Is.EqualTo("a"), "the boxed reading is the converted one");
            Assert.That(ordinals.RowCount, Is.EqualTo(1), "the source column is untouched");
            Assert.That(ordinals.Values.ToArray(), Is.EqualTo(new sbyte[] { 1 }));
        });
    }

    /// <summary>
    /// Verifies the error when a caller-built composite lacks its decoded columnar surface.
    /// </summary>
    [Test]
    public void ReadAs_CompositeColumnWithoutItsColumnarSurface_SaysWhichSurfaceItLacks()
    {
        Assert.Multiple(() =>
        {
            AssertLacksSurface<byte[]>("Nullable(String)", "INullableColumn");
            AssertLacksSurface<byte[][]>("Array(String)", "IArrayColumn");
            AssertLacksSurface<KeyValuePair<string, byte[]>[]>("Map(String, String)", "IMapColumn");
            AssertLacksSurface<ValueTuple<byte[]>>("Tuple(String)", "ITupleColumn");
            AssertLacksSurface<byte[]>("LowCardinality(String)", "ILowCardinalityColumn");
        });
    }

    [Test]
    [TestCase("Array(DateTime('UTC'))", typeof(DateTime[]))]
    [TestCase("Tuple(DateTime('UTC'), Time)", typeof((DateTime, TimeSpan)))]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, DateTime>[]))]
    [TestCase("Nullable(DateTime('UTC'))", typeof(DateTime?))]
    public void CanRead_CompositeOfConvertingChildren_IsTrue(string type, Type target)
        => Assert.That(ClickHouseTcpTypes.CanRead(type, target), Is.True);

    /// <summary>
    /// Verifies that JSON remains text-only despite using String serialization.
    /// </summary>
    [Test]
    public void CanRead_JsonAsBytes_IsFalse()
        => Assert.That(ClickHouseTcpTypes.CanRead("JSON", typeof(byte[])), Is.False);

    private static IColumn<T> ReadAs<T>(IColumn column)
        => ColumnCodecRegistry.Default.Projections.ReadAs<T>(column, new ResolveContext { ServerTimezone = "UTC" });

    private static void AssertLacksSurface<T>(string type, string surface)
    {
        using var mislabelled = new ArrayColumn<string>("c", type, new[] { "a" });

        var thrown = Assert.Throws<InvalidOperationException>(() => ReadAs<T>(mislabelled));

        Assert.That(thrown.Message, Does.Contain($"Column 'c' ({type})").And.Contain(surface));
    }
}
