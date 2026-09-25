using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Tests.Types;

// The round-trip behaviour lives in PublicSurfaceIntegrationTests. This covers what an insert cannot show:
// the shape of the column the factory hands back, and that it takes over the caller's array rather than
// copying it.
[TestFixture]
public class ClickHouseTcpColumnTests
{
    [Test]
    public void Create_FromArray_ReportsNameRowCountAndNoTypeName()
    {
        IColumn<int> column = ClickHouseTcpColumn.Create("id", new[] { 1, 2, 3 });

        Assert.Multiple(() =>
        {
            Assert.That(column.Name, Is.EqualTo("id"));
            Assert.That(column.RowCount, Is.EqualTo(3));

            // No header to take one from: the insert resolves the type from the target's schema.
            Assert.That(column.TypeName, Is.Null);
            Assert.That(column.ElementType, Is.EqualTo(typeof(int)));
        });
    }

    [Test]
    public void Create_FromArray_TakesOverTheArrayWithoutCopying()
    {
        var values = new[] { 1, 2, 3 };
        IColumn<int> column = ClickHouseTcpColumn.Create("id", values);

        values[0] = 99;

        Assert.That(column.Values[0], Is.EqualTo(99));
    }

    [Test]
    public void Create_FromAList_DoesNotAliasTheList()
    {
        // A List<T> is not a T[], so it is copied; mutating the list afterwards must not reach the column.
        var values = new List<int> { 1, 2, 3 };
        IColumn<int> column = ClickHouseTcpColumn.Create("id", values);

        values[0] = 99;

        Assert.That(column.Values[0], Is.EqualTo(1));
    }

    [Test]
    public void Create_FromASequenceThatIsAlreadyAnArray_AvoidsTheCopy()
    {
        var values = new[] { 1, 2, 3 };
        IColumn<int> column = ClickHouseTcpColumn.Create("id", values.AsEnumerable());

        values[0] = 99;

        Assert.That(column.Values[0], Is.EqualTo(99));
    }

    [Test]
    public void Create_EmptyArray_IsAZeroRowColumn()
    {
        IColumn<string> column = ClickHouseTcpColumn.Create("name", Array.Empty<string>());

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(0));
            Assert.That(column.Values.Length, Is.EqualTo(0));
        });
    }

    [Test]
    public void Create_JaggedRows_HasTheArrayAsItsElementType()
    {
        IColumn<uint[]> column = ClickHouseTcpColumn.Create("tags", new[] { new uint[] { 1, 2 }, Array.Empty<uint>() });

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(2));
            Assert.That(column.ElementType, Is.EqualTo(typeof(uint[])));
            Assert.That(column.GetValue(0), Is.EqualTo(new uint[] { 1, 2 }));
        });
    }

    [Test]
    public void CreateOrCreateArray_NullArguments_Throw()
    {
        using IColumn<uint> inner = ClickHouseTcpColumn.Create("tags", new uint[] { 1 });

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create(null, new[] { 1 }));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create<int>("id", (int[])null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create<int>("id", (IEnumerable<int>)null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.CreateArray<uint>("tags", null, new[] { 0 }));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.CreateArray(null, inner, new[] { 0, 1 }));
        });
    }

    [TestCaseSource(nameof(ArrayShapes))]
    public void CreateArray_FlatElementsAndOffsets_PresentsTheRowsThoseOffsetsDescribe(
        uint[] elements,
        int[] offsets,
        uint[][] expectedRows)
    {
        IArrayColumn<uint> column = ClickHouseTcpColumn.CreateArray(
            "tags",
            ClickHouseTcpColumn.Create("tags", elements),
            offsets);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(expectedRows.Length), "one row per offset pair");
            Assert.That(column.TypeName, Is.Null, "the insert takes the type from the target's schema");
            Assert.That(column.ElementType, Is.EqualTo(typeof(uint[])));
            Assert.That(column.Offsets.ToArray(), Is.EqualTo(offsets));
            Assert.That(column.InnerValues.ToArray(), Is.EqualTo(elements));
            Assert.That(column.Values.ToArray(), Is.EqualTo(expectedRows));
        });
    }

    /// <summary>
    /// The offsets decide which elements each row claims, so a wrong one either reads past the elements or sends
    /// the server rows the caller did not build. Each message says which rule was broken.
    /// </summary>
    [TestCaseSource(nameof(InvalidArrayOffsets))]
    public void CreateArray_OffsetsThatDoNotDescribeTheElements_AreRefusedWithTheRuleTheyBreak(
        int[] offsets,
        string[] messageFragments)
    {
        IColumn<uint> inner = ClickHouseTcpColumn.Create("tags", new uint[] { 10, 20, 30 });
        var thrown = Assert.Throws<ArgumentException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, offsets));

        Assert.Multiple(() =>
        {
            foreach (string fragment in messageFragments)
            {
                Assert.That(thrown.Message, Does.Contain(fragment));
            }
        });
    }

    private static IEnumerable<TestCaseData> ArrayShapes()
    {
        yield return new TestCaseData(
                new uint[] { 10, 20, 30 },
                new[] { 0, 2, 2, 3 },
                new[] { new uint[] { 10, 20 }, Array.Empty<uint>(), new uint[] { 30 } })
            .SetName("CreateArray_FlatElementsAndOffsets_PresentsThreeRows");
        yield return new TestCaseData(Array.Empty<uint>(), new[] { 0 }, Array.Empty<uint[]>())
            .SetName("CreateArray_OneLeadingOffsetAndNoElements_IsAZeroRowColumn");
    }

    private static IEnumerable<TestCaseData> InvalidArrayOffsets()
    {
        yield return InvalidArrayOffsetsCase(Array.Empty<int>(), "are empty");
        yield return InvalidArrayOffsetsCase(new[] { 1, 3 }, "start at 1");
        yield return InvalidArrayOffsetsCase(new[] { 0, 2, 1, 3 }, "go backwards at row 1");
        yield return InvalidArrayOffsetsCase(new[] { 0, 2 }, "end at 2", "holds 3 elements");
    }

    private static TestCaseData InvalidArrayOffsetsCase(int[] offsets, params string[] messageFragments)
        => new TestCaseData(offsets, messageFragments);
}
