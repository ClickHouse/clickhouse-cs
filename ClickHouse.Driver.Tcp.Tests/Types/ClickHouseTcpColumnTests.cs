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
    public void Create_NullArguments_Throw()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create(null, new[] { 1 }));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create<int>("id", (int[])null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create<int>("id", (IEnumerable<int>)null));
        });
    }

    [Test]
    public void CreateArray_FlatElementsAndOffsets_PresentsTheRowsThoseOffsetsDescribe()
    {
        IArrayColumn<uint> column = ClickHouseTcpColumn.CreateArray(
            "tags",
            ClickHouseTcpColumn.Create("tags", new uint[] { 10, 20, 30 }),
            new[] { 0, 2, 2, 3 });

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(3), "one row per offset pair");
            Assert.That(column.TypeName, Is.Null, "the insert takes the type from the target's schema");
            Assert.That(column.ElementType, Is.EqualTo(typeof(uint[])));
            Assert.That(column.Offsets.ToArray(), Is.EqualTo(new[] { 0, 2, 2, 3 }));
            Assert.That(column.InnerValues.ToArray(), Is.EqualTo(new uint[] { 10, 20, 30 }));
            Assert.That(column.GetValue(0), Is.EqualTo(new uint[] { 10, 20 }));
            Assert.That(column.GetValue(1), Is.EqualTo(Array.Empty<uint>()), "two equal offsets are an empty row");
            Assert.That(column.GetValue(2), Is.EqualTo(new uint[] { 30 }));
        });
    }

    [Test]
    public void CreateArray_OneLeadingOffsetAndNoElements_IsAZeroRowColumn()
    {
        IArrayColumn<uint> column = ClickHouseTcpColumn.CreateArray(
            "tags",
            ClickHouseTcpColumn.Create("tags", Array.Empty<uint>()),
            new[] { 0 });

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(0));
            Assert.That(column.Offsets.ToArray(), Is.EqualTo(new[] { 0 }));
        });
    }

    /// <summary>
    /// The offsets decide which elements each row claims, so a wrong one either reads past the elements or sends
    /// the server rows the caller did not build. Each message says which rule was broken.
    /// </summary>
    [Test]
    public void CreateArray_OffsetsThatDoNotDescribeTheElements_AreRefusedWithTheRuleTheyBreak()
    {
        IColumn<uint> inner = ClickHouseTcpColumn.Create("tags", new uint[] { 10, 20, 30 });

        Assert.Multiple(() =>
        {
            Assert.That(
                Assert.Throws<ArgumentException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, Array.Empty<int>())).Message,
                Does.Contain("are empty"));
            Assert.That(
                Assert.Throws<ArgumentException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, new[] { 1, 3 })).Message,
                Does.Contain("start at 1"));
            Assert.That(
                Assert.Throws<ArgumentException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, new[] { 0, 2, 1, 3 })).Message,
                Does.Contain("go backwards at row 1"));
            Assert.That(
                Assert.Throws<ArgumentException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, new[] { 0, 2 })).Message,
                Does.Contain("end at 2").And.Contain("holds 3 elements"));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.CreateArray<uint>("tags", null, new[] { 0 }));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.CreateArray("tags", inner, null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.CreateArray(null, inner, new[] { 0, 3 }));
        });
    }
}
