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
}
