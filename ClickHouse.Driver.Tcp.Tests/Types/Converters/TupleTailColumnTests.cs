using System;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

[TestFixture]
public class TupleTailColumnTests
{
    private const string Type = "Tuple(a Int32, b Int32, c Int32, d Int32, e Int32, f Int32, g Int32, h String, i Int64)";

    [Test]
    public void Constructor_NamedTuple_GivesTheChildrenAndNamesAfterTheOffset()
    {
        using IColumn source = DecodedColumns.Of("c", Type, (1, 2, 3, 4, 5, 6, 7, "eight", 9L), (-1, -2, -3, -4, -5, -6, -7, string.Empty, -9L));
        var tuple = (ITupleColumn)source;

        using var tail = new TupleTailColumn(tuple, 7);

        Assert.Multiple(() =>
        {
            Assert.That(tail.Children, Is.EqualTo(new[] { tuple.Children[7], tuple.Children[8] }));
            Assert.That(tail.FieldNames, Is.EqualTo(new[] { "h", "i" }));
            Assert.That(tail.Name, Is.EqualTo("c"));
            Assert.That(tail.TypeName, Is.EqualTo(Type));
            Assert.That(tail.RowCount, Is.EqualTo(2));
            Assert.That(() => tail.GetValue(0), Throws.TypeOf<NotSupportedException>());
        });
    }
}
