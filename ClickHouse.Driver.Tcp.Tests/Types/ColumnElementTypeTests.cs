using System;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>Tests <see cref="IColumn.ElementType"/> resolution.</summary>
[TestFixture]
public class ColumnElementTypeTests
{
    [Test]
    public void ElementType_ArrayBackedColumn_IsItsGenericArgument()
    {
        IColumn scalars = new ArrayColumn<uint>("c", "UInt32", Array.Empty<uint>());
        IColumn rows = new ArrayColumn<uint[]>("c", "Array(UInt32)", Array.Empty<uint[]>());
        IColumn nullables = new ArrayColumn<DateTime?>("c", "Nullable(DateTime)", Array.Empty<DateTime?>());

        Assert.Multiple(() =>
        {
            Assert.That(scalars.ElementType, Is.EqualTo(typeof(uint)));
            Assert.That(rows.ElementType, Is.EqualTo(typeof(uint[])));
            Assert.That(nullables.ElementType, Is.EqualTo(typeof(DateTime?)));
        });
    }

    [Test]
    public void ElementType_AskedTwice_IsTheSameAnswer()
    {
        IColumn column = new ArrayColumn<string>("c", "String", Array.Empty<string>());

        Assert.That(column.ElementType, Is.EqualTo(column.ElementType).And.EqualTo(typeof(string)));
    }

    [Test]
    public void ElementType_ColumnSurfacingTwoElementTypes_Throws()
        => Assert.That(
            () => ((IColumn)new AmbiguousColumn()).ElementType,
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("more than once"));

    [Test]
    public void ElementType_ColumnSurfacingNoElementType_Throws()
        => Assert.That(
            () => ((IColumn)new UntypedColumn()).ElementType,
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("does not implement"));

    private sealed class AmbiguousColumn : IColumn<int>, IColumn<string>
    {
        public string Name => string.Empty;

        public string TypeName => string.Empty;

        public int RowCount => 0;

        int IColumn<int>.this[int row] => 0;

        string IColumn<string>.this[int row] => string.Empty;

        ReadOnlySpan<int> IColumn<int>.Values => default;

        ReadOnlySpan<string> IColumn<string>.Values => default;

        public object GetValue(int row) => null;

        public void Dispose()
        {
        }
    }

    private sealed class UntypedColumn : IColumn
    {
        public string Name => string.Empty;

        public string TypeName => string.Empty;

        public int RowCount => 0;

        public object GetValue(int row) => null;

        public void Dispose()
        {
        }
    }
}
