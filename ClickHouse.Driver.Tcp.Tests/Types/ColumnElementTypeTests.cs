using System;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers <see cref="IColumn.ElementType"/>, the default interface member a codec uses to interrogate a column whose
/// <c>T</c> it does not know statically. Everything here is API surface: the resolution is reflection over the
/// implemented interface, so the cases that matter are the ones no real column reaches.
/// </summary>
[TestFixture]
public class ColumnElementTypeTests
{
    /// <summary>
    /// Read through <see cref="IColumn"/>, which is how every codec reaches it: a default interface member is not
    /// visible on the implementing class without a cast.
    /// </summary>
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

    /// <summary>The answer is cached per column type, so asking twice must not change it.</summary>
    [Test]
    public void ElementType_AskedTwice_IsTheSameAnswer()
    {
        IColumn column = new ArrayColumn<string>("c", "String", Array.Empty<string>());

        Assert.That(column.ElementType, Is.EqualTo(column.ElementType).And.EqualTo(typeof(string)));
    }

    /// <summary>
    /// A class surfacing two element types has no single one, so it is refused rather than resolved to whichever
    /// interface reflection happened to list first — that would make a codec silently write the wrong column.
    /// </summary>
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
