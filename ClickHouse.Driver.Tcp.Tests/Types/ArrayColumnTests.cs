using System;
using System.Buffers;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers owning and non-owning array-backed columns.
/// </summary>
[TestFixture]
public class ArrayColumnTests
{
    [Test]
    public void OverBuffer_ABufferLongerThanTheData_ExposesOnlyTheRows()
    {
        // Expose only the logical rows, not the full rented buffer.
        int[] buffer = { 1, 2, 3, 4, 5 };

        using ArrayColumn<int> column = ArrayColumn<int>.OverBuffer("c", "Int32", buffer, length: 3);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(3));
            Assert.That(column.Values.ToArray(), Is.EqualTo(new[] { 1, 2, 3 }));
            Assert.That(() => column[3], Throws.TypeOf<IndexOutOfRangeException>(), "past the row count, not past the buffer");
        });
    }

    [Test]
    public void OverBuffer_Disposed_DoesNotReturnACallersBuffer()
    {
        // The caller retains ownership of a non-owning buffer.
        int[] buffer = ArrayPool<int>.Shared.Rent(4);
        buffer[0] = 42;

        ArrayColumn<int>.OverBuffer("c", "Int32", buffer, length: 1).Dispose();

        Assert.That(ArrayPool<int>.Shared.Rent(4), Is.Not.SameAs(buffer), "the column returned a buffer it does not own");
        ArrayPool<int>.Shared.Return(buffer);
    }

    [Test]
    public void OverPooledBuffer_DisposedTwice_ReturnsTheBufferOnce()
    {
        // An owning column must return its buffer exactly once.
        ArrayColumn<int> column = ArrayColumn<int>.OverPooledBuffer("c", "Int32", ArrayPool<int>.Shared.Rent(4), length: 2);

        column.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(() => column.Dispose(), Throws.Nothing);
            Assert.That(column.RowCount, Is.EqualTo(2), "the row count is the column's own, not the released buffer's");
            Assert.That(() => column[0], Throws.InstanceOf<ArgumentOutOfRangeException>(), "the values are gone with the buffer");
        });
    }

    [Test]
    public void OverPooledBuffer_ZeroRows_IsDisposable()
    {
        // Zero-row inserts may produce the pool's shared empty array.
        ArrayColumn<int> column = ArrayColumn<int>.OverPooledBuffer("c", "Int32", ArrayPool<int>.Shared.Rent(0), length: 0);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(0));
            Assert.That(() => column.Dispose(), Throws.Nothing);
        });
    }
}
