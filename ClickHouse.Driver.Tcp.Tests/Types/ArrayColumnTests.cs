using System;
using System.Buffers;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// The array-backed column's ownership modes, which decide who returns a rented buffer. Only the modes matter here;
/// the values themselves ride every codec test and the round-trip corpus.
/// </summary>
[TestFixture]
public class ArrayColumnTests
{
    [Test]
    public void OverBuffer_ABufferLongerThanTheData_ExposesOnlyTheRows()
    {
        // A rented buffer is usually longer than what was asked for, so the row count is the length the column has
        // to present — reading the array's own length would surface whatever the pool last held there.
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
        // The non-owning mode: the caller keeps the buffer, so disposing the column must not hand it to the pool —
        // an array in the pool twice is handed to two owners at once.
        int[] buffer = ArrayPool<int>.Shared.Rent(4);
        buffer[0] = 42;

        ArrayColumn<int>.OverBuffer("c", "Int32", buffer, length: 1).Dispose();

        Assert.That(ArrayPool<int>.Shared.Rent(4), Is.Not.SameAs(buffer), "the column returned a buffer it does not own");
        ArrayPool<int>.Shared.Return(buffer);
    }

    [Test]
    public void OverPooledBuffer_DisposedTwice_ReturnsTheBufferOnce()
    {
        // The owning mode a gathered insert column uses. A second Dispose must be a no-op: returning one array twice
        // puts it in the pool twice, and the pool then hands the same storage to two callers.
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
        // ArrayPool hands out the empty array for a zero-length rent, and returning that is not something the pool
        // accepts — the zero-row insert reaches here.
        ArrayColumn<int> column = ArrayColumn<int>.OverPooledBuffer("c", "Int32", ArrayPool<int>.Shared.Rent(0), length: 0);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(0));
            Assert.That(() => column.Dispose(), Throws.Nothing);
        });
    }
}
