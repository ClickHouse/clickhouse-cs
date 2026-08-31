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

}
