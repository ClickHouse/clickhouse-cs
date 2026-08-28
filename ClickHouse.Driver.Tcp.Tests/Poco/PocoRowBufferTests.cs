using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Covers row buffering, pooled-array ownership, null validation, and cancellation.
/// </summary>
[TestFixture]
public class PocoRowBufferTests
{
    [Test]
    public void Create_ArrayInput_BorrowsTheCallerArray()
    {
        var source = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 } };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(2));
            Assert.That(buffer.Rows, Is.SameAs(source));
        });
    }

    [Test]
    public void Create_ListInput_CopiesEveryRowInOrder()
    {
        var first = new Row<int> { Value = 1 };
        var source = new List<Row<int>> { first, new() { Value = 2 } };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", CancellationToken.None);
        source[0] = new Row<int> { Value = 3 };

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(2));
            Assert.That(buffer.Rows[0], Is.SameAs(first));
            Assert.That(buffer.Rows[1], Is.SameAs(source[1]));
        });
    }

    [Test]
    public void Create_EmptyList_HoldsNoRows()
    {
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(new List<Row<int>>(), "rows", CancellationToken.None);

        Assert.That(buffer.Count, Is.EqualTo(0));
    }

    [Test]
    public void Create_ListContainingNull_ThrowsNamingTheRowAndTheParameter()
    {
        var source = new List<Row<int>> { new() { Value = 1 }, null };

        ArgumentException error = Assert.Throws<ArgumentException>(() => PocoRowBuffer<Row<int>>.Create(source, "rows", CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(error.ParamName, Is.EqualTo("rows"));
        });
    }

    [Test]
    public void Create_ArrayContainingNull_ThrowsNamingTheRowAndTheParameter()
    {
        Row<int>[] source = { new() { Value = 1 }, null };

        ArgumentException error = Assert.Throws<ArgumentException>(() => PocoRowBuffer<Row<int>>.Create(source, "rows", CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(error.ParamName, Is.EqualTo("rows"));
        });
    }

    [Test]
    public void Dispose_BorrowedArray_LeavesTheCallerArrayUntouched()
    {
        var row = new Row<int> { Value = 1 };
        var source = new[] { row };
        PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", CancellationToken.None);

        buffer.Dispose();

        Assert.That(source[0], Is.SameAs(row));
    }

    [Test]
    public void Dispose_OwnedCopyCalledTwice_ReleasesItOnce()
    {
        // A second disposal must not return the same array twice.
        PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(
            new List<Row<int>> { new() { Value = 1 } },
            "rows",
            CancellationToken.None);

        buffer.Dispose();

        Assert.DoesNotThrow(() => buffer.Dispose());
        Assert.That(buffer.Rows, Is.Empty);
    }

    [Test]
    public void Create_TokenCancelledBeforeTheCall_ThrowsForAnEmptyList()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => PocoRowBuffer<Row<int>>.Create(Array.Empty<Row<int>>(), "rows", cancellation.Token));
    }

    [Test]
    public void Create_TokenCancelledDuringCopy_StopsThere()
    {
        using var cancellation = new CancellationTokenSource();
        var source = new CancellingList(cancellation, count: 500, cancelAfter: 3);

        Assert.Throws<OperationCanceledException>(
            () => PocoRowBuffer<Row<int>>.Create(source, "rows", cancellation.Token));

        Assert.That(source.Read, Is.LessThan(10), "copying stopped where it was cancelled");
    }

    /// <summary>A list that cancels while its indexer is being read.</summary>
    private sealed class CancellingList : IReadOnlyList<Row<int>>
    {
        private readonly CancellationTokenSource cancellation;
        private readonly int count;
        private readonly int cancelAfter;

        public CancellingList(CancellationTokenSource cancellation, int count, int cancelAfter)
        {
            this.cancellation = cancellation;
            this.count = count;
            this.cancelAfter = cancelAfter;
        }

        public int Count => count;

        /// <summary>How many rows the indexer returned.</summary>
        public int Read { get; private set; }

        public Row<int> this[int index]
        {
            get
            {
                Read++;
                if (Read == cancelAfter)
                {
                    cancellation.Cancel();
                }

                return new Row<int> { Value = index };
            }
        }

        public IEnumerator<Row<int>> GetEnumerator() => throw new NotSupportedException();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
