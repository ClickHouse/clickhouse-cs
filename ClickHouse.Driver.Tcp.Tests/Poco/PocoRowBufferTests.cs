using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Covers row buffering, block staging, pooled-array ownership, null validation, and cancellation.
/// </summary>
[TestFixture]
public class PocoRowBufferTests
{
    [Test]
    public void Create_ArrayInput_BorrowsTheCallerArray()
    {
        var source = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 } };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(2));
            Assert.That(buffer.Rows, Is.SameAs(source));
        });
    }

    [Test]
    public void Prepare_ArrayInput_ReadsTheBlockWhereItIs()
    {
        var source = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 }, new Row<int> { Value = 3 } };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 2, CancellationToken.None);
        int offset = buffer.Prepare(start: 1, length: 2);

        Assert.Multiple(() =>
        {
            Assert.That(offset, Is.EqualTo(1), "the caller's array is indexed in place");
            Assert.That(buffer.Rows, Is.SameAs(source));
        });
    }

    [Test]
    public void Prepare_ListInput_StagesTheBlockAtTheStartOfTheWindow()
    {
        var third = new Row<int> { Value = 3 };
        var source = new List<Row<int>> { new() { Value = 1 }, new() { Value = 2 }, third };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 2, CancellationToken.None);
        int offset = buffer.Prepare(start: 2, length: 1);

        Assert.Multiple(() =>
        {
            Assert.That(offset, Is.Zero, "a staged block starts at the window's first slot");
            Assert.That(buffer.Rows[0], Is.SameAs(third));
        });
    }

    [Test]
    public void Prepare_ListInput_CopiesEveryRowOfTheBlockInOrder()
    {
        var first = new Row<int> { Value = 1 };
        var second = new Row<int> { Value = 2 };
        var source = new List<Row<int>> { first, second };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 2, CancellationToken.None);
        buffer.Prepare(start: 0, length: 2);

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(2));
            Assert.That(buffer.Rows[0], Is.SameAs(first));
            Assert.That(buffer.Rows[1], Is.SameAs(second));
        });
    }

    [Test]
    public void Create_ListLongerThanABlock_StagesOnlyOneBlockAtATime()
    {
        // The window is the block, not the insert: a whole-insert copy of the row references is what this avoids.
        var source = new List<Row<int>>();
        for (int i = 0; i < 500; i++)
        {
            source.Add(new Row<int> { Value = i });
        }

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 8, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(500));
            Assert.That(buffer.Rows, Has.Length.LessThan(500));
        });
    }

    [Test]
    public void Create_EmptyList_HoldsNoRows()
    {
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(new List<Row<int>>(), "rows", blockRows: 0, CancellationToken.None);

        Assert.That(buffer.Count, Is.EqualTo(0));
    }

    [Test]
    public void RowAt_ListInput_ReadsAnyRowWithoutStagingIt()
    {
        var third = new Row<int> { Value = 3 };
        var source = new List<Row<int>> { new() { Value = 1 }, new() { Value = 2 }, third };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 1, CancellationToken.None);

        Assert.That(buffer.RowAt(2), Is.SameAs(third));
    }

    [Test]
    public void Prepare_ListContainingNull_ThrowsNamingTheRowAndTheParameter()
    {
        var source = new List<Row<int>> { new() { Value = 1 }, null };
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 2, CancellationToken.None);

        ArgumentException error = Assert.Throws<ArgumentException>(() => buffer.Prepare(start: 0, length: 2));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(error.ParamName, Is.EqualTo("rows"));
        });
    }

    [Test]
    public void Prepare_ArrayContainingNull_ThrowsNamingTheRowAndTheParameter()
    {
        Row<int>[] source = { new() { Value = 1 }, null };
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 2, CancellationToken.None);

        ArgumentException error = Assert.Throws<ArgumentException>(() => buffer.Prepare(start: 0, length: 2));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(error.ParamName, Is.EqualTo("rows"));
        });
    }

    [Test]
    public void Prepare_NullInALaterBlock_IsNotReportedByAnEarlierOne()
    {
        // Each block validates only its own rows, so a bad row surfaces when its block is gathered.
        var source = new List<Row<int>> { new() { Value = 1 }, null };
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 1, CancellationToken.None);

        Assert.DoesNotThrow(() => buffer.Prepare(start: 0, length: 1));
        Assert.Throws<ArgumentException>(() => buffer.Prepare(start: 1, length: 1));
    }

    [Test]
    public void Dispose_BorrowedArray_LeavesTheCallerArrayUntouched()
    {
        var row = new Row<int> { Value = 1 };
        var source = new[] { row };
        PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 1, CancellationToken.None);

        buffer.Dispose();

        Assert.That(source[0], Is.SameAs(row));
    }

    [Test]
    public void Dispose_OwnedWindowCalledTwice_ReleasesItOnce()
    {
        // A second disposal must not return the same array twice.
        PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(
            new List<Row<int>> { new() { Value = 1 } },
            "rows",
            blockRows: 1,
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
            () => PocoRowBuffer<Row<int>>.Create(Array.Empty<Row<int>>(), "rows", blockRows: 0, cancellation.Token));
    }

    [Test]
    public void Prepare_TokenCancelledDuringStaging_StopsThere()
    {
        using var cancellation = new CancellationTokenSource();
        var source = new CancellingList(cancellation, count: 500, cancelAfter: 3);
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Create(source, "rows", blockRows: 500, cancellation.Token);

        Assert.Throws<OperationCanceledException>(() => buffer.Prepare(start: 0, length: 500));

        Assert.That(source.Read, Is.LessThan(10), "staging stopped where it was cancelled");
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
