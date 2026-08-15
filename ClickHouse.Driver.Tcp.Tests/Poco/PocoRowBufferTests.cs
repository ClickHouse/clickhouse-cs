using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The row buffer a row-oriented insert materializes its source into. Not reachable from a server round trip beyond
/// "the rows arrived": what is here is the sizing (a counted source rents once, a lazy one grows), the null-row
/// refusal — which has to name the row rather than surface as a NullReferenceException from compiled code — and
/// cancellation, this being the one stretch of an insert that runs before any I/O.
/// </summary>
[TestFixture]
public class PocoRowBufferTests
{
    [Test]
    public void Materialize_CountedSource_HoldsEveryRowInOrder()
    {
        var source = new List<Row<int>> { new() { Value = 1 }, new() { Value = 2 } };

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Materialize(source, "rows", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(2));
            Assert.That(buffer.Rows[0], Is.SameAs(source[0]));
            Assert.That(buffer.Rows[1], Is.SameAs(source[1]));
        });
    }

    [Test]
    public void Materialize_LazySource_GrowsPastTheInitialRent()
    {
        const int count = 500;

        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Materialize(Counting(count), "rows", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(buffer.Count, Is.EqualTo(count));
            Assert.That(buffer.Rows[0].Value, Is.EqualTo(0));
            Assert.That(buffer.Rows[count - 1].Value, Is.EqualTo(count - 1));
        });
    }

    [Test]
    public void Materialize_EmptySource_HoldsNoRows()
    {
        using PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Materialize(Array.Empty<Row<int>>(), "rows", CancellationToken.None);

        Assert.That(buffer.Count, Is.EqualTo(0));
    }

    [Test]
    public void Materialize_NullRow_ThrowsNamingTheRowAndTheParameter()
    {
        var source = new List<Row<int>> { new() { Value = 1 }, null };

        ArgumentException error = Assert.Throws<ArgumentException>(() => PocoRowBuffer<Row<int>>.Materialize(source, "rows", CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(error.ParamName, Is.EqualTo("rows"));
        });
    }

    [Test]
    public void Dispose_Twice_ReturnsTheArrayOnce()
    {
        // Returning a pooled array twice puts one array in the pool twice, which hands the same storage to two
        // callers — so the second Dispose has to be a no-op rather than a second Return.
        PocoRowBuffer<Row<int>> buffer = PocoRowBuffer<Row<int>>.Materialize(new[] { new Row<int> { Value = 1 } }, "rows", CancellationToken.None);

        buffer.Dispose();

        Assert.DoesNotThrow(() => buffer.Dispose());
        Assert.That(buffer.Rows, Is.Empty);
    }

    [Test]
    public void Materialize_TokenCancelledBeforeTheCall_StopsWithoutDrainingTheSource()
    {
        // The source is enumerated before any I/O, and it can be the long part of an insert, so the token has to be
        // observed here rather than only once the connection is rented.
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => PocoRowBuffer<Row<int>>.Materialize(Counting(500), "rows", cancellation.Token));
    }

    [Test]
    public void Materialize_TokenCancelledPartWayThroughACountedSource_StopsThere()
    {
        // A counted source sizes the rent to fit, so it never reaches a growth point: testing the token only there
        // would drain a long collection in full however early the caller cancelled.
        using var cancellation = new CancellationTokenSource();
        var source = new CancellingCollection(cancellation, count: 500, cancelAfter: 3);

        Assert.Throws<OperationCanceledException>(
            () => PocoRowBuffer<Row<int>>.Materialize(source, "rows", cancellation.Token));

        Assert.That(source.Yielded, Is.LessThan(10), "the enumeration stopped where it was cancelled");
    }

    private static IEnumerable<Row<int>> Counting(int count)
    {
        for (int value = 0; value < count; value++)
        {
            yield return new Row<int> { Value = value };
        }
    }

    /// <summary>
    /// A source that reports its count — so the buffer rents once and never grows — and cancels the token part-way
    /// through yielding. <see cref="ICollection{T}"/> rather than <see cref="IReadOnlyCollection{T}"/> because that
    /// is the interface a count is read through; the mutators are never called.
    /// </summary>
    private sealed class CancellingCollection : ICollection<Row<int>>
    {
        private readonly CancellationTokenSource cancellation;
        private readonly int count;
        private readonly int cancelAfter;

        public CancellingCollection(CancellationTokenSource cancellation, int count, int cancelAfter)
        {
            this.cancellation = cancellation;
            this.count = count;
            this.cancelAfter = cancelAfter;
        }

        public int Count => count;

        public bool IsReadOnly => true;

        /// <summary>How many rows the enumeration asked for before it stopped.</summary>
        public int Yielded { get; private set; }

        public IEnumerator<Row<int>> GetEnumerator()
        {
            for (int value = 0; value < count; value++)
            {
                if (value == cancelAfter)
                {
                    cancellation.Cancel();
                }

                Yielded++;
                yield return new Row<int> { Value = value };
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(Row<int> item) => throw new NotSupportedException();

        public void Clear() => throw new NotSupportedException();

        public bool Contains(Row<int> item) => throw new NotSupportedException();

        public void CopyTo(Row<int>[] array, int arrayIndex) => throw new NotSupportedException();

        public bool Remove(Row<int> item) => throw new NotSupportedException();
    }
}
