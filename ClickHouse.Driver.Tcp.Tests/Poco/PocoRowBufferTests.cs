using System;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The row buffer a row-oriented insert materializes its source into. Not reachable from a server round trip beyond
/// "the rows arrived": what is here is the sizing (a counted source rents once, a lazy one grows) and the null-row
/// refusal, which has to name the row rather than surface as a NullReferenceException from compiled code.
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
    public void Materialize_CancelledToken_StopsWithoutDrainingTheSource()
    {
        // The source is enumerated before any I/O, and it can be the long part of an insert, so the token has to be
        // observed here rather than only once the connection is rented.
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => PocoRowBuffer<Row<int>>.Materialize(Counting(500), "rows", cancellation.Token));
    }

    private static IEnumerable<Row<int>> Counting(int count)
    {
        for (int value = 0; value < count; value++)
        {
            yield return new Row<int> { Value = value };
        }
    }
}
