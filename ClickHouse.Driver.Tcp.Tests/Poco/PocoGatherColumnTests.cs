using System;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Covers the gather buffer a row insert reuses for every block: what it exposes, and its pooled ownership.
/// </summary>
[TestFixture]
public class PocoGatherColumnTests
{
    [Test]
    public void Publish_FewerRowsThanTheBuffer_ExposesOnlyThePublishedRows()
    {
        using var column = new PocoGatherColumn<int>("c", "Int32", capacity: 8);
        column.Buffer[0] = 1;
        column.Buffer[1] = 2;
        column.Buffer[2] = 3;

        column.Publish(2);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(2));
            Assert.That(column.Values.ToArray(), Is.EqualTo(new[] { 1, 2 }));
            Assert.That(column.GetValue(1), Is.EqualTo(2));
            Assert.That(() => column[2], Throws.TypeOf<IndexOutOfRangeException>(), "past the published rows, not past the buffer");
        });
    }

    [Test]
    public void Publish_ASecondBlock_ReplacesTheRowsOfTheFirst()
    {
        // The buffer is rented once and refilled, so the values are always the current block's.
        using var column = new PocoGatherColumn<int>("c", "Int32", capacity: 4);
        column.Buffer[0] = 1;
        column.Buffer[1] = 2;
        column.Publish(2);

        column.Buffer[0] = 9;
        column.Publish(1);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(1));
            Assert.That(column.Values.ToArray(), Is.EqualTo(new[] { 9 }));
        });
    }

    [Test]
    public void Dispose_CalledTwice_ReturnsTheBufferOnce()
    {
        // An owning column must return its buffer exactly once.
        var column = new PocoGatherColumn<int>("c", "Int32", capacity: 4);
        column.Publish(2);

        column.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(() => column.Dispose(), Throws.Nothing);
            Assert.That(column.RowCount, Is.Zero, "the values are gone with the buffer");
        });
    }

    [Test]
    public void Dispose_ZeroCapacity_IsStillDisposable()
    {
        // A zero-row insert rents the pool's shared empty array.
        var column = new PocoGatherColumn<int>("c", "Int32", capacity: 0);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.Zero);
            Assert.That(() => column.Dispose(), Throws.Nothing);
        });
    }
}
