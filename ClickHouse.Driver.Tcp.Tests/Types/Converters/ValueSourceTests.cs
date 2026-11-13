using System;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>Covers <see cref="ValueSource{T}"/>: the two shapes, their runs, and the marks.</summary>
[TestFixture]
public class ValueSourceTests
{
    [Test]
    public void Of_Span_HasOneRunAndItsFirstRow()
    {
        int[] values = { 1, 2, 3 };
        ValueSource<int> source = ValueSource<int>.Of(values.AsSpan(1), firstRow: 1);
        (int count, bool segmented, int firstRow, int runs, int[] span, bool marked) =
            (source.Count, source.IsSegmented, source.FirstRow, source.RunCount, source.Span.ToArray(), source.HasAbsent);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(2));
            Assert.That(segmented, Is.False);
            Assert.That(firstRow, Is.EqualTo(1));
            Assert.That(runs, Is.EqualTo(1));
            Assert.That(span, Is.EqualTo(new[] { 2, 3 }));
            Assert.That(marked, Is.False);
        });
    }

    [Test]
    public void OfSegments_NullAndEmptySegments_CountAsEmptyRuns()
    {
        int[][] segments = { new[] { 1 }, null, Array.Empty<int>(), new[] { 2, 3 } };
        ValueSource<int> source = ValueSource<int>.OfSegments(segments);
        (int count, bool segmented, int firstRow, int runs, bool nullRunIsEmpty, int[] last) =
            (source.Count, source.IsSegmented, source.FirstRow, source.RunCount, source.Run(1).IsEmpty, source.Run(3).ToArray());

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(3));
            Assert.That(segmented, Is.True);
            Assert.That(firstRow, Is.EqualTo(0));
            Assert.That(runs, Is.EqualTo(4));
            Assert.That(nullRunIsEmpty, Is.True);
            Assert.That(last, Is.EqualTo(new[] { 2, 3 }));
        });
    }

    [Test]
    public void WithAbsent_OneMarkForEachValue_MarksThePositions()
    {
        int[][] segments = { new[] { 1 }, new[] { 2, 3 } };
        ValueSource<int> source = ValueSource<int>.OfSegments(segments).WithAbsent(new byte[] { 0, 1, 0 });
        (bool marked, byte[] marks, int count) = (source.HasAbsent, source.Absent.ToArray(), source.Count);

        Assert.Multiple(() =>
        {
            Assert.That(marked, Is.True);
            Assert.That(marks, Is.EqualTo(new byte[] { 0, 1, 0 }));
            Assert.That(count, Is.EqualTo(3));
        });
    }

    [Test]
    public void WithAbsent_WrongNumberOfMarks_Throws()
    {
        var thrown = Assert.Throws<ArgumentException>(() => ValueSource<int>.Of(new[] { 1, 2 }).WithAbsent(new byte[] { 0 }));
        Assert.That(thrown.ParamName, Is.EqualTo("absent"));
    }

    [Test]
    public void ShapeAccessors_OtherShape_Throw()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidOperationException>(() => _ = ValueSource<int>.Of(new[] { 1 }).Segments);
            Assert.Throws<InvalidOperationException>(() => _ = ValueSource<int>.OfSegments(new[] { new[] { 1 } }).Span);
            Assert.Throws<ArgumentOutOfRangeException>(() => _ = ValueSource<int>.Of(new[] { 1 }).Run(1));
        });
    }
}
