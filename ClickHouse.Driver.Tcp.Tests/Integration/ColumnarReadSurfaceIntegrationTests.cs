using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Covers the public columnar read surface of the composite column types against a real server. Every concrete
/// column class is internal, so these interfaces — obtained by pattern-matching an <see cref="IColumn"/> — are the
/// only way a consumer reaches the wire layout underneath a composite. These tests assert the shape a real decoded
/// block presents: that the view is reachable at all, that its spans are sliced to the column's row count rather
/// than to a pooled buffer's length, and that the raw columnar data it exposes agrees with the materialized rows.
///
/// <para>
/// A yielded <see cref="Block"/> is borrowed — valid only for its iteration — so every test reads or copies what it
/// needs inside the <c>await foreach</c>, never retaining the block or a span taken from it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ColumnarReadSurfaceIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task QueryAsync_NullableColumn_ExposesInnerAndNullMapThroughINullableColumn()
    {
        // A Nullable(T) column's wire layout is a dense inner column (a decoded value at *every* row, placeholder
        // included where the row is null) plus the per-row null-map that says which rows are really null. The
        // materialized IColumn<int?> surface folds those two together and discards the distinction, so reaching the
        // pair is the whole point of INullableColumn<T>.
        //
        // Note the type argument is the *inner* type: a Nullable(Int32) column is an INullableColumn<int>, not an
        // INullableColumn<int?>, even though its IColumn<T> surface is IColumn<int?>.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matchedInner = false;
        bool matchedNullable = false;
        byte[] nullMap = null;
        int nullMapLength = 0;
        int rowCount = 0;
        int innerRowCount = 0;
        int?[] materialized = null;
        var innerAtNonNullRows = new int[3];

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(number = 1 ? NULL : toInt32(number * 10 - 10), 'Nullable(Int32)') FROM system.numbers LIMIT 4",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matchedInner = column is INullableColumn<int>;
            matchedNullable = column is INullableColumn<int?>;

            var nullable = (INullableColumn<int>)column;
            nullMap = nullable.NullMap.ToArray();
            nullMapLength = nullable.NullMap.Length;
            rowCount = nullable.RowCount;
            innerRowCount = nullable.Inner.RowCount;
            materialized = ((IColumn<int?>)column).Values.ToArray();

            // Read the non-null rows straight out of the inner column, skipping the nulls via the map — the
            // allocation-free access pattern the interface exists to enable.
            int next = 0;
            for (int row = 0; row < nullable.RowCount; row++)
            {
                if (nullable.NullMap[row] == 0)
                {
                    innerAtNonNullRows[next++] = nullable.Inner[row];
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matchedInner, Is.True, "the view is parameterized by the inner type");
            Assert.That(matchedNullable, Is.False, "and not by the nullable type");
            Assert.That(rowCount, Is.EqualTo(4));
            Assert.That(nullMap, Is.EqualTo(new byte[] { 0, 1, 0, 0 }), "one entry per row, non-zero marking null");
            Assert.That(nullMapLength, Is.EqualTo(rowCount), "the map is sliced to the row count, not the pooled buffer length");
            Assert.That(innerRowCount, Is.EqualTo(rowCount), "the inner column is dense: one decoded value per row");
            Assert.That(materialized, Is.EqualTo(new int?[] { -10, null, 10, 20 }));
            Assert.That(innerAtNonNullRows, Is.EqualTo(new[] { -10, 10, 20 }), "the null-marked row is skipped, the rest read from the inner column");
        });
    }

    [Test]
    public async Task QueryAsync_NullableReferenceColumn_ExposesInnerAndNullMapThroughINullableColumn()
    {
        // Nullable(String) decodes to a separate reference-typed column class with its own copy of the pair, whose
        // IColumn<T> surface is IColumn<string> (a reference is already nullable) rather than IColumn<T?>. Its view
        // is still parameterized by the inner type, so the pattern-match spelling stays uniform with the value case.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        byte[] nullMap = null;
        int nullMapLength = 0;
        int rowCount = 0;
        int innerRowCount = 0;
        string[] materialized = null;

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(number = 1 ? NULL : concat('v', toString(number)), 'Nullable(String)') FROM system.numbers LIMIT 3",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is INullableColumn<string>;

            var nullable = (INullableColumn<string>)column;
            nullMap = nullable.NullMap.ToArray();
            nullMapLength = nullable.NullMap.Length;
            rowCount = nullable.RowCount;
            innerRowCount = nullable.Inner.RowCount;
            materialized = ((IColumn<string>)column).Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(3));
            Assert.That(nullMap, Is.EqualTo(new byte[] { 0, 1, 0 }));
            Assert.That(nullMapLength, Is.EqualTo(rowCount));
            Assert.That(innerRowCount, Is.EqualTo(rowCount));
            Assert.That(materialized, Is.EqualTo(new[] { "v0", null, "v2" }));
        });
    }

    [Test]
    public async Task QueryAsync_ArrayColumn_ExposesFlatElementsAndOffsetsThroughIArrayColumn()
    {
        // An Array(T) column's wire layout is every row's elements concatenated into one flat run plus the per-row
        // offsets that delimit them. The materialized IColumn<T[]> surface allocates a fresh array per row;
        // IArrayColumn<TElement> is the allocation-free alternative, so the test walks the rows through the spans
        // and checks they reconstruct what the materialized surface produced.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int rowCount = 0;
        int[] offsets = null;
        int[] innerValues = null;
        int innerRowCount = 0;
        var slices = new List<int[]>();
        int[][] materialized = null;

        // Rows: [], [0], [0, 1], [0, 1, 2] — an empty leading row makes a zero-length slice part of the check.
        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(range(number), 'Array(Int32)') FROM system.numbers LIMIT 4",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is IArrayColumn<int>;

            var array = (IArrayColumn<int>)column;
            rowCount = array.RowCount;
            offsets = array.Offsets.ToArray();
            innerValues = array.InnerValues.ToArray();
            innerRowCount = array.Inner.RowCount;
            materialized = ((IColumn<int[]>)column).Values.ToArray();

            for (int row = 0; row < array.RowCount; row++)
            {
                slices.Add(array.InnerValues.Slice(array.Offsets[row], array.Offsets[row + 1] - array.Offsets[row]).ToArray());
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(4));
            Assert.That(offsets, Is.EqualTo(new[] { 0, 0, 1, 3, 6 }), "one more entry than rows; [0] is 0");
            Assert.That(offsets.Length, Is.EqualTo(rowCount + 1), "sliced to the row count, not the pooled buffer length");
            Assert.That(innerValues, Is.EqualTo(new[] { 0, 0, 1, 0, 1, 2 }), "every row's elements end-to-end");
            Assert.That(innerRowCount, Is.EqualTo(6), "the inner column is flat: one entry per element, not per row");
            Assert.That(slices, Is.EqualTo(materialized), "the zero-copy slices reconstruct the materialized rows");
        });
    }

    [Test]
    public async Task QueryAsync_NestedArrayColumn_ExposesInnerAsAnotherArrayColumnForRecursion()
    {
        // Why IArrayColumn exposes Inner as a column and not just InnerValues as a span: when the element type is
        // itself composite, the span's element type is the *materialized* form (here int[]), so reading it defeats
        // the point. Inner hands back the flat inner column instead, which pattern-matches to the element type's own
        // view — so a nested composite can be walked to the bottom without materializing an intermediate level.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool outerMatched = false;
        bool innerMatched = false;
        int[] outerOffsets = null;
        int[] innerOffsets = null;
        int[] leafValues = null;
        int[][][] materialized = null;

        // Rows: [[0], [0, 1]] and [[1], [1, 2]].
        await foreach (Block block in connection.QueryAsync(
            "SELECT [[toInt32(number)], [toInt32(number), toInt32(number + 1)]] FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            IColumn column = block[0];
            outerMatched = column is IArrayColumn<int[]>;

            var outer = (IArrayColumn<int[]>)column;
            outerOffsets = outer.Offsets.ToArray();
            materialized = ((IColumn<int[][]>)column).Values.ToArray();

            innerMatched = outer.Inner is IArrayColumn<int>;
            var inner = (IArrayColumn<int>)outer.Inner;
            innerOffsets = inner.Offsets.ToArray();
            leafValues = inner.InnerValues.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(outerMatched, Is.True, "the outer view's element type is the materialized inner array");
            Assert.That(innerMatched, Is.True, "and Inner re-enters the columnar surface one level down");
            Assert.That(outerOffsets, Is.EqualTo(new[] { 0, 2, 4 }), "two inner arrays per outer row");
            Assert.That(innerOffsets, Is.EqualTo(new[] { 0, 1, 3, 4, 6 }), "four inner arrays, of lengths 1, 2, 1, 2");
            Assert.That(leafValues, Is.EqualTo(new[] { 0, 0, 1, 1, 1, 2 }), "the leaf run, reached without materializing a level");
            Assert.That(materialized, Is.EqualTo(new[] { new[] { new[] { 0 }, new[] { 0, 1 } }, new[] { new[] { 1 }, new[] { 1, 2 } } }));
        });
    }
}
