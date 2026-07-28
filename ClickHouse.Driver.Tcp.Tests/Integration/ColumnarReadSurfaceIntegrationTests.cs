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
}
