using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Slice coverage for every type's write path. The per-type round-trip matrix in
/// <see cref="ClickHouseTcpConnectionInsertIntegrationTests"/> inserts each column whole, so every codec writes its
/// slice at start 0 — and at start 0 a codec that ignores <c>start</c> still writes correct bytes. That left the
/// slice path, which a real insert of more than
/// <see cref="ClickHouseTcpConnection.DefaultMaxRowsPerBlock"/> rows takes, unproven for every type.
///
/// <para>
/// These tests insert the same matrix as several wire blocks, so each block after the first writes a slice with a
/// non-zero start. That is what shows a codec rebases its own offsets, run starts, and element ranges. The tests
/// live in their own fixture so the matrix stays one parameterized test per concern rather than a hand-written
/// slice test per type.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class InsertSlicingIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // One row per block: the strongest guarantee of a non-zero start, since it splits every case with two or more
    // rows regardless of how many rows that case declares. Multi-row slices (a non-zero start *and* a length above
    // one) are covered by the per-type cases in the insert fixture, which split a longer column at 2 rows a block.
    //
    // The id column pins the read-back order; the Memory engine does not guarantee it across blocks.
    [TestCaseSource(typeof(InsertRoundTripCase), nameof(InsertRoundTripCase.Cases))]
    public async Task InsertAsync_ColumnarDataOneRowPerBlock_RoundTripsEveryRow(InsertRoundTripCase testCase)
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt32, value {testCase.ClickHouseType}) ENGINE = Memory");

            IColumn insert = testCase.BuildInsertColumn("value");
            IColumn expected = testCase.BuildExpectedColumn("value");
            IColumn[] columns = { RowIds(insert.RowCount), insert };
            await connection.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES", columns, maxRowsPerBlock: 1, cancellationToken: None);

            await AssertReadsBackAsync(connection, table, expected);
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    // Compares the read-back against a row cursor inside the iteration: the read-back may arrive as several blocks,
    // and a yielded block is borrowed, so no value may be retained past its own iteration.
    private static async Task AssertReadsBackAsync(ClickHouseTcpConnection connection, string table, IColumn expected)
    {
        int row = 0;
        await foreach (Block block in connection.QueryAsync($"SELECT value FROM {table} ORDER BY id", cancellationToken: None))
        {
            IColumn actual = block[0];
            for (int i = 0; i < actual.RowCount; i++, row++)
            {
                Assert.That(row, Is.LessThan(expected.RowCount), "the read-back returned more rows than were inserted");
                Assert.That(actual.GetValue(i), Is.EqualTo(expected.GetValue(row)), $"row {row}");
            }
        }

        Assert.That(row, Is.EqualTo(expected.RowCount), "every inserted row should read back");
    }

    private static IColumn RowIds(int rowCount)
    {
        var ids = new uint[rowCount];
        for (int i = 0; i < ids.Length; i++)
        {
            ids[i] = (uint)i;
        }

        return PrimitiveColumn<uint>.FromValues("id", "UInt32", ids);
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }

    private static string UniqueTableName() => $"tcp_slice_test_{Guid.NewGuid():N}";
}
