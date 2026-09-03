using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Block-splitting inserts for the deeply nested <c>Array(Array(...))</c> shapes. The insert fixture splits a single
/// <c>Array</c> level at two rows a block; these repeat that for the whole nesting ladder, where the per-block
/// arithmetic has to hold at every level at once rather than only the outermost.
///
/// <para>
/// The same shapes also reach <c>InsertRoundTripCase.Cases()</c>, so each one gets the unsplit round-trip, the dense
/// read-back re-insert, and the one-row-per-block slice for free. What is left to this fixture is the case those
/// cannot reach: a slice with a non-zero start <em>and</em> a length above one, at depth.
/// </para>
///
/// <para>
/// A yielded Block is borrowed — valid only for its iteration — so every read compares or copies inside the
/// <c>await foreach</c>, never retaining the block.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class NestedArrayInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [TestCaseSource(typeof(NestedArrayShape), nameof(NestedArrayShape.Shapes))]
    public async Task InsertAsync_JaggedNestedArrayColumnSplitAcrossBlocks_RoundTripsEveryRow(NestedArrayShape shape)
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt32, value {shape.ClickHouseType}) ENGINE = Memory");

            // The ergonomic write derives each block's offsets from that block's rows alone, and hands a nested Array
            // inner a ConcatColumn view over exactly those rows rather than driving it per row. maxRowsPerBlock: 2
            // makes every block a strict subset, so at depth 5 this stacks four of those views over a two-row window.
            // The id column pins the read-back order; the Memory engine does not guarantee it across blocks.
            IColumn[] columns = { RowIds(shape.RowCount), shape.BuildColumn("value") };
            await connection.InsertAsync($"INSERT INTO {table} (id, value) VALUES", columns, maxRowsPerBlock: 2, cancellationToken: None);

            object[] readBack = await ReadValuesOrderedByIdAsync(connection, table, shape.RowCount);
            Assert.Multiple(() =>
            {
                Assert.That(readBack, Is.EqualTo(ExpectedRows(shape)));
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [TestCaseSource(typeof(NestedArrayShape), nameof(NestedArrayShape.Shapes))]
    public async Task InsertAsync_DenseNestedArrayReadbackSplitAcrossBlocks_RoundTripsEveryRow(NestedArrayShape shape)
    {
        await using var source = await TcpServerFixture.ConnectAsync(None);
        await using var sink = await TcpServerFixture.ConnectAsync(None);
        string seedTable = UniqueTableName();
        string splitTable = UniqueTableName();
        try
        {
            await ExecuteAsync(source, $"CREATE TABLE {seedTable} (id UInt32, value {shape.ClickHouseType}) ENGINE = Memory");
            await ExecuteAsync(sink, $"CREATE TABLE {splitTable} (id UInt32, value {shape.ClickHouseType}) ENGINE = Memory");

            IColumn[] seed = { RowIds(shape.RowCount), shape.BuildColumn("value") };
            await source.InsertAsync($"INSERT INTO {seedTable} (id, value) VALUES", seed, cancellationToken: None);

            // The dense counterpart: a read-back block is a stack of ArrayValueColumns whose offsets, at every level,
            // count from the start of the whole column. Re-inserting it at two rows a block makes every block after
            // the first begin at a non-zero element offset *at each level of the shape*, so each level has to rebase
            // its own offsets on its own slice; a level that skipped it would emit offsets running past the elements
            // it wrote. Building the equivalent column by hand at depth 5 is unreadable, so the server's read-back
            // supplies it — re-inserted inside its own iteration, through a second connection, never retained.
            await foreach (Block block in source.QueryAsync($"SELECT id, value FROM {seedTable} ORDER BY id", cancellationToken: None))
            {
                await sink.InsertAsync(
                    $"INSERT INTO {splitTable} (id, value) VALUES",
                    new[] { block[0], block[1] },
                    maxRowsPerBlock: 2,
                    cancellationToken: None);
            }

            object[] readBack = await ReadValuesOrderedByIdAsync(sink, splitTable, shape.RowCount);
            Assert.Multiple(() =>
            {
                Assert.That(readBack, Is.EqualTo(ExpectedRows(shape)));
                Assert.That(source.State, Is.EqualTo(TcpConnectionState.Ready));
                Assert.That(sink.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await ExecuteAsync(source, $"DROP TABLE IF EXISTS {seedTable}");
            await ExecuteAsync(sink, $"DROP TABLE IF EXISTS {splitTable}");
        }
    }

    // The shape's rows as the nested arrays a read-back surfaces, so NUnit's recursive collection equality compares
    // them level by level.
    private static object[] ExpectedRows(NestedArrayShape shape)
    {
        IColumn expected = shape.BuildColumn("value");
        var rows = new object[expected.RowCount];
        for (int row = 0; row < rows.Length; row++)
        {
            rows[row] = expected.GetValue(row);
        }

        return rows;
    }

    // Reads the value column back in id order. An array column materializes each row into a fresh array on access, so
    // the collected rows outlive the borrowed block.
    private static async Task<object[]> ReadValuesOrderedByIdAsync(ClickHouseTcpConnection connection, string table, int expectedRows)
    {
        var rows = new List<object>(expectedRows);
        await foreach (Block block in connection.QueryAsync($"SELECT value FROM {table} ORDER BY id", cancellationToken: None))
        {
            for (int row = 0; row < block[0].RowCount; row++)
            {
                rows.Add(block[0].GetValue(row));
            }
        }

        return rows.ToArray();
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

    private static string UniqueTableName() => $"tcp_nested_array_test_{Guid.NewGuid():N}";
}
