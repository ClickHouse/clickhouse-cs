using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Covers inserts into tables with <c>DEFAULT</c>, <c>MATERIALIZED</c>, and <c>ALIAS</c> columns.
/// Also verifies framing around the optional <c>TableColumns</c> packet.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ComputedColumnInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // Makes the server send TableColumns before the insert schema block.
    private static readonly Dictionary<string, string> DefaultsForOmittedFields = new(StringComparer.Ordinal)
    {
        ["input_format_defaults_for_omitted_fields"] = "1",
    };

    private static string UniqueTableName() => $"tcp_computed_column_test_{Guid.NewGuid():N}";

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            block.Dispose();
        }
    }

    // The schema block includes id, plain, and withDefault; it omits materialized and aliased.
    private static async Task<string> CreateTableAsync(ClickHouseTcpConnection connection)
    {
        string table = UniqueTableName();
        await ExecuteAsync(connection, $@"CREATE TABLE {table} (
            id UInt64,
            plain String,
            withDefault String DEFAULT concat('d', toString(id)),
            materialized UInt64 MATERIALIZED id * 2,
            aliased UInt64 ALIAS id + 1
        ) ENGINE = MergeTree ORDER BY id");
        return table;
    }

    private static IColumn[] IdAndPlain() =>
    [
        PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
        new ArrayColumn<string>("plain", "String", ["a", "b"]),
    ];

    private static async Task<List<string>> ReadEveryColumnAsync(ClickHouseTcpConnection connection, string table)
    {
        var rows = new List<string>();
        await foreach (Block block in connection.QueryAsync(
            $"SELECT id, plain, withDefault, materialized, aliased FROM {table} ORDER BY id", cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                rows.Add(string.Join(
                    "/",
                    block[0].GetValue(row),
                    block[1].GetValue(row),
                    block[2].GetValue(row),
                    block[3].GetValue(row),
                    block[4].GetValue(row)));
            }
        }

        return rows;
    }

    /// <summary>
    /// Verifies framing across consecutive <c>TableColumns</c> packets on one connection.
    /// </summary>
    [Test]
    public async Task InsertAsync_UnderDefaultsForOmittedFields_StaysAlignedAroundTheTableColumnsPacket()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = await CreateTableAsync(connection);
        try
        {
            await connection.InsertAsync(
                $"INSERT INTO {table} (id, plain) VALUES", IdAndPlain(), settings: DefaultsForOmittedFields, cancellationToken: None);
            TcpConnectionState afterFirst = connection.State;

            await connection.InsertAsync(
                $"INSERT INTO {table} (id, plain) VALUES", IdAndPlain(), settings: DefaultsForOmittedFields, cancellationToken: None);

            // Read on the same connection to detect leftover packet bytes.
            List<string> rows = await ReadEveryColumnAsync(connection, table);

            Assert.Multiple(() =>
            {
                Assert.That(afterFirst, Is.EqualTo(TcpConnectionState.Ready), "the connection survives the packet");
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
                Assert.That(rows, Has.Count.EqualTo(4), "both inserts landed");
                Assert.That(rows[0], Is.EqualTo("1/a/d1/2/2"), "the server computed the DEFAULT, MATERIALIZED and ALIAS values");
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {table}");
        }
    }

    /// <summary>
    /// Verifies that the schema block contains only insertable columns.
    /// </summary>
    [Test]
    public async Task InsertAsync_NoColumnList_SchemaBlockNamesTheInsertableColumnsOnly()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = await CreateTableAsync(connection);
        try
        {
            var schemaColumns = new List<string>();
            await connection.InsertAsync(
                $"INSERT INTO {table} VALUES",
                rowCount: 2,
                buildColumns: schema =>
                {
                    for (int i = 0; i < schema.ColumnCount; i++)
                    {
                        schemaColumns.Add(schema[i].Name);
                    }

                    return new FixedColumnSource(
                    [
                        PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
                        new ArrayColumn<string>("plain", "String", ["a", "b"]),
                        new ArrayColumn<string>("withDefault", "String", ["given", "given"]),
                    ]);
                },
                settings: null,
                parameters: null,
                queryId: null,
                maxRowsPerBlock: null,
                cancellationToken: None);

            List<string> rows = await ReadEveryColumnAsync(connection, table);

            Assert.Multiple(() =>
            {
                Assert.That(schemaColumns, Is.EqualTo(new[] { "id", "plain", "withDefault" }));
                Assert.That(rows[0], Is.EqualTo("1/a/given/2/2"), "a supplied DEFAULT column keeps its value");
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {table}");
        }
    }

    /// <summary>
    /// Verifies that the client rejects a supplied <c>MATERIALIZED</c> column before writing rows.
    /// </summary>
    [Test]
    public async Task InsertAsync_SupplyingAMaterializedColumn_RefusesAndNamesTheColumn()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = await CreateTableAsync(connection);
        try
        {
            IColumn[] columns =
            [
                PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
                new ArrayColumn<string>("plain", "String", ["a", "b"]),
                PrimitiveColumn<ulong>.FromValues("materialized", "UInt64", [9, 9]),
            ];

            var refusal = Assert.ThrowsAsync<ArgumentException>(
                async () => await connection.InsertAsync($"INSERT INTO {table} (id, plain) VALUES", columns, cancellationToken: None));

            // Query on the same connection to verify that rejection wrote no rows and kept it usable.
            long committed = 0;
            await foreach (Block block in connection.QueryAsync($"SELECT count() FROM {table}", cancellationToken: None))
            {
                committed = Convert.ToInt64(block[0].GetValue(0));
            }

            Assert.Multiple(() =>
            {
                Assert.That(refusal.Message, Does.Contain("materialized"), "the caller has to be told which column");
                Assert.That(committed, Is.Zero);
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {table}");
        }
    }

    /// <summary>
    /// Verifies that the server rejects a statement naming a <c>MATERIALIZED</c> column.
    /// </summary>
    [Test]
    public async Task InsertAsync_StatementNamesAMaterializedColumn_IsRefusedByTheServer()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = await CreateTableAsync(connection);
        try
        {
            IColumn[] columns =
            [
                PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
                new ArrayColumn<string>("plain", "String", ["a", "b"]),
                PrimitiveColumn<ulong>.FromValues("materialized", "UInt64", [9, 9]),
            ];

            var refusal = Assert.ThrowsAsync<ClickHouseTcpServerException>(
                async () => await connection.InsertAsync(
                    $"INSERT INTO {table} (id, plain, materialized) VALUES", columns, cancellationToken: None));

            Assert.That(refusal.Message, Does.Contain("MATERIALIZED"));
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {table}");
        }
    }

    // Adapts already-built columns to the row-stream source interface.
    private sealed class FixedColumnSource(IReadOnlyList<IColumn> columns) : IInsertColumnSource
    {
        public IReadOnlyList<IColumn> Columns { get; } = columns;

        public void Gather(int start, int length)
        {
        }

        public void Dispose()
        {
        }
    }
}
