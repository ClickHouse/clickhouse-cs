using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Inserts into a table whose columns are not all insertable — <c>DEFAULT</c>, <c>MATERIALIZED</c> and
/// <c>ALIAS</c> — which is the shape of most real schemas and the one the test tables elsewhere in this suite
/// never have.
///
/// <para>
/// Two things only a real server settles here. The insert schema block does not name every column, and which
/// ones it omits decides whether a caller who built columns from the table definition can insert at all. And an
/// insert under <c>input_format_defaults_for_omitted_fields = 1</c> makes the server send a
/// <c>TableColumns</c> packet, whose body the client decodes only to stay aligned — a decoder reading the wrong
/// number of bytes would leave the rest of the response mis-framed.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ComputedColumnInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // What makes the server send the TableColumns packet before the insert schema block. Verified on 26.6: with
    // this on the packet arrives (one per insert), with it off it does not.
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

    // id, plain: insertable. withDefault: insertable, computed when omitted. materialized, aliased: never
    // insertable, and absent from the insert schema block.
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
    /// The <c>TableColumns</c> packet is discarded rather than surfaced, so this cannot assert that it arrived —
    /// only that the response stays aligned around it, which is what a wrong byte count would break. Two inserts
    /// on one connection, because the packet comes once per insert and a decoder that under-reads leaves the
    /// leftovers for the next operation.
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

            // On the same connection, so it reads whatever the two inserts left on the wire.
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
}
