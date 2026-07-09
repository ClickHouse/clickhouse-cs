using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// A yielded Block is borrowed — valid only for its iteration — so every read compares or copies inside the
// await foreach, never retaining the block.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpConnectionInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [TestCaseSource(typeof(InsertRoundTripCase), nameof(InsertRoundTripCase.Cases))]
    public async Task InsertAsync_ColumnarData_RoundTripsThroughSelect(InsertRoundTripCase testCase)
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (value {testCase.ClickHouseType}) ENGINE = Memory");

            IColumn insert = testCase.BuildInsertColumn("value");
            IColumn expected = testCase.BuildExpectedColumn("value");
            await connection.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { insert }, cancellationToken: None);

            int blockCount = 0;
            await foreach (Block block in connection.QueryAsync($"SELECT value FROM {table}", cancellationToken: None))
            {
                blockCount++;
                AssertColumnsEqual(expected, block[0]);
            }

            Assert.Multiple(() =>
            {
                Assert.That(blockCount, Is.EqualTo(1), "the round-trip should read back exactly one row-bearing block");
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_MultipleColumnsAndRows_RoundTripsEveryColumn()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt64, name String, at DateTime) ENGINE = Memory");

            const int rows = 5000;
            var ids = new ulong[rows];
            var names = new string[rows];
            var timestamps = new DateTime[rows];
            for (int i = 0; i < rows; i++)
            {
                ids[i] = (ulong)i;
                names[i] = $"row-{i}";
                timestamps[i] = DateTime.UnixEpoch.AddSeconds(i);
            }

            IColumn[] columns =
            {
                PrimitiveColumn<ulong>.FromValues("id", "UInt64", ids),
                new ArrayColumn<string>("name", "String", names),
                new ArrayColumn<DateTime>("at", "DateTime", timestamps),
            };

            await connection.InsertAsync($"INSERT INTO {table} (id, name, at) VALUES", columns, cancellationToken: None);

            var readIds = new List<ulong>();
            var readNames = new List<string>();
            var readTimestamps = new List<DateTime>();
            await foreach (Block block in connection.QueryAsync($"SELECT id, name, at FROM {table} ORDER BY id", cancellationToken: None))
            {
                readIds.AddRange(((IColumn<ulong>)block[0]).Values.ToArray());
                readNames.AddRange(((IColumn<string>)block[1]).Values.ToArray());
                readTimestamps.AddRange(((IColumn<DateTime>)block[2]).Values.ToArray());
            }

            Assert.Multiple(() =>
            {
                CollectionAssert.AreEqual(ids, readIds);
                CollectionAssert.AreEqual(names, readNames);
                CollectionAssert.AreEqual(timestamps, readTimestamps);
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ZeroRowColumns_InsertsNothingAndStaysReady()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (value Int32) ENGINE = Memory");

            IColumn empty = PrimitiveColumn<int>.FromValues("value", "Int32", Array.Empty<int>());
            await connection.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { empty }, cancellationToken: None);

            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(await CountAsync(connection, table), Is.EqualTo(0UL));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ColumnCountDisagreesWithSchema_AbortsCleanlyAndStaysReady()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (a Int32, b Int32) ENGINE = Memory");

            // The table has two columns but the insert supplies one.
            IColumn onlyOne = PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2, 3 });

            var thrown = Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.InsertAsync($"INSERT INTO {table} VALUES", new[] { onlyOne }, cancellationToken: None));
            Assert.That(thrown.Message, Does.Contain("schema"));

            // The insert was aborted cleanly (empty row stream), so nothing was written and the connection is reusable.
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(await CountAsync(connection, table), Is.EqualTo(0UL));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ZeroRowColumnCountDisagreesWithSchema_AbortsCleanlyAndStaysReady()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (a Int32, b Int32) ENGINE = Memory");

            // Two-column table but one (zero-row) column supplied: the count mismatch is caught even with no rows,
            // rather than silently succeeding.
            IColumn onlyOne = PrimitiveColumn<int>.FromValues("a", "Int32", Array.Empty<int>());

            var thrown = Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.InsertAsync($"INSERT INTO {table} VALUES", new[] { onlyOne }, cancellationToken: None));
            Assert.That(thrown.Message, Does.Contain("schema"));

            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(await CountAsync(connection, table), Is.EqualTo(0UL));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ColumnClrTypeNotWritableAsTarget_ThrowsThenConnectionIsReusable()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (value Int32) ENGINE = Memory");

            // The target column is Int32 but the value column carries longs: the Int32 codec cannot write it.
            // The type is resolved from the server's schema, so this is caught after the schema round-trip.
            IColumn mismatched = PrimitiveColumn<long>.FromValues("value", "Int32", new[] { 1L, 2L });

            var thrown = Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { mismatched }, cancellationToken: None));
            Assert.That(thrown.Message, Does.Contain("Int32"));

            // Only the terminator went out (no data block), so the server saw an insert of no rows and the
            // connection is left ready and usable — and nothing was actually inserted.
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(await CountAsync(connection, table), Is.EqualTo(0UL));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_SubsetOfColumns_LeavesOmittedColumnsAtTheirDefaults()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (a Int32, b Int32 DEFAULT 42, c String) ENGINE = Memory");

            // Name only a and c; b is not supplied, so the server fills it from its DEFAULT expression.
            IColumn[] columns =
            {
                PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2 }),
                new ArrayColumn<string>("c", "String", new[] { "x", "y" }),
            };
            await connection.InsertAsync($"INSERT INTO {table} (a, c) VALUES", columns, cancellationToken: None);

            var readA = new List<int>();
            var readB = new List<int>();
            var readC = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT a, b, c FROM {table} ORDER BY a", cancellationToken: None))
            {
                readA.AddRange(((IColumn<int>)block[0]).Values.ToArray());
                readB.AddRange(((IColumn<int>)block[1]).Values.ToArray());
                readC.AddRange(((IColumn<string>)block[2]).Values.ToArray());
            }

            Assert.Multiple(() =>
            {
                CollectionAssert.AreEqual(new[] { 1, 2 }, readA);
                CollectionAssert.AreEqual(new[] { 42, 42 }, readB); // filled from DEFAULT
                CollectionAssert.AreEqual(new[] { "x", "y" }, readC);
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ColumnsSuppliedOutOfSchemaOrder_AlignsByNameAndRoundTrips()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (a Int32, b String) ENGINE = Memory");

            // Supply the columns in the opposite order to the statement's (a, b); alignment is by name, not order.
            IColumn[] columns =
            {
                new ArrayColumn<string>("b", "String", new[] { "one", "two" }),
                PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2 }),
            };
            await connection.InsertAsync($"INSERT INTO {table} (a, b) VALUES", columns, cancellationToken: None);

            var readA = new List<int>();
            var readB = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT a, b FROM {table} ORDER BY a", cancellationToken: None))
            {
                readA.AddRange(((IColumn<int>)block[0]).Values.ToArray());
                readB.AddRange(((IColumn<string>)block[1]).Values.ToArray());
            }

            Assert.Multiple(() =>
            {
                CollectionAssert.AreEqual(new[] { 1, 2 }, readA);
                CollectionAssert.AreEqual(new[] { "one", "two" }, readB);
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ColumnNameNotInTarget_AbortsCleanlyAndStaysReady()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (a Int32) ENGINE = Memory");

            // The statement names only 'a', so the schema block carries 'a'; a column named 'z' matches nothing.
            IColumn wrongName = PrimitiveColumn<int>.FromValues("z", "Int32", new[] { 1, 2 });

            var thrown = Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.InsertAsync($"INSERT INTO {table} (a) VALUES", new[] { wrongName }, cancellationToken: None));
            Assert.Multiple(() =>
            {
                Assert.That(thrown.Message, Does.Contain("z"));
                Assert.That(thrown.Message, Does.Contain("a"));
            });

            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(await CountAsync(connection, table), Is.EqualTo(0UL));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ColumnsWithDifferingRowCounts_ThrowsWithoutTouchingConnection()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        IColumn two = PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2 });
        IColumn three = PrimitiveColumn<int>.FromValues("b", "Int32", new[] { 3, 4, 5 });

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await connection.InsertAsync("INSERT INTO whatever VALUES", new[] { two, three }, cancellationToken: None));

        // Validation happens before the connection is claimed, so it stays ready and usable.
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        byte probe = 0;
        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            probe = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.That(probe, Is.EqualTo((byte)1));
    }

    [Test]
    public async Task InsertAsync_ServerRejectsStatement_ThrowsThenConnectionIsReusable()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        IColumn column = PrimitiveColumn<int>.FromValues("value", "Int32", new[] { 1 });

        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await connection.InsertAsync("INSERT INTO table_that_does_not_exist_xyz (value) VALUES", new[] { column }, cancellationToken: None));
        Assert.That(thrown.Code, Is.GreaterThan(0));

        // The server Exception is a complete response, so the same connection can run another statement.
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        byte probe = 0;
        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            probe = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.That(probe, Is.EqualTo((byte)1));
    }

    private static void AssertColumnsEqual(IColumn expected, IColumn actual)
    {
        Assert.That(actual.RowCount, Is.EqualTo(expected.RowCount), "row count");
        for (int row = 0; row < expected.RowCount; row++)
        {
            Assert.That(actual.GetValue(row), Is.EqualTo(expected.GetValue(row)), $"row {row}");
        }
    }

    private static async Task<ulong> CountAsync(ClickHouseTcpConnection connection, string table)
    {
        ulong count = 0;
        await foreach (Block block in connection.QueryAsync($"SELECT count() FROM {table}", cancellationToken: None))
        {
            count = ((IColumn<ulong>)block[0]).Values[0];
        }

        return count;
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }

    private static string UniqueTableName() => $"tcp_insert_test_{Guid.NewGuid():N}";
}
