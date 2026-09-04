using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// A yielded Block is borrowed — valid only for its iteration — so every read copies or compares inside the
// await foreach, never retaining the block. The object[] rows from QueryAsync are owned and may be retained.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpClientIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static string UniqueTableName() => $"tcp_client_test_{Guid.NewGuid():N}";

    private sealed class IdRow
    {
        public ulong Id { get; set; }
    }

    [Test]
    public async Task StreamAsync_SelectLiteral_ReturnsSingleBlock()
    {
        await using var client = TcpServerFixture.CreateClient();

        int blockCount = 0;
        byte value = 0;
        await foreach (Block block in client.StreamAsync("SELECT 1", cancellationToken: None))
        {
            blockCount++;
            value = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.Multiple(() =>
        {
            Assert.That(blockCount, Is.EqualTo(1));
            Assert.That(value, Is.EqualTo((byte)1));
        });
    }

    [Test]
    public async Task StreamAsync_EnumeratorDisposedEarly_ConnectionReusableForNextQuery()
    {
        await using var client = TcpServerFixture.CreateClient();

        // Stop after the first block without draining — this disposes the enumerator mid-response, which
        // terminates the underlying connection. The next operation must transparently redial.
        await foreach (Block block in client.StreamAsync("SELECT number FROM system.numbers LIMIT 100000", cancellationToken: None))
        {
            _ = block.RowCount;
            break;
        }

        // A fresh query on the same client succeeds (the source redialed a new connection).
        var rows = await client.QueryAsync("SELECT 7").ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That((byte)rows[0][0], Is.EqualTo((byte)7));
    }

    [Test]
    public async Task StreamAsync_ServerError_ThrowsAndClientStillUsable()
    {
        await using var client = TcpServerFixture.CreateClient();

        // A server exception retires its connection; the pool transparently dials a replacement here.
        Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
        {
            await foreach (Block _ in client.StreamAsync("SELECT * FROM table_that_does_not_exist_xyz", cancellationToken: None))
            {
            }
        });

        var rows = await client.QueryAsync("SELECT 1").ToListAsync();
        Assert.That(rows, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task QueryAsync_SelectMultipleColumns_ReturnsObjectArrayRowsInHeaderOrder()
    {
        await using var client = TcpServerFixture.CreateClient();

        var rows = await client.QueryAsync("SELECT number, toString(number) FROM system.numbers LIMIT 3").ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.Multiple(() =>
        {
            for (int i = 0; i < 3; i++)
            {
                Assert.That(rows[i], Has.Length.EqualTo(2));
                Assert.That((ulong)rows[i][0], Is.EqualTo((ulong)i));
                Assert.That((string)rows[i][1], Is.EqualTo(i.ToString()));
            }
        });
    }

    [Test]
    public async Task QueryAsync_RowRetainedPastEnumeration_StillValid()
    {
        await using var client = TcpServerFixture.CreateClient();

        // Unlike a Block, an object[] row is owned and safe to keep after the enumeration ends.
        object[] firstRow = null;
        await foreach (object[] row in client.QueryAsync("SELECT number FROM system.numbers LIMIT 2"))
        {
            firstRow ??= row;
        }

        Assert.That(firstRow, Is.Not.Null);
        Assert.That((ulong)firstRow[0], Is.EqualTo(0UL));
    }

    [Test]
    public async Task ExecuteAsync_CreateInsertDropRoundTrip_Completes()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory");
            // INSERT ... SELECT runs entirely server-side (no client data block), so it exercises ExecuteAsync's
            // drain-to-completion without the inline-VALUES data-stream path (that is InsertAsync's job).
            await client.ExecuteAsync($"INSERT INTO {table} SELECT number FROM numbers(3)");

            var count = await client.QueryAsync($"SELECT count() FROM {table}").ToListAsync();
            Assert.That((ulong)count[0][0], Is.EqualTo(3UL));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public void ExecuteAsync_InvalidStatement_ThrowsServerException()
    {
        Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
        {
            await using var client = TcpServerFixture.CreateClient();
            await client.ExecuteAsync("THIS IS NOT VALID SQL");
        });
    }

    [Test]
    public async Task InsertAsync_ColumnarData_RoundTripsThroughSelect()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = Memory");

            const int rows = 2000;
            var ids = new ulong[rows];
            var names = new string[rows];
            for (int i = 0; i < rows; i++)
            {
                ids[i] = (ulong)i;
                names[i] = $"row-{i}";
            }

            IColumn[] columns =
            {
                PrimitiveColumn<ulong>.FromValues("id", "UInt64", ids),
                new ArrayColumn<string>("name", "String", names),
            };
            await client.InsertAsync($"INSERT INTO {table} (id, name) VALUES", columns);

            var readIds = new List<ulong>();
            var readNames = new List<string>();
            await foreach (Block block in client.StreamAsync($"SELECT id, name FROM {table} ORDER BY id"))
            {
                readIds.AddRange(((IColumn<ulong>)block[0]).Values.ToArray());
                readNames.AddRange(((IColumn<string>)block[1]).Values.ToArray());
            }

            Assert.Multiple(() =>
            {
                CollectionAssert.AreEqual(ids, readIds);
                CollectionAssert.AreEqual(names, readNames);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_TinyMaxSendBufferBytes_FlushesMidBlockAndRoundTrips()
    {
        // A send buffer far smaller than the encoded block forces repeated between-column flushes while the
        // single block is written. The data must still arrive intact — this proves the cap is threaded through
        // and that mid-block flushing does not corrupt the wire stream.
        var options = TcpServerFixture.Options();
        await using var client = new ClickHouseTcpClient(new ClickHouseTcpClientOptions
        {
            Host = options.Host,
            Port = options.Port,
            Username = options.Username,
            Password = options.Password,
            MaxSendBufferBytes = 4096,
        });

        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = Memory");

            const int rows = 20000;
            var ids = new ulong[rows];
            var names = new string[rows];
            for (int i = 0; i < rows; i++)
            {
                ids[i] = (ulong)i;
                names[i] = $"padding-value-to-exceed-the-tiny-buffer-{i}";
            }

            await client.InsertAsync(
                $"INSERT INTO {table} (id, name) VALUES",
                new IColumn[]
                {
                    PrimitiveColumn<ulong>.FromValues("id", "UInt64", ids),
                    new ArrayColumn<string>("name", "String", names),
                });

            var count = await client.QueryAsync($"SELECT count() FROM {table}").ToListAsync();
            var sum = await client.QueryAsync($"SELECT sum(id) FROM {table}").ToListAsync();
            Assert.Multiple(() =>
            {
                Assert.That((ulong)count[0][0], Is.EqualTo((ulong)rows));
                Assert.That((ulong)sum[0][0], Is.EqualTo((ulong)rows * (rows - 1) / 2));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_ZeroRows_IsNoOp()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory");

            IColumn empty = PrimitiveColumn<int>.FromValues("value", "Int32", Array.Empty<int>());
            await client.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { empty });

            var count = await client.QueryAsync($"SELECT count() FROM {table}").ToListAsync();
            Assert.That((ulong)count[0][0], Is.EqualTo(0UL));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task InsertAsync_SchemaMismatch_ThrowsArgumentExceptionAndClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (a Int32, b Int32) ENGINE = Memory");

            IColumn onlyOne = PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2, 3 });
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await client.InsertAsync($"INSERT INTO {table} VALUES", new[] { onlyOne }));

            // The failed insert aborted cleanly (server saw no rows), so the client is still usable.
            var count = await client.QueryAsync($"SELECT count() FROM {table}").ToListAsync();
            Assert.That((ulong)count[0][0], Is.EqualTo(0UL));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task QueryAsync_CustomSettingViaOptions_AppliesSetting()
    {
        await using var client = TcpServerFixture.CreateClient();

        // max_block_size = 2 forces the server to split the result into more, smaller blocks. Read at the block
        // tier so the effect (block count) is observable.
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["max_block_size"] = "2" },
        };

        int blockCount = 0;
        int rowCount = 0;
        await foreach (Block block in client.StreamAsync("SELECT number FROM system.numbers LIMIT 10", options))
        {
            blockCount++;
            rowCount += block.RowCount;
        }

        Assert.Multiple(() =>
        {
            Assert.That(rowCount, Is.EqualTo(10));
            Assert.That(blockCount, Is.GreaterThan(1), "a small max_block_size should split the result across several blocks");
        });
    }

    // 1 forces a block per row, null forces one block for all of them, and the unset case takes the default cap —
    // every row must survive each sizing.
    [TestCase(1, TestName = "InsertAsync_MaxRowsPerBlockOne_RoundTripsEveryRow")]
    [TestCase(null, TestName = "InsertAsync_MaxRowsPerBlockNull_RoundTripsEveryRow")]
    public async Task InsertAsync_MaxRowsPerBlockViaOptions_RoundTripsEveryRow(int? maxRowsPerBlock)
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory");

            const int rows = 50;
            var ids = new ulong[rows];
            for (int i = 0; i < rows; i++)
            {
                ids[i] = (ulong)i;
            }

            IColumn[] columns = { PrimitiveColumn<ulong>.FromValues("id", "UInt64", ids) };
            await client.InsertAsync(
                $"INSERT INTO {table} (id) VALUES",
                columns,
                new ClickHouseTcpInsertOptions { MaxRowsPerBlock = maxRowsPerBlock });

            var readBack = await client.QueryAsync($"SELECT id FROM {table} ORDER BY id").ToListAsync();
            CollectionAssert.AreEqual(ids, readBack.Select(r => (ulong)r[0]));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task Options_AfterConstruction_ExposesTheConfigurationIncludingTheSendBufferCap()
    {
        await using var client = new ClickHouseTcpClient(new ClickHouseTcpClientOptions
        {
            Host = TcpServerFixture.Host,
            Port = TcpServerFixture.Port,
            Username = TcpServerFixture.Username,
            Password = TcpServerFixture.Password,
            MaxSendBufferBytes = 4096,
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4" },
        });

        Assert.Multiple(() =>
        {
            Assert.That(client.Options.Host, Is.EqualTo(TcpServerFixture.Host));
            Assert.That(client.Options.MaxSendBufferBytes, Is.EqualTo(4096));
            Assert.That(client.Options.CustomSettings["max_threads"], Is.EqualTo("4"));
        });
    }

    [Test]
    public async Task IClickHouseTcpClient_EveryOperationUsableThroughTheInterface()
    {
        // The interface exists so consumers can code against it (and substitute a double), so it has to be
        // sufficient on its own: this drives the whole surface through an interface-typed reference, never
        // touching ClickHouseTcpClient.
        IClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using (client)
        {
            string table = UniqueTableName();
            try
            {
                await client.PingAsync(None);
                await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", cancellationToken: None);

                var ids = new ulong[] { 1, 2, 3 };
                IColumn[] columns = { PrimitiveColumn<ulong>.FromValues("id", "UInt64", ids) };
                await client.InsertAsync($"INSERT INTO {table} (id) VALUES", columns, cancellationToken: None);

                var rows = await client.QueryAsync($"SELECT id FROM {table} ORDER BY id").ToListAsync();
                var typed = await client.QueryAsync<IdRow>($"SELECT id FROM {table} ORDER BY id").ToListAsync();

                var streamed = new List<ulong>();
                await foreach (Block block in client.StreamAsync($"SELECT id FROM {table} ORDER BY id"))
                {
                    streamed.AddRange(((IColumn<ulong>)block[0]).Values.ToArray());
                }

                Assert.Multiple(() =>
                {
                    CollectionAssert.AreEqual(ids, rows.Select(r => (ulong)r[0]));
                    CollectionAssert.AreEqual(ids, typed.Select(row => row.Id));
                    CollectionAssert.AreEqual(ids, streamed);
                });
            }
            finally
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    [Test]
    public async Task Client_CallerMutatesCustomSettingsAfterConstruction_MutationNotApplied()
    {
        // getSetting reports the value the server actually applied to this query, so a client-level setting and any
        // later mutation of the caller's dictionary are both directly observable.
        const string ReadSetting = "SELECT toString(getSetting('max_block_size'))";

        var callerSettings = new Dictionary<string, string> { ["max_block_size"] = "1234" };
        await using var client = new ClickHouseTcpClient(new ClickHouseTcpClientOptions
        {
            Host = TcpServerFixture.Host,
            Port = TcpServerFixture.Port,
            Username = TcpServerFixture.Username,
            Password = TcpServerFixture.Password,
            CustomSettings = callerSettings,
        });

        var configured = await client.QueryAsync(ReadSetting).ToListAsync();
        Assert.That((string)configured[0][0], Is.EqualTo("1234"), "the client-level setting should reach the server");

        // Control: the server does apply a changed value, so the mutation below would be observable if it landed.
        var overridden = await client.QueryAsync(
            ReadSetting,
            new ClickHouseTcpQueryOptions { Settings = new Dictionary<string, string> { ["max_block_size"] = "4321" } })
            .ToListAsync();
        Assert.That((string)overridden[0][0], Is.EqualTo("4321"), "a per-query override should reach the server");

        // The client owns a copy taken at construction, so this mutation cannot reach the wire (nor fault the
        // settings merge that runs on every query).
        callerSettings["max_block_size"] = "4321";

        var afterMutation = await client.QueryAsync(ReadSetting).ToListAsync();
        Assert.That((string)afterMutation[0][0], Is.EqualTo("1234"), "the caller's later mutation must not be applied");
    }

    [Test]
    public async Task QueryAsync_DynamicColumn_DecodesWithoutCallerSettingFlattenedSerialization()
    {
        await using var client = TcpServerFixture.CreateClient();

        // The caller enables the Dynamic type but deliberately does NOT set
        // output_format_native_use_flattened_dynamic_and_json_serialization — the client injects it, so the
        // Dynamic column still decodes. Without that injection the block reader would desync.
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["allow_experimental_dynamic_type"] = "1" },
        };

        var rows = await client.QueryAsync("SELECT CAST(number, 'Dynamic') AS d FROM numbers(3)", options).ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows.All(r => r[0] is not null), Is.True);
    }

    [Test]
    public async Task QueryAsync_JsonColumn_DecodesAsTextWithoutCallerSettingJsonAsString()
    {
        await using var client = TcpServerFixture.CreateClient();

        // The caller sets no serialization setting at all — the client injects
        // output_format_native_write_json_as_string, so the column arrives as its JSON text. Without that injection
        // the server would send a per-path encoding and the codec would reject the version. (The experimental-type
        // flag is obsolete on every server this suite supports, so the query needs no settings whatsoever.)
        var rows = await client.QueryAsync("SELECT CAST('{\"a\":1}', 'JSON') AS j").ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0][0], Is.EqualTo("{\"a\":1}"));
    }

    // Registering the JSON codec also makes it reachable as a Dynamic runtime type, where the JSON version word is
    // nested inside the Dynamic state prefix (after the type-name list) rather than heading the column. Only the
    // read direction can be exercised: DynamicTypeInference maps a CLR string to String, not JSON, so a JSON value
    // cannot be written into a Dynamic ergonomically.
    [Test]
    public async Task QueryAsync_DynamicColumnHoldingJson_DecodesTheNestedJsonText()
    {
        await using var client = TcpServerFixture.CreateClient();

        var rows = await client.QueryAsync("SELECT CAST(CAST('{\"b\":2,\"a\":1}', 'JSON'), 'Dynamic') AS d").ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0][0], Is.EqualTo("{\"a\":1,\"b\":2}"));
    }

    [Test]
    public async Task PingAsync_LiveServer_Completes()
    {
        await using var client = TcpServerFixture.CreateClient();

        Assert.DoesNotThrowAsync(async () => await client.PingAsync(None));
    }

    [Test]
    public async Task Client_ConcurrentQueries_SerializeAndAllSucceed()
    {
        await using var client = TcpServerFixture.CreateClient();

        // The single-connection source serializes these on one connection; all should still complete correctly.
        Task<List<object[]>>[] queries = Enumerable.Range(0, 8)
            .Select(i => client.QueryAsync($"SELECT {i}").ToListAsync())
            .ToArray();

        List<object[]>[] results = await Task.WhenAll(queries);

        Assert.That(results, Has.All.Count.EqualTo(1));
    }

    [Test]
    public async Task Client_FromConnectionString_ConnectsAndQueries()
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.ConnectionString);

        var rows = await client.QueryAsync("SELECT 1").ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That((byte)rows[0][0], Is.EqualTo((byte)1));
    }
}

internal static class AsyncEnumerableTestExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
        {
            list.Add(item);
        }

        return list;
    }
}
