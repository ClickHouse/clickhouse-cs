using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies lossless read and write behavior for <c>String</c> values that are not valid UTF-8.
/// </summary>
[TestFixture]
[Category("Integration")]
public class StringBytesIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task StreamAsync_NonUtf8String_ExposesTheWireBytesThroughIStringColumn()
    {
        await using var client = TcpServerFixture.CreateClient();

        bool matched = false;
        byte[] rawBytes = null;
        string asText = null;
        string asLatin1 = null;

        await foreach (Block block in client.StreamAsync("SELECT unhex('41FFFE42') AS value", cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is IStringColumn;

            var text = (IStringColumn)column;
            rawBytes = text.GetBytes(0).ToArray();
            asText = text[0];
            asLatin1 = text.GetString(0, Encoding.Latin1);
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rawBytes, Is.EqualTo(new byte[] { 0x41, 0xFF, 0xFE, 0x42 }), "the bytes the server sent");
            Assert.That(asText, Is.EqualTo("A��B"), "which UTF-8 cannot spell, so the text reading loses them");
            Assert.That(asLatin1, Is.EqualTo("AÿþB"));
        });
    }

    [Test]
    public async Task InsertAsync_ByteRows_StoresThemVerbatimAndReadsThemBack()
    {
        // The write counterpart: a byte[] per row goes to the server as those bytes, so a caller can store data
        // that is not text at all without changing the column to FixedString(N), which would fix the width.
        var rows = new[] { new byte[] { 0x41 }, new byte[] { 0xFF, 0xFE }, Array.Empty<byte>() };

        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt8, value String) ENGINE = Memory", cancellationToken: None);
            await client.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new byte[] { 0, 1, 2 }),
                    ClickHouseTcpColumn.Create("value", rows),
                },
                cancellationToken: None);

            var hex = new string[3];
            await foreach (Block block in client.StreamAsync($"SELECT hex(value) FROM {table} ORDER BY id", cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    hex[row] = (string)block[0].GetValue(row);
                }
            }

            Assert.That(hex, Is.EqualTo(new[] { "41", "FFFE", string.Empty }));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_DecodedNonUtf8ColumnReinserted_KeepsTheBytesItRead()
    {
        // Reading a column and inserting it again is the documented way to copy data, and it is what the dense
        // re-insert path does. The bytes have to survive that: the column still holds what the wire carried, so it
        // is re-emitted rather than re-encoded from its text reading.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value String) ENGINE = Memory", cancellationToken: None);

            await foreach (Block block in client.StreamAsync("SELECT unhex('41FFFE42') AS value", cancellationToken: None))
            {
                await client.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { block[0] }, cancellationToken: None);
            }

            string hex = null;
            await foreach (Block block in client.StreamAsync($"SELECT hex(value) FROM {table}", cancellationToken: None))
            {
                hex = (string)block[0].GetValue(0);
            }

            Assert.That(hex, Is.EqualTo("41FFFE42"), "re-encoding the text reading would have stored EFBFBD twice instead");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// Verifies that <c>ReadAs&lt;byte[]&gt;</c> returns the wire bytes rather than re-encoded text.
    /// </summary>
    [Test]
    public async Task ReadAs_NonUtf8StringColumn_GivesTheWireBytesRatherThanTheTextReading()
    {
        var rows = new[] { new byte[] { 0x41, 0xFF, 0xFE, 0x42 }, Array.Empty<byte>(), new byte[] { 0x7A } };

        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt8, value String) ENGINE = Memory", cancellationToken: None);
            await client.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new byte[] { 0, 1, 2 }),
                    ClickHouseTcpColumn.Create("value", rows),
                },
                cancellationToken: None);

            var perRow = new List<byte[]>();
            var materialized = new List<byte[]>();
            var asText = new List<string>();
            await foreach (Block block in client.StreamAsync($"SELECT value FROM {table} ORDER BY id", cancellationToken: None))
            {
                IColumn<byte[]> bytes = block.ReadAs<byte[]>("value");
                for (int row = 0; row < block.RowCount; row++)
                {
                    perRow.Add(bytes[row]);
                }

                materialized.AddRange(bytes.Values.ToArray());
                asText.AddRange(block.Column<string>("value").Values.ToArray());
            }

            Assert.Multiple(() =>
            {
                Assert.That(perRow, Is.EqualTo(rows), "read one row at a time through the indexer");
                Assert.That(materialized, Is.EqualTo(rows), "Values converts the column once, and must agree");
                Assert.That(asText[0], Is.EqualTo("A\uFFFD\uFFFDB"), "the text reading of the same row, which loses the bytes");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// Verifies that nullable string projections preserve both raw bytes and NULL rows.
    /// </summary>
    [Test]
    public async Task ReadAs_NullableStringColumn_GivesTheBytesAndLeavesTheNullRowsNull()
    {
        var rows = new[] { new byte[] { 0x41, 0xFF }, null, Array.Empty<byte>() };

        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt8, value Nullable(String)) ENGINE = Memory", cancellationToken: None);
            await client.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new byte[] { 0, 1, 2 }),
                    ClickHouseTcpColumn.Create("value", rows),
                },
                cancellationToken: None);

            var read = new List<byte[]>();
            await foreach (Block block in client.StreamAsync($"SELECT value FROM {table} ORDER BY id", cancellationToken: None))
            {
                IColumn<byte[]> bytes = block.ReadAs<byte[]>("value");
                for (int row = 0; row < block.RowCount; row++)
                {
                    read.Add(bytes[row]);
                }
            }

            Assert.That(read, Is.EqualTo(rows));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// Verifies raw-byte POCO properties for <c>String</c> and <c>Nullable(String)</c>.
    /// </summary>
    [Test]
    public async Task QueryAsync_ByteArrayProperties_AreFilledWithTheWireBytes()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (Id UInt8, Value String, Optional Nullable(String)) ENGINE = Memory",
                cancellationToken: None);
            await client.InsertAsync(
                $"INSERT INTO {table} (Id, Value, Optional) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("Id", new byte[] { 0, 1 }),
                    ClickHouseTcpColumn.Create("Value", new[] { new byte[] { 0x41, 0xFF, 0xFE, 0x42 }, Array.Empty<byte>() }),
                    ClickHouseTcpColumn.Create("Optional", new[] { new byte[] { 0xFE }, null }),
                },
                cancellationToken: None);

            var rows = new List<BlobRow>();
            await foreach (BlobRow row in client.QueryAsync<BlobRow>($"SELECT Id, Value, Optional FROM {table} ORDER BY Id", cancellationToken: None))
            {
                rows.Add(row);
            }

            Assert.Multiple(() =>
            {
                Assert.That(rows, Has.Count.EqualTo(2));
                Assert.That(rows[0].Value, Is.EqualTo(new byte[] { 0x41, 0xFF, 0xFE, 0x42 }));
                Assert.That(rows[0].Optional, Is.EqualTo(new byte[] { 0xFE }));
                Assert.That(rows[1].Value, Is.EqualTo(Array.Empty<byte>()));
                Assert.That(rows[1].Optional, Is.Null);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// Verifies raw-byte POCO properties under array and low-cardinality wrappers.
    /// </summary>
    [Test]
    public async Task QueryAsync_ByteArrayPropertiesUnderAWrapper_AreFilledWithTheWireBytes()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (Id UInt8, Parts Array(String), Label LowCardinality(String)) ENGINE = Memory",
                cancellationToken: None);
            await client.InsertAsync(
                $"INSERT INTO {table} (Id, Parts, Label) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("Id", new byte[] { 0, 1, 2 }),
                    ClickHouseTcpColumn.Create(
                        "Parts",
                        new[] { new[] { new byte[] { 0x41, 0xFF }, Array.Empty<byte>() }, Array.Empty<byte[]>(), new[] { new byte[] { 0xFE } } }),
                    ClickHouseTcpColumn.Create("Label", new[] { "shared", "other", "shared" }),
                },
                cancellationToken: None);

            var rows = new List<NestedBlobRow>();
            await foreach (NestedBlobRow row in client.QueryAsync<NestedBlobRow>($"SELECT Id, Parts, Label FROM {table} ORDER BY Id", cancellationToken: None))
            {
                rows.Add(row);
            }

            Assert.Multiple(() =>
            {
                Assert.That(rows, Has.Count.EqualTo(3));
                Assert.That(rows[0].Parts, Is.EqualTo(new[] { new byte[] { 0x41, 0xFF }, Array.Empty<byte>() }));
                Assert.That(rows[1].Parts, Is.Empty);
                Assert.That(rows[2].Parts, Is.EqualTo(new[] { new byte[] { 0xFE } }));
                Assert.That(rows[0].Label, Is.EqualTo(new byte[] { 0x73, 0x68, 0x61, 0x72, 0x65, 0x64 }));
                Assert.That(rows[1].Label, Is.EqualTo(new byte[] { 0x6F, 0x74, 0x68, 0x65, 0x72 }));
                Assert.That(ReferenceEquals(rows[0].Label, rows[2].Label), Is.True, "both rows hold the same dictionary entry");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// Verifies that POCO materialization reuses one projected dictionary across row windows.
    /// </summary>
    [Test]
    public async Task QueryAsync_ByteArrayPropertyOverALowCardinalityColumn_ConvertsEachDictionaryEntryOnce()
    {
        await using var client = TcpServerFixture.CreateClient();

        var rows = new List<LabelRow>();
        await foreach (LabelRow row in client.QueryAsync<LabelRow>(
            @"SELECT toUInt64(number) AS Id, CAST(['aa', 'bb'][1 + number % 2] AS LowCardinality(String)) AS Label
              FROM system.numbers LIMIT 600",
            cancellationToken: None))
        {
            rows.Add(row);
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(600), "more rows than one materialization window holds");
            Assert.That(rows[0].Label, Is.EqualTo(new byte[] { 0x61, 0x61 }));
            Assert.That(rows[1].Label, Is.EqualTo(new byte[] { 0x62, 0x62 }));
            Assert.That(rows[2].Label, Is.SameAs(rows[0].Label), "the same window");
            Assert.That(rows[400].Label, Is.SameAs(rows[0].Label), "a later window of the same block");
        });
    }

    private static string UniqueTableName() => $"tcp_string_bytes_test_{Guid.NewGuid():N}";

    private sealed class BlobRow
    {
        public byte Id { get; set; }

        public byte[] Value { get; set; }

        public byte[] Optional { get; set; }
    }

    private sealed class NestedBlobRow
    {
        public byte Id { get; set; }

        public byte[][] Parts { get; set; }

        public byte[] Label { get; set; }
    }

    private sealed class LabelRow
    {
        public ulong Id { get; set; }

        public byte[] Label { get; set; }
    }
}
