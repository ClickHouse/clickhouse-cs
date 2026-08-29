using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// A ClickHouse <c>String</c> is a byte string: it holds any byte sequence, and the server neither validates nor
/// transcodes it. These tests use bytes UTF-8 cannot spell, because that is where the text surface and the byte
/// surface stop agreeing — the text reading of such a row is U+FFFD, and anything that goes back to the server
/// through that reading stores the replacement character instead of the data.
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

    private static string UniqueTableName() => $"tcp_string_bytes_test_{Guid.NewGuid():N}";
}
