using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Inserts of caller columns that the converter layer writes and the codecs alone did not:
/// <c>LowCardinality(String)</c> from <see cref="T:byte[]"/> (ClickHouse/integrations#792), <c>FixedString(N)</c> from
/// <see cref="string"/>, and a <c>Variant</c> value whose CLR type is the canonical type of no alternative. Each insert
/// goes out two rows to a block, so every block after the first is a slice that starts after earlier rows.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ConvertedInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly byte[][] Bytes = { new byte[] { 0x41 }, new byte[] { 0xFF, 0xFE }, new byte[] { 0x41 }, Array.Empty<byte>(), new byte[] { 0xFF, 0xFE } };

    /// <summary>
    /// The three shapes of ClickHouse/integrations#792: the server stores the bytes as they are, also bytes that UTF-8
    /// cannot spell.
    /// </summary>
    [TestCase("LowCardinality(String)", "hex(value)")]
    [TestCase("LowCardinality(Nullable(String))", "ifNull(hex(value), 'NULL')")]
    [TestCase("Array(LowCardinality(String))", "arrayStringConcat(arrayMap(x -> hex(x), value), ',')")]
    public async Task InsertAsync_BytesIntoLowCardinalityString_StoresTheBytes(string type, string readBack)
    {
        (IColumn column, string[] expected) = type switch
        {
            "LowCardinality(String)" => ((IColumn)ClickHouseTcpColumn.Create("value", Bytes), new[] { "41", "FFFE", "41", string.Empty, "FFFE" }),
            "LowCardinality(Nullable(String))" => (
                (IColumn)ClickHouseTcpColumn.Create("value", new[] { Bytes[0], null, Bytes[1], Bytes[3], null }),
                new[] { "41", "NULL", "FFFE", string.Empty, "NULL" }),
            _ => (
                (IColumn)ClickHouseTcpColumn.Create("value", new[] { new[] { Bytes[0], Bytes[1] }, Array.Empty<byte[]>(), new[] { Bytes[1] }, new[] { Bytes[3], Bytes[0] }, new[] { Bytes[1] } }),
                new[] { "41,FFFE", string.Empty, "FFFE", ",41", "FFFE" }),
        };

        Assert.That(ClickHouseTcpTypes.CanWrite(type, column.ElementType), Is.True);
        Assert.That(await InsertAndReadAsync(type, column, readBack), Is.EqualTo(expected));
    }

    /// <summary>
    /// <c>FixedString(N)</c> from text, bare and under a composite: the UTF-8 bytes of the text, then zero bytes up to N.
    /// </summary>
    [TestCase("FixedString(4)")]
    [TestCase("LowCardinality(FixedString(4))")]
    [TestCase("Nullable(FixedString(4))")]
    [TestCase("Array(FixedString(4))")]
    public async Task InsertAsync_TextIntoFixedString_StoresTheUtf8WithZeroBytesUpToN(string type)
    {
        string[] texts = { "ab", "é", "abcd", string.Empty, "ab" };
        string[] expected = { "61620000", "C3A90000", "61626364", "00000000", "61620000" };
        IColumn column = type.StartsWith("Array", StringComparison.Ordinal)
            ? ClickHouseTcpColumn.Create("value", Array.ConvertAll(texts, text => new[] { text }))
            : ClickHouseTcpColumn.Create("value", texts);
        string readBack = type.StartsWith("Array", StringComparison.Ordinal) ? "hex(value[1])" : "hex(value)";

        Assert.That(ClickHouseTcpTypes.CanWrite(type, column.ElementType), Is.True);
        Assert.That(await InsertAndReadAsync(type, column, readBack), Is.EqualTo(expected));
    }

    /// <summary>
    /// A text of more UTF-8 bytes than N is refused with its row in the column, also in a block that starts after
    /// earlier rows, and the block that holds it is not stored.
    /// </summary>
    [Test]
    public async Task InsertAsync_TextLongerThanFixedString_ThrowsWithItsRow()
    {
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt32, value FixedString(2)) ENGINE = MergeTree ORDER BY id");

            ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(async () => await connection.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new IColumn[] { ClickHouseTcpColumn.Create("id", new uint[] { 0, 1, 2 }), ClickHouseTcpColumn.Create("value", new[] { "ab", "a", "abc" }) },
                maxRowsPerBlock: 2,
                cancellationToken: None));

            // The insert that failed ends its connection, so count on a new one.
            await using ClickHouseTcpConnection counter = await TcpServerFixture.ConnectAsync(None);
            string[] count = await ReadAsync(counter, $"SELECT toString(count()) FROM {table}");

            Assert.Multiple(() =>
            {
                Assert.That(thrown.Message, Does.StartWith("A FixedString(2) value at row 2 is 3 bytes in UTF-8; a text value can have at most 2 bytes."));
                Assert.That(count, Is.EqualTo(new[] { "2" }).Or.EqualTo(new[] { "0" }), "the first block can arrive before the failure; the failing one does not");
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {table}");
        }
    }

    /// <summary>
    /// A Variant value whose CLR type is the canonical type of no alternative lands in the alternative that is written
    /// from its type: a <see cref="DateTimeOffset"/> in a DateTime alternative, bytes in a String alternative.
    /// </summary>
    [Test]
    public async Task InsertAsync_VariantValueOfAnotherClrType_LandsInTheAlternativeWrittenFromIt()
    {
        var instant = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.FromHours(2));
        object[] values = { instant, new byte[] { 0x41, 0xFF }, 7UL, null, "x" };
        IColumn column = ClickHouseTcpColumn.Create("value", values);

        string[] readBack = await InsertAndReadAsync(
            "Variant(DateTime('UTC'), String, UInt64)",
            column,
            "concat(toString(variantType(value)), ':', hex(ifNull(toString(value), '')))");

        Assert.That(readBack, Is.EqualTo(new[]
        {
            "DateTime('UTC'):" + Hex("2024-06-15 10:00:00"),
            "String:41FF",
            "UInt64:" + Hex("7"),
            "None:",
            "String:" + Hex("x"),
        }));
    }

    private static string Hex(string text) => Convert.ToHexString(Encoding.UTF8.GetBytes(text));

    // Inserts the column with an id, two rows to a block, and reads the expression back in id order.
    private static async Task<string[]> InsertAndReadAsync(string type, IColumn column, string readBack)
    {
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt32, value {type}) ENGINE = Memory");
            var ids = new uint[column.RowCount];
            for (int i = 0; i < ids.Length; i++)
            {
                ids[i] = (uint)i;
            }

            await connection.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new[] { ClickHouseTcpColumn.Create("id", ids), column },
                maxRowsPerBlock: 2,
                cancellationToken: None);

            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            return await ReadAsync(connection, $"SELECT {readBack} FROM {table} ORDER BY id");
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    private static async Task<string[]> ReadAsync(ClickHouseTcpConnection connection, string sql)
    {
        var values = new List<string>();
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                values.Add((string)block[0].GetValue(row));
            }
        }

        return values.ToArray();
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }

    private static string UniqueTableName() => $"tcp_converted_insert_test_{Guid.NewGuid():N}";
}
