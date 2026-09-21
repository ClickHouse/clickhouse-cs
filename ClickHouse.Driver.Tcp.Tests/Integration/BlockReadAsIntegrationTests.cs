using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies <see cref="Block.ReadAs{T}(string)"/> conversions against values returned by a real server.
/// </summary>
[TestFixture]
[Category("Integration")]
public class BlockReadAsIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task ReadAs_ColumnAlreadyOfTheRequestedType_IsTheColumnItself()
    {
        // The fast path: nothing is projected and nothing wrapped, so Values is still the block's borrowed span.
        await using var client = TcpServerFixture.CreateClient();

        bool sameInstance = false;
        ulong[] values = null;

        await foreach (Block block in client.StreamAsync("SELECT toUInt64(number) FROM system.numbers LIMIT 3", cancellationToken: None))
        {
            IColumn<ulong> read = block.ReadAs<ulong>(0);
            sameInstance = ReferenceEquals(read, block[0]);
            values = read.Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(sameInstance, Is.True);
            Assert.That(values, Is.EqualTo(new ulong[] { 0, 1, 2 }));
        });
    }

    [Test]
    public async Task ReadAs_DateTime64Column_ReadsTheCalendarValueWhileColumnReadsTheRawCount()
    {
        // The two readings of one column, side by side: the raw count the wire carries, and the instant it means.
        await using var client = TcpServerFixture.CreateClient();

        long rawCount = 0;
        DateTimeOffset offset = default;
        DateTime dateTime = default;
        DateTime[] materialized = null;

        await foreach (Block block in client.StreamAsync(
            "SELECT toDateTime64('2024-06-15 14:00:00.125', 3, 'UTC') + number AS ts FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            rawCount = block.Column<long>("ts")[0];
            offset = block.ReadAs<DateTimeOffset>("ts")[0];

            IColumn<DateTime> calendar = block.ReadAs<DateTime>("ts");
            dateTime = calendar[0];
            materialized = calendar.Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(offset.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture), Is.EqualTo("2024-06-15 14:00:00.125"));
            Assert.That(rawCount, Is.EqualTo(offset.ToUnixTimeMilliseconds()), "scale 3 means the count is milliseconds");
            Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Utc), "a zero offset presents as UTC");
            Assert.That(materialized, Has.Length.EqualTo(2), "Values converts the whole column");
            Assert.That(materialized[0], Is.EqualTo(dateTime), "and agrees with the indexer");
            Assert.That(materialized[1] - materialized[0], Is.EqualTo(TimeSpan.FromSeconds(1)));
        });
    }

    [Test]
    public async Task ReadAs_EnumColumn_ReadsTheDeclaredLabels()
    {
        await using var client = TcpServerFixture.CreateClient();

        string[] labels = null;
        sbyte[] ordinals = null;

        await foreach (Block block in client.StreamAsync(
            "SELECT CAST(number + 1 AS Enum8('queued' = 1, 'running' = 2, 'done' = 3)) AS state FROM system.numbers LIMIT 3",
            cancellationToken: None))
        {
            labels = block.ReadAs<string>("state").Values.ToArray();
            ordinals = block.Column<sbyte>("state").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(labels, Is.EqualTo(new[] { "queued", "running", "done" }));
            Assert.That(ordinals, Is.EqualTo(new sbyte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public async Task ReadAs_ArrayOfDateTime_ConvertsEveryElementOfEveryRow()
    {
        // The reading a composite offers is composed from its inner type's, so one call converts a whole row.
        await using var client = TcpServerFixture.CreateClient();

        DateTime[][] rows = null;

        await foreach (Block block in client.StreamAsync(
            "SELECT arrayMap(i -> toDateTime('2024-06-15 14:00:00', 'UTC') + i, range(number + 1)) AS stamps " +
            "FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            IColumn<DateTime[]> stamps = block.ReadAs<DateTime[]>("stamps");
            rows = new[] { stamps[0], stamps[1] };
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows[0], Is.EqualTo(new[] { new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Utc) }));
            Assert.That(rows[1], Has.Length.EqualTo(2));
            Assert.That(rows[1][1], Is.EqualTo(new DateTime(2024, 6, 15, 14, 0, 1, DateTimeKind.Utc)));
        });
    }

    [Test]
    public async Task ReadAs_NullableDateTime64_KeepsTheNullAndConvertsTheRest()
    {
        await using var client = TcpServerFixture.CreateClient();

        DateTime?[] readings = null;

        await foreach (Block block in client.StreamAsync(
            "SELECT if(number = 1, NULL, toDateTime64('2024-06-15 14:00:00.500', 3, 'UTC') + number) AS ts " +
            "FROM system.numbers LIMIT 3",
            cancellationToken: None))
        {
            readings = block.ReadAs<DateTime?>("ts").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(readings[1], Is.Null);
            Assert.That(
                readings[0]?.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                Is.EqualTo("2024-06-15 14:00:00.500"));
            Assert.That(readings[2] - readings[0], Is.EqualTo(TimeSpan.FromSeconds(2)));
        });
    }

    /// <summary>
    /// Verifies that cached projections keep the session timezone in their cache key.
    /// </summary>
    [Test]
    public async Task ReadAs_TimezoneLessDateTime_ResolvesTheSessionTimezoneOfEachQuery()
    {
        await using var client = TcpServerFixture.CreateClient();

        DateTimeOffset inUtc = await ReadOneAsync(client, "UTC");
        DateTimeOffset inAmsterdam = await ReadOneAsync(client, "Europe/Amsterdam");

        Assert.Multiple(() =>
        {
            Assert.That(inUtc.Offset, Is.EqualTo(TimeSpan.Zero));
            Assert.That(inAmsterdam.Offset, Is.EqualTo(TimeSpan.FromHours(2)), "June, so Amsterdam is at +02:00");
            Assert.That(inAmsterdam.UtcDateTime, Is.EqualTo(inUtc.UtcDateTime.AddHours(-2)), "same wall clock, two zones");
        });

        static async Task<DateTimeOffset> ReadOneAsync(ClickHouseTcpClient client, string timezone)
        {
            var options = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["session_timezone"] = timezone },
            };

            DateTimeOffset read = default;
            await foreach (Block block in client.StreamAsync("SELECT toDateTime('2024-06-15 14:00:00') AS ts", options, None))
            {
                read = block.ReadAs<DateTimeOffset>("ts")[0];
            }

            return read;
        }
    }

    [Test]
    public async Task ReadAs_TypeThatOffersNoSuchReading_ThrowsNamingWhatItDoesRead()
    {
        // No numeric widening: a UInt32 column reads as a uint and nothing else, on this tier and in a POCO alike.
        await using var client = TcpServerFixture.CreateClient();

        await foreach (Block block in client.StreamAsync("SELECT toUInt32(number) AS n FROM system.numbers LIMIT 1", cancellationToken: None))
        {
            var thrown = Assert.Throws<InvalidCastException>(() => block.ReadAs<long>("n"));
            Assert.That(thrown.Message, Does.Contain("'n'").And.Contain("UInt32").And.Contain("System.Int64"));
            Assert.That(thrown.Message, Does.Contain("It reads as: System.UInt32."));
        }
    }

    /// <summary>
    /// Verifies that <c>LowCardinality</c> preserves the inner type's projected values.
    /// </summary>
    [Test]
    public async Task ReadAs_LowCardinalityColumn_GivesTheSameValuesAsTheReadingWithoutADictionary()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = SuspiciousLowCardinality();

        DateTimeOffset[] throughDictionary = null;
        DateTimeOffset[] withoutDictionary = null;
        uint[] rawCounts = null;

        // Twelve rows over three distinct values, so most rows repeat an earlier one.
        const string Values = "toDateTime('2024-06-15 14:00:00', 'UTC') + INTERVAL (number % 3) DAY";
        await foreach (Block block in client.StreamAsync(
            $"SELECT CAST({Values} AS LowCardinality(DateTime('UTC'))) AS v FROM system.numbers LIMIT 12",
            options,
            cancellationToken: None))
        {
            throughDictionary = block.ReadAs<DateTimeOffset>("v").Values.ToArray();
            rawCounts = block.Column<uint>("v").Values.ToArray();
        }

        await foreach (Block block in client.StreamAsync(
            $"SELECT CAST({Values} AS DateTime('UTC')) AS v FROM system.numbers LIMIT 12",
            options,
            cancellationToken: None))
        {
            withoutDictionary = block.ReadAs<DateTimeOffset>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(throughDictionary, Is.EqualTo(withoutDictionary));
            Assert.That(throughDictionary[0].UtcDateTime, Is.EqualTo(new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Utc)));
            Assert.That(throughDictionary[1].UtcDateTime, Is.EqualTo(new DateTime(2024, 6, 16, 14, 0, 0, DateTimeKind.Utc)));
            Assert.That(throughDictionary[3], Is.EqualTo(throughDictionary[0]), "row 3 repeats row 0's value");

            // The undecoded reading of the same column, to pin that the keys really were resolved rather than read
            // as values: a raw count is the epoch second, not a dictionary index.
            Assert.That(rawCounts[0], Is.EqualTo(1718460000u));
        });
    }

    /// <summary>
    /// Verifies that nullable dictionary slot 0 projects as NULL.
    /// </summary>
    [Test]
    public async Task ReadAs_NullableLowCardinalityColumn_LiftsTheInnerReadingAndKeepsTheNullRowsNull()
    {
        await using var client = TcpServerFixture.CreateClient();

        DateTimeOffset?[] read = null;
        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST(if(number % 2 = 0, NULL, toDateTime('2024-06-15 14:00:00', 'UTC') + INTERVAL (number % 4) DAY), 'LowCardinality(Nullable(DateTime(''UTC'')))') AS v
              FROM system.numbers LIMIT 8",
            SuspiciousLowCardinality(),
            cancellationToken: None))
        {
            read = block.ReadAs<DateTimeOffset?>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(read.Length, Is.EqualTo(8));
            Assert.That(read[0], Is.Null);
            Assert.That(read[2], Is.Null, "every even row is NULL");
            Assert.That(read[1]?.UtcDateTime, Is.EqualTo(new DateTime(2024, 6, 16, 14, 0, 0, DateTimeKind.Utc)));
            Assert.That(read[3]?.UtcDateTime, Is.EqualTo(new DateTime(2024, 6, 18, 14, 0, 0, DateTimeKind.Utc)));
            Assert.That(read[5], Is.EqualTo(read[1]), "row 5 repeats row 1's value");
        });
    }

    /// <summary>
    /// Verifies that a <c>FixedString(N)</c> text projection includes padding and replaces invalid UTF-8.
    /// </summary>
    [Test]
    public async Task ReadAs_FixedStringColumn_DecodesEveryByteIncludingThePadding()
    {
        await using var client = TcpServerFixture.CreateClient();

        string[] text = null;
        byte[][] bytes = null;

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST(v AS FixedString(4)) AS v
              FROM (SELECT arrayJoin(['abcd', 'ab', unhex('41FF')]) AS v)",
            cancellationToken: None))
        {
            text = block.ReadAs<string>("v").Values.ToArray();
            bytes = block.Column<byte[]>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(text[0], Is.EqualTo("abcd"));
            Assert.That(text[1], Is.EqualTo("ab\0\0"), "the padding the server widened it with, not trimmed away");
            Assert.That(text[2], Is.EqualTo("A�\0\0"), "0xFF has no UTF-8 spelling");
            Assert.That(bytes[2], Is.EqualTo(new byte[] { 0x41, 0xFF, 0x00, 0x00 }), "which the byte reading still carries");
        });
    }

    /// <summary>
    /// The text reading composes through <c>LowCardinality</c>, where it is taken once per dictionary entry. Read
    /// through both accessors, since one converts a row at a time and the other materializes the whole column.
    /// </summary>
    [Test]
    public async Task ReadAs_LowCardinalityFixedStringColumn_ReadsEveryRowAsText()
    {
        await using var client = TcpServerFixture.CreateClient();

        string[] materialized = null;
        var perRow = new string[6];

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST(['alph', 'beta'][1 + number % 2] AS LowCardinality(FixedString(4))) AS v
              FROM system.numbers LIMIT 6",
            cancellationToken: None))
        {
            IColumn<string> read = block.ReadAs<string>("v");
            for (int row = 0; row < block.RowCount; row++)
            {
                perRow[row] = read[row];
            }

            materialized = read.Values.ToArray();
        }

        var expected = new[] { "alph", "beta", "alph", "beta", "alph", "beta" };

        Assert.Multiple(() =>
        {
            Assert.That(perRow, Is.EqualTo(expected), "read a row at a time through the indexer");
            Assert.That(materialized, Is.EqualTo(expected), "Values converts the column once, and must agree");
        });
    }

    /// <summary>
    /// Verifies that an array forwards the raw-byte projection to its string elements.
    /// </summary>
    [Test]
    public async Task ReadAs_ArrayOfString_ReadsEveryRowsElementsAsBytes()
    {
        await using var client = TcpServerFixture.CreateClient();

        byte[][][] rows = null;

        await foreach (Block block in client.StreamAsync(
            @"SELECT if(number = 0, [unhex('41FF'), 'b'], CAST([] AS Array(String))) AS v
              FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            rows = block.ReadAs<byte[][]>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows[0][0], Is.EqualTo(new byte[] { 0x41, 0xFF }), "0xFF has no UTF-8 spelling and must survive");
            Assert.That(rows[0][1], Is.EqualTo(new byte[] { (byte)'b' }));
            Assert.That(rows[1], Is.Empty, "an empty row projects to an empty array");
        });
    }

    /// <summary>
    /// A <c>Map</c> forwards the byte reading to whichever of its two entry columns holds a <c>String</c>, and pairs
    /// the projected columns per row. Here only the value side converts; the keys read as their own type.
    /// </summary>
    [Test]
    public async Task ReadAs_MapWithStringValues_ReadsThoseValuesAsBytes()
    {
        await using var client = TcpServerFixture.CreateClient();

        KeyValuePair<string, byte[]>[][] rows = null;

        await foreach (Block block in client.StreamAsync(
            @"SELECT if(number = 0, map('k', unhex('41FF')), CAST(map() AS Map(String, String))) AS v
              FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            rows = block.ReadAs<KeyValuePair<string, byte[]>[]>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Single().Key, Is.EqualTo("k"));
            Assert.That(rows[0].Single().Value, Is.EqualTo(new byte[] { 0x41, 0xFF }));
            Assert.That(rows[1], Is.Empty, "an empty row projects to an empty pair array");
        });
    }

    /// <summary>
    /// A <c>Tuple</c> forwards the byte reading to the child holding the <c>String</c> and rebuilds the tuple per
    /// row, so one field converts while the others read as their own types.
    /// </summary>
    [Test]
    public async Task ReadAs_TupleWithAStringField_ReadsThatFieldAsBytes()
    {
        await using var client = TcpServerFixture.CreateClient();

        (byte Ordinal, byte[] Bytes)[] rows = null;

        await foreach (Block block in client.StreamAsync(
            "SELECT tuple(toUInt8(number), unhex('41FF')) AS v FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            rows = block.ReadAs<(byte, byte[])>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(row => row.Ordinal), Is.EqualTo(new byte[] { 0, 1 }));
            Assert.That(rows[0].Bytes, Is.EqualTo(new byte[] { 0x41, 0xFF }));
            Assert.That(rows[1].Bytes, Is.EqualTo(new byte[] { 0x41, 0xFF }));
        });
    }

    /// <summary>
    /// Verifies that rows sharing a dictionary entry also share its projected reference value.
    /// </summary>
    [Test]
    public async Task ReadAs_LowCardinalityString_SharesOneByteArrayPerDictionaryEntry()
    {
        await using var client = TcpServerFixture.CreateClient();

        byte[][] rows = null;

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST([unhex('41FF'), 'bb'][1 + number % 2] AS LowCardinality(String)) AS v
              FROM system.numbers LIMIT 4",
            cancellationToken: None))
        {
            rows = block.ReadAs<byte[]>("v").Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows[0], Is.EqualTo(new byte[] { 0x41, 0xFF }));
            Assert.That(rows[1], Is.EqualTo(new byte[] { (byte)'b', (byte)'b' }));
            Assert.That(ReferenceEquals(rows[0], rows[2]), Is.True, "both rows hold the same dictionary entry");
            Assert.That(ReferenceEquals(rows[1], rows[3]), Is.True);
        });
    }

    /// <summary>
    /// The nullable low-cardinality shape keeps its NULL in the reserved dictionary slot, so a projected reading has
    /// to leave the rows pointing at it null instead of converting whatever placeholder the slot holds.
    /// </summary>
    [Test]
    public async Task ReadAs_NullableLowCardinalityString_ReadsTheBytesAndKeepsTheNullRowsNull()
    {
        await using var client = TcpServerFixture.CreateClient();

        byte[][] materialized = null;
        var perRow = new byte[6][];

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST(if(number % 3 = 2, NULL, [unhex('41FF'), 'bb'][1 + number % 2]) AS LowCardinality(Nullable(String))) AS v
              FROM system.numbers LIMIT 6",
            cancellationToken: None))
        {
            IColumn<byte[]> read = block.ReadAs<byte[]>("v");
            for (int row = 0; row < block.RowCount; row++)
            {
                perRow[row] = read[row];
            }

            materialized = read.Values.ToArray();
        }

        var expected = new[]
        {
            new byte[] { 0x41, 0xFF },
            new byte[] { (byte)'b', (byte)'b' },
            null,
            new byte[] { (byte)'b', (byte)'b' },
            new byte[] { 0x41, 0xFF },
            null,
        };

        Assert.Multiple(() =>
        {
            Assert.That(perRow, Is.EqualTo(expected), "read a row at a time through the indexer");
            Assert.That(materialized, Is.EqualTo(expected), "Values converts the column once, and must agree");
        });
    }

    // The server refuses to build a low-cardinality DateTime without this.
    private static ClickHouseTcpQueryOptions SuspiciousLowCardinality() => new()
    {
        Settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["allow_suspicious_low_cardinality_types"] = "1" },
    };

    [Test]
    public async Task ReadAs_IndexOutsideTheBlock_ThrowsNamingTheColumnCount()
    {
        await using var client = TcpServerFixture.CreateClient();

        await foreach (Block block in client.StreamAsync("SELECT 1 AS a, 2 AS b", cancellationToken: None))
        {
            var thrown = Assert.Throws<ArgumentOutOfRangeException>(() => block.ReadAs<int>(2));
            Assert.That(thrown.Message, Does.Contain("has 2 columns"));
        }
    }
}
