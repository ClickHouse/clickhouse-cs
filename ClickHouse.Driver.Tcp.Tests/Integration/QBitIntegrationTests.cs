using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The two <em>asymmetric</em> checks on <c>QBit(T, N)</c> that the insert round-trip corpus cannot make. A
/// round-trip writes and reads with the same client code, so a layout error that is self-consistent — the bit
/// order within a row, say — round-trips perfectly while putting bytes on the wire the server reads as different
/// values. Each test here has exactly one side done by the client and the other by the server.
///
/// <para>
/// This is not hypothetical: the byte order within a row runs opposite to the element order, and every fixture
/// narrower than 9 elements is one byte per row, where that is unobservable. These tests use a dimension wide
/// enough to see it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[RequiresServerFeature(TcpFeature.QBit)]
public class QBitIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // 17 elements is two whole 8-element groups plus a tail, so it spans three bytes per row and pins the byte
    // order. The values are distinct and asymmetric across the group boundary, so a swapped byte is a wrong value
    // rather than a coincidence.
    private const int Dimension = 17;
    private const string Float32Type = "QBit(Float32, 17)";
    private const string Int8Type = "QBit(Int8, 17)";

    private static float[] Vector() => Enumerable.Range(0, Dimension).Select(i => (i * 3f) - 20f).ToArray();

    // Spans the sign, both whole bytes and the one-element tail, with MinValue pinning the all-ones pattern.
    private static sbyte[] Int8Vector()
        => Enumerable.Range(0, Dimension).Select(i => i == 16 ? sbyte.MinValue : (sbyte)((i * 7) - 60)).ToArray();

    [Test]
    public async Task InsertAsync_QBitWrittenByTheClient_IsReadBackByTheServerAsTheSameVector()
    {
        // The client transposes; the server de-transposes. toString() makes the server do that work and hand back
        // its own rendering, so nothing about the client's read path is involved in the comparison.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        await Drain(client, $"CREATE TABLE {table} (v {Float32Type}) ENGINE = Memory");
        try
        {
            float[] vector = Vector();
            using var column = new ArrayColumn<float[]>("v", Float32Type, new[] { vector });
            await client.InsertAsync($"INSERT INTO {table} (v) VALUES", new IColumn[] { column }, cancellationToken: None);

            string rendered = await ScalarStringAsync(client, $"SELECT toString(v) FROM {table}");

            Assert.That(rendered, Is.EqualTo(Expected(vector)));
        }
        finally
        {
            await Drain(client, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task StreamAsync_QBitWrittenByTheServer_DecodesToTheSameVector()
    {
        // The mirror: the server transposes (it parses the VALUES literal), the client de-transposes. Together
        // with the test above this pins both directions against an independent implementation.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        await Drain(client, $"CREATE TABLE {table} (v {Float32Type}) ENGINE = Memory");
        try
        {
            float[] vector = Vector();
            string literal = string.Join(",", vector.Select(v => v.ToString("R", CultureInfo.InvariantCulture)));
            await Drain(client, $"INSERT INTO {table} VALUES ([{literal}])");

            float[] decoded = null;
            await foreach (Block block in client.StreamAsync($"SELECT v FROM {table}", cancellationToken: None))
            {
                decoded = (float[])block[0].GetValue(0);
            }

            CollectionAssert.AreEqual(vector, decoded);
        }
        finally
        {
            await Drain(client, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task GetPlane_QBitWrittenByTheServer_AgreesWithTheServersOwnBitExtraction()
    {
        // IQBitColumn is the whole point of the type — a caller reading the high planes to approximate a distance
        // — and no round-trip touches it. bitTest on the server's own value is the independent answer for whether
        // element i has bit b set, so the plane the client hands out can be checked bit by bit against it.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        await Drain(client, $"CREATE TABLE {table} (v {Float32Type}) ENGINE = Memory");
        try
        {
            float[] vector = Vector();
            string literal = string.Join(",", vector.Select(v => v.ToString("R", CultureInfo.InvariantCulture)));
            await Drain(client, $"INSERT INTO {table} VALUES ([{literal}])");

            byte[] signPlane = null;
            await foreach (Block block in client.StreamAsync($"SELECT v FROM {table}", cancellationToken: None))
            {
                signPlane = ((IQBitColumn)block[0]).GetPlane(31).ToArray();
            }

            // The sign bit of element i, straight from the client's plane.
            for (int i = 0; i < Dimension; i++)
            {
                int slot = signPlane.Length - 1 - (i / 8);
                bool negativeInPlane = (signPlane[slot] & (1 << (i % 8))) != 0;
                Assert.That(negativeInPlane, Is.EqualTo(vector[i] < 0 || float.IsNegative(vector[i])), $"element {i}");
            }
        }
        finally
        {
            await Drain(client, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    [RequiresServerFeature(TcpFeature.QBitInt8)]
    public async Task InsertAsync_Int8QBitWrittenByTheClient_IsReadBackByTheServerAsTheSameVector()
    {
        // QBit(Int8, N) has its own hand-written transpose loop, so the Float32 check above does not cover it. The
        // frozen byte fixture in the unit suite is 16 elements — two whole bytes — and this is the non-multiple-of-8
        // width, where the last byte is partly unused. Server-side toString() keeps the client's read path out of it.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        await Drain(client, $"CREATE TABLE {table} (v {Int8Type}) ENGINE = Memory");
        try
        {
            sbyte[] vector = Int8Vector();
            using var column = new ArrayColumn<sbyte[]>("v", Int8Type, new[] { vector });
            await client.InsertAsync($"INSERT INTO {table} (v) VALUES", new IColumn[] { column }, cancellationToken: None);

            string rendered = await ScalarStringAsync(client, $"SELECT toString(v) FROM {table}");

            Assert.That(rendered, Is.EqualTo("[" + string.Join(",", vector.Select(v => v.ToString(CultureInfo.InvariantCulture))) + "]"));
        }
        finally
        {
            await Drain(client, $"DROP TABLE IF EXISTS {table}");
        }
    }

    private static string Expected(float[] vector)
        => "[" + string.Join(",", vector.Select(v => v.ToString("R", CultureInfo.InvariantCulture))) + "]";

    private static string UniqueTableName() => $"tcp_qbit_test_{Guid.NewGuid():N}";

    private static async Task<string> ScalarStringAsync(ClickHouseTcpClient client, string sql)
    {
        string value = null;
        await foreach (Block block in client.StreamAsync(sql, cancellationToken: None))
        {
            if (block.RowCount > 0)
            {
                value = (string)block[0].GetValue(0);
            }
        }

        return value;
    }

    private static async Task Drain(ClickHouseTcpClient client, string sql)
    {
        await foreach (Block block in client.StreamAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }
}
