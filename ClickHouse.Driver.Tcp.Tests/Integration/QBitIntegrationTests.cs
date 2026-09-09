using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

[TestFixture]
[Category("Integration")]
[RequiresServerFeature(TcpFeature.QBit)]
public class QBitIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // Spans two complete bitmap bytes and a partial third.
    private const int Dimension = 17;
    private const string Float32Type = "QBit(Float32, 17)";
    private const string Int8Type = "QBit(Int8, 17)";

    private static float[] Vector() => Enumerable.Range(0, Dimension).Select(i => (i * 3f) - 20f).ToArray();

    private static sbyte[] Int8Vector()
        => Enumerable.Range(0, Dimension).Select(i => i == 16 ? sbyte.MinValue : (sbyte)((i * 7) - 60)).ToArray();

    [Test]
    public async Task InsertAsync_QBitWrittenByTheClient_IsReadBackByTheServerAsTheSameVector()
    {
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

    [Test]
    [RequiresServerFeature(TcpFeature.QBitInt8)]
    public async Task StreamAsync_Int8QBitWrittenByTheServer_DecodesToTheSameVector()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        await Drain(client, $"CREATE TABLE {table} (v {Int8Type}) ENGINE = Memory");
        try
        {
            sbyte[] vector = Int8Vector();
            string literal = string.Join(",", vector.Select(v => v.ToString(CultureInfo.InvariantCulture)));
            await Drain(client, $"INSERT INTO {table} VALUES ([{literal}])");

            sbyte[] decoded = null;
            await foreach (Block block in client.StreamAsync($"SELECT v FROM {table}", cancellationToken: None))
            {
                decoded = (sbyte[])block[0].GetValue(0);
            }

            CollectionAssert.AreEqual(vector, decoded);
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
