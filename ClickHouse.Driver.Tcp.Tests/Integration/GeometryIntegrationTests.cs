using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Pins the <c>Geometry</c> discriminator order against the server. The column header carries only the alias, so
/// the client expands it to a six-alternative <c>Variant</c> from a constant of its own; nothing on the wire
/// carries that ordering, and a caller of the dense column depends on it to mean what they intend.
///
/// <para>
/// A round trip cannot check that constant, because the client applies it to the write and to the read. If two
/// alternatives are transposed, the write picks the wrong discriminator and the read of that discriminator picks
/// the same wrong alternative back, so the value returns intact while the server holds it under the other type.
/// Transposing <c>Point</c> or <c>MultiPolygon</c> is still caught: the wrong alternative has a different layout,
/// so the bytes stop parsing. The other four are two structurally identical pairs (<c>Ring</c> with
/// <c>LineString</c>, <c>Polygon</c> with <c>MultiLineString</c>) whose blocks are byte-identical, so nothing
/// objects. Their CLR types are identical too, so knowing a value's type does not name its alternative either.
/// </para>
///
/// <para>
/// Only the server breaks that symmetry, and it has to be asked what it calls a given row. Asking what it calls
/// discriminator <c>i</c> answers from its own list and never consults the client's. So one row is written against
/// each discriminator, and the server's name for that row is compared with the client's name for it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[RequiresServerFeature(TcpFeature.Geometry)]
public class GeometryIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;
    private static readonly (double, double)[] Square = { (0d, 0d), (2d, 0d), (2d, 2d), (0d, 0d) };

    [Test]
    public async Task InsertAsync_OneRowPerDiscriminator_TheServerNamesEachOneWhatTheClientCallsIt()
    {
        var codec = (VariantColumnCodec)ColumnCodecRegistry.Default.Resolve("Geometry", ResolveContext.ForWrite);
        IReadOnlyList<string> clientOrder = codec.AlternativeTypeNames;

        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt8, value Geometry) ENGINE = Memory");

            // Row i selects discriminator i and carries a value of the shape the client's alternative i expects, so
            // the server's name for row i is directly comparable with clientOrder[i].
            var alternatives = new IColumn[clientOrder.Count];
            var discriminators = new byte[clientOrder.Count];
            var ids = new byte[clientOrder.Count];
            for (int i = 0; i < clientOrder.Count; i++)
            {
                alternatives[i] = SampleFor(clientOrder[i]);
                discriminators[i] = (byte)i;
                ids[i] = (byte)i;
            }

            using var value = new VariantColumn("value", "Geometry", discriminators, alternatives, clientOrder.Count, pooledDiscriminators: false, ownsColumns: false);
            var id = PrimitiveColumn<byte>.FromValues("id", "UInt8", ids);
            await connection.InsertAsync($"INSERT INTO {table} (id, value) VALUES", new IColumn[] { id, value }, cancellationToken: None);

            // toString, because variantType returns an Enum8 and this client surfaces an enum as its raw ordinal —
            // which is the very numbering under test, so reading it back would prove nothing.
            var served = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT toString(variantType(value)) FROM {table} ORDER BY id", cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    served.Add((string)block[0].GetValue(row));
                }
            }

            Assert.That(served, Is.EqualTo(clientOrder), "the client's alternative order must be the server's");
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task QueryAsync_GeometryColumn_NamesItsAlternativesThroughTypeNames()
    {
        // Geometry is the case that leaves a caller with nothing to parse: the column header is the single word
        // "Geometry", so the alternatives appear nowhere in the type string, and the discriminator order is the
        // server's name-sorted one rather than any order a caller declared. TypeNames is the whole answer.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        string headerType = null;
        string[] typeNames = null;
        string selectedAlternative = null;

        // Through Point: the server casts to Geometry only from one of the alternatives, not from a bare tuple.
        await foreach (Block block in connection.QueryAsync("SELECT CAST(CAST((1.5, -2.5), 'Point'), 'Geometry')", cancellationToken: None))
        {
            IColumn column = block[0];
            headerType = column.TypeName;

            var geometry = (IVariantColumn)column;
            typeNames = geometry.TypeNames.ToArray();
            selectedAlternative = geometry.TypeNames[geometry.Discriminators[0]];
        }

        Assert.Multiple(() =>
        {
            Assert.That(headerType, Is.EqualTo("Geometry"), "the header names the alias, not its alternatives");
            Assert.That(
                typeNames,
                Is.EqualTo(new[] { "LineString", "MultiLineString", "MultiPolygon", "Point", "Polygon", "Ring" }),
                "the server's canonical name-sorted order, which is the discriminator order");
            Assert.That(selectedAlternative, Is.EqualTo("Point"), "a coordinate pair selects the Point alternative");
        });
    }

    // A one-row column of the shape the named geo alias surfaces as.
    private static IColumn SampleFor(string alias) => alias switch
    {
        "Point" => new ArrayColumn<(double, double)>("value", alias, new[] { (1.5d, -2.5d) }),
        "Ring" or "LineString" => new ArrayColumn<(double, double)[]>("value", alias, new[] { Square }),
        "Polygon" or "MultiLineString" => new ArrayColumn<(double, double)[][]>("value", alias, new[] { new[] { Square } }),
        "MultiPolygon" => new ArrayColumn<(double, double)[][][]>("value", alias, new[] { new[] { new[] { Square } } }),
        _ => throw new ArgumentException($"Geometry gained an alternative this test does not know: '{alias}'.", nameof(alias)),
    };

    private static string UniqueTableName() => $"tcp_geometry_test_{Guid.NewGuid():N}";

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }
}
