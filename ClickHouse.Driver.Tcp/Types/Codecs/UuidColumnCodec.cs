using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>UUID</c> column: 16 bytes per row surfaced as a <see cref="Guid"/>. ClickHouse
/// stores a UUID as two little-endian 64-bit halves, which is neither .NET's mixed-endian <see cref="Guid"/>
/// layout nor plain big-endian, so the bytes are shuffled on both read and write.
/// </summary>
internal sealed class UuidColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly UuidColumnCodec Instance = new();

    private const int UuidSize = 16;

    private UuidColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "UUID";

    /// <inheritdoc/>
    public Type ElementType => typeof(Guid);

    /// <inheritdoc/>
    public object NullPlaceholder => Guid.Empty;

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => ArrayColumn<Guid>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * UuidSize), Fill, cancellationToken);

    private static void Fill(ReadOnlySpan<byte> source, Span<Guid> destination)
    {
        for (int i = 0; i < destination.Length; i++)
        {
            destination[i] = FromWire(source.Slice(i * UuidSize, UuidSize));
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<Guid>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        Span<byte> wire = stackalloc byte[UuidSize];
        var typed = (IColumn<Guid>)column;
        for (int i = 0; i < length; i++)
        {
            ToWire(typed[start + i], wire);
            writer.WriteBytes(wire);
        }
    }

    private static Guid FromWire(ReadOnlySpan<byte> wire)
    {
        // Rebuild .NET's Guid byte layout from ClickHouse's two little-endian halves: the wire's first 8 bytes
        // fill the Guid's first three (little-endian) fields in a swapped order; the last 8 are reversed.
        Span<byte> g = stackalloc byte[UuidSize];
        g[6] = wire[0];
        g[7] = wire[1];
        g[4] = wire[2];
        g[5] = wire[3];
        g[0] = wire[4];
        g[1] = wire[5];
        g[2] = wire[6];
        g[3] = wire[7];
        for (int j = 0; j < 8; j++)
        {
            g[8 + j] = wire[15 - j];
        }

        return new Guid(g);
    }

    private static void ToWire(Guid value, Span<byte> wire)
    {
        Span<byte> g = stackalloc byte[UuidSize];
        value.TryWriteBytes(g);
        wire[0] = g[6];
        wire[1] = g[7];
        wire[2] = g[4];
        wire[3] = g[5];
        wire[4] = g[0];
        wire[5] = g[1];
        wire[6] = g[2];
        wire[7] = g[3];
        for (int j = 0; j < 8; j++)
        {
            wire[8 + j] = g[15 - j];
        }
    }
}
