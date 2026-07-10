using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>IPv4</c> column: a 4-byte address surfaced as an <see cref="IPAddress"/>.
/// ClickHouse stores IPv4 as a little-endian <c>UInt32</c> — the reverse of network byte order — so the four
/// bytes are reversed on both read and write.
/// </summary>
internal sealed class IPv4ColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly IPv4ColumnCodec Instance = new();

    private const int Size = 4;

    private IPv4ColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "IPv4";

    /// <inheritdoc/>
    public int? FixedRowByteSize => Size;

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => ArrayColumn<IPAddress>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * Size), Fill, cancellationToken);

    private static void Fill(ReadOnlySpan<byte> source, Span<IPAddress> destination)
    {
        Span<byte> network = stackalloc byte[Size];
        for (int i = 0; i < destination.Length; i++)
        {
            ReadOnlySpan<byte> wire = source.Slice(i * Size, Size);
            for (int j = 0; j < Size; j++)
            {
                network[j] = wire[Size - 1 - j];
            }

            destination[i] = new IPAddress(network);
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<IPAddress>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        Span<byte> network = stackalloc byte[Size];
        Span<byte> wire = stackalloc byte[Size];
        foreach (IPAddress value in ((IColumn<IPAddress>)column).Values.Slice(start, length))
        {
            if (value.AddressFamily != AddressFamily.InterNetwork || !value.TryWriteBytes(network, out _))
            {
                throw new ArgumentException($"An IPv4 column requires IPv4 addresses; got '{value}'.", nameof(column));
            }

            for (int j = 0; j < Size; j++)
            {
                wire[j] = network[Size - 1 - j];
            }

            writer.WriteBytes(wire);
        }
    }
}

/// <summary>
/// A codec for the ClickHouse <c>IPv6</c> column: a 16-byte address in network byte order, surfaced as an
/// <see cref="IPAddress"/> with no transformation.
/// </summary>
internal sealed class IPv6ColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly IPv6ColumnCodec Instance = new();

    private const int Size = 16;

    private IPv6ColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "IPv6";

    /// <inheritdoc/>
    public int? FixedRowByteSize => Size;

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => ArrayColumn<IPAddress>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * Size), Fill, cancellationToken);

    private static void Fill(ReadOnlySpan<byte> source, Span<IPAddress> destination)
    {
        for (int i = 0; i < destination.Length; i++)
        {
            destination[i] = new IPAddress(source.Slice(i * Size, Size));
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<IPAddress>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        Span<byte> network = stackalloc byte[Size];
        foreach (IPAddress value in ((IColumn<IPAddress>)column).Values.Slice(start, length))
        {
            IPAddress address = value.AddressFamily == AddressFamily.InterNetwork ? value.MapToIPv6() : value;
            if (address.AddressFamily != AddressFamily.InterNetworkV6 || !address.TryWriteBytes(network, out int written) || written != Size)
            {
                throw new ArgumentException($"An IPv6 column requires IPv6 addresses; got '{value}'.", nameof(column));
            }

            writer.WriteBytes(network);
        }
    }
}
