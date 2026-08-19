using System;
using System.Buffers.Binary;
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
    public Type ElementType => typeof(IPAddress);

    /// <inheritdoc/>
    public object NullPlaceholder => IPAddress.Any;

    /// <inheritdoc/>
    public Type CanonicalWriteElementType => typeof(uint);

    /// <inheritdoc/>
    public object CanonicalWritePlaceholder => ToWireValue((IPAddress)NullPlaceholder);

    /// <inheritdoc/>
    // The address family is the whole of the IPv4/IPv6 tie-break: an IPv4 address is this alternative's, and the
    // IPv6 codec declines it so that exactly one claims.
    public bool ClaimsValue(object value) => value is IPAddress address && address.AddressFamily == AddressFamily.InterNetwork;

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
    public IColumn ToCanonicalWriteColumn(IColumn column)
        => column is IColumn<IPAddress> values
            ? new ProjectedColumn<IPAddress, uint>(TypeName, values, ToWireValue)
            : throw new ArgumentException($"An IPv4 column must hold IPAddress values, not {column.GetType()}.", nameof(column));

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var values = (IColumn<IPAddress>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt32(ToWireValue(values[start + i]));
        }
    }

    /// <inheritdoc/>
    public void WriteCanonicalColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var values = (IColumn<uint>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt32(values[start + i]);
        }
    }

    private static uint ToWireValue(IPAddress value)
    {
        Span<byte> network = stackalloc byte[Size];
        if (value?.AddressFamily != AddressFamily.InterNetwork || !value.TryWriteBytes(network, out _))
        {
            throw new ArgumentException($"An IPv4 column requires IPv4 addresses; got '{value}'.", nameof(value));
        }

        // ClickHouse writes the numeric address little-endian, reversing the network-order bytes.
        return BinaryPrimitives.ReadUInt32BigEndian(network);
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
    public Type ElementType => typeof(IPAddress);

    /// <inheritdoc/>
    public object NullPlaceholder => IPAddress.IPv6Any;

    /// <inheritdoc/>
    public Type CanonicalWriteElementType => typeof(IPv6WireValue);

    /// <inheritdoc/>
    public object CanonicalWritePlaceholder => ToWireValue((IPAddress)NullPlaceholder);

    /// <inheritdoc/>
    // Declines an IPv4 address even though the writer below maps one into 16 bytes: beside an IPv4 alternative
    // that address means IPv4, and claiming it too would leave the tie unresolved. A standalone IPv6 column, and
    // an IPv6 alternative with no IPv4 sibling, never reach here and still accept it.
    public bool ClaimsValue(object value) => value is IPAddress address && address.AddressFamily == AddressFamily.InterNetworkV6;

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
    public IColumn ToCanonicalWriteColumn(IColumn column)
        => column is IColumn<IPAddress> values
            ? new ProjectedColumn<IPAddress, IPv6WireValue>(TypeName, values, ToWireValue)
            : throw new ArgumentException($"An IPv6 column must hold IPAddress values, not {column.GetType()}.", nameof(column));

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var values = (IColumn<IPAddress>)column;
        for (int i = 0; i < length; i++)
        {
            WriteWireValue(writer, ToWireValue(values[start + i]));
        }
    }

    /// <inheritdoc/>
    public void WriteCanonicalColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var values = (IColumn<IPv6WireValue>)column;
        for (int i = 0; i < length; i++)
        {
            WriteWireValue(writer, values[start + i]);
        }
    }

    private static void WriteWireValue(ClickHouseBinaryWriter writer, IPv6WireValue value)
    {
        writer.WriteUInt64(value.First);
        writer.WriteUInt64(value.Second);
    }

    private static IPv6WireValue ToWireValue(IPAddress value)
    {
        Span<byte> network = stackalloc byte[Size];
        WriteNetworkBytes(value, network);
        return new IPv6WireValue(
            BinaryPrimitives.ReadUInt64LittleEndian(network),
            BinaryPrimitives.ReadUInt64LittleEndian(network.Slice(sizeof(ulong))));
    }

    private static void WriteNetworkBytes(IPAddress value, Span<byte> destination)
    {
        IPAddress address = value?.AddressFamily == AddressFamily.InterNetwork ? value.MapToIPv6() : value;
        if (address?.AddressFamily != AddressFamily.InterNetworkV6
            || !address.TryWriteBytes(destination, out int written)
            || written != Size)
        {
            throw new ArgumentException($"An IPv6 column requires IPv6 addresses; got '{value}'.", nameof(value));
        }
    }
}
