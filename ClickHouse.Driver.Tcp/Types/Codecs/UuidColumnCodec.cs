using System;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>UUID</c> column: 16 bytes per row surfaced as a <see cref="Guid"/>. ClickHouse
/// stores a UUID as two little-endian 64-bit halves, which is neither .NET's mixed-endian <see cref="Guid"/>
/// layout nor plain big-endian, so the 16 bytes are a fixed permutation between the wire and a <see cref="Guid"/>.
///
/// <para>
/// That permutation is a within-16-byte byte shuffle, so on a SIMD-capable target it is one
/// <c>PSHUFB</c>/<c>TBL</c> per value via <see cref="Vector128.Shuffle{T}(Vector128{T}, Vector128{T})"/>. On read
/// the whole column blob is shuffled straight into the destination <see cref="Guid"/>[]'s own memory (no per-value
/// <c>new Guid(...)</c> parse); on write each <see cref="Guid"/> is shuffled through the indexer, since a write
/// column may be a per-element view (e.g. the Nullable placeholder substitution) whose <c>Values</c> is not a
/// contiguous span. This relies on the little-endian assumption the reinterpret paths already make (a
/// <see cref="Guid"/>'s in-memory bytes equal <see cref="Guid.TryWriteBytes(Span{byte})"/>'s output on
/// little-endian). A scalar fallback covers targets without SIMD acceleration.
/// </para>
/// </summary>
internal sealed class UuidColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly UuidColumnCodec Instance = new();

    private const int UuidSize = 16;

    // Shuffle control vectors for Vector128.Shuffle, whose semantics are result[i] = source[control[i]]. Both are
    // derived directly from the scalar maps below (WireToGuid is the read map, GuidToWire the write map).
    // Benchmarked to be ~10x faster than the manual byte swapping.
    private static readonly Vector128<byte> WireToGuid = Vector128.Create((byte)4, 5, 6, 7, 2, 3, 0, 1, 15, 14, 13, 12, 11, 10, 9, 8);
    private static readonly Vector128<byte> GuidToWire = Vector128.Create((byte)6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8);

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
        // The destination Guid[] is 16 contiguous bytes per element; on little-endian its memory equals the byte
        // layout new Guid(bytes)/TryWriteBytes uses, so the shuffled wire bytes are written straight into it.
        Span<byte> guidBytes = MemoryMarshal.AsBytes(destination);
        if (Vector128.IsHardwareAccelerated)
        {
            ShuffleBlock(source, guidBytes, WireToGuid, destination.Length);
        }
        else
        {
            for (int i = 0; i < destination.Length; i++)
            {
                ShuffleScalar(source.Slice(i * UuidSize, UuidSize), guidBytes.Slice(i * UuidSize, UuidSize), WireToGuid);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<Guid>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // The write column may be a per-element view whose Values throws (e.g. the Nullable placeholder-
        // substitution column), so each Guid is read through the indexer and shuffled individually — not bulk
        // over Values. On little-endian a Guid's in-memory bytes equal TryWriteBytes's output, so the shuffle
        // maps straight to the wire layout.
        var typed = (IColumn<Guid>)column;
        Span<byte> wire = stackalloc byte[UuidSize];
        bool simd = Vector128.IsHardwareAccelerated;
        for (int i = 0; i < length; i++)
        {
            Guid value = typed[start + i];
            ReadOnlySpan<byte> guidBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
            if (simd)
            {
                Vector128.Shuffle(Vector128.Create(guidBytes), GuidToWire).CopyTo(wire);
            }
            else
            {
                ShuffleScalar(guidBytes, wire, GuidToWire);
            }

            writer.WriteBytes(wire);
        }
    }

    // Shuffles a run of contiguous 16-byte values from source into destination, one Vector128.Shuffle per value.
    private static void ShuffleBlock(ReadOnlySpan<byte> source, Span<byte> destination, Vector128<byte> control, int count)
    {
        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        for (int i = 0; i < count; i++)
        {
            nuint offset = (nuint)i * UuidSize;
            Vector128.Shuffle(Vector128.LoadUnsafe(ref src, offset), control).StoreUnsafe(ref dst, offset);
        }
    }

    // The scalar equivalent of one Vector128.Shuffle: destination[i] = source[control[i]].
    private static void ShuffleScalar(ReadOnlySpan<byte> source, Span<byte> destination, Vector128<byte> control)
    {
        for (int i = 0; i < UuidSize; i++)
        {
            destination[i] = source[control[i]];
        }
    }
}
