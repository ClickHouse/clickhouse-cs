using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The wire constants and per-key encoding shared by the low-cardinality codec (read side) and its shape (write
/// side). A <c>LowCardinality</c> column replaces <c>N</c> inner values with a block-local dictionary of the
/// distinct values plus <c>N</c> keys indexing into it; the key width is the smallest unsigned integer that can
/// index the dictionary.
/// </summary>
internal static class LowCardinalityWire
{
    /// <summary>The single defined serialization-state version (<c>sharedDictionariesWithAdditionalKeys</c>).</summary>
    public const long StatePrefixVersion = 1;

    /// <summary>The low byte of the metadata word carries the key-width code.</summary>
    public const ulong IndexTypeMask = 0xFF;

    /// <summary>Key-width codes: the key is <c>1 &lt;&lt; code</c> bytes wide.</summary>
    public const int KeyUInt8 = 0;
    public const int KeyUInt16 = 1;
    public const int KeyUInt32 = 2;
    public const int KeyUInt64 = 3;

    /// <summary>A single dictionary shared across blocks — never set in the Native format (on-disk MergeTree only).</summary>
    public const ulong NeedGlobalDictionaryBit = 1UL << 8;

    /// <summary>Set when the block carries its own dictionary keys — always set for a non-empty Native block.</summary>
    public const ulong HasAdditionalKeysBit = 1UL << 9;

    /// <summary>Set when the block carries a dictionary update — always set for a non-empty Native block.</summary>
    public const ulong NeedUpdateDictionaryBit = 1UL << 10;

    /// <summary>The metadata flag bits a self-contained per-block dictionary carries (<c>0x600</c>).</summary>
    public const ulong NativeFlags = HasAdditionalKeysBit | NeedUpdateDictionaryBit;

    /// <summary>
    /// The key-width code to encode a dictionary of <paramref name="dictSize"/> entries with. The thresholds
    /// mirror the reference client (clickhouse-go) rather than the theoretical minimum: a strict <c>&lt;</c>
    /// against each type's max value, so a dictionary of exactly 255 entries already promotes to a 2-byte key
    /// even though index 254 would fit a byte. A wider-than-strictly-necessary key at the boundary is harmless —
    /// the server reads keys at the width the metadata declares — and matching the reference keeps the write
    /// output byte-compatible with what other clients and the server itself produce.
    /// </summary>
    /// <param name="dictSize">The number of dictionary entries.</param>
    /// <returns>The key-width code (0..3).</returns>
    public static int SelectKeyWidthCode(int dictSize)
    {
        // A dictionary never exceeds int.MaxValue entries, so the 8-byte key is never selected here.
        if (dictSize < byte.MaxValue)
        {
            return KeyUInt8;
        }

        if (dictSize < ushort.MaxValue)
        {
            return KeyUInt16;
        }

        return KeyUInt32;
    }

    /// <summary>The byte width of a key encoded with <paramref name="code"/>.</summary>
    /// <param name="code">The key-width code (0..3).</param>
    /// <returns>The width in bytes (1, 2, 4, or 8).</returns>
    public static int KeyByteWidth(int code) => 1 << code;

    /// <summary>Writes a single dictionary key at the width selected by <paramref name="code"/>.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="code">The key-width code (0..3).</param>
    /// <param name="key">The dictionary index to write.</param>
    public static void WriteKey(ClickHouseBinaryWriter writer, int code, int key)
    {
        switch (code)
        {
            case KeyUInt8:
                writer.WriteUInt8((byte)key);
                break;
            case KeyUInt16:
                writer.WriteUInt16((ushort)key);
                break;
            case KeyUInt32:
                writer.WriteUInt32((uint)key);
                break;
            default:
                writer.WriteUInt64((ulong)key);
                break;
        }
    }
}

/// <summary>
/// A codec for the ClickHouse <c>LowCardinality(T)</c> column. Its serialization-state prefix is a single fixed
/// version marker (an <c>Int64</c> = 1), written once per non-empty block; the dictionary and keys live in the
/// column body. The body is a metadata word (the key-width code plus the block-local dictionary flags), the
/// dictionary size, the dictionary values encoded with the inner codec, the key count, and the keys themselves —
/// each key <c>1 &lt;&lt; code</c> bytes indexing the dictionary. The decoded column surfaces each row as the
/// inner CLR value (<c>dict[keys[row]]</c>).
///
/// <para>
/// Each Native block ships a self-contained, block-local dictionary — there is no cross-block dictionary state,
/// so the codec instance holds none and stays a shared singleton. The codec is non-generic; the generic work —
/// building the typed column and deduplicating values on write — is delegated to a cached, per-element-type
/// <see cref="ILowCardinalityShape"/>.
/// </para>
/// </summary>
internal sealed class LowCardinalityColumnCodec : IColumnCodec
{
    private readonly IColumnCodec inner;
    private readonly ILowCardinalityShape shape;
    private readonly bool nullable;
    private readonly bool innerCanWrite;

    private LowCardinalityColumnCodec(string typeName, IColumnCodec inner, bool nullable)
    {
        TypeName = typeName;
        this.inner = inner;
        this.nullable = nullable;
        shape = LowCardinalityShapes.For(inner.ElementType, nullable);
        innerCanWrite = shape.CanInnerWrite(inner);
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => shape.SurfaceElementType;

    /// <summary>
    /// The placeholder for an absent value: for a nullable inner it is <see langword="null"/> itself (the reserved
    /// NULL dictionary slot), otherwise the inner codec's placeholder. Relevant only if a composite nests a
    /// <c>LowCardinality</c> and asks for its placeholder; the server rejects <c>Nullable(LowCardinality(...))</c>,
    /// so this is not exercised by a nullable wrapper.
    /// </summary>
    public object NullPlaceholder => nullable ? null : inner.NullPlaceholder;

    /// <summary>Builds a <c>LowCardinality(T)</c> codec, resolving the inner type <c>T</c> through the registry.</summary>
    /// <param name="node">The parsed <c>LowCardinality</c> type node; its single argument is the inner type.</param>
    /// <param name="context">The resolution context, forwarded to the inner codec's factory.</param>
    /// <param name="registry">The registry used to resolve the inner type's codec.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type does not have exactly one inner type argument, or a <c>Nullable</c> inner is malformed or itself <c>Nullable</c>.</exception>
    public static LowCardinalityColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count != 1)
        {
            throw new FormatException($"LowCardinality type '{node}' must have exactly one inner type argument.");
        }

        TypeNode innerNode = node.Arguments[0];
        bool nullable = innerNode.Name == "Nullable";
        if (nullable)
        {
            // The dictionary is serialized as the bare inner type (no null-map stream); nullability is expressed
            // positionally by the reserved dict[0] slot that all NULL keys point at. So resolve the codec for the
            // bare inner type, not the Nullable codec, which would frame a null-map inside the dictionary stream.
            if (innerNode.Arguments.Count != 1)
            {
                throw new FormatException($"Nullable inner of '{node}' must have exactly one inner type argument.");
            }

            innerNode = innerNode.Arguments[0];
            if (innerNode.Name == "Nullable")
            {
                throw new FormatException($"Nullable cannot be nested inside '{node}'.");
            }
        }

        IColumnCodec inner = registry.ResolveNode(innerNode, in context);
        return new LowCardinalityColumnCodec(node.ToString(), inner, nullable);
    }

    /// <inheritdoc/>
    public ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
        => ReadAndValidateVersionAsync(reader, cancellationToken);

    private static async ValueTask ReadAndValidateVersionAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        long version = await reader.ReadInt64Async(cancellationToken).ConfigureAwait(false);
        if (version != LowCardinalityWire.StatePrefixVersion)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality serialization version {version} is not supported (expected {LowCardinalityWire.StatePrefixVersion}).");
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            // An empty (or empty-flattened) column writes no body — no metadata, dictionary, or keys. Wrap a
            // zero-row dictionary (which reads nothing) with an empty key array.
            IColumn emptyDict = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, 0, cancellationToken).ConfigureAwait(false);
            return shape.Wrap(columnName, columnType, emptyDict, Array.Empty<int>(), rowCount: 0, pooledKeys: false);
        }

        ulong metadata = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        int code = (int)(metadata & LowCardinalityWire.IndexTypeMask);
        if (code is < LowCardinalityWire.KeyUInt8 or > LowCardinalityWire.KeyUInt64)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality column '{columnName}' declares an unknown key-width code {code}.");
        }

        if ((metadata & LowCardinalityWire.NeedGlobalDictionaryBit) != 0)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality column '{columnName}' requests a global dictionary, which the Native format does not use.");
        }

        if ((metadata & LowCardinalityWire.HasAdditionalKeysBit) == 0)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality column '{columnName}' carries no additional keys; a non-empty Native block must.");
        }

        // NeedUpdateDictionaryBit is expected to be set (each block ships a fresh dictionary), but it is not
        // required on read: a self-contained block-local dictionary is what the following bytes describe either
        // way, and being lenient here avoids rejecting a stream that merely differs in this advisory bit.
        ulong dictSizeRaw = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        if (dictSizeRaw > int.MaxValue)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality column '{columnName}' declares {dictSizeRaw} dictionary entries, exceeding the maximum this client can address.");
        }

        int dictSize = (int)dictSizeRaw;
        IColumn dictionary = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, dictSize, cancellationToken).ConfigureAwait(false);

        int[] keys = null;
        try
        {
            ulong keysCount = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
            if (keysCount != (ulong)rowCount)
            {
                throw new ClickHouseProtocolException(
                    $"LowCardinality column '{columnName}' declares {keysCount} keys but the block expects {rowCount}.");
            }

            keys = ArrayPool<int>.Shared.Rent(rowCount);
            await ReadKeysAsync(reader, keys, rowCount, code, dictSize, columnName, cancellationToken).ConfigureAwait(false);

            // Wrap inside the try: only a successful Wrap takes ownership of the rented keys and the dictionary
            // column, so a throw leaks neither.
            return shape.Wrap(columnName, columnType, dictionary, keys, rowCount, pooledKeys: true);
        }
        catch
        {
            if (keys is not null)
            {
                ArrayPool<int>.Shared.Return(keys);
            }

            dictionary.Dispose();
            throw;
        }
    }

    // Bulk-reads rowCount keys of (1 << code) bytes each into a scratch buffer, decodes them into keys, and
    // validates each index is within the dictionary.
    private static async ValueTask ReadKeysAsync(
        ClickHouseBinaryReader reader, int[] keys, int rowCount, int code, int dictSize, string columnName, CancellationToken cancellationToken)
    {
        int width = LowCardinalityWire.KeyByteWidth(code);
        long keyBytes = (long)rowCount * width;
        if (keyBytes > Array.MaxLength)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality column '{columnName}' declares {rowCount} keys, whose stream exceeds the maximum this client can buffer.");
        }

        byte[] scratch = ArrayPool<byte>.Shared.Rent((int)keyBytes);
        try
        {
            await reader.ReadBytesAsync(scratch.AsMemory(0, (int)keyBytes), cancellationToken).ConfigureAwait(false);
            DecodeKeys(scratch.AsSpan(0, (int)keyBytes), keys.AsSpan(0, rowCount), code, dictSize, columnName);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    // Decodes the raw little-endian keys stream at the given width into int indices, validating each is in range.
    private static void DecodeKeys(ReadOnlySpan<byte> source, Span<int> keys, int code, int dictSize, string columnName)
    {
        switch (code)
        {
            case LowCardinalityWire.KeyUInt8:
                for (int i = 0; i < keys.Length; i++)
                {
                    keys[i] = ValidateKey(source[i], dictSize, columnName);
                }

                break;
            case LowCardinalityWire.KeyUInt16:
                ReadOnlySpan<ushort> u16 = MemoryMarshal.Cast<byte, ushort>(source);
                for (int i = 0; i < keys.Length; i++)
                {
                    keys[i] = ValidateKey(u16[i], dictSize, columnName);
                }

                break;
            case LowCardinalityWire.KeyUInt32:
                ReadOnlySpan<uint> u32 = MemoryMarshal.Cast<byte, uint>(source);
                for (int i = 0; i < keys.Length; i++)
                {
                    keys[i] = ValidateKey(u32[i], dictSize, columnName);
                }

                break;
            default:
                ReadOnlySpan<ulong> u64 = MemoryMarshal.Cast<byte, ulong>(source);
                for (int i = 0; i < keys.Length; i++)
                {
                    keys[i] = ValidateKey(u64[i], dictSize, columnName);
                }

                break;
        }
    }

    private static int ValidateKey(ulong key, int dictSize, string columnName)
    {
        if (key >= (ulong)dictSize)
        {
            throw new ClickHouseProtocolException(
                $"LowCardinality column '{columnName}' has a key {key} outside its {dictSize}-entry dictionary; the stream is corrupt.");
        }

        return (int)key;
    }

    /// <inheritdoc/>
    public long MeasureRowBytes(IColumn column, int row) => shape.MeasureRow(inner, column, row);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => innerCanWrite && shape.CanWrite(column);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer) => writer.WriteInt64(LowCardinalityWire.StatePrefixVersion);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => shape.WriteBody(inner, writer, column, start, length);
}
