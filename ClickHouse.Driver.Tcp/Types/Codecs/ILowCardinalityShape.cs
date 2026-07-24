using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The generic bridge for one low-cardinality element type: it knows how to build the typed
/// <see cref="LowCardinalityColumn{T}"/>, test a writable column, build the dictionary and keys on write, and
/// price a row. One implementation covers every element type; the concrete instance is chosen once per element
/// type and cached.
///
/// <para>
/// This exists because the codec pipeline is non-generic — the registry parses a type string at runtime and
/// hands codecs back through <see cref="IColumnCodec"/>, so the element type <c>T</c> arrives only as a
/// <see cref="Type"/>. The T-independent machinery (the version prefix, the metadata word, the keys stream)
/// stays in <see cref="LowCardinalityColumnCodec"/>; the thin slice that genuinely needs <c>T</c> — building the
/// typed column, the write-side casts, deduplicating values into a dictionary — lives here.
/// </para>
/// </summary>
internal interface ILowCardinalityShape
{
    /// <summary>
    /// The CLR element type the decoded column surfaces — the inner element type for a non-nullable
    /// <c>LowCardinality(T)</c>, or that type made nullable for <c>LowCardinality(Nullable(T))</c>.
    /// </summary>
    Type SurfaceElementType { get; }

    /// <summary>Wraps a decoded dictionary column and its per-row keys into the typed low-cardinality column.</summary>
    IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys);

    /// <summary>Whether <paramref name="column"/> is a column of this element type, writable by the codec.</summary>
    bool CanWrite(IColumn column);

    /// <summary>Whether the inner codec can write an inner-typed column at all (e.g. <c>Nothing</c> cannot).</summary>
    bool CanInnerWrite(IColumnCodec inner);

    /// <summary>
    /// Writes the low-cardinality body for rows [<paramref name="start"/>, start + length): the metadata word,
    /// the dictionary size, the dictionary values, the keys count, then the keys. A dense
    /// <see cref="LowCardinalityColumn{T}"/> re-emits its dictionary and keys with no rebuild; an ergonomic
    /// <c>IColumn&lt;T&gt;</c> is deduplicated into a fresh block-local dictionary. Writes nothing when
    /// <paramref name="length"/> is zero (the empty body a composite emits under its own state prefix).
    /// </summary>
    void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length);
}
