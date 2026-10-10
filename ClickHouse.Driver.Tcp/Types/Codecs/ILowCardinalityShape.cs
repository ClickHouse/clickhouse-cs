using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The generic bridge for one low-cardinality element type: it builds the typed column of a read, and it tells the
/// decoded column that the codec writes. One implementation covers every element type; the concrete instance is chosen
/// once per element type and cached.
///
/// <para>
/// This exists because the codec pipeline is non-generic: the registry parses a type string at runtime and
/// hands codecs back through <see cref="IColumnCodec"/>, so the element type <c>T</c> arrives only as a
/// <see cref="Type"/>. The T-independent machinery (the version prefix, the metadata word, the keys stream)
/// stays in <see cref="LowCardinalityColumnCodec"/>; the thin slice that genuinely needs <c>T</c> lives here.
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

    /// <summary>
    /// Whether <paramref name="column"/> is a decoded column of this shape, whose dictionary and keys the codec writes
    /// again. The dictionary of <c>LowCardinality(Nullable(T))</c> has two reserved slots (NULL and the default), and
    /// the dictionary of <c>LowCardinality(T)</c> has one, so each shape accepts only its own columns.
    /// </summary>
    /// <param name="column">The column to test.</param>
    /// <returns>Whether the column is a decoded column of this shape.</returns>
    bool CanWrite(IColumn column);
}
