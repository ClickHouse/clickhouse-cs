using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A leaf codec whose values encode as one flat per-element stream with no framing sections (no null-map, no
/// dictionary, no offsets) — the fixed-width types, <c>String</c>, and <c>FixedString</c>. Because the stream is
/// just the elements back to back, a run of elements can be written on its own, which lets a concatenating
/// composite (<c>Array</c>, <c>Map</c>, <c>Nested</c>) drive the wire straight from the ergonomic per-row arrays
/// one run at a time — a contiguous bulk blit for the fixed-width codecs — with no flat buffer built first.
///
/// <para>
/// This is <em>only</em> valid for a codec with no serialization sections spanning the whole run; a sectioned
/// inner (<c>Nullable</c>, <c>LowCardinality</c>, a nested <c>Array</c>, <c>Dynamic</c>) must instead see every
/// element as one column so its section is emitted once, so those codecs do not implement this and are fed a
/// flattening view instead.
/// </para>
/// </summary>
/// <typeparam name="T">The codec's CLR element type.</typeparam>
internal interface ISpanWritableCodec<T>
{
    /// <summary>Writes <paramref name="values"/> as a run of the codec's per-element encoding, in order.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The contiguous run of values to write.</param>
    void WriteValues(ClickHouseBinaryWriter writer, System.ReadOnlySpan<T> values);
}
