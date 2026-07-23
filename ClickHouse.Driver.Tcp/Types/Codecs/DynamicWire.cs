namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The wire constants and layout rules for the flattened <c>Dynamic</c> serialization.</summary>
internal static class DynamicWire
{
    /// <summary>
    /// The serialization version that heads a <c>Dynamic</c> column's state prefix in the flattened layout — the
    /// only layout this client reads or writes. It is selected by the query setting
    /// <c>output_format_native_use_flattened_dynamic_and_json_serialization = 1</c>; the non-flat versions
    /// (1, 2, 4) carry an internal/on-disk representation this client rejects.
    /// </summary>
    public const ulong FlattenedVersion = 3;

    /// <summary>
    /// A defensive ceiling on the runtime type count read from the wire, so a corrupt length prefix cannot drive
    /// an unbounded allocation. Far larger than any real <c>Dynamic</c> type set.
    /// </summary>
    public const int MaxTypes = 1_000_000;

    /// <summary>
    /// The discriminator width for <paramref name="typeCount"/> runtime types: the smallest unsigned integer that
    /// indexes the types plus the NULL slot (NULL is the value <paramref name="typeCount"/>). One byte up to 255
    /// types, then 2, then 4 — a count is capped at <see cref="MaxTypes"/> and held in an <see cref="int"/>, so it
    /// never needs the wire format's 8-byte width.
    /// </summary>
    /// <param name="typeCount">The number of runtime types.</param>
    /// <returns>The discriminator width in bytes (1, 2, or 4).</returns>
    public static int DiscriminatorWidth(int typeCount)
        => typeCount <= byte.MaxValue ? 1
            : typeCount <= ushort.MaxValue ? 2
            : 4;
}
