using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A column whose values live in one contiguous run of memory, so a codec can blit them in a single bulk copy
/// rather than reading them one at a time. The dense read-back columns and a caller's own array-backed column
/// implement this; the lazy write-path <em>views</em> (which compute a value per index over another column) do
/// not, so a codec handed one of those falls back to its per-element write.
/// </summary>
/// <typeparam name="T">The CLR element type.</typeparam>
internal interface ISpanColumn<T>
{
    /// <summary>The values as one contiguous span — the same sequence as the indexer, addressable in bulk.</summary>
    ReadOnlySpan<T> Span { get; }
}
