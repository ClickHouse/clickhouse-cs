using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Per-operation scratch a codec computes once for one slice of one write, then reuses across the state-prefix
/// and body phases of that same write. Most codecs need none: their prefix is absent or a fixed constant, so the
/// prefix and body phases share nothing. A codec whose prefix is <em>data-dependent</em> — its bytes derive from
/// the values themselves (the runtime type list a <c>Dynamic</c> column emits) — must discover that shape before
/// writing the prefix and reuse it when writing the body; an element-flattening composite wrapping such a codec
/// must likewise flatten its rows once and hand the same flattened column to both phases. That shared shape lives
/// here.
///
/// <para>
/// The write layer creates the state with <see cref="IColumnCodec.BeginWrite"/> before the prefix phase, passes
/// it to <see cref="IColumnCodec.WriteStatePrefix(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/>
/// then <see cref="IColumnCodec.WriteColumn(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/>, and
/// disposes it after the body — returning any rented buffers to the pool. Codecs that need no scratch return
/// <see langword="null"/> from <see cref="IColumnCodec.BeginWrite"/>, and the state-aware write overloads then
/// default to the self-contained ones.
///
/// <para>
/// A codec that does need scratch has exactly one way in per phase: the state-aware overload, given the state its
/// own <see cref="IColumnCodec.BeginWrite"/> returned. It does not also accept null there and rebuild. Writing a
/// slice without a shared state is what the state-free overloads are for; they build one and dispose it.
/// </para>
/// </para>
/// </summary>
internal interface IColumnWriteState : IDisposable
{
}

/// <summary>Helpers for consuming a write state handed back through <see cref="IColumnWriteState"/>.</summary>
internal static class ColumnWriteStateExtensions
{
    /// <summary>
    /// Narrows the shared scratch to the codec's own state type. A state of any other type means a composite paired
    /// one codec's scratch with a different codec's column — a bug where it was opened, not something the receiving
    /// codec can recover from, so this throws instead of quietly rebuilding a state and writing correct bytes that
    /// hide it.
    /// </summary>
    /// <typeparam name="TState">The receiving codec's own state type.</typeparam>
    /// <param name="state">The state to narrow.</param>
    /// <param name="typeName">The receiving codec's ClickHouse type name, for the message.</param>
    /// <returns><paramref name="state"/> as <typeparamref name="TState"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="state"/> is null or another codec's state.</exception>
    public static TState Expect<TState>(this IColumnWriteState state, string typeName)
        where TState : class, IColumnWriteState
        => state as TState
            ?? throw new ArgumentException(
                $"The codec for '{typeName}' needs a {typeof(TState).Name} write state but was given " +
                $"{(state is null ? "null" : state.GetType().Name)}. Each child of a composite must be given the " +
                "state its own BeginWrite returned, alongside the column that state was opened over.",
                nameof(state));
}
