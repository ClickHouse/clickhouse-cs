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
/// fall back to the self-contained ones.
/// </para>
/// </summary>
internal interface IColumnWriteState : IDisposable
{
}
