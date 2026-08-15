namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// How a compiled scatter sources one value from a column. The three tiers differ only in that sourcing step — the
/// value conversion and the property assignment are identical — so they must produce equal values, which is what
/// the parity tests assert.
///
/// <para>
/// Equal <em>values</em>, not identical references: a <c>LowCardinality</c> column's
/// <see cref="Types.IColumn{T}.Values"/> hands every row sharing a dictionary entry the same element instance, while
/// its indexer materializes one per row. That is observable only for a mutable element type, i.e. the
/// <c>byte[]</c> of a <c>LowCardinality(FixedString(N))</c> — see the remarks on
/// <see cref="ClickHouseTcpClient.QueryAsync{T}"/>.
/// </para>
/// </summary>
internal enum PocoScatterTier
{
    /// <summary>
    /// Reads <see cref="Types.IColumn{T}.Values"/> once and indexes the span per row, so nothing is boxed and a
    /// column whose storage is its values (the fixed-width and calendar columns) is read with no copy at all. The
    /// default wherever the runtime compiles expression trees to IL.
    /// </summary>
    Span,

    /// <summary>
    /// Reads <c>IColumn&lt;T&gt;[row]</c> per row: still box-free, and the default when
    /// <see cref="System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled"/> is false (NativeAOT and
    /// friends), where a tree is interpreted rather than compiled and cannot hold a <c>ReadOnlySpan&lt;T&gt;</c>.
    /// </summary>
    Indexer,

    /// <summary>
    /// Reads <see cref="Types.IColumn.GetValue"/> per row and unboxes: one box per cell, so it is not chosen for a
    /// column that surfaces its element type generically. It is the fallback for a column that does not implement
    /// <see cref="Types.IColumn{T}"/> over its codec's element type, and the third leg of the parity harness.
    /// </summary>
    Boxed,
}
