namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// How a compiled scatter sources one value from a column. The two tiers differ only in that sourcing step — the
/// value conversion and the property assignment are identical — so they must produce equal values, which is what
/// the parity tests assert.
///
/// <para>
/// Equal <em>values</em>, not identical references, and the two are not the same claim for a reference-typed
/// element. A <c>FixedString(N)</c> column builds a fresh <c>byte[]</c> per row either way, but not the
/// <em>same</em> one: <see cref="Types.IColumn{T}.Values"/> caches the array it built, while the indexer copies the
/// row's bytes out again on each access. So the two tiers hand out arrays with equal contents and different
/// identities, which is why parity is asserted on values.
/// </para>
///
/// <para>
/// Within one <c>LowCardinality</c> column the opposite holds, on both tiers: every row sharing a dictionary entry
/// gets that entry's own instance, because both the indexer and <see cref="Types.IColumn{T}.Values"/> read the
/// dictionary through its <c>Values</c>. That is deliberate (it is what stops a <c>String</c> dictionary being
/// decoded once per row) and it is observable for a mutable element type, i.e. the <c>byte[]</c> of a
/// <c>LowCardinality(FixedString(N))</c> — see the remarks on <see cref="ClickHouseTcpClient.QueryAsync{T}"/> and
/// on <see cref="ClickHouseTcpClient.QueryAsync(string, ClickHouseTcpQueryOptions, System.Threading.CancellationToken)"/>.
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
}
