using System;
using System.Collections.Concurrent;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Caches the POCO mapping work, so a type is reflected over and its constructor compiled once instead of once per
/// query or insert. One registry lives per <see cref="ClickHouseTcpClient"/>: the client is meant to be held as a
/// singleton, so the cost is amortized anyway, and a per-client cache cannot pin types loaded into an
/// <see cref="System.Runtime.Loader.AssemblyLoadContext"/> the caller later wants to unload.
///
/// <para>
/// Only the per-type level lives here. The compiled loops are per <c>(type, wire shape)</c> — the column types come
/// from the server, not from the type — so they cache separately, keyed by the block or sample-block signature.
/// </para>
/// </summary>
internal sealed class PocoTypeRegistry
{
    private readonly ConcurrentDictionary<Type, PocoTypeDescriptor> descriptors = new();

    /// <summary>
    /// Gets the descriptor for <typeparamref name="T"/>, building it on first use.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <returns>The cached descriptor.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped at all — see
    /// <see cref="PocoTypeDescriptor{T}.Build"/>. Nothing is cached in that case, so the same failure is reported
    /// every time rather than only to the first caller.</exception>
    public PocoTypeDescriptor<T> DescriptorFor<T>()
        where T : class
        // Keyed by the Type object, not its name, so two same-named types from different assemblies stay distinct.
        // A race can build twice; the build is pure, so the loser's copy is simply dropped.
        => (PocoTypeDescriptor<T>)descriptors.GetOrAdd(typeof(T), static _ => PocoTypeDescriptor<T>.Build());
}
