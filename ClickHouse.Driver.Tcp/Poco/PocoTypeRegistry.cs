using System;
using System.Collections.Concurrent;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Caches the POCO mapping work, so a type is reflected over and its constructor compiled once instead of once per
/// query or insert. One registry lives per <see cref="ClickHouseTcpClient"/> rather than one per process: the client
/// is meant to be held as a singleton, so the cost is amortized either way, and scoping the cache to the client is
/// what lets dropping the client release it.
///
/// <para>
/// Two levels, because only half of a mapping is per-type. The descriptor is; the compiled loops are per
/// <c>(type, wire shape)</c> — the column types come from the server, not from the type — so they cache under their
/// own key, the block signature.
/// </para>
///
/// <para>
/// Neither level evicts, which has two consequences worth stating. The descriptors are bounded by the POCO types a
/// program has, but the plans are bounded by the distinct <em>shapes</em> it reads and writes, and a shape is
/// caller-controlled (aliases, ad-hoc column lists): a long-lived client issuing endlessly varied shapes for one type
/// accumulates a plan per shape. And both levels hold their <see cref="Type"/> keys — plus compiled delegates over
/// that type's members — for as long as the client lives, so a POCO loaded into a collectible
/// <see cref="System.Runtime.Loader.AssemblyLoadContext"/> keeps that context loaded until the client is disposed.
/// Neither is the shape of the workloads this is for; both want a weak or bounded cache if they ever show up.
/// </para>
/// </summary>
internal sealed class PocoTypeRegistry
{
    private readonly ConcurrentDictionary<Type, PocoTypeDescriptor> descriptors = new();

    private readonly ConcurrentDictionary<(Type PocoType, string Signature, PocoScatterTier? ForcedTier), object> readPlans = new();

    private readonly ConcurrentDictionary<(Type PocoType, string Signature), object> writePlans = new();

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

    /// <summary>
    /// Gets the read plan for <typeparamref name="T"/> over <paramref name="block"/>'s shape, compiling it on first
    /// use. Keyed by the block signature, so one type queried through several shapes keeps a plan per shape, and by
    /// the forced tier, so a caller asking for a particular tier is never handed another tier's plan.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="block">A block of the shape to plan for.</param>
    /// <param name="forcedTier">A scatter tier to compile regardless of the runtime, or null to choose one.</param>
    /// <returns>The cached plan.</returns>
    /// <exception cref="InvalidOperationException">The shape cannot be read into <typeparamref name="T"/> — see
    /// <see cref="PocoReadPlan{T}.Build"/>. Nothing is cached in that case, so every caller is told, not just the
    /// first.</exception>
    public PocoReadPlan<T> ReadPlanFor<T>(Block block, PocoScatterTier? forcedTier)
        where T : class
    {
        PocoTypeDescriptor<T> descriptor = DescriptorFor<T>();
        return (PocoReadPlan<T>)readPlans.GetOrAdd(
            (typeof(T), PocoReadPlan.SignatureOf(block), forcedTier),
            _ => PocoReadPlan<T>.Build(descriptor, block, forcedTier));
    }

    /// <summary>
    /// Gets the write plan for <typeparamref name="T"/> over <paramref name="schema"/>'s target columns, compiling it
    /// on first use. Keyed by the sample block's signature, so one type inserted into several tables — or into one
    /// table through several column lists — keeps a plan per target shape.
    /// </summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="schema">The server's sample block for the INSERT.</param>
    /// <returns>The cached plan.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot fill the target — see
    /// <see cref="PocoWritePlan{T}.Build"/>. Nothing is cached in that case, so every caller is told, not just the
    /// first.</exception>
    public PocoWritePlan<T> WritePlanFor<T>(Block schema)
        where T : class
    {
        PocoTypeDescriptor<T> descriptor = DescriptorFor<T>();
        return (PocoWritePlan<T>)writePlans.GetOrAdd(
            (typeof(T), PocoWritePlan.SignatureOf(schema)),
            _ => PocoWritePlan<T>.Build(descriptor, schema));
    }
}
