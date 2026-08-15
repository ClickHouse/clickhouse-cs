using System;
using System.Collections.Concurrent;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Caches descriptors per POCO type and plans per type and wire shape. The per-client caches do not evict;
/// disposing the client releases their type keys and compiled delegates.
/// </summary>
internal sealed class PocoTypeRegistry
{
    private readonly ConcurrentDictionary<Type, PocoTypeDescriptor> descriptors = new();

    private readonly ConcurrentDictionary<(Type PocoType, string Signature, PocoScatterTier? ForcedTier), object> readPlans = new();

    private readonly ConcurrentDictionary<(Type PocoType, string Signature), object> writePlans = new();

    /// <summary>Gets or builds the descriptor for <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <returns>The cached descriptor.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped.</exception>
    public PocoTypeDescriptor<T> DescriptorFor<T>()
        where T : class
        // Keyed by the Type object, not its name, so two same-named types from different assemblies stay distinct.
        // A race can build twice; the build is pure, so the loser's copy is simply dropped.
        => (PocoTypeDescriptor<T>)descriptors.GetOrAdd(typeof(T), static _ => PocoTypeDescriptor<T>.Build());

    /// <summary>
    /// Gets or builds the read plan for <typeparamref name="T"/>, the block shape, and the requested tier.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="block">A block of the shape to plan for.</param>
    /// <param name="forcedTier">A scatter tier to compile regardless of the runtime, or null to choose one.</param>
    /// <returns>The cached plan.</returns>
    /// <exception cref="InvalidOperationException">The shape cannot be read into <typeparamref name="T"/>.</exception>
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
