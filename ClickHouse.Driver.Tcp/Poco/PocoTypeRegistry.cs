using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
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

    private readonly ConcurrentDictionary<(Type PocoType, string Signature, PocoGatherTier? ForcedTier), object> writePlans = new();

    /// <summary>
    /// The scatter tier of every read plan that does not ask for one, or null to let the runtime choose. The tests set
    /// it to run the client's queries through <see cref="PocoScatterTier.Fill"/>, the tier of a runtime without dynamic
    /// code.
    /// </summary>
    internal PocoScatterTier? ForcedTier { get; init; }

    /// <summary>
    /// The gather tier of every write plan, or null to let the runtime choose. The tests set it to run the client's row
    /// inserts through <see cref="PocoGatherTier.Delegate"/>, the tier of a runtime without dynamic code.
    /// </summary>
    internal PocoGatherTier? ForcedGatherTier { get; init; }

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
    /// <param name="forcedTier">A scatter tier to use regardless of the runtime, or null for <see cref="ForcedTier"/>.</param>
    /// <returns>The cached plan.</returns>
    /// <exception cref="InvalidOperationException">The shape cannot be read into <typeparamref name="T"/>.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public PocoReadPlan<T> ReadPlanFor<T>(Block block, PocoScatterTier? forcedTier)
        where T : class
    {
        PocoTypeDescriptor<T> descriptor = DescriptorFor<T>();
        PocoScatterTier? tier = forcedTier ?? ForcedTier;
        return (PocoReadPlan<T>)readPlans.GetOrAdd(
            (typeof(T), PocoReadPlan.SignatureOf(block), tier),
            _ => PocoReadPlan<T>.Build(descriptor, block, tier));
    }

    /// <summary>
    /// Gets or builds the write plan for <typeparamref name="T"/> and the target schema.
    /// </summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="schema">The server's sample block for the INSERT.</param>
    /// <returns>The cached plan.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot fill the target schema.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public PocoWritePlan<T> WritePlanFor<T>(Block schema)
        where T : class
    {
        PocoTypeDescriptor<T> descriptor = DescriptorFor<T>();
        PocoGatherTier? tier = ForcedGatherTier;
        return (PocoWritePlan<T>)writePlans.GetOrAdd(
            (typeof(T), PocoWritePlan.SignatureOf(schema), tier),
            _ => PocoWritePlan<T>.Build(descriptor, schema, tier));
    }
}
