using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The caching contract around <see cref="PocoTypeDescriptor{T}"/>: one descriptor per type, keyed by the type
/// itself, and a type that cannot be mapped reported to every caller rather than only the first.
/// </summary>
[TestFixture]
public class PocoTypeRegistryTests
{
    [Test]
    public void DescriptorFor_SameTypeTwice_ReturnsTheCachedDescriptor()
    {
        var registry = new PocoTypeRegistry();

        Assert.That(registry.DescriptorFor<SimplePoco>(), Is.SameAs(registry.DescriptorFor<SimplePoco>()));
    }

    [Test]
    public void DescriptorFor_DifferentTypes_ReturnsADescriptorPerType()
    {
        var registry = new PocoTypeRegistry();

        PocoTypeDescriptor<SimplePoco> first = registry.DescriptorFor<SimplePoco>();
        PocoTypeDescriptor<OtherPoco> second = registry.DescriptorFor<OtherPoco>();

        Assert.That(first.PocoType, Is.EqualTo(typeof(SimplePoco)));
        Assert.That(second.PocoType, Is.EqualTo(typeof(OtherPoco)));
    }

    [Test]
    public void DescriptorFor_SeparateRegistries_DoNotShareDescriptors()
    {
        // Per-client caching: one client's compiles must not be reachable from another, so a caller can drop a
        // client and, with it, everything it pinned.
        Assert.That(new PocoTypeRegistry().DescriptorFor<SimplePoco>(), Is.Not.SameAs(new PocoTypeRegistry().DescriptorFor<SimplePoco>()));
    }

    [Test]
    public void DescriptorFor_UnmappableType_ThrowsOnEveryCallRatherThanCachingTheFailure()
    {
        var registry = new PocoTypeRegistry();

        Assert.Throws<InvalidOperationException>(() => registry.DescriptorFor<EmptyPoco>());
        Assert.Throws<InvalidOperationException>(() => registry.DescriptorFor<EmptyPoco>());
    }

    [Test]
    public async Task DescriptorFor_ConcurrentCallers_AllSeeOneDescriptor()
    {
        var registry = new PocoTypeRegistry();
        var results = new PocoTypeDescriptor<SimplePoco>[16];

        var racers = new Task[results.Length];
        for (int i = 0; i < racers.Length; i++)
        {
            int slot = i;
            racers[i] = Task.Run(() => results[slot] = registry.DescriptorFor<SimplePoco>());
        }

        await Task.WhenAll(racers).ConfigureAwait(false);

        // A race may build twice, but only one build can win the dictionary, so every caller must be handed that
        // one — otherwise two callers could hold descriptors whose compiled plans cache separately.
        Assert.That(results, Is.All.SameAs(results[0]));
    }

    private class SimplePoco
    {
        public long Id { get; set; }
    }

    private class OtherPoco
    {
        public string Name { get; set; }
    }

    private class EmptyPoco
    {
    }
}
