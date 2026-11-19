namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Registers each reference arm a second time, as a candidate. The per-case tests then compare two runs of the
/// old path, for every facet. This shows that the old path gives the same outcome on each run, and that the
/// comparison runs for every facet.
/// </summary>
internal sealed class OldPathAgainstItselfRegistration : IDifferentialRegistration
{
    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.AddForEveryFacet(RenamedArm.Of("Old path again: ReadAs", ReferenceArms.ReadAs));
        registry.AddForEveryFacet(RenamedArm.Of("Old path again: Poco", ReferenceArms.Poco));
        registry.AddForEveryFacet(RenamedArm.Of("Old path again: CanRead", ReferenceArms.CanRead));
        registry.AddForEveryFacet(RenamedArm.Of("Old path again: Write", ReferenceArms.Write));
        registry.AddForEveryFacet(RenamedArm.Of("Old path again: CanWrite", ReferenceArms.CanWrite));
    }
}
