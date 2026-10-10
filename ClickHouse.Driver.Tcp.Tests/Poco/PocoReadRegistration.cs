using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Runs the client's POCO read plan with <see cref="PocoScatterTier.Fill"/>, the tier of a runtime without dynamic code,
/// for every POCO facet of the case list, against the client's POCO read plan with the scatter tier that the runtime
/// chooses (<see cref="PocoScatterTier.Emit"/>, one compiled loop for each column, <see cref="ClientArms.Poco"/>).
/// </summary>
internal sealed class PocoReadRegistration : IDifferentialRegistration
{
    // The POCO facets of the case list. A new case changes this count.
    internal const int PocoFacets = 587;

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.Add(new ClientArms.PocoArm("Client.Poco: Fill", PocoScatterTier.Fill), PocoFacets);
    }
}
