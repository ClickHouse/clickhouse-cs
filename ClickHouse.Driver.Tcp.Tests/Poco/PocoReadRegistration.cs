using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Runs the client's POCO read plan in the differential tests, for every POCO facet of the case list, against the old
/// plan (<see cref="LegacyPocoRead"/>): with the scatter tier that the runtime chooses (<see cref="PocoScatterTier.Emit"/>,
/// one compiled loop for each column), and with <see cref="PocoScatterTier.Fill"/>, the tier of a runtime without
/// dynamic code. No outcome changes and nothing is declared: the converter derivation has the read rules of POCO
/// mapping (<see cref="ReadRules"/>), and the scatter keeps the NULL message of POCO mapping.
/// </summary>
internal sealed class PocoReadRegistration : IDifferentialRegistration
{
    // The POCO facets of the case list. A new case changes this count.
    internal const int PocoFacets = 567;

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.Add(ClientArms.Poco, PocoFacets);
        registry.Add(new ClientArms.PocoArm("Client.Poco: Fill", PocoScatterTier.Fill), PocoFacets);
    }
}
