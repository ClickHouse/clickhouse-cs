using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Runs the POCO write plan of <c>InsertRowsAsync&lt;T&gt;</c> with the gather of a runtime without dynamic code
/// (<see cref="PocoGatherTier.Delegate"/>) against the compiled gather (<see cref="ClientArms.PocoWrite"/>), and
/// <c>ClickHouseTcpTypes.CanWrite</c> in the answer tiers of both row inserts, so it gives the answer of the row inserts.
/// </summary>
internal sealed class RowWriteRegistration : IDifferentialRegistration
{
    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.AddForEveryFacet(new RowWriteArms.ClientPocoArm("Client.PocoWrite: Delegate", PocoGatherTier.Delegate));
        registry.AddForEveryFacet(new ClientArms.FunctionAnswerArm("Client.CanWrite: PocoCanWrite", Tier.PocoCanWrite, ClickHouseTcpTypes.CanWrite));
        registry.AddForEveryFacet(new ClientArms.FunctionAnswerArm("Client.CanWrite: UntypedCanWrite", Tier.UntypedCanWrite, ClickHouseTcpTypes.CanWrite));
    }
}
