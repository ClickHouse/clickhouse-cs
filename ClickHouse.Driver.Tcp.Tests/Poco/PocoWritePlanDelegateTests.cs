using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The tests of <see cref="PocoWritePlanTests"/> in the gather tier of a runtime without dynamic code
/// (<see cref="PocoGatherTier.Delegate"/>): a getter delegate for each row, with no compiled loop.
/// </summary>
[TestFixture]
public sealed class PocoWritePlanDelegateTests : PocoWritePlanTests
{
    /// <inheritdoc/>
    private protected override PocoGatherTier? Tier => PocoGatherTier.Delegate;
}
