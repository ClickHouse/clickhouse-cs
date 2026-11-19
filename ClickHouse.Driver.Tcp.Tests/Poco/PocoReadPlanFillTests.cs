using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The tests of <see cref="PocoReadPlanTests"/>, through <see cref="PocoScatterTier.Fill"/>: the tier of a runtime
/// without dynamic code, which the test host does not choose by itself.
/// </summary>
[TestFixture]
public sealed class PocoReadPlanFillTests : PocoReadPlanTests
{
    /// <inheritdoc/>
    private protected override PocoScatterTier? Tier => PocoScatterTier.Fill;
}
