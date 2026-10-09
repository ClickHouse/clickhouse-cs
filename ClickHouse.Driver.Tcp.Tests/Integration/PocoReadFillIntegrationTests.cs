using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The tests of <see cref="PocoReadIntegrationTestBase"/>, with a client whose read plans use
/// <see cref="PocoScatterTier.Fill"/>: the tier of a runtime without dynamic code, which the test host does not choose
/// by itself. The fixture is for coverage, so it does not carry the Cloud category.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class PocoReadFillIntegrationTests : PocoReadIntegrationTestBase
{
    /// <inheritdoc/>
    private protected override ClickHouseTcpClient CreateClient()
        => new(TcpServerFixture.Options()) { PocoTypes = new PocoTypeRegistry { ForcedTier = PocoScatterTier.Fill } };
}
