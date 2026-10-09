using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The tests of <see cref="PocoReadIntegrationTests"/>, with a client whose read plans use
/// <see cref="PocoScatterTier.Fill"/>: the tier of a runtime without dynamic code, which the test host does not choose
/// by itself.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public sealed class PocoReadFillIntegrationTests : PocoReadIntegrationTests
{
    /// <inheritdoc/>
    private protected override ClickHouseTcpClient CreateClient()
        => new(TcpServerFixture.Options()) { PocoTypes = new PocoTypeRegistry { ForcedTier = PocoScatterTier.Fill } };
}
