namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The tests of <see cref="PocoReadIntegrationTestBase"/>, with a client whose read plans use the scatter tier that the
/// runtime chooses.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public sealed class PocoReadIntegrationTests : PocoReadIntegrationTestBase
{
}
