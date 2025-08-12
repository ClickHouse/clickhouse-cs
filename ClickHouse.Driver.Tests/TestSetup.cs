using System.Net;

namespace ClickHouse.Driver.Tests;

[SetUpFixture]
public class TestSetup
{
    [OneTimeSetUp]
    public void RunBeforeAnyTests()
    {
#if NETFRAMEWORK
        // In .NET Framework TFMs, we need to account for connection limits not present in other implementations
        ServicePointManager.DefaultConnectionLimit = 1000;
        ServicePointManager.Expect100Continue = false;
        ServicePointManager.UseNagleAlgorithm = false;
        ServicePointManager.MaxServicePointIdleTime = 10000;
        ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls12;
#endif
    }
}
