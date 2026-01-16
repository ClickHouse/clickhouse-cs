using System.Net;
using System.Net.Http;
using System.Threading;
namespace ClickHouse.Driver.Tests.Utilities;

public class TestHttpClientFactory : IHttpClientFactory
{
    private int createClientCallCount;

    public int CreateClientCallCount => createClientCallCount;
    public string LastRequestedClientName { get; private set; }

    public HttpClient CreateClient(string name)
    {
        Interlocked.Increment(ref createClientCallCount);
        LastRequestedClientName = name;

        var handler = new HttpClientHandler()
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        };
        return new HttpClient(handler);
    }
}
