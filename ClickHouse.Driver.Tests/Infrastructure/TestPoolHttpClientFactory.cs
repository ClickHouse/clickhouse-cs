using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Threading;

namespace ClickHouse.Driver.Tests.Infrastructure;

/// <summary>
/// HttpClientFactory that uses connection pooling to prevent port exhaustion during heavy parallel load in .NET Framework TFMs.
/// </summary>
internal sealed class TestPoolHttpClientFactory : IHttpClientFactory
{
#if NETFRAMEWORK
    private const int PoolSize = 16;
    private static readonly ConcurrentBag<HttpClientHandler> HandlerPool = new ConcurrentBag<HttpClientHandler>();
    private static readonly int[] Slots = new int[1];
    private static readonly HttpClientHandler[] Handlers;

    static TestPoolHttpClientFactory()
    {
        Handlers = new HttpClientHandler[PoolSize];
        for (int i = 0; i < PoolSize; i++)
        {
            var handler = new HttpClientHandler()
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                MaxConnectionsPerServer = 100,
                UseProxy = false
            };
            Handlers[i] = handler;
            HandlerPool.Add(handler);
        }
    }

    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(2);

    public HttpClient CreateClient(string name)
    {
        var index = (uint)Interlocked.Increment(ref Slots[0]) % PoolSize;
        return new HttpClient(Handlers[index], false) { Timeout = Timeout };
    }
#else
    private static readonly HttpClientHandler DefaultHttpClientHandler = new() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };

    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(2);

    public HttpClient CreateClient(string name) => new(DefaultHttpClientHandler, false) { Timeout = Timeout };
#endif
}
