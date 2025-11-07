using System;
using System.Net.Http;

namespace ClickHouse.Driver.Http;

internal class SingleConnectionHttpClientFactory : IHttpClientFactory, IDisposable
{
    private readonly DefaultHttpClientHandler handler;

    public TimeSpan Timeout { get; init; }

    public SingleConnectionHttpClientFactory(bool skipServerCertificateValidation)
    {
        handler = new(skipServerCertificateValidation) { MaxConnectionsPerServer = 1 };
    }

    public HttpClient CreateClient(string name) => new(handler, false) { Timeout = Timeout };

    public void Dispose() => handler.Dispose();
}
