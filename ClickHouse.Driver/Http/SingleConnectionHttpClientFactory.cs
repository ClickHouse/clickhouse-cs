using System;
using System.Net.Http;

namespace ClickHouse.Driver.Http;

internal class SingleConnectionHttpClientFactory : IHttpClientFactory, IDisposable
{
    private readonly HttpMessageHandler handler;

    public TimeSpan Timeout { get; init; }

    public SingleConnectionHttpClientFactory(bool skipServerCertificateValidation)
    {
        handler = HttpHandlerProvider.CreateHandler(skipServerCertificateValidation, maxConnectionsPerServer: 1);
    }

    public HttpClient CreateClient(string name) => new(handler, false) { Timeout = Timeout };

    public void Dispose() => handler.Dispose();
}
