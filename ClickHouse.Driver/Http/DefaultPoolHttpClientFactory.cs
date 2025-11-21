using System;
using System.Net.Http;

namespace ClickHouse.Driver.Http;

internal class DefaultPoolHttpClientFactory : IHttpClientFactory, IDisposable
{
    private readonly HttpMessageHandler handler;

    public TimeSpan Timeout { get; init; }

    public DefaultPoolHttpClientFactory(bool skipServerCertificateValidation)
    {
        handler = HttpHandlerProvider.CreateHandler(skipServerCertificateValidation);
    }

    public HttpClient CreateClient(string name) => new(handler, false) { Timeout = Timeout };

    public void Dispose() => handler.Dispose();
}
