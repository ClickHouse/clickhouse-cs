using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.Utilities;

public static class MockHttpClientHelper
{
    public static HttpClient Create(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler)
    {
        return new HttpClient(new MockHttpMessageHandler(handler), disposeHandler: true);
    }

    public static HttpClient Create(HttpResponseMessage response)
    {
        return Create((req, ct) => Task.FromResult(response));
    }

    public static HttpClient Create(Exception exception)
    {
        return Create((req, ct) => Task.FromException<HttpResponseMessage>(exception));
    }

    private class MockHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsynchandler;

        public MockHttpMessageHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsynchandler)
        {
            this.sendAsynchandler = sendAsynchandler ?? throw new ArgumentNullException(nameof(sendAsynchandler));
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return sendAsynchandler(request, cancellationToken);
        }
    }
}
