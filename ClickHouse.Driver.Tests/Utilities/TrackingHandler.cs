using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.Utilities;

public class TrackingHandler : DelegatingHandler
{
    private readonly List<HttpRequestMessage> requests = new();
    private readonly Func<HttpRequestMessage, HttpResponseMessage> fakeResponseFactory;

    public IReadOnlyList<HttpRequestMessage> Requests => requests;

    public int RequestCount => requests.Count;

    /// <summary>
    /// Creates a tracking handler that forwards requests to the inner handler.
    /// </summary>
    public TrackingHandler(HttpMessageHandler innerHandler) : base(innerHandler) { }

    /// <summary>
    /// Creates a tracking handler that returns fake responses without forwarding.
    /// </summary>
    public TrackingHandler(Func<HttpRequestMessage, HttpResponseMessage> fakeResponseFactory)
    {
        this.fakeResponseFactory = fakeResponseFactory ?? throw new ArgumentNullException(nameof(fakeResponseFactory));
    }

    /// <summary>
    /// Creates a tracking handler that returns a fixed fake response without forwarding.
    /// </summary>
    public TrackingHandler(HttpResponseMessage fakeResponse)
        : this(_ => fakeResponse) { }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        requests.Add(request);

        if (fakeResponseFactory != null)
        {
            return Task.FromResult(fakeResponseFactory(request));
        }

        return base.SendAsync(request, cancellationToken);
    }
}
