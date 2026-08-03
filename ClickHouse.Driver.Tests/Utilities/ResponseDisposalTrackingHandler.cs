using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.Utilities;

/// <summary>
/// A <see cref="TrackingHandler"/> that additionally makes response disposal observable: every
/// response's content is replaced with a <see cref="DisposalTrackingContent"/>, and disposing an
/// <see cref="HttpResponseMessage"/> disposes its content.
/// </summary>
public class ResponseDisposalTrackingHandler : TrackingHandler
{
    private readonly List<DisposalTrackingContent> responses = new();

    public ResponseDisposalTrackingHandler(HttpMessageHandler innerHandler) : base(innerHandler) { }

    /// <summary>
    /// The content of every response returned so far, in the order the requests were made.
    /// </summary>
    public IReadOnlyList<DisposalTrackingContent> Responses => responses;

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);

        var trackingContent = new DisposalTrackingContent(
            await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false));
        foreach (var header in response.Content.Headers)
        {
            trackingContent.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        responses.Add(trackingContent);
        response.Content = trackingContent;
        return response;
    }

    public sealed class DisposalTrackingContent : StreamContent
    {
        internal DisposalTrackingContent(Stream content) : base(content) { }

        public bool IsDisposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            base.Dispose(disposing);
        }
    }
}
