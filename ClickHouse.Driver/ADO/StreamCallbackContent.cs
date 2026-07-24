using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// HttpContent implementation allowing streaming large payloads without having to materialize
/// the entire stream up-front.
/// </summary>
internal class StreamCallbackContent : HttpContent
{
    private readonly Func<Stream, CancellationToken, Task> callback;
    private readonly CancellationToken cancellationToken;

    public StreamCallbackContent(Func<Stream, CancellationToken, Task> callback, CancellationToken cancellationToken)
    {
        this.callback = callback;
        this.cancellationToken = cancellationToken;
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        => SerializeToStreamAsync(stream, context, CancellationToken.None);

    // HttpClient supplies its own cancellation token here (folding in HttpClient.Timeout and internal
    // aborts). Serialization should observe both it and the caller's token, but only pay for a linked
    // CancellationTokenSource when both can actually fire — the caller often passes a non-cancelable
    // token, and this content is created per insert batch.
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext context, CancellationToken httpCancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
            return callback(stream, httpCancellationToken);

        if (!httpCancellationToken.CanBeCanceled)
            return callback(stream, cancellationToken);

        return SerializeLinkedAsync(stream, httpCancellationToken);
    }

    private async Task SerializeLinkedAsync(Stream stream, CancellationToken httpCancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, httpCancellationToken);
        await callback(stream, linked.Token).ConfigureAwait(false);
    }

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }
}
