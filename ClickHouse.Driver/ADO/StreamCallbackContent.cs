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

    // The caller's token wins when it can fire, otherwise the callback gets the one HttpClient supplies
    // here (which folds in HttpClient.Timeout and internal aborts), so serialization is never handed a
    // token that cannot be cancelled at all.
    //
    // The two are deliberately not combined into a linked CancellationTokenSource. Callbacks use this
    // token to bail out before writing, not to poll mid-write, and a transport-side abort tears the
    // request stream down regardless — the write then fails on its own. Linking would allocate a
    // CancellationTokenSource plus two registrations per call (per batch, on the insert path) to
    // forward a signal the stream already delivers.
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext context, CancellationToken httpCancellationToken)
        => callback(stream, cancellationToken.CanBeCanceled ? cancellationToken : httpCancellationToken);

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }
}
