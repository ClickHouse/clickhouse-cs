using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Pins which cancellation token a streamed request body is serialized under. Binary inserts serialize
/// straight into the request body, so the callback's token is the only thing that lets serialization
/// bail out before writing a batch; the transport handles aborts once the write is under way.
/// </summary>
[TestFixture]
public class StreamCallbackTokenTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task PostStreamAsync_WithCancellableCallerToken_PassesCallerTokenToCallbackVerbatim()
    {
        // Verbatim, not wrapped in a linked token: the caller's token is what carries caller-initiated
        // cancellation (and, on the insert path, a sibling batch's failure) into serialization.
        using var cts = new CancellationTokenSource();
        var observed = default(CancellationToken);

        using var response = await client.PostStreamAsync(
            "SELECT version()",
            (Stream _, CancellationToken ct) =>
            {
                observed = ct;
                return Task.CompletedTask;
            },
            isCompressed: false,
            cts.Token);

        Assert.That(observed, Is.EqualTo(cts.Token));
    }

    [Test]
    public async Task PostStreamAsync_WithNonCancellableCallerToken_PassesHttpClientTokenToCallback()
    {
        // With nothing to inherit from the caller, serialization still has to observe the transport's
        // own token (HttpClient.Timeout and internal aborts) rather than a token that can never fire.
        var observed = default(CancellationToken);

        using var response = await client.PostStreamAsync(
            "SELECT version()",
            (Stream _, CancellationToken ct) =>
            {
                observed = ct;
                return Task.CompletedTask;
            },
            isCompressed: false,
            CancellationToken.None);

        Assert.That(observed.CanBeCanceled, Is.True);
    }
}
