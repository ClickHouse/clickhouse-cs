using System;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Bounds how long the transport may go without delivering a byte, so an operation against a server that has
/// stopped answering fails instead of waiting for TCP to give up.
/// </summary>
/// <remarks>
/// <para>
/// The deadline is armed immediately before each read from the transport and disarmed as soon as that read
/// returns, so it measures silence rather than elapsed time: a result that streams for an hour never trips it,
/// and neither does a consumer that holds a block for longer than the deadline before asking for the next one.
/// </para>
/// <para>
/// Not thread-safe, and scoped to one operation: <see cref="Begin"/> opens it over the caller's token and
/// returns the token every read of that operation must observe, <see cref="End"/> closes it.
/// </para>
/// </remarks>
internal sealed class IdleReadDeadline
{
    private readonly TimeSpan timeout;
    private CancellationTokenSource source;
    private CancellationToken callerToken;

    /// <summary>Initializes a deadline of <paramref name="timeout"/>.</summary>
    /// <param name="timeout">
    /// How long the transport may stay silent before the read fails. Must be positive and within what a timer
    /// can hold, which <see cref="ClickHouseTcpClientOptions.Validate"/> checks; a connection given
    /// <see cref="TimeSpan.Zero"/> builds no deadline at all rather than one of zero length.
    /// </param>
    internal IdleReadDeadline(TimeSpan timeout) => this.timeout = timeout;

    /// <summary>Whether the deadline elapsed, as opposed to the caller cancelling.</summary>
    internal bool Elapsed => source is { IsCancellationRequested: true } && !callerToken.IsCancellationRequested;

    /// <summary>
    /// Opens the deadline over an operation's token. The returned token fires when either the caller cancels or
    /// the transport stays silent too long.
    /// </summary>
    /// <remarks>
    /// For the operation's <b>reads only</b>. Writes keep the caller's token: disarming cannot recall a timer
    /// callback that has already begun, so a deadline elapsing just as a read completes would otherwise cancel
    /// the write that follows, for a token the caller never cancelled.
    /// </remarks>
    /// <param name="operationToken">The caller's token for this operation.</param>
    /// <returns>The token every read of this operation must observe.</returns>
    internal CancellationToken Begin(CancellationToken operationToken)
    {
        callerToken = operationToken;
        source = CancellationTokenSource.CreateLinkedTokenSource(operationToken);
        return source.Token;
    }

    /// <summary>Closes the deadline. Safe to call without a matching <see cref="Begin"/>.</summary>
    internal void End()
    {
        source?.Dispose();
        source = null;
        callerToken = default;
    }

    /// <summary>Starts the clock, immediately before a read from the transport.</summary>
    internal void Arm() => Reschedule(timeout);

    /// <summary>Stops the clock, as soon as a read from the transport returns.</summary>
    internal void Disarm() => Reschedule(Timeout.InfiniteTimeSpan);

    /// <summary>The failure to report when a read gave up because the transport stayed silent.</summary>
    /// <returns>A timeout naming the option that set the deadline.</returns>
    internal TimeoutException ToException()
        => new($"The server sent nothing for {timeout.TotalSeconds:0.###}s while a response was being read (ReadTimeout).");

    private void Reschedule(TimeSpan delay)
    {
        // Already fired, or never opened: CancelAfter would throw on the disposed source, and there is nothing
        // left to bound either way — the read is about to unwind.
        if (source is null || source.IsCancellationRequested)
        {
            return;
        }

        source.CancelAfter(delay);
    }
}
