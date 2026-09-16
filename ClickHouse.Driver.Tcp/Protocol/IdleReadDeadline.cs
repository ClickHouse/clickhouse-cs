using System;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>Applies a timeout to each transport read within an operation.</summary>
/// <remarks>
/// The timer is armed before each read and disarmed when it completes. It remains disarmed while the consumer
/// processes a returned block. Each operation must pair <see cref="Begin"/> with <see cref="End"/>.
/// This class is not thread-safe.
/// </remarks>
internal sealed class IdleReadDeadline
{
    private readonly TimeSpan timeout;
    private CancellationTokenSource source;
    private CancellationToken callerToken;

    /// <summary>Initializes a deadline of <paramref name="timeout"/>.</summary>
    /// <param name="timeout">
    /// Positive read timeout, validated by <see cref="ClickHouseTcpClientOptions.Validate"/> against the timer's
    /// supported range. Connections with a zero timeout do not create a deadline.
    /// </param>
    internal IdleReadDeadline(TimeSpan timeout) => this.timeout = timeout;

    /// <summary>Whether the deadline token is cancelled without caller cancellation.</summary>
    internal bool Elapsed => source is { IsCancellationRequested: true } && !callerToken.IsCancellationRequested;

    /// <summary>Initializes cancellation for an operation. Pair with <see cref="End"/> in a finally block.</summary>
    /// <param name="operationToken">The caller's token for this operation.</param>
    internal void Begin(CancellationToken operationToken)
    {
        callerToken = operationToken;
        source = CancellationTokenSource.CreateLinkedTokenSource(operationToken);
    }

    /// <summary>Disposes the operation token source. Safe to call without a matching <see cref="Begin"/>.</summary>
    internal void End()
    {
        source?.Dispose();
        source = null;
        callerToken = default;
    }

    /// <summary>Arms the timer and returns the token for this transport read.</summary>
    /// <param name="cancellationToken">The caller's token, used while no operation is active (during the handshake).</param>
    /// <returns>The read's deadline token, or the caller's token when no operation is active.</returns>
    internal CancellationToken Arm(CancellationToken cancellationToken)
    {
        if (source is null)
        {
            return cancellationToken;
        }

        source.CancelAfter(timeout);
        return source.Token;
    }

    /// <summary>Disarms the timer and prepares the token source for the next read.</summary>
    internal void Disarm()
    {
        if (source is null || source.TryReset())
        {
            return;
        }

        // TryReset fails if cancellation was requested or a timer callback was queued. Replace the source
        // so that callback can only cancel the completed read's token.
        source.Dispose();
        source = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
    }

    /// <summary>Creates the exception for a transport read timeout.</summary>
    /// <returns>A timeout naming the option that set the deadline.</returns>
    internal TimeoutException ToException()
        => new($"The server sent nothing for {timeout.TotalSeconds:0.###}s while a response was being read (ReadTimeout).");
}
