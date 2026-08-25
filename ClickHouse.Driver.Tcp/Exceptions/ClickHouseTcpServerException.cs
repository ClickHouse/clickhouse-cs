using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// An error the server reported for a query, a handshake or a ping. Carries the server-side error code,
/// exception class name and stack trace alongside the message.
/// </summary>
/// <remarks>
/// The server can report a chain of causes; each one becomes an inner exception of the same type, so the
/// instance thrown is the outermost frame and <see cref="Exception.InnerException"/> walks inward.
/// </remarks>
public sealed class ClickHouseTcpServerException : ClickHouseTcpException
{
    // A corrupt or hostile server could stream an endless nested chain; cap it so a bad response can't drive
    // unbounded allocation. Far more frames than any legitimate server produces (nesting is usually 0 or 1).
    private const int MaxNestedFrames = 256;

    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpServerException"/> class.</summary>
    /// <param name="code">The server-side error code.</param>
    /// <param name="name">The server exception class name (e.g. <c>"DB::Exception"</c>).</param>
    /// <param name="message">The human-readable error message.</param>
    /// <param name="serverStackTrace">The server-side stack trace.</param>
    /// <param name="innerException">The nested server exception, or null for the innermost frame.</param>
    public ClickHouseTcpServerException(int code, string name, string message, string serverStackTrace, Exception innerException = null)
        : base(message, innerException)
    {
        RawCode = code;
        Code = Enum.IsDefined((ClickHouseErrorCode)code) ? (ClickHouseErrorCode)code : ClickHouseErrorCode.Unknown;
        Name = name;
        ServerStackTrace = serverStackTrace;

        // DbException surfaces this as ErrorCode, matching how the HTTP client reports the same number.
        HResult = code;
    }

    /// <summary>The server-side error code, exactly as the server sent it.</summary>
    public int RawCode { get; }

    /// <summary>
    /// <see cref="RawCode"/> as a named constant, or <see cref="ClickHouseErrorCode.Unknown"/> when this
    /// client does not name it.
    /// </summary>
    public ClickHouseErrorCode Code { get; }

    /// <summary>The server exception class name (e.g. <c>"DB::Exception"</c>).</summary>
    public string Name { get; }

    /// <summary>The server-side stack trace.</summary>
    public string ServerStackTrace { get; }

    /// <summary>
    /// Whether retrying the same operation later could plausibly succeed. True for load, contention and
    /// server-side network failures.
    /// </summary>
    /// <remarks>
    /// Judged from <see cref="Code"/> alone, so an unnamed code reads as not transient. Deterministic
    /// failures are excluded even when they look temporary:
    /// <see cref="ClickHouseErrorCode.MemoryLimitExceeded"/> repeats for the same query at the same size,
    /// and <see cref="ClickHouseErrorCode.TooSlow"/> repeats for the same query plan.
    /// </remarks>
    public override bool IsTransient => Code switch
    {
        ClickHouseErrorCode.TimeoutExceeded => true,
        ClickHouseErrorCode.TooManySimultaneousQueries => true,
        ClickHouseErrorCode.NoFreeConnection => true,
        ClickHouseErrorCode.SocketTimeout => true,
        ClickHouseErrorCode.NetworkError => true,
        ClickHouseErrorCode.TooManyParts => true,
        ClickHouseErrorCode.AllConnectionTriesFailed => true,
        ClickHouseErrorCode.ServerOverloaded => true,
        ClickHouseErrorCode.KeeperException => true,
        _ => false,
    };

    /// <summary>
    /// Decodes an Exception packet body (the bytes after the packet type code): <c>Int32 code</c>,
    /// <c>String name</c>, <c>String message</c>, <c>String stack_trace</c>, <c>Bool has_nested</c>. When
    /// <c>has_nested</c> is set, the nested exception follows and becomes the inner exception.
    /// </summary>
    /// <param name="reader">The reader positioned at the start of the Exception body.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded exception, with any nested exceptions chained as inner exceptions.</returns>
    internal static async ValueTask<ClickHouseTcpServerException> ReadAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        var frames = new List<(int Code, string Name, string Message, string StackTrace)>();
        bool hasNested;
        do
        {
            int code = await reader.ReadInt32Async(cancellationToken).ConfigureAwait(false);
            string name = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
            string message = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
            string stackTrace = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
            hasNested = await reader.ReadBoolAsync(cancellationToken).ConfigureAwait(false);
            frames.Add((code, name, message, stackTrace));
            if (frames.Count > MaxNestedFrames)
            {
                throw new ClickHouseTcpProtocolException($"Server exception chain exceeds the supported maximum of {MaxNestedFrames} frames (corrupt stream).");
            }
        }
        while (hasNested);

        // Frames are read outermost-first; rebuild from the innermost so each wraps the next as its cause.
        ClickHouseTcpServerException current = null;
        for (int i = frames.Count - 1; i >= 0; i--)
        {
            (int code, string name, string message, string stackTrace) = frames[i];
            current = new ClickHouseTcpServerException(code, name, message, stackTrace, current);
        }

        return current;
    }
}
