using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// An error returned by the server as an Exception packet. Carries the server-side error code, exception
/// class name, and stack trace alongside the message. Nested server exceptions are chained through
/// <see cref="Exception.InnerException"/>.
/// </summary>
internal sealed class ClickHouseServerException : Exception
{
    // A corrupt or hostile server could stream an endless nested chain; cap it so a bad response can't drive
    // unbounded allocation. Far more frames than any legitimate server produces (nesting is usually 0 or 1).
    private const int MaxNestedFrames = 256;

    /// <summary>Initializes a new instance of the <see cref="ClickHouseServerException"/> class.</summary>
    /// <param name="code">The server-side error code.</param>
    /// <param name="name">The server exception class name (e.g. <c>"DB::Exception"</c>).</param>
    /// <param name="message">The human-readable error message.</param>
    /// <param name="serverStackTrace">The server-side stack trace.</param>
    /// <param name="innerException">The nested server exception, or null for the innermost frame.</param>
    public ClickHouseServerException(int code, string name, string message, string serverStackTrace, Exception innerException = null)
        : base(message, innerException)
    {
        Code = code;
        Name = name;
        ServerStackTrace = serverStackTrace;
    }

    /// <summary>The server-side error code.</summary>
    public int Code { get; }

    /// <summary>The server exception class name (e.g. <c>"DB::Exception"</c>).</summary>
    public string Name { get; }

    /// <summary>The server-side stack trace.</summary>
    public string ServerStackTrace { get; }

    /// <summary>
    /// Decodes an Exception packet body (the bytes after the packet type code): <c>Int32 code</c>,
    /// <c>String name</c>, <c>String message</c>, <c>String stack_trace</c>, <c>Bool has_nested</c>. When
    /// <c>has_nested</c> is set, the nested exception follows and becomes the inner exception.
    /// </summary>
    /// <param name="reader">The reader positioned at the start of the Exception body.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded exception, with any nested exceptions chained as inner exceptions.</returns>
    public static async ValueTask<ClickHouseServerException> ReadAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
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
                throw new InvalidDataException($"Server exception chain exceeds the supported maximum of {MaxNestedFrames} frames (corrupt stream).");
            }
        }
        while (hasNested);

        // Frames are read outermost-first; rebuild from the innermost so each wraps the next as its cause.
        ClickHouseServerException current = null;
        for (int i = frames.Count - 1; i >= 0; i--)
        {
            (int code, string name, string message, string stackTrace) = frames[i];
            current = new ClickHouseServerException(code, name, message, stackTrace, current);
        }

        return current;
    }
}
