using System;
using System.IO;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Builds the stream a batch's query line and rows are written to.
/// </summary>
/// <remarks>
/// The destination is the HTTP request body, which has no known length and is therefore sent with
/// <c>Transfer-Encoding: chunked</c> - every write that reaches it becomes its own chunk. Row
/// serialization issues one write per field, so the payload always has to pass through a write buffer
/// first, or the chunk framing alone inflates the request body (~70% for a one-column <c>Int64</c>
/// insert). A compressor brings its own buffer; without one it is interposed here.
/// </remarks>
internal static class BatchWriteTarget
{
    // Matches the default the compressors buffer with, so both paths coalesce into equally large blocks.
    private const int BufferSize = 256 * 1024;

    /// <summary>
    /// Wraps <paramref name="destination"/> in the compressor's stream, or in a plain write buffer when
    /// there is no compressor. Either way <paramref name="destination"/> is left open, and disposing
    /// the returned stream flushes everything still pending into it.
    /// </summary>
    public static Stream Create(Stream destination, IClickHouseCompressor compressor) =>
        compressor != null
            ? compressor.Compress(destination, leaveOpen: true)
            : new PooledWriteBufferStream(destination, BufferSize, leaveOpen: true);

    /// <summary>
    /// Disposes <paramref name="writer"/>, discarding whatever it throws. For use while another
    /// exception is already propagating: disposing flushes the bytes still sitting in the buffer, so it
    /// performs a real write to the transport, and that write can fail in its own right - typically for
    /// the very reason serialization failed. Letting it through would replace the error the caller needs
    /// to see (and, for a serialization fault, lose the failing row attached to it).
    /// </summary>
    public static void DisposeSuppressingErrors(IDisposable writer)
    {
        try
        {
            writer.Dispose();
        }
        catch
        {
            // Deliberately swallowed: the exception already on its way out is the one worth reporting.
        }
    }
}
