using System;
using System.Buffers;
using System.IO;
using System.Text;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Writes a single line of text to a stream using a pooled scratch buffer, avoiding the per-call
/// allocation a <see cref="StreamWriter"/> incurs. Used for the query line that precedes the binary
/// rows of a batch payload (e.g. <c>INSERT INTO ... FORMAT RowBinary</c>).
/// </summary>
internal static class PooledStreamWriter
{
    /// <summary>
    /// Writes <paramref name="text"/> as UTF-8 followed by a single <c>'\n'</c> to
    /// <paramref name="target"/>, using a pooled scratch buffer so no per-call allocation is
    /// incurred. Unlike a <see cref="StreamWriter"/>, this emits no BOM and a deterministic
    /// (cross-platform) newline. A <see langword="null"/> <paramref name="text"/> is treated as
    /// empty (only the newline is written), matching the prior <see cref="StreamWriter"/> behavior.
    /// </summary>
    public static void WriteLine(Stream target, string text)
    {
        if (target is null)
            throw new ArgumentNullException(nameof(target));

        text ??= string.Empty;

        int max = Encoding.UTF8.GetMaxByteCount(text.Length) + 1; // +1 for the trailing '\n'
        byte[] buffer = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            int n = Encoding.UTF8.GetBytes(text, 0, text.Length, buffer, 0);
            buffer[n++] = (byte)'\n';
            target.Write(buffer, 0, n);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
