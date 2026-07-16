using System.Buffers;
using System.IO;
using System.Text;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Writes the single query line that precedes the binary rows of a batch payload
/// (e.g. <c>INSERT INTO ... FORMAT RowBinary</c>).
/// </summary>
internal static class QueryLineWriter
{
    /// <summary>
    /// Writes <paramref name="query"/> as UTF-8 followed by a single <c>'\n'</c> to
    /// <paramref name="target"/>, using a pooled scratch buffer so no per-batch allocation is
    /// incurred. Unlike a <see cref="StreamWriter"/>, this emits no BOM and a deterministic
    /// (cross-platform) newline.
    /// </summary>
    public static void Write(Stream target, string query)
    {
        int max = Encoding.UTF8.GetMaxByteCount(query.Length) + 1; // +1 for the trailing '\n'
        byte[] buffer = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            int n = Encoding.UTF8.GetBytes(query, 0, query.Length, buffer, 0);
            buffer[n++] = (byte)'\n';
            target.Write(buffer, 0, n);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
