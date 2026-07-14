using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Serializes POCO batches directly without intermediate object[] allocation.
/// Mirrors <see cref="BatchSerializer"/> but reads property values from POCOs.
/// </summary>
/// <remarks>
/// The <see cref="RowBinaryFormat.RowBinary"/> path uses pre-compiled per-column write delegates that
/// fuse the property read with the writer call, avoiding the box-per-value the boxed getters incur
/// (issue #505). The <see cref="RowBinaryFormat.RowBinaryWithDefaults"/> path keeps the boxed row
/// serializer because it must inspect a boxed <c>DBDefault</c> sentinel before writing.
/// </remarks>
internal class PocoBatchSerializer
{
    public static PocoBatchSerializer GetByRowBinaryFormat(RowBinaryFormat format)
    {
        return format switch
        {
            RowBinaryFormat.RowBinary => new PocoBatchSerializer(rowSerializer: null),
            RowBinaryFormat.RowBinaryWithDefaults => new PocoBatchSerializer(new PocoRowBinaryWithDefaultsSerializer()),
            _ => throw new NotSupportedException(format.ToString()),
        };
    }

    // Null for the RowBinary fast path (which uses the compiled write delegates); non-null for the
    // RowBinaryWithDefaults boxed path.
    private readonly IPocoRowSerializer rowSerializer;

    private PocoBatchSerializer(IPocoRowSerializer rowSerializer)
    {
        this.rowSerializer = rowSerializer;
    }

    /// <summary>
    /// Serializes a batch of POCO rows into the target stream using GZip compression.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="batch">The batch of rows, query text, and resolved column types.</param>
    /// <param name="getters">Compiled boxed property accessors, ordered to match the batch's column types.
    /// Used by the boxed (RowBinaryWithDefaults) path and to materialize the failing row for diagnostics.</param>
    /// <param name="writers">Compiled per-column write delegates for the RowBinary fast path, or null for
    /// the boxed path.</param>
    /// <param name="stream">The output stream (typically a recyclable memory stream).</param>
    public void Serialize<T>(PocoBatch<T> batch, Func<T, object>[] getters, Action<T, ExtendedBinaryWriter>[] writers, Stream stream)
    {
        using var gzipStream = new BufferedStream(new GZipStream(stream, CompressionLevel.Fastest, true), 256 * 1024);
        using (var textWriter = new StreamWriter(gzipStream, Encoding.UTF8, 4 * 1024, true))
        {
            textWriter.WriteLine(batch.Query);
        }

        using var writer = new ExtendedBinaryWriter(gzipStream);

        var types = batch.Types;

        T current = default;
        try
        {
            if (writers != null)
            {
                for (int i = 0; i < batch.Size; i++)
                {
                    current = batch.Rows[i];
                    for (int col = 0; col < writers.Length; col++)
                        writers[col](current, writer);
                }
            }
            else
            {
                for (int i = 0; i < batch.Size; i++)
                {
                    current = batch.Rows[i];
                    rowSerializer.Serialize(current, getters, types, writer);
                }
            }
        }
        catch (Exception e)
        {
            // Best-effort: materialize the failing row for diagnostics.
            // Getters may throw again, so swallow secondary failures to preserve
            // the original exception in the wrapper.
            var failedRow = new object[getters.Length];
            if (current != null)
            {
                for (int col = 0; col < getters.Length; col++)
                {
                    try
                    {
                        failedRow[col] = getters[col](current);
                    }
                    catch
                    {
                        // Ignore, we don't want to throw again inside the catch. Keep the info we got.
                    }
                }
            }

            throw new ClickHouseBulkCopySerializationException(failedRow, e);
        }
    }
}
