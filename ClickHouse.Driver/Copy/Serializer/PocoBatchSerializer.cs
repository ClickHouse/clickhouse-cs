using System;
using System.IO;
using System.Text;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Serializes POCO batches directly without intermediate object[] allocation.
/// Mirrors <see cref="BatchSerializer"/> but reads property values from POCOs via compiled getters.
/// </summary>
internal class PocoBatchSerializer
{
    public static PocoBatchSerializer GetByRowBinaryFormat(RowBinaryFormat format)
    {
        return format switch
        {
            RowBinaryFormat.RowBinary => new PocoBatchSerializer(new PocoRowBinarySerializer()),
            RowBinaryFormat.RowBinaryWithDefaults => new PocoBatchSerializer(new PocoRowBinaryWithDefaultsSerializer()),
            _ => throw new NotSupportedException(format.ToString()),
        };
    }

    private readonly IPocoRowSerializer rowSerializer;

    private PocoBatchSerializer(IPocoRowSerializer rowSerializer)
    {
        this.rowSerializer = rowSerializer;
    }

    /// <summary>
    /// Serializes a batch of POCO rows into the target stream, optionally compressed via the supplied compressor.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="batch">The batch of rows, query text, and resolved column types.</param>
    /// <param name="getters">Compiled property accessors, ordered to match the batch's column types.</param>
    /// <param name="stream">The output stream (typically a recyclable memory stream).</param>
    /// <param name="compressor">Compressor for the payload, or <c>null</c> to write uncompressed.</param>
    public void Serialize<T>(PocoBatch<T> batch, Func<T, object>[] getters, Stream stream, IClickHouseCompressor compressor)
    {
        // See BatchSerializer.Serialize for the leaveOpen/flush rationale.
        var compressing = compressor != null;
        var target = compressing ? compressor.Compress(stream, leaveOpen: true) : stream;

        using (var textWriter = new StreamWriter(target, Encoding.UTF8, 4 * 1024, true))
        {
            textWriter.WriteLine(batch.Query);
        }

        using var writer = new ExtendedBinaryWriter(target, leaveOpen: !compressing);

        var types = batch.Types;

        T current = default;
        try
        {
            for (int i = 0; i < batch.Size; i++)
            {
                current = batch.Rows[i];
                rowSerializer.Serialize(current, getters, types, writer);
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
