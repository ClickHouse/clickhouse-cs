using System;
using System.IO;
using System.IO.Compression;
using System.Text;
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

    public void Serialize<T>(PocoBatch<T> batch, Stream stream)
    {
        using var gzipStream = new BufferedStream(new GZipStream(stream, CompressionLevel.Fastest, true), 256 * 1024);
        using (var textWriter = new StreamWriter(gzipStream, Encoding.UTF8, 4 * 1024, true))
        {
            textWriter.WriteLine(batch.Query);
        }

        using var writer = new ExtendedBinaryWriter(gzipStream);

        var getters = batch.Getters;
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
            // Materialize the failing row for diagnostics (rare error path)
            var failedRow = new object[getters.Length];
            if (current != null)
            {
                for (int col = 0; col < getters.Length; col++)
                    failedRow[col] = getters[col](current);
            }

            throw new ClickHouseBulkCopySerializationException(failedRow, e);
        }
    }
}
