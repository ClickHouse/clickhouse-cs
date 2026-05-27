using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Copy.Serializer;

internal class BatchSerializer : IBatchSerializer
{
    public static BatchSerializer GetByRowBinaryFormat(RowBinaryFormat format)
    {
        return format switch
        {
            RowBinaryFormat.RowBinary => new BatchSerializer(new RowBinarySerializer()),
            RowBinaryFormat.RowBinaryWithDefaults => new BatchSerializer(new RowBinaryWithDefaultsSerializer()),
            _ => throw new NotSupportedException(format.ToString())
        };
    }

    private readonly IRowSerializer rowSerializer;

    public BatchSerializer(IRowSerializer rowSerializer)
    {
        this.rowSerializer = rowSerializer;
    }

    public void Serialize(Batch batch, Stream stream)
    {
        using var gzipStream = new BufferedStream(new GZipStream(stream, CompressionLevel.Fastest, true), 256 * 1024);
        using (var textWriter = new StreamWriter(gzipStream, Encoding.UTF8, 4 * 1024, true))
        {
            textWriter.WriteLine(batch.Query);
        }

        using var writer = new ExtendedBinaryWriter(gzipStream);

        object[] row = null;
        try
        {
            var rows = batch.Rows.AsSpan()[..batch.Size];
            var types = batch.Types;
            for (int i = 0; i < rows.Length; i++)
            {
                row = rows[i];
                rowSerializer.Serialize(row, types, writer);
            }
        }
        catch (Exception e)
        {
            throw new ClickHouseBulkCopySerializationException(row, e);
        }
    }
}
