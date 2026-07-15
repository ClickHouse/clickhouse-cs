using System;
using System.IO;
using System.Text;
using ClickHouse.Driver.Compression;
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

    public void Serialize(Batch batch, Stream stream, IClickHouseCompressor compressor)
    {
        // With a compressor, write through it (it leaves the base stream open); disposing the writer
        // flushes the compressed bytes into it. With no compressor, write straight to the base stream
        // and leave it open so the caller can seek/read it afterwards.
        var compressing = compressor != null;
        var target = compressing ? compressor.Compress(stream, leaveOpen: true) : stream;

        using (var textWriter = new StreamWriter(target, Encoding.UTF8, 4 * 1024, true))
        {
            textWriter.WriteLine(batch.Query);
        }

        using var writer = new ExtendedBinaryWriter(target, leaveOpen: !compressing);

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
