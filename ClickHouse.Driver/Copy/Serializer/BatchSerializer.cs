using System;
using System.IO;
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

    public void Serialize(Batch batch, Stream stream, IClickHouseCompressor compressor, InsertQueryPlacement queryPlacement)
    {
        // The batch is written through a buffering (and optionally compressing) stream that leaves the
        // base stream open, so disposing the writer flushes the pending bytes into it while the caller
        // can still seek/read it afterwards. See BatchWriteTarget for why the buffer is not optional.
        var target = BatchWriteTarget.Create(stream, compressor);
        var writer = new ExtendedBinaryWriter(target, leaveOpen: false);

        object[] row = null;

        // With the statement in the URL the body is rows alone: not even a newline may precede them,
        // as the server would read it as row data. Nothing can fail before the rows then, so the flag
        // that distinguishes a prologue failure from a row failure starts out set.
        var writeQueryLine = queryPlacement == InsertQueryPlacement.Body;
        var serializingRows = !writeQueryLine;
        try
        {
            if (writeQueryLine)
            {
                PooledStreamWriter.WriteLine(target, batch.Query);
                serializingRows = true;
            }

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
            BatchWriteTarget.DisposeSuppressingErrors(writer);

            // A failure writing the query line is not a serialization fault, so it propagates as it is.
            if (!serializingRows)
                throw;

            throw new ClickHouseBulkCopySerializationException(row, e);
        }

        writer.Dispose();
    }
}
