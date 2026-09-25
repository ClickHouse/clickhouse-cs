using System;
using System.IO;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Serializes POCO batches directly without intermediate object[] allocation.
/// Mirrors <see cref="BatchSerializer"/> but reads property values from POCOs.
/// </summary>
/// <remarks>
/// The <see cref="RowBinaryFormat.RowBinary"/> path uses pre-compiled per-column write delegates that
/// fuse the property read with the writer call, avoiding the box-per-value the boxed getters incur.
/// The <see cref="RowBinaryFormat.RowBinaryWithDefaults"/> path keeps the boxed row
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
    /// Serializes a batch of POCO rows into the target stream, optionally compressed via the supplied compressor.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="batch">The batch of rows, query text, and resolved column types.</param>
    /// <param name="getters">Compiled boxed property accessors, ordered to match the batch's column types.
    /// Used by the boxed (RowBinaryWithDefaults) path and to materialize the failing row for diagnostics.</param>
    /// <param name="writers">Compiled per-column write delegates for the RowBinary fast path, or null for
    /// the boxed path.</param>
    /// <param name="stream">The output stream (typically a recyclable memory stream).</param>
    /// <param name="compressor">Compressor for the payload, or <c>null</c> to write uncompressed.</param>
    /// <param name="queryPlacement">Whether the <c>INSERT</c> statement precedes the rows in the body
    /// (<see cref="InsertQueryPlacement.Body"/>) or is sent by the caller in the URL
    /// (<see cref="InsertQueryPlacement.Url"/>), leaving the body to the rows alone.</param>
    public void Serialize<T>(PocoBatch<T> batch, Func<T, object>[] getters, Action<T, ExtendedBinaryWriter>[] writers, Stream stream, IClickHouseCompressor compressor, InsertQueryPlacement queryPlacement)
    {
        // See BatchSerializer.Serialize for the leaveOpen/flush rationale.
        var target = BatchWriteTarget.Create(stream, compressor);
        var writer = new ExtendedBinaryWriter(target, leaveOpen: false);

        var types = batch.Types;

        T current = default;
        int currentRowIndex = 0;

        // See BatchSerializer.Serialize: in URL mode the body must start at the first row, so no
        // prologue is written and the row/prologue discriminator starts out set.
        var writeQueryLine = queryPlacement == InsertQueryPlacement.Body;
        var serializingRows = !writeQueryLine;
        try
        {
            if (writeQueryLine)
            {
                PooledStreamWriter.WriteLine(target, batch.Query);
                serializingRows = true;
            }

            if (writers != null)
            {
                // RowBinary path
                for (; currentRowIndex < batch.Size; currentRowIndex++)
                {
                    current = batch.Rows[currentRowIndex];
                    for (int col = 0; col < writers.Length; col++)
                        writers[col](current, writer);
                }
            }
            else
            {
                // RowBinaryWithDefaults path
                for (; currentRowIndex < batch.Size; currentRowIndex++)
                {
                    current = batch.Rows[currentRowIndex];
                    rowSerializer.Serialize(current, getters, types, writer);
                }
            }
        }
        catch (Exception e)
        {
            BatchWriteTarget.DisposeSuppressingErrors(writer);

            // A failure writing the query line is not a serialization fault, so it propagates as it is.
            if (!serializingRows)
                throw;

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

            throw new ClickHouseBulkCopySerializationException(currentRowIndex, failedRow, e);
        }

        writer.Dispose();
    }
}
