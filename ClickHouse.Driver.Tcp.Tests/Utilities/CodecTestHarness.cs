using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Shared write/read plumbing for the per-codec unit tests: encode to a buffer, then read it back. A write goes the way
/// that an insert writes a column (<see cref="InsertColumnWrite.For"/>): the codec writes a column from its storage
/// (<see cref="IColumnCodec.CanWrite"/>), and the converter tree of the column's CLR type writes any other column.
/// <see cref="WriteStoredAsync"/> writes through the codec only.
/// </summary>
internal static class CodecTestHarness
{
    public static readonly CancellationToken None = CancellationToken.None;

    /// <summary>The context of <see cref="WriteFull(IColumnCodec, ClickHouseBinaryWriter, IColumn)"/>: the session timezone is UTC.</summary>
    private static readonly ResolveContext UtcContext = new() { ServerTimezone = "UTC" };

    /// <summary>Encodes <paramref name="write"/> into a flushed byte buffer.</summary>
    public static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    /// <summary>A reader over the given bytes.</summary>
    public static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));

    /// <summary>
    /// The write of <paramref name="column"/> as <paramref name="typeName"/> that an insert plans. It throws when the
    /// insert refuses the column.
    /// </summary>
    public static InsertColumnWrite InsertWrite(IColumnCodec codec, IColumn column, string typeName, in ResolveContext context)
        => InsertColumnWrite.For(codec, column, typeName, in context, ColumnCodecRegistry.Default.Converters)
            ?? throw new InvalidOperationException($"An insert refuses a column of {column.GetType()} as {typeName}.");

    /// <summary>
    /// Writes rows [<paramref name="start"/>, start + <paramref name="length"/>) of <paramref name="column"/> as an insert
    /// writes them: the state prefix when <paramref name="prefix"/> is true, then the body.
    /// </summary>
    public static void WriteRows(ClickHouseBinaryWriter writer, IColumnCodec codec, IColumn column, string typeName, int start, int length, in ResolveContext context, bool prefix)
    {
        InsertColumnWrite write = InsertWrite(codec, column, typeName, in context);
        IColumnWriteState state = write.Begin(column, start, length);
        try
        {
            if (prefix)
            {
                write.WritePrefix(writer, column, start, length, state);
            }

            write.Write(writer, column, start, length, state);
        }
        finally
        {
            state?.Dispose();
        }
    }

    /// <summary>
    /// Writes every row of <paramref name="column"/> as an insert writes a column of the codec's type, with the session
    /// timezone UTC: the state prefix, then the body. A zero-row column writes nothing, as the block writer does.
    /// </summary>
    public static void WriteFull(this IColumnCodec codec, ClickHouseBinaryWriter writer, IColumn column)
        => codec.WriteFull(writer, column, UtcContext);

    /// <summary>Writes every row of <paramref name="column"/> as the other overload does, in <paramref name="context"/>.</summary>
    public static void WriteFull(this IColumnCodec codec, ClickHouseBinaryWriter writer, IColumn column, in ResolveContext context)
    {
        if (column.RowCount > 0)
        {
            WriteRows(writer, codec, column, codec.TypeName, 0, column.RowCount, in context, prefix: true);
        }
    }

    /// <summary>
    /// Writes <paramref name="column"/> as an insert writes it into a column of <paramref name="columnType"/>, then reads
    /// it back through <paramref name="codec"/> (the state prefix too, when the column has rows).
    /// </summary>
    public static Task<IColumn> RoundTripAsync(IColumnCodec codec, IColumn column, string columnType, int rowCount)
        => RoundTripAsync(codec, column, columnType, rowCount, ResolveContext.ForWrite);

    /// <summary>
    /// Writes and reads back as the other overload does, in <paramref name="context"/> (the context that resolved
    /// <paramref name="codec"/>).
    /// </summary>
    public static async Task<IColumn> RoundTripAsync(IColumnCodec codec, IColumn column, string columnType, int rowCount, ResolveContext context)
    {
        byte[] bytes = await WriteAsync(w =>
        {
            if (column.RowCount > 0)
            {
                WriteRows(w, codec, column, columnType, 0, column.RowCount, in context, prefix: true);
            }
        });
        using ClickHouseBinaryReader reader = ReaderOver(bytes);
        if (rowCount > 0)
        {
            await codec.ReadStatePrefixAsync(reader, None);
        }

        return await codec.ReadColumnAsync(reader, column.Name, columnType, rowCount, None);
    }

    /// <summary>
    /// The body of rows [<paramref name="start"/>, start + <paramref name="length"/>) of <paramref name="column"/> as an
    /// insert writes it into a column of the codec's type, without the state prefix.
    /// </summary>
    public static Task<byte[]> WriteSliceAsync(IColumnCodec codec, IColumn column, int start, int length)
        => WriteSliceAsync(codec, column, start, length, ResolveContext.ForWrite);

    /// <summary>The body of a slice as the other overload gives it, in <paramref name="context"/>.</summary>
    public static Task<byte[]> WriteSliceAsync(IColumnCodec codec, IColumn column, int start, int length, ResolveContext context)
        => WriteAsync(w => WriteRows(w, codec, column, codec.TypeName, start, length, in context, prefix: false));

    /// <summary>
    /// Writes rows [<paramref name="start"/>, start + <paramref name="length"/>) of <paramref name="column"/> through the
    /// codec from its storage, as an insert writes a decoded column: the state prefix when <paramref name="prefix"/> is
    /// true, then the body. It fails the test when the codec does not write the column from its storage.
    /// </summary>
    public static Task<byte[]> WriteStoredAsync(IColumnCodec codec, IColumn column, int start, int length, bool prefix = false)
    {
        Assert.That(codec.CanWrite(column), Is.True, $"the {codec.TypeName} codec writes the {column.GetType().Name} from its storage");
        return WriteAsync(w =>
        {
            IColumnWriteState state = codec.BeginWrite(column, start, length);
            try
            {
                if (prefix)
                {
                    codec.WriteStatePrefix(w, column, start, length, state);
                }

                codec.WriteColumn(w, column, start, length, state);
            }
            finally
            {
                state?.Dispose();
            }
        });
    }
}
