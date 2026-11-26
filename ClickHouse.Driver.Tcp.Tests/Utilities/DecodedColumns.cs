using System.IO;
using System.Threading;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Builds a column in the form that a query reads it: the converter tree of the type writes the values, and the codec
/// of the type reads the bytes. A test that builds a dense composite column (<c>Nested</c>, <c>Variant</c>) gives it such
/// children, because an insert writes a dense column from its storage only when the codec of each child writes the child
/// column from its storage.
/// </summary>
internal static class DecodedColumns
{
    private static readonly ResolveContext Context = new() { ServerTimezone = "UTC" };

    /// <summary>The column of <paramref name="values"/> as a query of <paramref name="type"/> reads it.</summary>
    /// <typeparam name="T">The CLR type that the type is written from.</typeparam>
    /// <param name="name">The column name.</param>
    /// <param name="type">The ClickHouse type.</param>
    /// <param name="values">The values.</param>
    /// <returns>The decoded column.</returns>
    public static IColumn Of<T>(string name, string type, params T[] values)
    {
        ColumnWriter<T> writer = ColumnCodecRegistry.Default.Converters.Writer<T>(type, Context);
        using var stream = new MemoryStream();
        using (var output = new ClickHouseBinaryWriter(stream))
        {
            Write(writer, output, values);
            output.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, Context);
        using var reader = new ClickHouseBinaryReader(new MemoryStream(stream.ToArray()));
        if (values.Length > 0)
        {
            codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        return codec.ReadColumnAsync(reader, name, type, values.Length, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    // A query reads the prefix of a column only when the block has rows.
    private static void Write<T>(ColumnWriter<T> writer, ClickHouseBinaryWriter output, T[] values)
    {
        ValueSource<T> source = ValueSource<T>.Of(values);
        IColumnWriteState state = writer.Begin(source);
        try
        {
            if (values.Length > 0)
            {
                writer.WritePrefix(output, source, state);
            }

            writer.Write(output, source, state);
        }
        finally
        {
            state?.Dispose();
        }
    }
}
