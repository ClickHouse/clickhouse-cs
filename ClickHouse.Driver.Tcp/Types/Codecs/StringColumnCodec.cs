using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>String</c> column: each row is a VarUInt byte-length prefix followed by that
/// many bytes, decoded as UTF-8. Values are read row-by-row (each carries its own length).
/// </summary>
internal sealed class StringColumnCodec : IColumnCodec, ISpanWritableCodec<string>
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly StringColumnCodec Instance = new();

    private StringColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "String";

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        var values = new string[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
        }

        return new ArrayColumn<string>(columnName, columnType, values);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<string>;

    /// <inheritdoc/>
    // Read per element through the indexer so a scattered write-path view (a substitute for a nullable string, a
    // Tuple field) writes with no materialized copy; a contiguous column reads each row just the same.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<string>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteString(typed[start + i]);
        }
    }

    /// <inheritdoc/>
    // Each element is its own length-prefixed byte run, so a run of values is just written in order.
    public void WriteValues(ClickHouseBinaryWriter writer, ReadOnlySpan<string> values)
    {
        foreach (string value in values)
        {
            writer.WriteString(value);
        }
    }
}
