using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>String</c> column: each row is a VarUInt byte-length prefix followed by that
/// many bytes, decoded as UTF-8. Values are read row-by-row (each carries its own length).
/// </summary>
internal sealed class StringColumnCodec : IColumnCodec
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
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column)
    {
        foreach (string value in ((IColumn<string>)column).Values)
        {
            writer.WriteString(value);
        }
    }
}
