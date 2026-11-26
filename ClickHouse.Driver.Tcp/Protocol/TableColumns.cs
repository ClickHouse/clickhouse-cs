using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// A decoded TableColumns packet: an optional column-defaults description the server may send before an INSERT
/// schema block. The client does not use defaults yet, so the body is decoded (to stay stream-synced) and kept
/// opaque.
/// </summary>
internal readonly struct TableColumns
{
    /// <summary>Initializes a new instance of the <see cref="TableColumns"/> struct.</summary>
    /// <param name="externalTableName">The external table name.</param>
    /// <param name="columnsDescription">The free-form columns description string.</param>
    public TableColumns(string externalTableName, string columnsDescription)
    {
        ExternalTableName = externalTableName;
        ColumnsDescription = columnsDescription;
    }

    /// <summary>The external table name.</summary>
    public string ExternalTableName { get; }

    /// <summary>The free-form columns-description string.</summary>
    public string ColumnsDescription { get; }

    /// <summary>Reads a TableColumns packet body.</summary>
    /// <param name="reader">The reader positioned at the packet body.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded table columns.</returns>
    public static async ValueTask<TableColumns> ReadAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        string externalTableName = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
        string columnsDescription = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
        return new TableColumns(externalTableName, columnsDescription);
    }
}
