using System;

namespace ClickHouse.Driver.Copy;

public class ClickHouseBulkCopySerializationException : Exception
{
    [Obsolete("This constructor does not populate RowIndex (it stays at 0 even for a later row). Use the overload that takes the row index so the failing row's position is reported; will be removed in a future version.")]
    public ClickHouseBulkCopySerializationException(object[] row, Exception innerException)
        : base("Error when serializing data", innerException)
    {
        Row = row;
    }

    public ClickHouseBulkCopySerializationException(int rowIndex, object[] row, Exception innerException)
        : base("Error when serializing data", innerException)
    {
        RowIndex = rowIndex;
        Row = row;
    }

    /// <summary>
    /// Gets row index at which exception happened
    /// </summary>
    public int RowIndex { get; }

    /// <summary>
    /// Gets row at which exception happened
    /// </summary>
    public object[] Row { get; }
}
