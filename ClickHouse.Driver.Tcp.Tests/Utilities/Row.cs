namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A one-property POCO used to drive reads from <see cref="InsertRoundTripCase"/>.
/// </summary>
/// <typeparam name="TValue">The property type, matched against the column named <c>value</c> case-insensitively.</typeparam>
public sealed class Row<TValue>
{
    /// <summary>The column's value for this row.</summary>
    public TValue Value { get; set; }
}
