namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A one-property POCO over a one-column result, so the POCO read path can be driven from the per-type corpus in
/// <see cref="InsertRoundTripCase"/> instead of a corpus of its own: each case already knows its ClickHouse type and
/// the CLR type it reads back as, which is exactly <c>Row&lt;that CLR type&gt;</c>.
/// </summary>
/// <typeparam name="TValue">The property type, matched against the column named <c>value</c> case-insensitively.</typeparam>
public sealed class Row<TValue>
{
    /// <summary>The column's value for this row.</summary>
    public TValue Value { get; set; }
}
