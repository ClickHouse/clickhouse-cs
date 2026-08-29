using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The calendar read surface of a decoded <c>Time</c> or <c>Time64(scale)</c> column. Both name a time of day
/// rather than an instant, and both are stored as a signed integer count from midnight: seconds for
/// <c>Time</c>, and units of 10^-<see cref="Scale"/> seconds for <c>Time64</c>.
///
/// <para>
/// That count is what the default <see cref="IColumn{T}"/> view exposes — <c>IColumn&lt;int&gt;</c> for
/// <c>Time</c>, <c>IColumn&lt;long&gt;</c> for <c>Time64</c> — because it is the layout the wire carried. This
/// interface converts it, so a caller does not have to know which unit a given column counts in.
/// </para>
///
/// <para>
/// Obtain it by pattern-matching a column, e.g. <c>if (column is ITimeColumn times)</c>.
/// </para>
/// </summary>
/// <remarks>
/// The count is signed and ClickHouse does not clamp it to one day, so a value outside
/// <c>00:00:00</c>–<c>23:59:59</c> is representable and surfaces as a <see cref="TimeSpan"/> that is negative or
/// longer than a day. A <c>DateTime</c> or <c>DateTime64</c> column names an instant instead, and implements
/// <see cref="IDateTimeColumn"/>.
/// </remarks>
public interface ITimeColumn : IColumn
{
    /// <summary>
    /// The number of decimal digits of sub-second precision the stored count carries: the <c>scale</c> of
    /// <c>Time64(scale)</c>, and <c>0</c> for <c>Time</c>, which counts whole seconds.
    /// </summary>
    int Scale { get; }

    /// <summary>Reads one row as an offset from midnight.</summary>
    /// <remarks>
    /// <see cref="TimeSpan"/> counts 100 ns ticks, so this is lossy above <see cref="Scale"/> 7: a
    /// <c>Time64(8)</c> or <c>Time64(9)</c> column has its sub-100 ns digits truncated toward zero. The exact
    /// count stays in the <see cref="IColumn{T}"/> view.
    /// </remarks>
    /// <param name="row">The row index, from 0 to <see cref="IColumn.RowCount"/> - 1.</param>
    /// <returns>The offset from midnight the row's stored count names, to 100 ns.</returns>
    TimeSpan GetTimeSpan(int row);

    /// <summary>
    /// Reads the whole column as offsets from midnight.
    /// </summary>
    /// <remarks>Lossy above <see cref="Scale"/> 7, for the reason given on
    /// <see cref="GetTimeSpan"/>.</remarks>
    /// <returns>A new array of <see cref="IColumn.RowCount"/> offsets. Unlike the borrowed spans elsewhere on
    /// the block tier, this array is the caller's and outlives the block.</returns>
    TimeSpan[] ToTimeSpans();
}
