using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The calendar read surface of a decoded <c>DateTime</c> or <c>DateTime64(scale)</c> column. Both name an
/// instant, and both are stored as a plain integer count: seconds since the Unix epoch for <c>DateTime</c>, and
/// units of 10^-<see cref="Scale"/> seconds for <c>DateTime64</c>.
///
/// <para>
/// That count is what the default <see cref="IColumn{T}"/> view exposes — <c>IColumn&lt;uint&gt;</c> for
/// <c>DateTime</c>, <c>IColumn&lt;long&gt;</c> for <c>DateTime64</c> — because it is the layout the wire
/// carried, and reading it costs no conversion. Turning a count into an instant needs the column's
/// <see cref="TimeZone"/>, which the column header declares and which nothing on <see cref="IColumn"/> reports,
/// so this interface exists to make that conversion possible without leaving the block tier.
/// </para>
///
/// <para>
/// Obtain it by pattern-matching a column, e.g. <c>if (column is IDateTimeColumn instants)</c>. It is not
/// generic: <c>DateTime</c> and <c>DateTime64</c> store counts of different widths, and a caller asking for an
/// instant does not care which.
/// </para>
/// </summary>
/// <remarks>
/// A <c>Time</c> or <c>Time64</c> column names a time of day rather than an instant, has no timezone, and
/// therefore implements <see cref="ITimeColumn"/> instead.
/// </remarks>
public interface IDateTimeColumn : IColumn
{
    /// <summary>
    /// The timezone the column's counts are read in. The column type's own argument wins — the
    /// <c>Europe/Amsterdam</c> of <c>DateTime('Europe/Amsterdam')</c>. A type that names none resolves to the
    /// query's <c>session_timezone</c> setting if it has one, and to the timezone the handshake reported
    /// otherwise.
    /// </summary>
    TimeZoneInfo TimeZone { get; }

    /// <summary>
    /// The number of decimal digits of sub-second precision the stored count carries: the <c>scale</c> of
    /// <c>DateTime64(scale)</c>, and <c>0</c> for <c>DateTime</c>, which counts whole seconds.
    /// </summary>
    int Scale { get; }

    /// <summary>Reads one row as an instant, offset to <see cref="TimeZone"/>.</summary>
    /// <remarks>
    /// <see cref="DateTimeOffset"/> counts 100 ns ticks, so this is lossy above <see cref="Scale"/> 7: a
    /// <c>DateTime64(8)</c> or <c>DateTime64(9)</c> column has its sub-100 ns digits truncated toward zero. The
    /// exact count stays in the <see cref="IColumn{T}"/> view.
    /// </remarks>
    /// <param name="row">The row index, from 0 to <see cref="IColumn.RowCount"/> - 1.</param>
    /// <returns>The instant the row's stored count names, to 100 ns.</returns>
    DateTimeOffset GetDateTimeOffset(int row);

    /// <summary>
    /// Reads the whole column as instants, offset to <see cref="TimeZone"/>.
    /// </summary>
    /// <remarks>Lossy above <see cref="Scale"/> 7, for the reason given on
    /// <see cref="GetDateTimeOffset"/>.</remarks>
    /// <returns>A new array of <see cref="IColumn.RowCount"/> instants. Unlike the borrowed spans elsewhere on
    /// the block tier, this array is the caller's and outlives the block.</returns>
    DateTimeOffset[] ToDateTimeOffsets();
}
