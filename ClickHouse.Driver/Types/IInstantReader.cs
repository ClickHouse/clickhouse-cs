using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Implemented by the date/time types that decode an absolute instant, so a reader can obtain the instant a
/// value was stored as instead of re-deriving it from the decoded wall-clock value. A wall clock inside a DST
/// fall-back hour occurs at two offsets, so that derivation cannot preserve the instant.
/// The date-only types (<c>Date</c>, <c>Date32</c>) encode a day number rather than an instant, so they do
/// not implement this.
/// </summary>
internal interface IInstantReader
{
    /// <summary>
    /// Reads one value, consuming exactly the bytes <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/>
    /// would, and returns the instant it decoded.
    /// </summary>
    Instant ReadInstant(ExtendedBinaryReader reader);
}
