using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Implemented by types that decode an absolute instant, so a reader can obtain the instant a value was
/// stored as instead of re-deriving it from the decoded wall-clock value. A wall clock inside a DST
/// fall-back hour occurs at two offsets, so that derivation cannot preserve the instant.
/// </summary>
internal interface IInstantReader
{
    /// <summary>
    /// Whether <see cref="ReadWithInstant"/> can report an instant. Transparent wrappers report what their
    /// underlying type does, so <c>Nullable(Date)</c> is false while <c>Nullable(DateTime)</c> is true.
    /// </summary>
    bool ReportsInstant { get; }

    /// <summary>
    /// Reads a value exactly like <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/>, additionally
    /// reporting in <paramref name="instant"/> the instant it decoded, or <see langword="null"/> when the
    /// value carries none (a NULL, for example).
    /// </summary>
    object ReadWithInstant(ExtendedBinaryReader reader, out Instant? instant);
}
