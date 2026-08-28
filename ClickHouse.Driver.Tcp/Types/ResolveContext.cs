namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The per-resolution context a codec factory may consult when building a codec, carrying information that is
/// not part of the type string itself. Today that is the session/server timezone, which a timezone-bearing
/// type (<c>DateTime</c>, <c>DateTime64</c>) falls back to when its type string carries no explicit timezone.
/// </summary>
internal readonly struct ResolveContext
{
    /// <summary>
    /// The server/session timezone (an IANA id such as <c>UTC</c> or <c>Europe/London</c>), used to resolve the
    /// offset of a timezone-bearing column whose type string omits an explicit timezone. Empty when unknown.
    /// </summary>
    public string ServerTimezone { get; init; }

    /// <summary>
    /// A context with no known server/session timezone. Suitable for self-described writes where no server sample
    /// block exists; timezone-less columns then fall back to UTC. INSERTs with a sample block use that block's
    /// context so an <c>Unspecified</c> wall clock is interpreted in the session timezone.
    /// </summary>
    public static ResolveContext ForWrite => default;
}
