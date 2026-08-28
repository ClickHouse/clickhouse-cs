using System;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Server capabilities the integration tests gate on, each tagged with the release that introduced it.
/// </summary>
/// <remarks>
/// This mirrors <c>ClickHouse.Driver.ADO.Feature</c> in the HTTP suite, and is a separate copy on purpose: the
/// TCP projects do not reference <c>ClickHouse.Driver</c>, which is what lets the TCP CI job build and test
/// only the two TCP projects. Keep the versions here in step with that enum when a shared one changes.
/// Only add a flag when a test needs it; an unused flag is a version claim nothing checks.
/// </remarks>
[Flags]
public enum TcpFeature
{
    /// <summary>No capability. The value a server older than every entry below resolves to.</summary>
    None = 0,

    /// <summary>The <c>Variant</c> type.</summary>
    [SinceVersion("24.1")]
    Variant = 1 << 0,

    /// <summary>The rewritten <c>JSON</c> type.</summary>
    [SinceVersion("24.1")]
    Json = 1 << 1,

    /// <summary>The <c>Dynamic</c> type.</summary>
    [SinceVersion("25.1")]
    Dynamic = 1 << 2,

    /// <summary>The <c>Time</c> and <c>Time64</c> types.</summary>
    [SinceVersion("25.6")]
    Time = 1 << 3,

    /// <summary>The <c>QBit</c> type.</summary>
    [SinceVersion("25.11")] // Technically 25.10, but limitations break testing. Matches the HTTP suite.
    QBit = 1 << 4,

    /// <summary>The <c>Geometry</c> type, as distinct from the older named geo types.</summary>
    [SinceVersion("25.11")]
    Geometry = 1 << 5,

    /// <summary><c>Nullable(Tuple(...))</c> with its Beta setting enabled.</summary>
    [SinceVersion("26.6")]
    NullableTuple = 1 << 6,

    /// <summary>Every capability. What an unrecognised or unpinned server version resolves to.</summary>
    All = ~None,
}

/// <summary>Marks the release a <see cref="TcpFeature"/> became available in.</summary>
/// <param name="version">The release, as a version string such as <c>25.11</c>.</param>
[AttributeUsage(AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
public sealed class SinceVersionAttribute(string version) : Attribute
{
    /// <summary>The release the feature became available in.</summary>
    public Version Version { get; } = Version.Parse(version);
}
