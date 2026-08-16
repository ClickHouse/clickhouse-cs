using System;
using System.Linq;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Resolves which <see cref="TcpFeature"/> flags the server under test has, so a test for a type the server
/// predates is skipped rather than failed.
/// </summary>
/// <remarks>
/// <para>
/// The version comes from <c>CLICKHOUSE_VERSION</c>, which is the same variable the CI matrix sets to choose
/// the server image and <see cref="Integration.TcpServerFixture"/> reads to tag the container. That is the
/// only source available here: a <c>TestCaseSource</c> is enumerated while tests are being discovered, before
/// the fixture has started the container, so asking the server is not an option at the point the answer is
/// needed. In CI, where the gating has to be right, the variable is always set.
/// </para>
/// <para>
/// Anything the variable cannot be read as a version — unset, <c>latest</c>, <c>head</c>, a digest — resolves
/// to <see cref="TcpFeature.All"/>. Those all mean a recent server, so assuming full support keeps the tests
/// running and lets a genuine gap fail loudly instead of being skipped in silence.
/// </para>
/// </remarks>
public static class TcpServerFeatures
{
    /// <summary>The server version the gating is based on, or null when it could not be determined.</summary>
    public static Version Version { get; } = Parse(Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION"));

    /// <summary>The features the server under test supports.</summary>
    public static TcpFeature Supported { get; } = Resolve(Version);

    /// <summary>Reports whether the server under test has a feature.</summary>
    /// <param name="feature">The feature to test for.</param>
    /// <returns>True when the server supports it.</returns>
    public static bool Has(TcpFeature feature) => Supported.HasFlag(feature);

    /// <summary>Reads a version out of the environment variable's value.</summary>
    /// <param name="versionString">The value of <c>CLICKHOUSE_VERSION</c>, which may be null or a moving tag.</param>
    /// <returns>The version, or null when the value does not name one.</returns>
    internal static Version Parse(string versionString)
        => Version.TryParse(versionString?.Split(':').Last().Trim(), out Version parsed) ? parsed : null;

    /// <summary>Maps a server version to the features a server of that version has.</summary>
    /// <param name="version">The server version, or null when it is not known.</param>
    /// <returns>The supported features.</returns>
    internal static TcpFeature Resolve(Version version)
    {
        // A moving tag names a recent server but not which one, so it cannot gate anything.
        if (version is null)
        {
            return TcpFeature.All;
        }

        TcpFeature supported = TcpFeature.None;
        foreach (FieldInfo field in typeof(TcpFeature).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            SinceVersionAttribute since = field.GetCustomAttribute<SinceVersionAttribute>();
            if (since is not null && version >= since.Version)
            {
                supported |= (TcpFeature)field.GetRawConstantValue();
            }
        }

        return supported;
    }
}
