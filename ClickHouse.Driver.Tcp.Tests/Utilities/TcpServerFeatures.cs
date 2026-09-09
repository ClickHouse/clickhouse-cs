using System;
using System.Linq;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Resolves the <see cref="TcpFeature"/> flags supported by the server under test.
/// </summary>
/// <remarks>
/// Reads <c>CLICKHOUSE_VERSION</c> because case sources run before the server starts. Unparseable values such as
/// <c>latest</c> resolve to <see cref="TcpFeature.All"/> so unsupported features fail instead of being skipped.
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
