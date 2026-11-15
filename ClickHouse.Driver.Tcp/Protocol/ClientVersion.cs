using System;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// The client version the driver reports in its handshake and in the ClientInfo of every query: the version of this
/// package, as major, minor and patch.
/// </summary>
/// <remarks>
/// A server refuses to send a distributed query (<c>clusterAllReplicas</c>, <c>remote</c>, a <c>Distributed</c>
/// table) on to another server when the client that started it reports 0.0.0. So a build whose version is missing,
/// cannot be read, or is 0.0.0 reports <see cref="Fallback"/> instead.
/// </remarks>
internal static class ClientVersion
{
    /// <summary>The version reported when the build carries no usable one.</summary>
    /// <remarks>A property rather than a field, so <see cref="Current"/> can read it whatever the field order.</remarks>
    internal static (int Major, int Minor, int Patch) Fallback => (1, 0, 0);

    /// <summary>The version of this build, read once from its informational version.</summary>
    internal static (int Major, int Minor, int Patch) Current { get; } = Parse(
        typeof(ClientVersion).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion);

    /// <summary>Reads major, minor and patch from an informational version such as <c>1.5.0+3f2a1c</c>.</summary>
    /// <param name="informationalVersion">The informational version, or null.</param>
    /// <returns>The version, or <see cref="Fallback"/> when there is no usable one.</returns>
    internal static (int Major, int Minor, int Patch) Parse(string informationalVersion)
    {
        if (string.IsNullOrEmpty(informationalVersion))
        {
            return Fallback;
        }

        // The build metadata after '+' and a pre-release label after '-' are not part of the number.
        int end = informationalVersion.AsSpan().IndexOfAny('+', '-');
        ReadOnlySpan<char> number = end < 0 ? informationalVersion : informationalVersion.AsSpan(0, end);
        if (!Version.TryParse(number, out Version parsed))
        {
            return Fallback;
        }

        int patch = Math.Max(parsed.Build, 0);
        return parsed.Major == 0 && parsed.Minor == 0 && patch == 0
            ? Fallback
            : (parsed.Major, parsed.Minor, patch);
    }
}
