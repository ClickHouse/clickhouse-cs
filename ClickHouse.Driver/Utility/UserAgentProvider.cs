using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace ClickHouse.Driver.Utility;

/// <summary>
/// Per-client User-Agent token holder. Exposes <see cref="DriverProductInfo"/> and
/// <see cref="MetadataProductInfo"/> as the two <see cref="ProductInfoHeaderValue"/>
/// items the client adds to the <c>User-Agent</c> header. The driver/runtime parts are
/// initialized once per process; the metadata comment token (system tags + caller-supplied
/// <c>ApplicationInfo</c>) is built once per instance.
/// </summary>
internal sealed class UserAgentProvider
{
    private const int MaxKeyLength = 32;
    private const int MaxValueLength = 256;

    private static readonly ProductInfoHeaderValue StaticDriverProductInfo;
    private static readonly IReadOnlyDictionary<string, string> StaticSystemTags;

    static UserAgentProvider()
    {
        ProductInfoHeaderValue driver = null;
        IReadOnlyDictionary<string, string> tags = null;
        try
        {
            var versionAndHash = Assembly
                .GetExecutingAssembly()
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                .InformationalVersion ?? "unknown";
            var version = versionAndHash.Split('+')[0];
            driver = new ProductInfoHeaderValue("ClickHouse.Driver", version);

            tags = new Dictionary<string, string>
            {
                ["platform"] = Environment.OSVersion.Platform.ToString(),
                ["os"] = RuntimeInformation.OSDescription,
                ["lv"] = Environment.Version.ToString(),
                ["arch"] = RuntimeInformation.ProcessArchitecture.ToString(),
            };
        }
        catch
        {
            // Falls through to the fallback values below.
        }

        StaticDriverProductInfo = driver ?? new ProductInfoHeaderValue("ClickHouse.Driver", "unknown");
        StaticSystemTags = tags ?? new Dictionary<string, string>
        {
            ["platform"] = "unknown",
            ["os"] = "unknown",
            ["lv"] = "unknown",
            ["arch"] = "unknown",
        };
    }

    public UserAgentProvider(IReadOnlyDictionary<string, string> applicationInfo)
    {
        MetadataProductInfo = BuildMetadataHeader(StaticSystemTags, applicationInfo);
    }

    /// <summary>
    /// Driver product token, e.g. <c>ClickHouse.Driver/1.0.0</c>. Shared across all instances.
    /// </summary>
    public ProductInfoHeaderValue DriverProductInfo => StaticDriverProductInfo;

    /// <summary>
    /// Comment-style metadata token built from the runtime/OS tags and any caller-supplied
    /// <c>ApplicationInfo</c> (e.g. <c>(platform:Unix; os:...; lv:...; arch:...; app:MyApp)</c>).
    /// </summary>
    public ProductInfoHeaderValue MetadataProductInfo { get; }

    private static ProductInfoHeaderValue BuildMetadataHeader(
        IReadOnlyDictionary<string, string> systemTags,
        IReadOnlyDictionary<string, string> applicationInfo)
    {
        var sb = new StringBuilder();
        sb.Append('(');
        AppendEntries(sb, systemTags);
        AppendEntries(sb, applicationInfo);
        sb.Append(')');
        return new ProductInfoHeaderValue(sb.ToString());
    }

    private static void AppendEntries(StringBuilder sb, IReadOnlyDictionary<string, string> info)
    {
        if (info == null || info.Count == 0)
            return;

        foreach (var kvp in info)
        {
            ValidateKey(kvp.Key);
            var sanitized = SanitizeValue(kvp.Value);
            if (string.IsNullOrEmpty(sanitized))
                continue;
            // Length == 1 means we've only written the opening '('; no separator needed yet.
            if (sb.Length > 1) sb.Append("; ");
            sb.Append(kvp.Key).Append(':').Append(sanitized);
        }
    }

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("User-Agent tag keys cannot be null or empty.", nameof(key));
        if (key.Length > MaxKeyLength)
            throw new ArgumentException($"User-Agent tag key '{key}' exceeds maximum length of {MaxKeyLength}.", nameof(key));
        for (int i = 0; i < key.Length; i++)
        {
            var c = key[i];
            bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
            if (!ok)
                throw new ArgumentException($"User-Agent tag key '{key}' contains disallowed character '{c}'. Allowed: [A-Za-z0-9_.-].", nameof(key));
        }
    }

    private static string SanitizeValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        if (ContainsNonAscii(value))
            value = WebUtility.UrlEncode(value);

        var sb = new StringBuilder(value.Length);
        foreach (var ch in value)
        {
            switch (ch)
            {
                case '(':
                case ')':
                case '\\':
                case ';':
                case ',':
                    sb.Append('|');
                    continue;
            }
            if (ch < 0x20 || ch == 0x7F)
            {
                sb.Append('|');
                continue;
            }
            sb.Append(ch);
        }

        var sanitized = sb.ToString();
        if (sanitized.Length > MaxValueLength)
            sanitized = sanitized.Substring(0, MaxValueLength);
        return sanitized;
    }

    private static bool ContainsNonAscii(string value)
    {
        if (value is null) return false;
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] > 0x7F) return true;
        }
        return false;
    }
}
