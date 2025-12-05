using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;

namespace ClickHouse.Driver.ADO;

public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
{
    public ClickHouseConnectionStringBuilder()
    {
    }

    public ClickHouseConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    public string Database
    {
        get => GetStringOrDefault("Database", ClickHouseDefaults.Database);
        set => this["Database"] = value;
    }

    public string Username
    {
        get => GetStringOrDefault("Username", ClickHouseDefaults.Username);
        set => this["Username"] = value;
    }

    public string Password
    {
        get => GetStringOrDefault("Password", ClickHouseDefaults.Password);
        set => this["Password"] = value;
    }

    public string Protocol
    {
        get => GetStringOrDefault("Protocol", "http");
        set => this["Protocol"] = value;
    }

    public string Host
    {
        get => GetStringOrDefault("Host", "localhost");
        set => this["Host"] = value;
    }

    public string Path
    {
        get => GetStringOrDefault("Path", null);
        set => this["Path"] = value;
    }

    public bool Compression
    {
        get => GetBooleanOrDefault("Compression", true);
        set => this["Compression"] = value;
    }

    public bool UseSession
    {
        get => GetBooleanOrDefault("UseSession", false);
        set => this["UseSession"] = value;
    }

    public string SessionId
    {
        get => GetStringOrDefault("SessionId", null);
        set => this["SessionId"] = value;
    }

    public ushort Port
    {
        get => (ushort)GetIntOrDefault("Port", Protocol == "https" ? 8443 : 8123);
        set => this["Port"] = value;
    }

    public bool UseServerTimezone
    {
        get => GetBooleanOrDefault("UseServerTimezone", true);
        set => this["UseServerTimezone"] = value;
    }

    public bool UseCustomDecimals
    {
        get => GetBooleanOrDefault("UseCustomDecimals", true);
        set => this["UseCustomDecimals"] = value;
    }

    /// <summary>
    /// Gets or sets the ClickHouse roles to use for queries.
    /// Multiple roles can be specified as a comma-separated string.
    /// </summary>
    public IReadOnlyList<string> Roles
    {
        get
        {
            var rolesString = GetStringOrDefault("Roles", null);
            if (string.IsNullOrEmpty(rolesString))
                return Array.Empty<string>();

            return rolesString
                .Split(',')
                .Select(r => r.Trim())
                .Where(r => !string.IsNullOrEmpty(r))
                .ToArray();
        }
        set
        {
            if (value == null || value.Count == 0)
                Remove("Roles");
            else
                this["Roles"] = string.Join(",", value);
        }
    }

    public TimeSpan Timeout
    {
        get
        {
            return TryGetValue("Timeout", out var value) && value is string @string && double.TryParse(@string, NumberStyles.Any, CultureInfo.InvariantCulture, out var timeout)
                ? TimeSpan.FromSeconds(timeout)
                : TimeSpan.FromMinutes(2);
        }
        set => this["Timeout"] = value.TotalSeconds;
    }

    private bool GetBooleanOrDefault(string name, bool @default)
    {
        if (TryGetValue(name, out var value))
            return "true".Equals(value as string, StringComparison.OrdinalIgnoreCase);
        else
            return @default;
    }

    private string GetStringOrDefault(string name, string @default)
    {
        if (TryGetValue(name, out var value))
            return (string)value;
        else
            return @default;
    }

    private int GetIntOrDefault(string name, int @default)
    {
        if (TryGetValue(name, out object o) && o is string s && int.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out int @int))
            return @int;
        else
            return @default;
    }

    /// <summary>
    /// Converts this connection string builder to a ClickHouseClientSettings object.
    /// </summary>
    /// <returns>A ClickHouseClientSettings instance with values from this builder</returns>
    public ClickHouseClientSettings ToSettings()
    {
        return ClickHouseClientSettings.FromConnectionStringBuilder(this);
    }

    /// <summary>
    /// Creates a connection string builder from a ClickHouseClientSettings object.
    /// </summary>
    /// <param name="settings">The settings to convert</param>
    /// <returns>A ClickHouseConnectionStringBuilder instance</returns>
    public static ClickHouseConnectionStringBuilder FromSettings(ClickHouseClientSettings settings)
    {
        if (settings == null)
            throw new ArgumentNullException(nameof(settings));

        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = settings.Host,
            Port = settings.Port,
            Protocol = settings.Protocol,
            Database = settings.Database,
            Username = settings.Username,
            Password = settings.Password,
            Path = settings.Path,
            Compression = settings.UseCompression,
            UseSession = settings.UseSession,
            SessionId = settings.SessionId,
            Timeout = settings.Timeout,
            UseServerTimezone = settings.UseServerTimezone,
            UseCustomDecimals = settings.UseCustomDecimals,
            Roles = settings.Roles,
        };

        // Add custom settings with the set_ prefix
        const string customSettingPrefix = "set_";
        foreach (var kvp in settings.CustomSettings)
        {
            builder[customSettingPrefix + kvp.Key] = kvp.Value;
        }

        return builder;
    }
}
