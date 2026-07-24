using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Parses and builds ClickHouse native-protocol connection strings. Recognizes the endpoint and credential
/// keys plus <c>set_&lt;name&gt;</c> keys, which become client-level custom settings applied to every query.
/// Unknown keys are preserved (the base <see cref="DbConnectionStringBuilder"/> holds them) but ignored.
/// </summary>
#pragma warning disable CA1010 // Type inherits ICollection without implementing generic version - inherent to DbConnectionStringBuilder
public sealed class ClickHouseTcpConnectionStringBuilder : DbConnectionStringBuilder
#pragma warning restore CA1010
{
    /// <summary>The prefix that marks a connection-string key as a ClickHouse custom setting.</summary>
    private const string CustomSettingPrefix = "set_";

    /// <summary>Initializes an empty builder.</summary>
    public ClickHouseTcpConnectionStringBuilder()
    {
    }

    /// <summary>Initializes a builder from an existing connection string.</summary>
    /// <param name="connectionString">The connection string to parse.</param>
    public ClickHouseTcpConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>The server host name or address. Defaults to <c>localhost</c>.</summary>
    public string Host
    {
        get => GetStringOrDefault("Host", ClickHouseTcpClientOptions.DefaultHost);
        set => this["Host"] = value;
    }

    /// <summary>The server's native-protocol port. Defaults to <c>9000</c>.</summary>
    public int Port
    {
        get => GetIntOrDefault("Port", ClickHouseTcpClientOptions.DefaultPort);
        set => this["Port"] = value;
    }

    /// <summary>The user to authenticate as. Defaults to <c>default</c>.</summary>
    public string Username
    {
        get => GetStringOrDefault("Username", ClickHouseTcpClientOptions.DefaultUsername);
        set => this["Username"] = value;
    }

    /// <summary>The password. Defaults to empty.</summary>
    public string Password
    {
        get => GetStringOrDefault("Password", string.Empty);
        set => this["Password"] = value;
    }

    /// <summary>The default database. Defaults to <c>default</c>.</summary>
    public string Database
    {
        get => GetStringOrDefault("Database", ClickHouseTcpClientOptions.DefaultDatabase);
        set => this["Database"] = value;
    }

    /// <summary>The keyed-quota resource key. Defaults to empty.</summary>
    public string QuotaKey
    {
        get => GetStringOrDefault("QuotaKey", string.Empty);
        set => this["QuotaKey"] = value;
    }

    /// <summary>The connect-plus-handshake deadline, in seconds. Defaults to 30.</summary>
    public TimeSpan DialTimeout
    {
        get => GetTimeSpanSecondsOrDefault("DialTimeout", ClickHouseTcpClientOptions.DefaultDialTimeout);
        set => this["DialTimeout"] = value.TotalSeconds;
    }

    /// <summary>The idle read deadline, in seconds. Defaults to 300.</summary>
    public TimeSpan ReadTimeout
    {
        get => GetTimeSpanSecondsOrDefault("ReadTimeout", ClickHouseTcpClientOptions.DefaultReadTimeout);
        set => this["ReadTimeout"] = value.TotalSeconds;
    }

    /// <summary>The soft send-buffer cap, in bytes. Defaults to 10 MiB.</summary>
    public int MaxSendBufferBytes
    {
        get => GetIntOrDefault("MaxSendBufferBytes", ClickHouseTcpClientOptions.DefaultMaxSendBufferBytes);
        set => this["MaxSendBufferBytes"] = value;
    }

    /// <summary>Materializes these keys into a <see cref="ClickHouseTcpClientOptions"/>, folding <c>set_*</c> keys into <see cref="ClickHouseTcpClientOptions.CustomSettings"/>.</summary>
    /// <returns>The equivalent options.</returns>
    public ClickHouseTcpClientOptions ToOptions()
    {
        Dictionary<string, string> customSettings = null;
        foreach (string key in Keys)
        {
            if (key.StartsWith(CustomSettingPrefix, StringComparison.OrdinalIgnoreCase))
            {
                string name = key.Substring(CustomSettingPrefix.Length);
                (customSettings ??= new Dictionary<string, string>(StringComparer.Ordinal))[name] = GetStringOrDefault(key, null);
            }
        }

        return new ClickHouseTcpClientOptions
        {
            Host = Host,
            Port = Port,
            Username = Username,
            Password = Password,
            Database = Database,
            QuotaKey = QuotaKey,
            DialTimeout = DialTimeout,
            ReadTimeout = ReadTimeout,
            MaxSendBufferBytes = MaxSendBufferBytes,
            CustomSettings = customSettings,
        };
    }

    private string GetStringOrDefault(string name, string @default)
        => TryGetValue(name, out object value) && value is string s ? s : @default;

    // A numeric value read back may be the string a connection string was parsed into, or the boxed int/double a
    // typed setter stored on this same instance — handle both so a set-then-get on one builder round-trips.
    private int GetIntOrDefault(string name, int @default)
    {
        if (!TryGetValue(name, out object value))
        {
            return @default;
        }

        return value switch
        {
            int i => i,
            string s when int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int result) => result,
            _ => @default,
        };
    }

    private TimeSpan GetTimeSpanSecondsOrDefault(string name, TimeSpan @default)
    {
        if (!TryGetValue(name, out object value))
        {
            return @default;
        }

        return value switch
        {
            double d => TimeSpan.FromSeconds(d),
            string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out double seconds) => TimeSpan.FromSeconds(seconds),
            _ => @default,
        };
    }
}
