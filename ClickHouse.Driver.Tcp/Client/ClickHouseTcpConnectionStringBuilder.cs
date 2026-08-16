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

    /// <summary>
    /// The server's native-protocol port. Absent, the default, derives it from <see cref="UseTls"/> —
    /// <c>9440</c> with TLS, <c>9000</c> without. Setting null removes the key.
    /// </summary>
    public int? Port
    {
        get => GetIntOrNull("Port");
        set
        {
            if (value is { } port)
            {
                this["Port"] = port;
            }
            else
            {
                Remove("Port");
            }
        }
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

    /// <summary>Whether to encrypt the transport with TLS. Defaults to false.</summary>
    /// <exception cref="ArgumentException">The stored value is not a boolean.</exception>
    public bool UseTls
    {
        get => GetBoolOrDefault("UseTls", false);
        set => this["UseTls"] = value;
    }

    /// <summary>The host name to match the server certificate against. Absent, the default, uses <see cref="Host"/>.</summary>
    public string TlsServerName
    {
        get => GetStringOrDefault("TlsServerName", null);
        set => this["TlsServerName"] = value;
    }

    /// <summary>
    /// Whether to accept a server certificate that fails validation. Defaults to false; setting it true removes
    /// the protection TLS provides against an intercepted connection, so keep it to development.
    /// </summary>
    /// <exception cref="ArgumentException">The stored value is not a boolean.</exception>
    public bool TlsAllowInvalidCertificates
    {
        get => GetBoolOrDefault("TlsAllowInvalidCertificates", false);
        set => this["TlsAllowInvalidCertificates"] = value;
    }

    /// <summary>Path to a PEM file of certificate authorities to validate the server against. Absent uses the host's trust store.</summary>
    public string TlsCaCertificatePath
    {
        get => GetStringOrDefault("TlsCaCertificatePath", null);
        set => this["TlsCaCertificatePath"] = value;
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

    /// <summary>
    /// The number of connections the pool keeps open, counting the ones in use as well as the idle ones. An
    /// expired connection is still retired, and a fresh one opened in its place. Defaults to 0.
    /// </summary>
    public int MinPoolSize
    {
        get => GetIntOrDefault("MinPoolSize", ClickHouseTcpClientOptions.DefaultMinPoolSize);
        set => this["MinPoolSize"] = value;
    }

    /// <summary>The cap on connections, and so on concurrent operations. Defaults to 20.</summary>
    public int MaxPoolSize
    {
        get => GetIntOrDefault("MaxPoolSize", ClickHouseTcpClientOptions.DefaultMaxPoolSize);
        set => this["MaxPoolSize"] = value;
    }

    /// <summary>How long an operation waits for a free connection, in seconds. Defaults to 30.</summary>
    public TimeSpan PoolTimeout
    {
        get => GetTimeSpanSecondsOrDefault("PoolTimeout", ClickHouseTcpClientOptions.DefaultPoolTimeout);
        set => this["PoolTimeout"] = value.TotalSeconds;
    }

    /// <summary>How long a connection may be reused after opening, in seconds; 0 disables. Defaults to 1800.</summary>
    public TimeSpan MaxConnectionLifetime
    {
        get => GetTimeSpanSecondsOrDefault("MaxConnectionLifetime", ClickHouseTcpClientOptions.DefaultMaxConnectionLifetime);
        set => this["MaxConnectionLifetime"] = value.TotalSeconds;
    }

    /// <summary>
    /// How long a connection may sit unused before it is retired rather than reused, in seconds; 0 disables.
    /// Defaults to 300. Keep it below the shortest idle timeout on the path to the server.
    /// </summary>
    public TimeSpan IdleTimeout
    {
        get => GetTimeSpanSecondsOrDefault("IdleTimeout", ClickHouseTcpClientOptions.DefaultIdleTimeout);
        set => this["IdleTimeout"] = value.TotalSeconds;
    }

    /// <summary>
    /// How often the pool looks for connections to retire, in seconds. Absent, the default, derives the period
    /// from the limits in force; a value overrides that and must be positive. Setting null removes the key.
    /// </summary>
    public TimeSpan? SweepInterval
    {
        get => GetTimeSpanSecondsOrNull("SweepInterval");
        set
        {
            if (value is { } interval)
            {
                this["SweepInterval"] = interval.TotalSeconds;
            }
            else
            {
                Remove("SweepInterval");
            }
        }
    }

    /// <summary>Which idle connection is handed out next, <c>Lifo</c> or <c>Fifo</c>. Defaults to <c>Lifo</c>.</summary>
    /// <exception cref="ArgumentException">The stored value names no policy.</exception>
    public ClickHouseTcpPoolReusePolicy PoolReusePolicy
    {
        get => GetEnumOrDefault("PoolReusePolicy", ClickHouseTcpClientOptions.DefaultPoolReusePolicy);
        set => this["PoolReusePolicy"] = value.ToString();
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
                if (name.Length == 0)
                {
                    // '<prefix>' alone names an empty setting, which would terminate the wire settings list early.
                    throw new ArgumentException($"Connection-string key '{key}' names an empty custom setting; a setting name must be non-empty (e.g. '{CustomSettingPrefix}max_threads').");
                }

                // Never store null (it cannot be written): a value-less set_ key becomes an empty-string setting.
                (customSettings ??= new Dictionary<string, string>(StringComparer.Ordinal))[name] = GetCustomSettingValue(key);
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
            UseTls = UseTls,
            TlsServerName = TlsServerName,
            TlsAllowInvalidCertificates = TlsAllowInvalidCertificates,
            TlsCaCertificatePath = TlsCaCertificatePath,
            DialTimeout = DialTimeout,
            ReadTimeout = ReadTimeout,
            MaxSendBufferBytes = MaxSendBufferBytes,
            MinPoolSize = MinPoolSize,
            MaxPoolSize = MaxPoolSize,
            PoolTimeout = PoolTimeout,
            MaxConnectionLifetime = MaxConnectionLifetime,
            IdleTimeout = IdleTimeout,
            SweepInterval = SweepInterval,
            PoolReusePolicy = PoolReusePolicy,
            CustomSettings = customSettings,
        };
    }

    // A custom setting's value as an invariant string: a string is taken as-is; a typed value set programmatically
    // (e.g. builder["set_max_threads"] = 4) is formatted invariantly rather than silently dropped; an absent or
    // null value becomes empty (never null, which cannot be written on the wire).
    private string GetCustomSettingValue(string key)
        => TryGetValue(key, out object value) && value is not null
            ? value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
            : string.Empty;

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

    // The nullable form, for a key whose absence means "derive it" rather than "use the default".
    private int? GetIntOrNull(string name)
    {
        if (!TryGetValue(name, out object value))
        {
            return null;
        }

        return value switch
        {
            int i => i,
            string s when int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int result) => result,
            _ => null,
        };
    }

    // Unlike the numeric getters, an unreadable value throws rather than falling back. These keys decide whether
    // the transport is encrypted and how far its certificate is checked, so 'UseTls=yes' reading as false would
    // hand the caller a plaintext connection they believe is secure. '1' and '0' are accepted alongside the names
    // bool.TryParse takes, matching how boolean settings are written elsewhere in ClickHouse.
    //
    // The base indexer converts whatever it is given to a string, so a value set programmatically arrives here as
    // one too and the bool arm is only reached through this class's own typed setter. The final arm is therefore
    // defensive rather than reachable; it throws because that is the safe direction for these keys.
    private bool GetBoolOrDefault(string name, bool @default)
    {
        if (!TryGetValue(name, out object value))
        {
            return @default;
        }

        return value switch
        {
            bool b => b,
            string s when bool.TryParse(s, out bool parsed) => parsed,
            string s when s == "1" => true,
            string s when s == "0" => false,
            _ => throw new ArgumentException(
                $"Connection-string key '{name}' has value '{value}', which is not a boolean. Use true, false, 1, or 0."),
        };
    }

    // An enum value read back is normally the name a connection string carried (the typed setter stores a name
    // too); a boxed enum arrives only through the untyped indexer. Names are matched case-insensitively, as
    // connection-string keys are, and an unrecognized name throws here rather than silently defaulting. A
    // numeric name parses, so an out-of-range number reaches ClickHouseTcpClientOptions.Validate instead. Any
    // other stored type falls back to the default, matching how the numeric getters behave.
    private TEnum GetEnumOrDefault<TEnum>(string name, TEnum @default)
        where TEnum : struct, Enum
    {
        if (!TryGetValue(name, out object value))
        {
            return @default;
        }

        return value switch
        {
            TEnum typed => typed,
            string s when Enum.TryParse(s, ignoreCase: true, out TEnum parsed) => parsed,
            string s => throw new ArgumentException(
                $"Connection-string key '{name}' has value '{s}', which is not one of: {string.Join(", ", Enum.GetNames<TEnum>())}."),
            _ => @default,
        };
    }

    // The nullable form, for a key whose absence means "derive it" rather than "use the default". An unparseable
    // value reads as absent, matching how the non-nullable helper falls back.
    private TimeSpan? GetTimeSpanSecondsOrNull(string name)
    {
        if (!TryGetValue(name, out object value))
        {
            return null;
        }

        return value switch
        {
            double d => TimeSpan.FromSeconds(d),
            string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out double seconds) => TimeSpan.FromSeconds(seconds),
            _ => null,
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
