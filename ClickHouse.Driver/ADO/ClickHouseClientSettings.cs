using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Represents the settings for a ClickHouse client connection.
/// Provides a structured way to configure connections using strongly-typed properties.
/// </summary>
public class ClickHouseClientSettings : IEquatable<ClickHouseClientSettings>
{
    private readonly object sessionIdLock = new object();
    private string sessionId;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClientSettings"/> class with default values.
    /// </summary>
    public ClickHouseClientSettings()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClientSettings"/> class by parsing a connection string.
    /// </summary>
    public ClickHouseClientSettings(string connectionString)
        : this(FromConnectionString(connectionString))
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClientSettings"/> class by taking the values from a ClickHouseConnectionStringBuilder.
    /// </summary>
    public ClickHouseClientSettings(ClickHouseConnectionStringBuilder builder)
        : this(FromConnectionStringBuilder(builder))
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClientSettings"/> class by copying values from another instance.
    /// </summary>
    /// <param name="other">The settings instance to copy from</param>
    public ClickHouseClientSettings(ClickHouseClientSettings other)
    {
        if (other == null)
            throw new ArgumentNullException(nameof(other));

        Host = other.Host;
        Port = other.Port;
        Protocol = other.Protocol;
        Database = other.Database;
        Path = other.Path;
        Username = other.Username;
        Password = other.Password;
        UseCompression = other.UseCompression;
        UseServerTimezone = other.UseServerTimezone;
        UseCustomDecimals = other.UseCustomDecimals;
        UseSession = other.UseSession;
        SessionId = other.SessionId;
        SkipServerCertificateValidation = other.SkipServerCertificateValidation;
        UseFormDataParameters = other.UseFormDataParameters;
        Timeout = other.Timeout;
        HttpClient = other.HttpClient;
        HttpClientFactory = other.HttpClientFactory;
        HttpClientName = other.HttpClientName;
        LoggerFactory = other.LoggerFactory;

        // Deep copy the CustomSettings dictionary
        CustomSettings = new Dictionary<string, object>(other.CustomSettings);
    }

    /// <summary>
    /// Gets or sets the host name or IP address of the ClickHouse server.
    /// Default: "localhost"
    /// </summary>
    public string Host { get; init; } = ClickHouseDefaults.Host;

    /// <summary>
    /// Gets or sets the port number of the ClickHouse server.
    /// Default: 8123 for HTTP, 8443 for HTTPS
    /// </summary>
    public ushort Port { get; init; } = ClickHouseDefaults.HttpPort;

    /// <summary>
    /// Gets or sets the protocol to use (http or https).
    /// Default: "http"
    /// </summary>
    public string Protocol { get; init; } = ClickHouseDefaults.Protocol;

    /// <summary>
    /// Gets or sets the database name.
    /// Default: "" (if empty, will use the user's default database if it has been configured).
    /// </summary>
    public string Database { get; init; } = ClickHouseDefaults.Database;

    /// <summary>
    /// Gets or sets the path component of the URL (for reverse proxy scenarios).
    /// Default: null
    /// </summary>
    public string Path { get; init; } = ClickHouseDefaults.Path;

    /// <summary>
    /// Gets or sets the username for authentication.
    /// Default: "default"
    /// </summary>
    public string Username { get; init; } = ClickHouseDefaults.Username;

    /// <summary>
    /// Gets or sets the password for authentication.
    /// Default: "" (empty string)
    /// </summary>
    public string Password { get; init; } = ClickHouseDefaults.Password;

    /// <summary>
    /// Gets or sets whether to use compression for data transfer.
    /// Default: true
    /// </summary>
    public bool UseCompression { get; init; } = ClickHouseDefaults.Compression;

    /// <summary>
    /// Gets or sets whether to use server timezone for DateTime values.
    /// Default: true
    /// </summary>
    public bool UseServerTimezone { get; init; } = ClickHouseDefaults.UseServerTimezone;

    /// <summary>
    /// Gets or sets whether to use custom decimal types.
    /// Default: true
    /// </summary>
    public bool UseCustomDecimals { get; init; } = ClickHouseDefaults.UseCustomDecimals;

    /// <summary>
    /// Gets or sets whether to use sessions for the connection.
    /// Default: false
    /// </summary>
    public bool UseSession { get; init; } = ClickHouseDefaults.UseSession;

    /// <summary>
    /// Gets or sets the session ID to use (the value is only used if UseSession is true).
    /// If null and UseSession is true, a new GUID will be generated.
    /// Default: null
    /// </summary>
    public string SessionId
    {
        get
        {
            if (!UseSession) return sessionId;

            if (sessionId == null)
            {
                lock (sessionIdLock)
                {
                    sessionId ??= Guid.NewGuid().ToString();
                }
            }

            return sessionId;
        }
        init => sessionId = value;
    }

    /// <summary>
    /// Gets or sets whether to skip server certificate validation (for development/testing).
    /// Default: false
    /// </summary>
    public bool SkipServerCertificateValidation { get; init; } = ClickHouseDefaults.SkipServerCertificateValidation;

    /// <summary>
    /// Gets or sets whether to send parameters as form data.
    /// Default: false
    /// </summary>
    public bool UseFormDataParameters { get; init; } = ClickHouseDefaults.UseFormDataParameters;

    /// <summary>
    /// Gets or sets the timeout for operations.
    /// Default: 2 minutes
    /// </summary>
    public TimeSpan Timeout { get; init; } = ClickHouseDefaults.Timeout;

    /// <summary>
    /// Gets or sets a custom HttpClient to use for connections.
    /// Note: HttpClient must have AutomaticDecompression enabled if compression is not disabled.
    /// Default: null (driver will create its own)
    /// </summary>
    public HttpClient HttpClient { get; init; }

    /// <summary>
    /// Gets or sets a custom IHttpClientFactory to use for creating HttpClient instances.
    /// Default: null (driver will create its own)
    /// </summary>
    public IHttpClientFactory HttpClientFactory { get; init; }

    /// <summary>
    /// Gets or sets the name of the HTTP client to create from the HttpClientFactory.
    /// Only used when HttpClientFactory is provided.
    /// Default: "" (empty string creates default client)
    /// </summary>
    public string HttpClientName { get; init; }

    /// <summary>
    /// Gets or sets the logger factory that the client will use for logging.
    /// Default: null
    /// </summary>
    public ILoggerFactory LoggerFactory { get; init; }

    /// <summary>
    /// Gets or sets custom ClickHouse settings to pass with queries.
    /// Default: empty dictionary
    /// </summary>
    public IDictionary<string, object> CustomSettings { get; init; } = new Dictionary<string, object>();

    /// <summary>
    /// Creates a ClickHouseClientSettings object from a connection string.
    /// Values not specified in the connection string will use default values.
    /// </summary>
    /// <param name="connectionString">The connection string to parse</param>
    /// <returns>A new ClickHouseClientSettings instance</returns>
    internal static ClickHouseClientSettings FromConnectionString(string connectionString)
    {
        if (connectionString == null)
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        var builder = new ClickHouseConnectionStringBuilder(connectionString);
        return FromConnectionStringBuilder(builder);
    }

    /// <summary>
    /// Creates a ClickHouseClientSettings object from a ClickHouseConnectionStringBuilder.
    /// Values not specified in the connection string builder will use default values.
    /// </summary>
    /// <param name="builder">The connection string builder</param>
    /// <returns>A new ClickHouseClientSettings instance</returns>
    internal static ClickHouseClientSettings FromConnectionStringBuilder(ClickHouseConnectionStringBuilder builder)
    {
        if (builder == null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        var settings = new ClickHouseClientSettings
        {
            Host = builder.Host,
            Port = builder.Port,
            Protocol = builder.Protocol,
            Database = builder.Database,
            Username = builder.Username,
            Password = builder.Password,
            Path = builder.Path,
            UseCompression = builder.Compression,
            UseSession = builder.UseSession,
            SessionId = builder.SessionId,
            Timeout = builder.Timeout,
            UseServerTimezone = builder.UseServerTimezone,
            UseCustomDecimals = builder.UseCustomDecimals,
        };

        // Extract custom settings from connection string builder
        const string customSettingPrefix = "set_";
        foreach (var key in builder.Keys)
        {
            var keyString = key.ToString();
            if (keyString.StartsWith(customSettingPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var settingName = keyString.Substring(customSettingPrefix.Length);
                settings.CustomSettings[settingName] = builder[keyString];
            }
        }

        return settings;
    }

    /// <summary>
    /// Determines whether the specified ClickHouseClientSettings is equal to this instance.
    /// </summary>
    public bool Equals(ClickHouseClientSettings other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;

        return Host == other.Host &&
               Port == other.Port &&
               Protocol == other.Protocol &&
               Database == other.Database &&
               Path == other.Path &&
               Username == other.Username &&
               Password == other.Password &&
               UseCompression == other.UseCompression &&
               UseServerTimezone == other.UseServerTimezone &&
               UseCustomDecimals == other.UseCustomDecimals &&
               UseSession == other.UseSession &&
               SessionId == other.SessionId &&
               SkipServerCertificateValidation == other.SkipServerCertificateValidation &&
               UseFormDataParameters == other.UseFormDataParameters &&
               Timeout == other.Timeout &&
               HttpClient == other.HttpClient &&
               HttpClientFactory == other.HttpClientFactory &&
               HttpClientName == other.HttpClientName;
    }

    /// <summary>
    /// Determines whether the specified object is equal to this instance.
    /// </summary>
    public override bool Equals(object obj)
    {
        return Equals(obj as ClickHouseClientSettings);
    }

    /// <summary>
    /// Returns a hash code for this instance.
    /// </summary>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Host);
        hash.Add(Port);
        hash.Add(Protocol);
        hash.Add(Database);
        hash.Add(Path);
        hash.Add(Username);
        hash.Add(Password);
        hash.Add(UseCompression);
        hash.Add(UseServerTimezone);
        hash.Add(UseCustomDecimals);
        hash.Add(UseSession);
        hash.Add(SessionId);
        hash.Add(SkipServerCertificateValidation);
        hash.Add(UseFormDataParameters);
        hash.Add(Timeout);
        foreach (var kvp in CustomSettings)
        {
            hash.Add(HashCode.Combine(kvp.Key, kvp.Value));
        }
        return hash.ToHashCode();
    }

    /// <summary>
    /// Equality operator.
    /// </summary>
    public static bool operator ==(ClickHouseClientSettings left, ClickHouseClientSettings right)
    {
        return Equals(left, right);
    }

    /// <summary>
    /// Inequality operator.
    /// </summary>
    public static bool operator !=(ClickHouseClientSettings left, ClickHouseClientSettings right)
    {
        return !Equals(left, right);
    }

    /// <summary>
    /// Returns a string representation of the settings (with password redacted).
    /// </summary>
    public override string ToString()
    {
        return $"Host={Host};Port={Port};Protocol={Protocol};Database={Database};" +
               $"Username={Username};Password=****;Compression={UseCompression};" +
               $"UseServerTimezone={UseServerTimezone};UseCustomDecimals={UseCustomDecimals};" +
               $"UseSession={UseSession};Timeout={Timeout.TotalSeconds}s";
    }

    /// <summary>
    /// Validates the settings and throws an exception if any setting is invalid.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
            throw new InvalidOperationException("Host cannot be null or whitespace");

        if (Port < 1 || Port > 65535)
            throw new InvalidOperationException($"Port must be between 1 and 65535, got {Port}");

        if (string.IsNullOrWhiteSpace(Protocol))
            throw new InvalidOperationException("Protocol cannot be null or whitespace");

        if (Protocol != "http" && Protocol != "https")
            throw new InvalidOperationException($"Protocol must be 'http' or 'https', got '{Protocol}'");

        if (Timeout < TimeSpan.Zero)
            throw new InvalidOperationException("Timeout cannot be negative");

        if (UseSession && HttpClient != null)
            throw new InvalidOperationException("UseSession cannot be combined with a custom HttpClient (sessions require a single persistent connection)");

        if (UseSession && HttpClientFactory != null)
            throw new InvalidOperationException("UseSession cannot be combined with a custom HttpClientFactory (sessions require a single persistent connection)");

        if (HttpClient != null && HttpClientFactory != null)
            throw new InvalidOperationException("Cannot specify both HttpClient and HttpClientFactory");
    }
}
