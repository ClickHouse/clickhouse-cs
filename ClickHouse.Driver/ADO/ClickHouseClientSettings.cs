using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;
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
        BearerToken = other.BearerToken;
        UseCompression = other.UseCompression;
        UseCustomDecimals = other.UseCustomDecimals;
        ReadStringsAsByteArrays = other.ReadStringsAsByteArrays;
        UseSession = other.UseSession;
        SessionId = other.SessionId;
        SkipServerCertificateValidation = other.SkipServerCertificateValidation;
        UseFormDataParameters = other.UseFormDataParameters;
        ReadBufferSize = other.ReadBufferSize;
        Timeout = other.Timeout;
        HttpClient = other.HttpClient;
        HttpClientFactory = other.HttpClientFactory;
        HttpClientName = other.HttpClientName;
        LoggerFactory = other.LoggerFactory;
        EnableDebugMode = other.EnableDebugMode;

        // Deep copy the CustomSettings dictionary
        CustomSettings = new Dictionary<string, object>(other.CustomSettings);

        // Copy roles list
        Roles = other.Roles.ToArray();

        // Deep copy the CustomHeaders dictionary
        CustomHeaders = new Dictionary<string, string>(other.CustomHeaders.ToDictionary(x => x.Key, x => x.Value));

        // Deep copy the ApplicationInfo dictionary (null is treated as empty)
        ApplicationInfo = other.ApplicationInfo == null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string>(other.ApplicationInfo.ToDictionary(x => x.Key, x => x.Value));

        // Copy JSON mode settings
        JsonReadMode = other.JsonReadMode;
        JsonWriteMode = other.JsonWriteMode;

        // Copy parameter type resolver
        ParameterTypeResolver = other.ParameterTypeResolver;

        // Copy parameter formatter
        ParameterFormatter = other.ParameterFormatter;

        // Copy read value converter
        ReadValueConverter = other.ReadValueConverter;
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
    public string Database { get; set; } = ClickHouseDefaults.Database;

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
    /// Gets or sets the bearer token for JWT authentication.
    /// When set, Bearer authentication is used instead of Basic authentication
    /// (Username and Password are ignored for the Authorization header).
    /// The token should be provided as-is (already encoded if required by your auth provider).
    /// Default: null
    /// </summary>
    public string BearerToken { get; init; }

    /// <summary>
    /// Gets or sets whether to use compression for data transfer.
    /// Default: true
    /// </summary>
    public bool UseCompression { get; init; } = ClickHouseDefaults.Compression;

    /// <summary>
    /// Gets or sets whether to use custom decimal types.
    /// Default: true
    /// </summary>
    public bool UseCustomDecimals { get; init; } = ClickHouseDefaults.UseCustomDecimals;

    /// <summary>
    /// Gets or sets whether to read String/FixedString columns as byte arrays instead of strings.
    /// This is useful when storing binary data that may not be valid UTF-8.
    /// Default: false
    /// </summary>
    public bool ReadStringsAsByteArrays { get; init; } = ClickHouseDefaults.ReadStringsAsByteArrays;

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
    /// Gets or sets the size, in bytes, of the buffer used when reading HTTP query responses.
    /// <para>
    /// A larger buffer reduces the number of refills for large responses,
    /// but any value at or above 85,000 bytes is allocated on the LOH,
    /// which is not compacted and only reclaimed by gen2 collections; under high query
    /// throughput that can cause LOH fragmentation, inflated committed memory and longer GC pauses.
    /// </para>
    /// Default: 8192 (8 KiB)
    /// </summary>
    public int ReadBufferSize { get; init; } = ClickHouseDefaults.ReadBufferSize;

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
    /// Gets or sets whether to enable debug mode for low-level .NET network tracing (.NET 5+).
    /// When enabled, traces System.Net events including HTTP, Sockets, DNS, and TLS operations.
    /// Requires LoggerFactory to be set and configured with Trace-level logging enabled.
    /// WARNING: This can significantly impact performance and generate large amounts of log data.
    /// Not recommended for production use.
    /// Default: false
    /// </summary>
    public bool EnableDebugMode { get; init; }

    /// <summary>
    /// Gets or sets custom ClickHouse settings to pass with queries.
    /// Default: empty dictionary
    /// </summary>
    public IDictionary<string, object> CustomSettings { get; init; } = new Dictionary<string, object>();

    /// <summary>
    /// Gets or sets the ClickHouse roles to use for queries.
    /// Multiple roles can be specified and will be sent as separate role= query parameters.
    /// Default: empty list
    /// </summary>
    public IReadOnlyList<string> Roles { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Gets or sets custom HTTP headers to send with each request.
    /// These headers are applied after the default headers, allowing you to override most headers.
    /// The following headers cannot be overridden and will be silently ignored:
    /// Connection, Authorization, User-Agent
    /// Default: empty dictionary
    /// </summary>
    public IReadOnlyDictionary<string, string> CustomHeaders { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Gets or sets application-identity tags appended to the HTTP User-Agent header as
    /// a comment token (e.g. <c>(app:MyApp; ver:2.3.1; env:prod)</c>).
    /// <para>
    /// <b>Keys</b> must match <c>[A-Za-z0-9_.-]</c> and be 1–32 characters long; invalid keys
    /// throw <see cref="ArgumentException"/> when the client is constructed. The driver
    /// emits its own tags (<c>platform</c>, <c>os</c>, <c>lv</c>, <c>arch</c>) in the same
    /// comment token; supplying any of these as keys here appends a second entry with the
    /// same name rather than overriding the driver's value.
    /// </para>
    /// <para>
    /// <b>Values</b> are sanitized to keep the header RFC 7230-compliant and capped at
    /// 256 characters (longer values are truncated). If a value contains non-ASCII characters,
    /// the entire value is URL-encoded (which also encodes spaces as <c>+</c> and punctuation).
    /// After that, any remaining structural characters (<c>( ) \ ; ,</c>) and control characters
    /// are replaced with <c>|</c>.
    /// </para>
    /// <para>
    /// The dictionary's contents are read when <see cref="ClickHouseClient"/> is constructed
    /// and the resulting <c>User-Agent</c> token is cached on the client. Mutating the
    /// dictionary you passed in <em>after</em> the client has been constructed has no effect
    /// on subsequent requests and is not supported, pass a fresh settings instance instead.
    /// </para>
    /// Default: empty dictionary (no additional tags appended).
    /// </summary>
    public IReadOnlyDictionary<string, string> ApplicationInfo { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Gets or sets how JSON columns are returned when reading data.
    /// Binary (default): Returns System.Text.Json.Nodes.JsonObject
    /// String: Returns the raw JSON string (requires server setting output_format_binary_write_json_as_string=1)
    /// </summary>
    public JsonReadMode JsonReadMode { get; init; } = JsonReadMode.Binary;

    /// <summary>
    /// Gets or sets how JSON data is sent when writing.
    /// String (default): Client sends JSON as string. Accepts JsonObject, JsonNode, strings, and POCOs.
    /// Binary: Client serializes to binary JSON format. Only registered POCO types are supported.
    /// </summary>
    public JsonWriteMode JsonWriteMode { get; init; } = JsonWriteMode.String;

    /// <summary>
    /// Gets or sets a custom resolver for mapping .NET types to ClickHouse types
    /// during @-style parameter substitution. When set, this resolver is consulted
    /// after explicit ClickHouseType/SQL type hints but before default type inference.
    /// Default: null (use built-in type inference)
    /// </summary>
    public IParameterTypeResolver ParameterTypeResolver { get; init; }

    /// <summary>
    /// Gets or sets a custom formatter for serializing parameter values for HTTP transport.
    /// When set, this formatter is consulted before the built-in value formatting logic.
    /// Return null from the formatter to fall through to default formatting.
    /// Default: null (use built-in formatting)
    /// </summary>
    public IParameterFormatter ParameterFormatter { get; init; }

    /// <summary>
    /// Gets or sets a custom converter for same-type transformation of values returned by the data reader.
    /// When set, this converter is called on every GetValue/GetFieldValue call,
    /// allowing same-type transformations (e.g., setting DateTime.Kind, trimming strings).
    /// The converter must not change the runtime type of values.
    /// Default: null (no conversion)
    /// </summary>
    public IReadValueConverter ReadValueConverter { get; init; }

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
            UseCustomDecimals = builder.UseCustomDecimals,
            ReadStringsAsByteArrays = builder.ReadStringsAsByteArrays,
            ReadBufferSize = builder.ReadBufferSize,
            Roles = builder.Roles,
            JsonReadMode = builder.JsonReadMode,
            JsonWriteMode = builder.JsonWriteMode,
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
               BearerToken == other.BearerToken &&
               UseCompression == other.UseCompression &&
               UseCustomDecimals == other.UseCustomDecimals &&
               ReadStringsAsByteArrays == other.ReadStringsAsByteArrays &&
               UseSession == other.UseSession &&
               SessionId == other.SessionId &&
               SkipServerCertificateValidation == other.SkipServerCertificateValidation &&
               UseFormDataParameters == other.UseFormDataParameters &&
               ReadBufferSize == other.ReadBufferSize &&
               Timeout == other.Timeout &&
               HttpClient == other.HttpClient &&
               HttpClientFactory == other.HttpClientFactory &&
               HttpClientName == other.HttpClientName &&
               EnableDebugMode == other.EnableDebugMode &&
               JsonReadMode == other.JsonReadMode &&
               JsonWriteMode == other.JsonWriteMode &&
               ParameterTypeResolver == other.ParameterTypeResolver &&
               ParameterFormatter == other.ParameterFormatter &&
               ReadValueConverter == other.ReadValueConverter &&
               Roles.SequenceEqual(other.Roles) &&
               CustomHeaders.EntriesEqual(other.CustomHeaders) &&
               ApplicationInfo.EntriesEqual(other.ApplicationInfo);
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
        HashCode hash = default;
        hash.Add(Host);
        hash.Add(Port);
        hash.Add(Protocol);
        hash.Add(Database);
        hash.Add(Path);
        hash.Add(Username);
        hash.Add(Password);
        hash.Add(BearerToken);
        hash.Add(UseCompression);
        hash.Add(UseCustomDecimals);
        hash.Add(ReadStringsAsByteArrays);
        hash.Add(UseSession);
        hash.Add(SessionId);
        hash.Add(SkipServerCertificateValidation);
        hash.Add(UseFormDataParameters);
        hash.Add(ReadBufferSize);
        hash.Add(Timeout);
        hash.Add(EnableDebugMode);
        hash.Add(JsonReadMode);
        hash.Add(JsonWriteMode);
        hash.Add(ParameterTypeResolver);
        hash.Add(ParameterFormatter);
        hash.Add(ReadValueConverter);
        foreach (var kvp in CustomSettings)
        {
            hash.Add(HashCode.Combine(kvp.Key, kvp.Value));
        }
        foreach (var role in Roles)
        {
            hash.Add(role);
        }
        if (CustomHeaders != null)
        {
            foreach (var kvp in CustomHeaders)
                hash.Add(HashCode.Combine(kvp.Key, kvp.Value));
        }
        if (ApplicationInfo != null)
        {
            foreach (var kvp in ApplicationInfo)
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
        var result = $"Host={Host};Port={Port};Protocol={Protocol};Database={Database};" +
               $"Username={Username};Password=****;Compression={UseCompression};" +
               $"UseCustomDecimals={UseCustomDecimals};ReadStringsAsByteArrays={ReadStringsAsByteArrays};" +
               $"UseSession={UseSession};Timeout={Timeout.TotalSeconds}s;" +
               $"ReadBufferSize={ReadBufferSize};" +
               $"JsonReadMode={JsonReadMode};JsonWriteMode={JsonWriteMode}";

        if (Roles.Count > 0)
        {
            result += $";Roles={string.Join(",", Roles)}";
        }

        if (CustomHeaders.Count > 0)
        {
            result += $";CustomHeaders={string.Join(",", CustomHeaders.Select(x => $"{x.Key}:***"))}";
        }

        return result;
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

        if (ReadBufferSize < 1)
            throw new InvalidOperationException($"ReadBufferSize must be greater than zero, got {ReadBufferSize}");

        if (HttpClient != null && HttpClientFactory != null)
            throw new InvalidOperationException("Cannot specify both HttpClient and HttpClientFactory");

        if (EnableDebugMode && LoggerFactory == null)
            throw new InvalidOperationException("LoggerFactory must be provided when EnableDebugMode is true.");
    }
}
