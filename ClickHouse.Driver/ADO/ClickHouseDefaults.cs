using System;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Provides default values for ClickHouse client settings
/// </summary>
public static class ClickHouseDefaults
{
    /// <summary>
    /// Default protocol for connections (http)
    /// </summary>
    public const string Protocol = "http";

    /// <summary>
    /// Default host for connections (localhost)
    /// </summary>
    public const string Host = "localhost";

    /// <summary>
    /// Default port for HTTP connections
    /// </summary>
    public const ushort HttpPort = 8123;

    /// <summary>
    /// Default port for HTTPS connections
    /// </summary>
    public const ushort HttpsPort = 8443;

    /// <summary>
    /// Default database name
    /// </summary>
    public const string Database = "";

    /// <summary>
    /// Default username
    /// </summary>
    public const string Username = "default";

    /// <summary>
    /// Default password (empty)
    /// </summary>
    public const string Password = "";

    /// <summary>
    /// Default path for connections (null/empty)
    /// </summary>
    public const string Path = null;

    /// <summary>
    /// Default compression setting (enabled)
    /// </summary>
    public const bool Compression = true;

    /// <summary>
    /// Default session usage setting (disabled)
    /// </summary>
    public const bool UseSession = false;

    /// <summary>
    /// Default server timezone usage setting (enabled)
    /// </summary>
    public const bool UseServerTimezone = true;

    /// <summary>
    /// Default custom decimals setting (enabled)
    /// </summary>
    public const bool UseCustomDecimals = true;

    /// <summary>
    /// Default setting for server certificate validation (false = validate certificates)
    /// </summary>
    public const bool SkipServerCertificateValidation = false;

    /// <summary>
    /// Default setting for form data parameters (disabled)
    /// </summary>
    public const bool UseFormDataParameters = false;

    /// <summary>
    /// Default timeout for operations (2 minutes)
    /// </summary>
    public static readonly TimeSpan Timeout = TimeSpan.FromMinutes(2);
}
