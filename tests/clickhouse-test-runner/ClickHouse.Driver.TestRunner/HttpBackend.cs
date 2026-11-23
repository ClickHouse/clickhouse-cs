using System.Data.Common;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// The HTTP client, through its ADO.NET API: <see cref="ClickHouseConnection"/> from a connection string,
/// <see cref="ClickHouseCommand"/>, <see cref="DbCommand.ExecuteReaderAsync()"/> and
/// <see cref="DbCommand.ExecuteNonQueryAsync()"/>.
/// </summary>
/// <remarks>
/// All requests of the invocation have the same session id (<c>UseSession</c>, <c>SessionId</c>). The
/// driver sends the database of the connection with each request, and that database replaces the current
/// database of the session, so a <c>USE</c> calls <see cref="ClickHouseConnection.ChangeDatabase"/>. The
/// driver reads every result as <c>RowBinaryWithNamesAndTypes</c>, so the runner removes the FORMAT clause of
/// a query. Values: <see cref="ClickHouseDataReader.GetValue"/>; types:
/// <see cref="ClickHouseDataReader.GetDataTypeName"/>.
/// </remarks>
internal sealed class HttpBackend : IBackend
{
    private readonly ClickHouseConnection connection;

    public HttpBackend(Options opts, string sessionId, InvocationLog log)
    {
        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = opts.Host,
            Port = (ushort)HttpPort(opts.Port, opts.Secure),
            Protocol = opts.Secure ? "https" : "http",
            Username = opts.User,
            Password = opts.Password,
            Database = opts.Database,
            UseSession = true,
            SessionId = sessionId,
            // Keep the server order of map entries and every repeated key (the default Dictionary mode does
            // neither). A documented option of the client.
            MapReadMode = MapReadMode.KeyValuePairs,
            // String stays string (no ReadStringsAsByteArrays), so a String that is not UTF-8 reads with U+FFFD.
            // With byte[] strings, a String and an Array(UInt8) in a Dynamic or Variant value are the same .NET
            // value, and the client does not give the type of such a value (shim-http-string-bytes in
            // allowlist.txt).
            ReadStringsAsByteArrays = false,
            // tests/clickhouse-test stops a test after its own timeout; the default of the client is 2
            // minutes.
            Timeout = TimeSpan.FromSeconds(600),
        };
        connection = new ClickHouseConnection(builder.ConnectionString);
        log.Add("session_id", sessionId);
        log.Add("connection", builder.Host + ":" + builder.Port);
    }

    // The HTTP client keeps the session after an error.
    public bool SessionLost => false;

    /// <summary>
    /// Maps the port that tests/clickhouse-test gives (the native TCP port, from CLICKHOUSE_PORT_TCP) to the
    /// HTTP port that the runner exports. Any other port is used as given.
    /// </summary>
    public static int HttpPort(int port, bool secure)
    {
        var (nativeEnv, httpEnv, nativeDefault, httpDefault) = secure
            ? ("CLICKHOUSE_PORT_TCP_SECURE", "CLICKHOUSE_PORT_HTTPS", 9440, 8443)
            : ("CLICKHOUSE_PORT_TCP", "CLICKHOUSE_PORT_HTTP", 9000, 8123);
        var native = EnvInt(nativeEnv, nativeDefault);
        return port != 0 && port != native ? port : EnvInt(httpEnv, httpDefault);
    }

    public async Task OpenAsync() => await connection.OpenAsync().ConfigureAwait(false);

    public Task ReopenAsync() => Task.CompletedTask;

    public void UseDatabase(string database) => connection.ChangeDatabase(database);

    public async Task ExecuteAsync(string sql, QueryContext context)
    {
        await using var cmd = Command(sql, context);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task QueryAsync(string sql, OutputFormat format, ResultWriter writer, QueryContext context)
    {
        await using var cmd = Command(sql, context);
        await using var reader = (ClickHouseDataReader)await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        writer.Renderer.HttpServerTimezone = cmd.ServerTimezone;
        var count = reader.FieldCount;
        if (count == 0)
        {
            // A statement with no result block (an empty body).
            return;
        }
        var names = new string[count];
        var types = new string[count];
        for (var i = 0; i < count; i++)
        {
            names[i] = reader.GetName(i);
            types[i] = reader.GetDataTypeName(i);
        }
        writer.Columns(names, types);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            writer.Row(reader.GetValue);
        }
    }

    public ValueTask DisposeAsync()
    {
        ((IDisposable)connection).Dispose();
        return ValueTask.CompletedTask;
    }

    private static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : fallback;

    private ClickHouseCommand Command(string sql, QueryContext context)
    {
        var cmd = connection.CreateCommand(sql);
        foreach (var (name, value) in context.Settings)
        {
            cmd.CustomSettings[name] = value;
        }
        foreach (var (name, value) in context.Parameters)
        {
            cmd.AddParameter(name, value);
        }
        return cmd;
    }
}
