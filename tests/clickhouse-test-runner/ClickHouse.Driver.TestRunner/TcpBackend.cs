using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// The native TCP client: <see cref="ClickHouseTcpClient"/> from a connection string, and one
/// <see cref="IClickHouseTcpSession"/> (one pinned connection) for the invocation, so that <c>SET</c>,
/// <c>USE</c> and temporary tables last.
/// </summary>
/// <remarks>
/// Statements that return rows run with <see cref="IClickHouseTcpOperations.StreamAsync"/>; the shim reads
/// each value with <see cref="IColumn.GetValue"/> (what <c>QueryAsync</c> gives per row) and the type with
/// <see cref="IColumn.TypeName"/>. Other statements run with <see cref="IClickHouseTcpOperations.ExecuteAsync"/>.
/// The pool has one connection (<c>MaxPoolSize=1</c>), so the client cannot open a second one without the
/// shim seeing it. Totals and extremes come from <see cref="ClickHouseTcpQueryCallbacks"/>.
/// </remarks>
internal sealed class TcpBackend : IBackend
{
    private readonly ClickHouseTcpClient client;
    private readonly InvocationLog log;
    private IClickHouseTcpSession? session;

    public TcpBackend(Options opts, InvocationLog log)
    {
        this.log = log;
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = opts.Host,
            Port = opts.Port != 0 ? opts.Port : null,
            Username = opts.User,
            Password = opts.Password,
            Database = opts.Database,
            UseTls = opts.Secure,
            MaxPoolSize = 1,
            // tests/clickhouse-test stops a test after its own timeout.
            ReadTimeout = TimeSpan.FromSeconds(600),
        };
        client = new ClickHouseTcpClient(builder.ConnectionString);
        log.Add("connection", builder.Host + ":" + (builder.Port?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "default"));
    }

    /// <summary>Gets the time zone of the server, from the handshake.</summary>
    public TimeZoneInfo ServerTimeZone { get; private set; } = TimeZoneInfo.Utc;

    public bool SessionLost => session is { IsOpen: false };

    public async Task OpenAsync()
    {
        session = await client.OpenSessionAsync().ConfigureAwait(false);
        var info = await session.GetServerInfoAsync().ConfigureAwait(false);
        if (!string.IsNullOrEmpty(info.Timezone))
        {
            ServerTimeZone = ValueRenderer.FindZone(info.Timezone);
        }
    }

    public async Task ReopenAsync()
    {
        log.Add("session", "the session is closed; opening a new one");
        if (session is not null)
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
        session = await client.OpenSessionAsync().ConfigureAwait(false);
    }

    // The session holds the current database.
    public void UseDatabase(string database)
    {
    }

    public async Task ExecuteAsync(string sql, QueryContext context) =>
        await Session.ExecuteAsync(sql, QueryOptions(context, null)).ConfigureAwait(false);

    public async Task QueryAsync(string sql, OutputFormat format, ResultWriter writer, QueryContext context)
    {
        // The totals and the extremes come in callbacks, as borrowed blocks; render them at once, and write
        // them after the data.
        var plain = format with { Names = false, Types = false };
        Utf8Buffer? totals = null, extremes = null;
        Exception? callbackError = null;
        void Capture(Block block, ref Utf8Buffer? into)
        {
            try
            {
                into ??= new Utf8Buffer();
                WriteBlock(new ResultWriter(into, plain, writer.Renderer), block);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                callbackError ??= e;
            }
        }
        var callbacks = new ClickHouseTcpQueryCallbacks
        {
            OnTotals = block => Capture(block, ref totals),
            OnExtremes = block => Capture(block, ref extremes),
        };
        await foreach (var block in Session.StreamAsync(sql, QueryOptions(context, callbacks)).ConfigureAwait(false))
        {
            WriteBlock(writer, block);
        }
        if (callbackError is not null)
        {
            throw callbackError;
        }
        // `WITH TOTALS`: an empty line, then the totals row. `extremes = 1`: an empty line, then the minimum and
        // the maximum row.
        if (totals is not null)
        {
            writer.Separator();
            writer.Append(totals);
        }
        if (extremes is not null)
        {
            writer.Separator();
            writer.Append(extremes);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (session is not null)
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
        await client.DisposeAsync().ConfigureAwait(false);
    }

    private static void WriteBlock(ResultWriter writer, Block block)
    {
        var columns = block.Columns;
        writer.Columns(block.ColumnNames, columns.Select(c => c.TypeName).ToArray());
        for (var row = 0; row < block.RowCount; row++)
        {
            var r = row;
            writer.Row(i => columns[i].GetValue(r), i => columns[i], r);
        }
    }

    private static ClickHouseTcpQueryOptions QueryOptions(QueryContext context, ClickHouseTcpQueryCallbacks? callbacks)
    {
        ClickHouseTcpParameterCollection? parameters = null;
        if (context.Parameters.Count > 0)
        {
            parameters = new ClickHouseTcpParameterCollection();
            foreach (var (name, value) in context.Parameters)
            {
                parameters.Add(name, value);
            }
        }
        return new ClickHouseTcpQueryOptions
        {
            Settings = context.Settings.Count > 0 ? context.Settings : null,
            Parameters = parameters,
            Callbacks = callbacks,
        };
    }

    private IClickHouseTcpSession Session => session ?? throw new InvalidOperationException("the session is not open");
}
