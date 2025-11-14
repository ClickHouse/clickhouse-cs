namespace ClickHouse.Driver.Diagnostic;

public static class ClickHouseDiagnosticsOptions
{
    public const string ActivitySourceName = "ClickHouse.Driver";

    /// <summary>
    /// Gets or sets a value indicating whether to include SQL statements in OpenTelemetry activity tags.
    /// When false, the db.statement tag will be omitted. Default: false.
    /// </summary>
    public static bool IncludeSqlInActivityTags { get; set; }

    /// <summary>
    /// Gets or sets maximum length of SQL statements included in activity tags. Default: 300 characters.
    /// </summary>
    public static int StatementMaxLength { get; set; } = 300;
}
