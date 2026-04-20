#nullable enable

using System.Collections.Generic;
using ClickHouse.Driver.Copy;

namespace ClickHouse.Driver;

/// <summary>
/// Options for binary insert operations that can override client-level defaults.
/// </summary>
public sealed class InsertOptions : QueryOptions
{
    /// <summary>
    /// Gets or sets the number of rows per batch. Default is 100,000.
    /// </summary>
    public int BatchSize { get; init; } = 100_000;

    /// <summary>
    /// Gets or sets the maximum number of parallel batch insert operations. Default is 1.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;

    /// <summary>
    /// Gets or sets the row binary format to use. Default is RowBinary.
    /// </summary>
    public RowBinaryFormat Format { get; init; } = RowBinaryFormat.RowBinary;

    /// <summary>
    /// Gets or sets explicit column type mappings (key: column name; value: ClickHouse type string).
    /// When set, the schema probe query (<c>SELECT ... WHERE 1=0</c>) is skipped entirely.
    /// Takes priority over <see cref="UseSchemaCache"/>.
    /// <br/>
    /// If this is used, a list of columns <b>must</b> be provided to InsertBinaryAsync().
    /// </summary>
    public IReadOnlyDictionary<string, string>? ColumnTypes { get; init; }

    /// <summary>
    /// Gets or sets whether to cache the table schema per (database, table) combination.
    /// When <c>true</c>, the full table schema is fetched once and reused for subsequent
    /// inserts on the same <see cref="ClickHouseClient"/> instance, regardless of which columns are selected.
    /// Schema changes (e.g. <c>ALTER TABLE</c>) are not detected while cached.
    /// </summary>
    public bool UseSchemaCache { get; init; }

    internal new InsertOptions WithQueryId(string queryId)
    {
        return new InsertOptions
        {
            QueryId = queryId,
            Database = Database,
            Roles = Roles,
            CustomSettings = CustomSettings,
            CustomHeaders = CustomHeaders,
            UseSession = UseSession,
            SessionId = SessionId,
            BearerToken = BearerToken,
            ParameterTypeResolver = ParameterTypeResolver,
            ParameterFormatter = ParameterFormatter,
            MaxExecutionTime = MaxExecutionTime,
            BatchSize = BatchSize,
            MaxDegreeOfParallelism = MaxDegreeOfParallelism,
            Format = Format,
            ColumnTypes = ColumnTypes,
            UseSchemaCache = UseSchemaCache,
        };
    }

    internal InsertOptions WithColumnTypes(IReadOnlyDictionary<string, string> columnTypes)
    {
        return new InsertOptions
        {
            QueryId = QueryId,
            Database = Database,
            Roles = Roles,
            CustomSettings = CustomSettings,
            CustomHeaders = CustomHeaders,
            UseSession = UseSession,
            SessionId = SessionId,
            BearerToken = BearerToken,
            ParameterFormatter = ParameterFormatter,
            MaxExecutionTime = MaxExecutionTime,
            BatchSize = BatchSize,
            MaxDegreeOfParallelism = MaxDegreeOfParallelism,
            Format = Format,
            ColumnTypes = columnTypes,
            UseSchemaCache = UseSchemaCache,
        };
    }
}
