using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Utility;

/// <summary>
/// Resolves table schema (column names and types) for <see cref="ClickHouseClient.InsertBinaryAsync"/>.
/// Supports three strategies, evaluated in priority order:
/// <list type="number">
///   <item>User-provided schema via <see cref="InsertOptions.ColumnTypes"/> (no query).</item>
///   <item>Cached schema via <see cref="InsertOptions.UseSchemaCache"/> (one <c>SELECT *</c> per table).</item>
///   <item>Default: a <c>SELECT … WHERE 1=0</c> probe query on every call.</item>
/// </list>
/// </summary>
internal class SchemaResolver
{
    /// <summary>
    /// Cached table schema: stores the server-ordered column arrays (for the all-columns case)
    /// plus a dictionary for O(1) lookups when filtering to a column subset.
    /// </summary>
    private readonly ConcurrentDictionary<string, CachedSchema> cache = new();
    private readonly ClickHouseClient client;

    internal SchemaResolver(ClickHouseClient client)
    {
        this.client = client;
    }

    /// <summary>
    /// Resolves the column names and types for the given table and column set,
    /// using the strategy determined by <paramref name="options"/>.
    /// </summary>
    /// <param name="table">The destination table name (without database prefix).</param>
    /// <param name="columns">The columns to insert, or <c>null</c> for all columns.</param>
    /// <param name="options">Insert options controlling schema resolution strategy.</param>
    /// <returns>Enclosed column names and their parsed <see cref="ClickHouseType"/> instances.</returns>
    internal async Task<(string[] names, ClickHouseType[] types)> ResolveAsync(
        string table, IEnumerable<string> columns, InsertOptions options)
    {
        // Priority 1: User-provided schema
        if (options.ColumnTypes is { Count: > 0 })
        {
            return BuildFromColumnTypes(columns, options.ColumnTypes);
        }

        // Priority 2: Cached schema — fetches SELECT * once per table, filters to requested columns
        if (options.UseSchemaCache)
        {
            var cacheKey = BuildCacheKey(table, options);
            if (!cache.TryGetValue(cacheKey, out var cached))
            {
                var (names, types) = await LoadAsync(table, options).ConfigureAwait(false);
                cached = new CachedSchema(names, types);
                cache.TryAdd(cacheKey, cached);
            }

            return cached.Select(columns);
        }

        // Priority 3: Default, query every time
        return await LoadAsync(table, options, columns).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a <c>SELECT … FROM table WHERE 1=0</c> probe query to retrieve column metadata
    /// from the server without transferring any row data.
    /// </summary>
    private async Task<(string[] names, ClickHouseType[] types)> LoadAsync(
        string table, QueryOptions options, IEnumerable<string> columns = null)
    {
        var columnsExpr = columns == null || !columns.Any() ? "*" : string.Join(",", columns);
        using var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(
            $"SELECT {columnsExpr} FROM {table} WHERE 1=0", null, options).ConfigureAwait(false);
        var types = reader.GetClickHouseColumnTypes();
        var names = reader.GetColumnNames().Select(c => c.EncloseColumnName()).ToArray();
        return (names, types);
    }

    /// <summary>
    /// Builds the schema from user-provided <see cref="InsertOptions.ColumnTypes"/> by parsing
    /// each ClickHouse type string (e.g. <c>"UInt64"</c>, <c>"Nullable(String)"</c>).
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="columns"/> is null/empty, or a column is missing from <paramref name="columnTypes"/>.
    /// </exception>
    private (string[] names, ClickHouseType[] types) BuildFromColumnTypes(
        IEnumerable<string> columns, IReadOnlyDictionary<string, string> columnTypes)
    {
        var columnList = columns?.ToArray()
            ?? throw new ArgumentException("columns must be specified when ColumnTypes is provided");

        if (columnList.Length == 0)
            throw new ArgumentException("columns must be specified when ColumnTypes is provided");

        var names = new string[columnList.Length];
        var types = new ClickHouseType[columnList.Length];

        for (var i = 0; i < columnList.Length; i++)
        {
            if (!columnTypes.TryGetValue(columnList[i], out var typeStr))
                throw new ArgumentException($"ColumnTypes does not contain an entry for column '{columnList[i]}'");

            names[i] = columnList[i].EncloseColumnName();
            types[i] = TypeConverter.ParseClickHouseType(typeStr, client.TypeSettings);
        }

        return (names, types);
    }

    /// <summary>
    /// Builds a cache key from the resolved database and table name.
    /// The database is resolved from <see cref="InsertOptions.Database"/> first,
    /// falling back to <see cref="ADO.ClickHouseClientSettings.Database"/>.
    /// </summary>
    private string BuildCacheKey(string table, InsertOptions options)
    {
        var database = options.Database ?? client.Settings.Database ?? string.Empty;
        return $"`{database}`.`{table}`";
    }

    /// <summary>
    /// Immutable snapshot of a table's full schema, preserving the server-reported column order.
    /// Stores both ordered arrays (for the all-columns case) and a dictionary (for column subset lookups).
    /// </summary>
    private sealed class CachedSchema
    {
        private readonly string[] names;
        private readonly ClickHouseType[] types;
        private readonly Dictionary<string, int> index;

        internal CachedSchema(string[] names, ClickHouseType[] types)
        {
            this.names = names;
            this.types = types;
            index = new Dictionary<string, int>(names.Length, StringComparer.Ordinal);
            for (var i = 0; i < names.Length; i++)
                index[names[i]] = i;
        }

        /// <summary>
        /// Returns the schema filtered to the requested columns, preserving their order.
        /// When <paramref name="columns"/> is null or empty, returns all columns in server order.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when a requested column is not present in the schema.</exception>
        internal (string[] names, ClickHouseType[] types) Select(IEnumerable<string> columns)
        {
            if (columns == null || !columns.Any())
                return (this.names, this.types);

            var columnList = columns.ToArray();
            var selectedNames = new string[columnList.Length];
            var selectedTypes = new ClickHouseType[columnList.Length];

            for (var i = 0; i < columnList.Length; i++)
            {
                var enclosed = columnList[i].EncloseColumnName();
                if (!index.TryGetValue(enclosed, out var idx))
                    throw new ArgumentException($"Column '{columnList[i]}' not found in table schema");

                selectedNames[i] = enclosed;
                selectedTypes[i] = this.types[idx];
            }

            return (selectedNames, selectedTypes);
        }
    }
}
