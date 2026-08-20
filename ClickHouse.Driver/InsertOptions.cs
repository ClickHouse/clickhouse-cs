#nullable enable

using System.Collections.Generic;
using ClickHouse.Driver.Compression;
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
    /// Gets or sets the compressor applied to the binary insert body. Defaults to
    /// <see cref="ZstdCompressor.Default"/> (ZSTD at level <see cref="ZstdCompressor.DefaultLevel"/>).
    /// Set to <see langword="null"/> to send the RowBinary payload uncompressed — useful over a
    /// fast/local link where the compression CPU outweighs the bandwidth savings, or when a proxy
    /// already compresses on the client's behalf.
    /// Provide a configured <see cref="ZstdCompressor"/> (or another <see cref="IClickHouseCompressor"/>,
    /// e.g. <see cref="GZipCompressor"/>) to change the level, buffer size, or codec — note that
    /// unlike the <c>Accept-Encoding</c> the driver sends for responses, which the server is free to
    /// decline, a request body's <c>Content-Encoding</c> is a declaration: an intermediary that
    /// re-encodes request bodies and does not understand the codec fails the insert outright, so
    /// <c>gzip</c> remains the safer choice behind such a tier. This is independent of
    /// <see cref="ADO.ClickHouseClientSettings.UseCompression"/>, which governs query/response transport compression.
    /// </summary>
    public IClickHouseCompressor? Compressor { get; init; } = ZstdCompressor.Default;

    /// <summary>
    /// Gets or sets where the <c>INSERT INTO ... FORMAT ...</c> statement is sent. Defaults to
    /// <see cref="InsertQueryPlacement.Body"/>, which writes it ahead of the rows in the request body.
    /// Set <see cref="InsertQueryPlacement.Url"/> to send it as the <c>query</c> URL parameter instead,
    /// so that proxies, gateways and access logs can read it — they cannot see into the body, which
    /// <see cref="Compressor"/> compresses by default. The statement then counts towards the URL length:
    /// a long column list can exceed the server's <c>max_uri_size</c> (1 MiB by default), which the server
    /// rejects with <c>400 Bad Request</c>, or a lower limit of an intermediary. This is independent of
    /// <see cref="Compressor"/>, which governs the body encoding only.
    /// </summary>
    public InsertQueryPlacement QueryPlacement { get; init; } = InsertQueryPlacement.Body;

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
            ReadValueConverter = ReadValueConverter,
            MaxExecutionTime = MaxExecutionTime,
            AcceptEncoding = AcceptEncoding,
            BatchSize = BatchSize,
            MaxDegreeOfParallelism = MaxDegreeOfParallelism,
            Format = Format,
            Compressor = Compressor,
            QueryPlacement = QueryPlacement,
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
            ParameterTypeResolver = ParameterTypeResolver,
            ParameterFormatter = ParameterFormatter,
            ReadValueConverter = ReadValueConverter,
            MaxExecutionTime = MaxExecutionTime,
            AcceptEncoding = AcceptEncoding,
            BatchSize = BatchSize,
            MaxDegreeOfParallelism = MaxDegreeOfParallelism,
            Format = Format,
            Compressor = Compressor,
            QueryPlacement = QueryPlacement,
            ColumnTypes = columnTypes,
            UseSchemaCache = UseSchemaCache,
        };
    }
}
