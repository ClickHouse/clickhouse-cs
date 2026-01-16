using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy.Serializer;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.IO;

namespace ClickHouse.Driver.Copy;

public class ClickHouseBulkCopy : IDisposable
{
    private static readonly RecyclableMemoryStreamManager CommonMemoryStreamManager = new(new RecyclableMemoryStreamManager.Options
    {
        MaximumLargePoolFreeBytes = 512 * 1024 * 1024,
        MaximumSmallPoolFreeBytes = 128 * 1024 * 1024,
        BlockSize = 256 * 1024,
    });

    private readonly ClickHouseConnection connection;
    private readonly BatchSerializer batchSerializer;
    private readonly RowBinaryFormat rowBinaryFormat;
    private readonly bool ownsConnection;
    private readonly RecyclableMemoryStreamManager memoryStreamManager;
    private long rowsWritten;
    private (string[] names, ClickHouseType[] types) columnNamesAndTypes;

    public ClickHouseBulkCopy(ClickHouseConnection connection)
        : this(connection, RowBinaryFormat.RowBinary) { }

    public ClickHouseBulkCopy(string connectionString)
        : this(connectionString, RowBinaryFormat.RowBinary) { }

    public ClickHouseBulkCopy(ClickHouseConnection connection, RowBinaryFormat rowBinaryFormat)
    {
        this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        this.rowBinaryFormat = rowBinaryFormat;
        batchSerializer = BatchSerializer.GetByRowBinaryFormat(rowBinaryFormat);
    }

    public ClickHouseBulkCopy(string connectionString, RowBinaryFormat rowBinaryFormat)
        : this(
            string.IsNullOrWhiteSpace(connectionString)
                ? throw new ArgumentNullException(nameof(connectionString))
                : new ClickHouseConnection(connectionString),
            rowBinaryFormat)
    {
        ownsConnection = true;
    }

    /// <summary>
    /// Bulk insert progress event.
    /// </summary>
    public event EventHandler<BatchSentEventArgs> BatchSent;

    /// <summary>
    /// Gets or sets size of batch in rows.
    /// </summary>
    public int BatchSize { get; set; } = 100000;

    /// <summary>
    /// Gets or sets maximum number of parallel processing tasks.
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = 4;

    /// <summary>
    /// Gets name of destination table to insert to.
    /// </summary>
    public string DestinationTableName { get; init; }

    /// <summary>
    /// Gets columns
    /// </summary>
    public IReadOnlyCollection<string> ColumnNames { get; init; }

    public sealed class BatchSentEventArgs : EventArgs
    {
        internal BatchSentEventArgs(long rowsWritten)
        {
            RowsWritten = rowsWritten;
        }

        public long RowsWritten
        {
            get;
        }
    }

    /// <summary>
    /// Gets RecyclableMemoryStreamManager used to create recyclable streams.
    /// </summary>
    public RecyclableMemoryStreamManager MemoryStreamManager
    {
        get { return memoryStreamManager ?? CommonMemoryStreamManager; }
        init { memoryStreamManager = value; }
    }

    /// <summary>
    /// Gets total number of rows written by this instance.
    /// </summary>
    public long RowsWritten => Interlocked.Read(ref rowsWritten);

    /// <summary>
    /// One-time init operation to load column types using provided names
    /// Required to call before WriteToServerAsync
    /// </summary>
    /// <returns>Awaitable task</returns>
    public async Task InitAsync()
    {
        if (DestinationTableName is null)
            throw new InvalidOperationException($"{nameof(DestinationTableName)} is null");

        var logger = connection.GetLogger(ClickHouseLogCategories.BulkCopy);
        logger?.LogDebug("Loading metadata for table {Table}.", DestinationTableName);

        columnNamesAndTypes = await LoadNamesAndTypesAsync(DestinationTableName, ColumnNames).ConfigureAwait(false);

        if (logger?.IsEnabled(LogLevel.Debug) ?? false)
        {
            logger.LogDebug("Metadata loaded for table {Table}. Columns: {Columns}.", DestinationTableName, string.Join(", ", columnNamesAndTypes.names ?? Array.Empty<string>()));
        }
    }

    public Task WriteToServerAsync(IDataReader reader) => WriteToServerAsync(reader, CancellationToken.None);

    public Task WriteToServerAsync(IDataReader reader, CancellationToken token)
    {
        if (reader is null)
            throw new ArgumentNullException(nameof(reader));

        return WriteToServerAsync(reader.AsEnumerable(), token);
    }

    public Task WriteToServerAsync(DataTable table, CancellationToken token)
    {
        if (table is null)
            throw new ArgumentNullException(nameof(table));

        var rows = table.Rows.Cast<DataRow>().Select(r => r.ItemArray);
        return WriteToServerAsync(rows, token);
    }

    public Task WriteToServerAsync(IEnumerable<object[]> rows) => WriteToServerAsync(rows, CancellationToken.None);

    public async Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token)
    {
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));

        var logger = connection.GetLogger(ClickHouseLogCategories.BulkCopy);

        if (string.IsNullOrWhiteSpace(DestinationTableName))
            throw new InvalidOperationException("Destination table not set");

        var (columnNames, columnTypes) = columnNamesAndTypes;
        if (columnNames == null || columnTypes == null)
            throw new InvalidOperationException("Column names not initialized. Call InitAsync once to load column data");

        var query = $"INSERT INTO {DestinationTableName} ({string.Join(", ", columnNames)}) FORMAT {rowBinaryFormat.ToString()}";

        var isDebugLoggingEnabled = logger?.IsEnabled(LogLevel.Debug) ?? false;
        Stopwatch stopwatch = null;
        if (isDebugLoggingEnabled)
        {
            stopwatch = Stopwatch.StartNew();
            logger.LogDebug("Starting bulk copy into {Table} with batch size {BatchSize} and degree {Degree}.", DestinationTableName, BatchSize, MaxDegreeOfParallelism);
        }

        var tasks = new Task[MaxDegreeOfParallelism];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.CompletedTask;
        }

        foreach (var batch in IntoBatches(rows, query, columnTypes))
        {
            while (true)
            {
                var completedTaskIndex = Array.FindIndex(tasks, t => t.IsCompleted);
                if (completedTaskIndex >= 0)
                {
                    tasks[completedTaskIndex] = SendBatchAsync(batch, token);
                    break; // while (true); go to next batch
                }
                else
                {
                    var completedTask = await Task.WhenAny(tasks).ConfigureAwait(false);
                    await completedTask.ConfigureAwait(false);
                }
            }
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        if (isDebugLoggingEnabled)
        {
            stopwatch.Stop();
            logger.LogDebug("Bulk copy into {Table} completed in {ElapsedMilliseconds:F2} ms. Total rows: {Rows}.", DestinationTableName, stopwatch.Elapsed.TotalMilliseconds, RowsWritten);
        }
    }

    private async Task<(string[] names, ClickHouseType[] types)> LoadNamesAndTypesAsync(string destinationTableName, IReadOnlyCollection<string> columns = null)
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {GetColumnsExpression(columns)} FROM {DestinationTableName} WHERE 1=0").ConfigureAwait(false);
        var types = reader.GetClickHouseColumnTypes();
        var names = reader.GetColumnNames().Select(c => c.EncloseColumnName()).ToArray();
        return (names, types);
    }

    private async Task SendBatchAsync(Batch batch, CancellationToken token)
    {
        var logger = connection.GetLogger(ClickHouseLogCategories.BulkCopy);

        using (batch) // Dispose object regardless whether sending succeeds
        {
            using var stream = MemoryStreamManager.GetStream(nameof(SendBatchAsync), 128 * 1024);
            // Async serialization
            await Task.Run(() => batchSerializer.Serialize(batch, stream), token).ConfigureAwait(false);

            // Seek to beginning as after writing it's at end
            stream.Seek(0, SeekOrigin.Begin);

            // Async sending
            logger?.LogDebug("Sending batch of {Rows} rows to {Table}.", batch.Size, DestinationTableName);
            await connection.PostStreamAsync(null, stream, true, token).ConfigureAwait(false);

            // Increase counter
            var batchRowsWritten = Interlocked.Add(ref rowsWritten, batch.Size);

            // Raise BatchSent event
            BatchSent?.Invoke(this, new BatchSentEventArgs(batchRowsWritten));

            logger?.LogDebug("Batch sent to {Table}. Total rows written: {TotalRows}.", DestinationTableName, batchRowsWritten);
        }
    }

    public void Dispose()
    {
        if (ownsConnection)
        {
            connection?.Dispose();
        }
        GC.SuppressFinalize(this);
    }

    private static string GetColumnsExpression(IReadOnlyCollection<string> columns) => columns == null || columns.Count == 0 ? "*" : string.Join(",", columns);

    private IEnumerable<Batch> IntoBatches(IEnumerable<object[]> rows, string query, ClickHouseType[] types)
    {
        foreach (var (batch, size) in rows.BatchRented(BatchSize))
        {
            yield return new Batch { Rows = batch, Size = size, Query = query, Types = types };
        }
    }
}
