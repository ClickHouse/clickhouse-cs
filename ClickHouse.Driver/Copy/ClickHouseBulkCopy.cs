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

[Obsolete("The BulkCopy class functionality can now be found in ClickHouseClient. ClickHouseBulkCopy will be removed in a future version.")]
public class ClickHouseBulkCopy : IDisposable
{
    private readonly ClickHouseConnection connection;
    private readonly ClickHouseClient client;
    private readonly BatchSerializer batchSerializer;
    private readonly RowBinaryFormat rowBinaryFormat;
    private readonly bool ownsConnection;
    private long rowsWritten;

    public ClickHouseBulkCopy(ClickHouseConnection connection)
        : this(connection, RowBinaryFormat.RowBinary) { }

    public ClickHouseBulkCopy(string connectionString)
        : this(connectionString, RowBinaryFormat.RowBinary) { }

    public ClickHouseBulkCopy(ClickHouseConnection connection, RowBinaryFormat rowBinaryFormat)
    {
        this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        client = connection.ClickHouseClient;
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
    /// Gets total number of rows written by this instance.
    /// </summary>
    public long RowsWritten => Interlocked.Read(ref rowsWritten);

    /// <summary>
    /// One-time init operation to load column types using provided names.
    /// This method is now called automatically before the first WriteToServerAsync call.
    /// </summary>
    /// <returns>Awaitable task</returns>
    [Obsolete("InitAsync is no longer required and will be removed in a future version. Initialization now occurs automatically before the first write operation.")]
    public async Task InitAsync()
    {
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
        var options = new InsertOptions
        {
            BatchSize = BatchSize,
            MaxDegreeOfParallelism = MaxDegreeOfParallelism,
            Format = rowBinaryFormat,
        };

        await client.InsertBinaryAsync(
            DestinationTableName,
            ColumnNames,
            rows,
            options,
            onBatchSent: batchSize =>
            {
                var totalWritten = Interlocked.Add(ref rowsWritten, batchSize);
                BatchSent?.Invoke(this, new BatchSentEventArgs(totalWritten));
            },
            token).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (ownsConnection)
        {
            client?.Dispose();
            connection?.Dispose();
        }
        GC.SuppressFinalize(this);
    }
}
