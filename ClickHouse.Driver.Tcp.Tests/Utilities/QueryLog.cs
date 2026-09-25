using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>Reads <c>system.query_log</c> through the TCP client, retrying until the record is available.</summary>
/// <remarks>
/// Query log records may be queued after the response reaches the client, so a single flush can miss them.
/// Retries allow cancelled queries time to stop and produce a log record. Only <c>query_log</c> is flushed
/// because the framework suites share one server.
/// </remarks>
internal static class QueryLog
{
    /// <summary>Number of flush-and-read attempts before giving up.</summary>
    internal const int MaxAttempts = 40;

    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Returns the first column of the first matching row, retrying until a non-null value is available.
    /// Fails the test when the retry limit is reached.
    /// </summary>
    /// <remarks>
    /// A missing row and a NULL value are indistinguishable here, so select an expression that is never NULL for
    /// a row that exists.
    /// </remarks>
    /// <param name="client">Client to run the flush and the lookup on.</param>
    /// <param name="sql">Lookup returning one row with the value under test in its first column.</param>
    /// <returns>The value read once the row became visible.</returns>
    internal static async Task<object> ScalarAsync(ClickHouseTcpClient client, string sql)
    {
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            await client.ExecuteAsync("SYSTEM FLUSH LOGS query_log", cancellationToken: CancellationToken.None);

            object value = await ReadFirstAsync(client, sql);
            if (value is not null)
            {
                return value;
            }

            if (attempt < MaxAttempts)
            {
                await Task.Delay(RetryDelay);
            }
        }

        string message = $"No system.query_log row appeared after {MaxAttempts} flush attempts, so the value under test could not be determined. Query: {sql}";
        Assert.Fail(message);

        // Assert.Fail can return inside Assert.Multiple; throw to prevent returning a missing value.
        throw new InvalidOperationException(message);
    }

    // Consume the full response to keep the connection reusable between lookups.
    private static async Task<object> ReadFirstAsync(ClickHouseTcpClient client, string sql)
    {
        object first = null;
        await foreach (object[] row in client.QueryAsync(sql, cancellationToken: CancellationToken.None))
        {
            first ??= row[0];
        }

        return first;
    }
}
