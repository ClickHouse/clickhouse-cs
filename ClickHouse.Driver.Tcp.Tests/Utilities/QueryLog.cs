using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Tests.Integration;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Reads <c>system.query_log</c> from tests without racing the server. The native-transport counterpart of
/// <c>ClickHouse.Driver.Tests.QueryLog</c>, which takes the HTTP client and cannot be shared.
/// </summary>
/// <remarks>
/// <c>SYSTEM FLUSH LOGS</c> only flushes what the server has already queued, and a query's record is queued
/// independently of its response reaching the client — so a flush issued right after the query can miss it.
/// Every method here retries the flush-and-read, so assert on what they return rather than on a raw read taken
/// after a single flush.
/// <para>
/// The retry budget is larger than the HTTP helper's, because the queries this one waits on are cancelled ones:
/// the record is written when the server actually stops, which is after the client has already given up on it.
/// </para>
/// <para>
/// The flush names <c>query_log</c> rather than flushing everything: the framework suites share one server.
/// </para>
/// </remarks>
internal static class QueryLog
{
    /// <summary>Number of flush-and-read attempts before giving up.</summary>
    internal const int MaxAttempts = 40;

    /// <summary>
    /// Ignores the calling test where <c>system.query_log</c> cannot answer for the query under test. On a
    /// multi-replica service the table is local to each replica, and both the flush and the lookup go over
    /// whichever connection the pool hands out, so the record is written on one replica and looked for on
    /// another. Retrying cannot fix that, and a read that finds nothing is indistinguishable from a query that
    /// never ran.
    /// </summary>
    /// <remarks>
    /// Reading through <c>clusterAllReplicas</c>, with a flush on every replica, would make these assertions
    /// work on Cloud. It is not done here because the flush would have to reach every replica too, and the
    /// column one caller wants (<c>port</c>) means something different behind a load balancer.
    /// </remarks>
    private static void SkipWhereTheLogIsNotSharedAcrossReplicas()
    {
        if (TcpServerFixture.IsCloud)
        {
            Assert.Ignore("system.query_log is local to each replica on a Cloud service, so it cannot confirm a query that ran on another.");
        }
    }

    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Returns the first column of the first row of <paramref name="sql"/>, retrying while it matches no rows,
    /// and failing the test with a distinct message if it never does.
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
        SkipWhereTheLogIsNotSharedAcrossReplicas();

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

        // Only reached inside Assert.Multiple, where Assert.Fail records the failure and continues: throw rather
        // than hand the caller a null it would misread as a value.
        throw new InvalidOperationException(message);
    }

    // Drains the result rather than returning from inside the enumeration: abandoning it cancels the query and
    // drops the connection, so every retry would pay a reconnect.
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
