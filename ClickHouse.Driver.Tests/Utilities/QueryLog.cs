using System;
using System.Globalization;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Reads <c>system.query_log</c> from tests without racing the server.
/// </summary>
/// <remarks>
/// <c>SYSTEM FLUSH LOGS</c> only flushes what the server has already queued, and a query's
/// QueryFinish record is queued independently of its HTTP response reaching the client — so a flush
/// issued right after the query can miss it, and the lookup then matches fewer rows than the test
/// expects. Every method here retries the flush-and-read a few times so the record has a chance to
/// materialise. Assert on the value they return, never on a raw read taken straight after a single
/// flush.
/// <para>
/// Identify the query under test by <c>query_id</c> where possible. A lookup that instead matches a
/// marker embedded in the query text (<c>query LIKE '%marker%'</c>) also matches this helper's own
/// lookups, which carry the same marker — so exclude them, e.g. with
/// <c>AND query NOT LIKE '%system.query_log%'</c>.
/// </para>
/// </remarks>
public static class QueryLog
{
    /// <summary>Number of flush-and-read attempts before giving up.</summary>
    public const int MaxAttempts = 3;

    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Returns the first column of the first row of <paramref name="sql"/>, retrying while the
    /// query matches no rows, and failing the test with a distinct message if it never does.
    /// </summary>
    /// <remarks>
    /// A missing row and a NULL value are indistinguishable here, so select an expression that is
    /// never NULL for an existing row — e.g. <c>mapContains(Settings, 'name')</c> rather than
    /// <c>Settings['name']</c>, whose empty string for an absent key cannot be told apart from a
    /// row that has not been flushed yet.
    /// </remarks>
    /// <param name="client">Client to run the flush and the lookup on.</param>
    /// <param name="sql">Lookup returning one row with the value under test in its first column.</param>
    /// <param name="parameters">Parameters for <paramref name="sql"/>, if it is parameterized.</param>
    /// <returns>The value read once the row became visible.</returns>
    public static async Task<object> ScalarAsync(ClickHouseClient client, string sql, ClickHouseParameterCollection parameters = null)
    {
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            await FlushAsync(client);

            var value = await client.ExecuteScalarAsync(sql, parameters);

            // null means the lookup matched no rows, i.e. the record is not visible yet.
            if (value != null && value != DBNull.Value)
                return value;

            if (attempt < MaxAttempts)
                await Task.Delay(RetryDelay);
        }

        var message =
            $"No system.query_log row appeared after {MaxAttempts} flush attempts, so the value under " +
            $"test could not be determined. Query: {sql}";
        Assert.Fail(message);

        // Only reached inside Assert.Multiple, where Assert.Fail records the failure and continues:
        // throw rather than hand the caller a null it would misread as a value.
        throw new InvalidOperationException(message);
    }

    /// <summary>
    /// Returns the count produced by <paramref name="sql"/>, retrying while it stays below
    /// <paramref name="minimumCount"/>. The count reached on the last attempt is returned as-is, so
    /// a test may still assert that it is zero — waiting first only makes such an assertion stronger.
    /// </summary>
    /// <param name="client">Client to run the flush and the lookup on.</param>
    /// <param name="sql">Lookup returning a single count.</param>
    /// <param name="parameters">Parameters for <paramref name="sql"/>, if it is parameterized.</param>
    /// <param name="minimumCount">
    /// Number of rows the test expects, i.e. how many must show up before retrying is pointless.
    /// Leave at the default when asserting either "at least one" or "none at all".
    /// </param>
    /// <returns>The count reached once it satisfied <paramref name="minimumCount"/>, or the last one read.</returns>
    public static async Task<ulong> CountAsync(ClickHouseClient client, string sql, ClickHouseParameterCollection parameters = null, ulong minimumCount = 1)
    {
        var count = 0UL;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            await FlushAsync(client);

            count = Convert.ToUInt64(await client.ExecuteScalarAsync(sql, parameters), CultureInfo.InvariantCulture);
            if (count >= minimumCount)
                return count;

            if (attempt < MaxAttempts)
                await Task.Delay(RetryDelay);
        }

        return count;
    }

    private static Task FlushAsync(ClickHouseClient client) =>
        client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
}
