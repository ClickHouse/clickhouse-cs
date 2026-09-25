using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// Inserts into tables and columns whose names are legal ClickHouse identifiers but are not legal
/// unquoted ones, so they only reach the server correctly if the insert paths quote them.
/// </summary>
[TestFixture]
public class QuotedIdentifierInsertTests : AbstractConnectionTestFixture
{
    /// <summary>Quoted names this fixture created, dropped on teardown.</summary>
    private readonly ConcurrentQueue<string> quotedTables = new();

    /// <summary>
    /// Name shapes that require quoting. <c>{0}</c> is replaced with a unique token, so a shape can
    /// place the offending characters anywhere in the name.
    /// </summary>
    private static IEnumerable<TestCaseData> NameShapesRequiringQuoting()
    {
        yield return new TestCaseData("{0}-hyphen").SetName("{m}(hyphen)");
        yield return new TestCaseData("{0} space").SetName("{m}(space)");
        yield return new TestCaseData("{0}`backtick").SetName("{m}(backtick)");
    }

    private static string Quote(string identifier) => $"`{identifier.Replace("`", "\\`")}`";

    /// <summary>
    /// Creates a table whose name matches <paramref name="nameShape"/>.
    /// </summary>
    /// <returns>
    /// The database-qualified name as a caller passes it to the client (unquoted), and the quoted
    /// form for use in DDL and read-back queries.
    /// </returns>
    private async Task<(string table, string quoted)> CreateTableRequiringQuotingAsync(
        string nameShape, string columnDefinitions = "id UInt64, value String")
    {
        var unique = TestUtilities.BareTableName(TestUtilities.CreateTableName("quoted_identifier"));
        var bare = string.Format(nameShape, unique);
        var quoted = $"{Quote(TestUtilities.TestDatabase)}.{Quote(bare)}";

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {quoted} ({columnDefinitions}) ENGINE = MergeTree() ORDER BY id");
        quotedTables.Enqueue(quoted);

        return ($"{TestUtilities.TestDatabase}.{bare}", quoted);
    }

    [OneTimeTearDown]
    public void DropQuotedTables()
    {
        while (quotedTables.TryDequeue(out var quoted))
        {
            try
            {
                client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {quoted}").GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                TestContext.Progress.WriteLine($"Failed to drop test table {quoted}: {e.Message}");
            }
        }
    }

    [TestCaseSource(nameof(NameShapesRequiringQuoting))]
    public async Task InsertBinaryAsync_TableNameRequiringQuoting_ShouldInsertRows(string nameShape)
    {
        var (table, quoted) = await CreateTableRequiringQuotingAsync(nameShape);

        await client.InsertBinaryAsync(table, new[] { "id", "value" }, new[] { new object[] { 1UL, "a" } });

        Assert.That(await client.ExecuteScalarAsync($"SELECT value FROM {quoted}"), Is.EqualTo("a"));
    }

    /// <summary>
    /// A caller that worked around the missing quoting by pre-quoting the name must keep working:
    /// the name is quoted once, not twice.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_PreQuotedTableName_ShouldInsertRows()
    {
        var (_, quoted) = await CreateTableRequiringQuotingAsync("{0}-hyphen");

        await client.InsertBinaryAsync(quoted, new[] { "id", "value" }, new[] { new object[] { 1UL, "a" } });

        Assert.That(await client.ExecuteScalarAsync($"SELECT value FROM {quoted}"), Is.EqualTo("a"));
    }

    /// <summary>
    /// A caller that worked around the missing quoting with the double quotes ClickHouse also accepts
    /// around an identifier must keep working.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_TableNameInDoubleQuotes_ShouldInsertRows()
    {
        var (table, quoted) = await CreateTableRequiringQuotingAsync("{0}-hyphen");
        var bare = TestUtilities.BareTableName(table);

        await client.InsertBinaryAsync(
            $"\"{TestUtilities.TestDatabase}\".\"{bare}\"",
            new[] { "id", "value" },
            new[] { new object[] { 1UL, "a" } });

        Assert.That(await client.ExecuteScalarAsync($"SELECT value FROM {quoted}"), Is.EqualTo("a"));
    }

    /// <summary>
    /// With <see cref="InsertOptions.ColumnTypes"/> there is no schema probe, so the INSERT statement
    /// is the only place the table name is used.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_TableNameRequiringQuoting_WithColumnTypes_ShouldInsertRows()
    {
        var (table, quoted) = await CreateTableRequiringQuotingAsync("{0}-hyphen");
        var options = new InsertOptions
        {
            ColumnTypes = new Dictionary<string, string> { ["id"] = "UInt64", ["value"] = "String" },
        };

        await client.InsertBinaryAsync(
            table, new[] { "id", "value" }, new[] { new object[] { 1UL, "a" } }, options);

        Assert.That(await client.ExecuteScalarAsync($"SELECT value FROM {quoted}"), Is.EqualTo("a"));
    }

    [Test]
    public async Task InsertRawStreamAsync_TableNameRequiringQuoting_ShouldInsertRows()
    {
        var (table, quoted) = await CreateTableRequiringQuotingAsync("{0}-hyphen");

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("1\ta\n"));
        using var response = await client.InsertRawStreamAsync(table, stream, "TSV");

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(await client.ExecuteScalarAsync($"SELECT value FROM {quoted}"), Is.EqualTo("a"));
    }

    [Test]
    public async Task InsertRawStreamAsync_ColumnNameRequiringQuoting_ShouldInsertRows()
    {
        var (table, quoted) = await CreateTableRequiringQuotingAsync(
            "{0}-hyphen", "id UInt64, `value-column` String");

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("1\ta\n"));
        using var response = await client.InsertRawStreamAsync(
            table, stream, "TSV", columns: new[] { "id", "value-column" });

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(await client.ExecuteScalarAsync($"SELECT `value-column` FROM {quoted}"), Is.EqualTo("a"));
    }
}
