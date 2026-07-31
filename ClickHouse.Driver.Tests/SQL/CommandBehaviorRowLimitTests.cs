using System.Data;
using System.Threading.Tasks;
using ClickHouse.Driver;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

public class CommandBehaviorRowLimitTests : AbstractConnectionTestFixture
{
    [TestCase("SELECT 13 AS a", TestName = "plain")]
    [TestCase("SELECT 13 AS a -- trailing comment", TestName = "line_comment")]
    [TestCase("SELECT 13 AS a # trailing comment", TestName = "hash_comment")]
    [TestCase("SELECT 13 AS a /* trailing comment */", TestName = "block_comment")]
    [TestCase("SELECT 13 AS a;", TestName = "semicolon")]
    [TestCase("SELECT 13 AS a; -- trailing comment", TestName = "semicolon_comment")]
    public async Task ExecuteReaderAsync_SchemaOnlyWithTrailingCommentOrSemicolon_ReturnsNoRows(string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly);

        var rows = 0;
        while (await reader.ReadAsync())
            rows++;
        Assert.That(rows, Is.Zero, "SchemaOnly must return no rows");
    }

    [TestCase("SELECT * FROM numbers(10)", TestName = "plain")]
    [TestCase("SELECT * FROM numbers(10) -- trailing comment", TestName = "line_comment")]
    [TestCase("SELECT * FROM numbers(10) # trailing comment", TestName = "hash_comment")]
    [TestCase("SELECT * FROM numbers(10);", TestName = "semicolon")]
    [TestCase("SELECT * FROM numbers(10); -- trailing comment", TestName = "semicolon_comment")]
    public async Task ExecuteReaderAsync_SingleRowWithTrailingCommentOrSemicolon_ReturnsOneRow(string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

        var rows = 0;
        while (await reader.ReadAsync())
            rows++;
        Assert.That(rows, Is.EqualTo(1), "SingleRow must return exactly one row");
    }

    [Test]
    public async Task ExecuteReaderAsync_SingleRowWithSemicolonInsideStringLiteral_PreservesValue()
    {
        // A ';' inside a string literal must not be treated as a trailing statement terminator.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT ';' AS a";

        using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader.GetValue(0), Is.EqualTo(";"));
        Assert.That(await reader.ReadAsync(), Is.False, "SingleRow must return exactly one row");
    }

    [Test]
    public async Task ExecuteReaderAsync_DefaultWithTrailingSemicolonAndComment_ReturnsAllRows()
    {
        // Contrast: CommandBehavior.Default appends no LIMIT and sends CommandText verbatim,
        // so a trailing semicolon/comment is accepted and every row is returned.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM numbers(10); -- trailing comment";

        using var reader = await command.ExecuteReaderAsync(CommandBehavior.Default);

        var rows = 0;
        while (await reader.ReadAsync())
            rows++;
        Assert.That(rows, Is.EqualTo(10), "Default must return all rows unaffected by row-limit handling");
    }

    [TestCase(CommandBehavior.Default, TestName = "default")]
    [TestCase(CommandBehavior.SchemaOnly, TestName = "schema_only")]
    [TestCase(CommandBehavior.SingleRow, TestName = "single_row")]
    public void ExecuteReaderAsync_NullCommandText_ReachesServerNotArgumentNull(CommandBehavior behavior)
    {
        // A null CommandText is normalized to an empty query on every behavior (as the previous
        // StringBuilder(CommandText) path did), so it is rejected server-side rather than throwing a
        // client-side ArgumentNullException when the null reaches StringContent. The no-LIMIT Default
        // path must keep this normalization; the SchemaOnly/SingleRow cases already had it.
        // The contract is "the request reaches the server", so we assert the exception type
        // (a server-side ClickHouseServerException, not a client-side ArgumentNullException). The
        // concrete server error code is intentionally not asserted: it varies by ClickHouse version
        // (observed 62 on 26.x, 354 on 25.8 for the empty-query Default path).
        using var command = connection.CreateCommand();
        command.CommandText = null;

        Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            using var reader = await command.ExecuteReaderAsync(behavior);
        });
    }

    [Test]
    public void ExecuteScalarAsync_NullCommandText_ReachesServerNotArgumentNull()
    {
        // ExecuteScalarAsync routes through the same no-LIMIT Default path, so a null CommandText must
        // reach the server as an empty query rather than throwing a client-side ArgumentNullException.
        // Only the exception type is asserted; the concrete server error code varies by version
        // (observed 62 on 26.x, 354 on 25.8 for the empty-query path).
        using var command = connection.CreateCommand();
        command.CommandText = null;

        Assert.ThrowsAsync<ClickHouseServerException>(async () => await command.ExecuteScalarAsync());
    }
}
