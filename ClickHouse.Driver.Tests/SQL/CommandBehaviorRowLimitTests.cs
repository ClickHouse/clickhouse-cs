using System.Data;
using System.Threading.Tasks;
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
}
