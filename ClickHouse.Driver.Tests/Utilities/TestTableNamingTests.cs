using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Utilities;

/// <summary>
/// Guards the table-naming contract the rest of the suite relies on for isolation: every generated
/// name must be unique, legal as an unquoted ClickHouse identifier, and attributable to its test.
/// </summary>
public class TestTableNamingTests : AbstractConnectionTestFixture
{
    private static readonly Regex UniqueSuffix = new(@"_net\d+_[0-9a-f]{12}$", RegexOptions.Compiled);

    [Test]
    public void CreateTableName_WithNoArguments_UsesCallingMemberName()
    {
        var name = TestUtilities.CreateTableName();

        Assert.That(name, Does.StartWith($"test.{nameof(CreateTableName_WithNoArguments_UsesCallingMemberName)}_"));
    }

    [Test]
    public void CreateTableName_WithNoArguments_AppendsFrameworkAndRandomToken()
    {
        var name = TestUtilities.CreateTableName();

        Assert.That(UniqueSuffix.IsMatch(name), Is.True, $"'{name}' should end in _net<major>_<12 hex>");
    }

    // The moniker must track the compiled target framework, not Environment.Version, which reports the
    // *runtime* version: under roll-forward (a net6.0 build on the .NET 10 runtime, when 6.0 is absent)
    // every suite reports the same major and tables get attributed to the wrong one.
    // The expectation is spelled out per target framework on purpose — deriving it from
    // TargetFrameworkAttribute here would just re-read the same source the implementation uses, and
    // would pass either way.
    [Test]
    public void CreateTableName_WithNoArguments_UsesCompiledTargetFrameworkNotRuntimeVersion()
    {
#if NET6_0
        const string expected = "net6";
#elif NET8_0
        const string expected = "net8";
#elif NET9_0
        const string expected = "net9";
#elif NET10_0
        const string expected = "net10";
#else
#error Unhandled target framework — add it here and to the moniker expectation above.
#endif
        Assert.That(TestUtilities.CreateTableName(), Does.Contain($"_{expected}_"));
    }

    [Test]
    public void CreateTableName_CalledRepeatedly_ReturnsDistinctNames()
    {
        var names = Enumerable.Range(0, 1000).Select(_ => TestUtilities.CreateTableName()).ToList();

        Assert.That(names.Distinct(), Has.Exactly(names.Count).Items);
    }

    [Test]
    public void CreateTableName_WithDefaultDatabase_QualifiesWithTestDatabase()
    {
        Assert.That(TestUtilities.CreateTableName("t"), Does.StartWith("test."));
    }

    [Test]
    public void CreateTableName_WithExplicitDatabase_QualifiesWithThatDatabase()
    {
        Assert.That(TestUtilities.CreateTableName("t", database: "test_secondary"), Does.StartWith("test_secondary.t_"));
    }

    [Test]
    public void CreateTableName_WithNullDatabase_ReturnsUnqualifiedName()
    {
        var name = TestUtilities.CreateTableName("t", database: null);

        Assert.That(name, Does.Not.Contain("."));
        Assert.That(name, Does.StartWith("t_"));
    }

    // Prefixes carry parametrized case values (ClickHouse type names, timezone ids, column names),
    // which routinely contain characters that are illegal in an unquoted identifier.
    [TestCase("Nullable(Int32)", "NullableInt32")]
    [TestCase("Europe/Berlin", "EuropeBerlin")]
    [TestCase("with space", "withspace")]
    [TestCase("Array(Tuple(String, UInt8))", "ArrayTupleStringUInt8")]
    [TestCase("keep_underscores_1", "keep_underscores_1")]
    // Non-ASCII letters are rejected by the server in an unquoted identifier, even though
    // char.IsLetterOrDigit considers them letters.
    [TestCase("Çay", "ay")]
    [TestCase("emoji_🙂_tail", "emoji__tail")]
    public void CreateTableName_WithUnsafeCharactersInPrefix_StripsThem(string prefix, string expected)
    {
        Assert.That(TestUtilities.CreateTableName(prefix), Does.StartWith($"test.{expected}_net"));
    }

    [TestCase("()-/")]
    [TestCase("Добрый")]
    public void CreateTableName_WithPrefixThatSanitizesToNothing_FallsBackToPlaceholder(string prefix)
    {
        Assert.That(TestUtilities.CreateTableName(prefix), Does.StartWith("test.table_net"));
    }

    [Test]
    public void CreateTableName_WithOverlongPrefix_TruncatesPrefixTo80Characters()
    {
        var name = TestUtilities.CreateTableName(new string('a', 200));

        Assert.That(name, Does.StartWith("test." + new string('a', 80) + "_net"));
    }

    [TestCase("abc_123", "abc_123")]
    [TestCase("a.b", "ab")]
    [TestCase("", "")]
    [TestCase(null, "")]
    public void SanitizeTableName_WithVariousInput_KeepsOnlyIdentifierCharacters(string input, string expected)
    {
        Assert.That(TestUtilities.SanitizeTableName(input), Is.EqualTo(expected));
    }

    [TestCase("test.my_table", "my_table")]
    [TestCase("test_secondary.my_table", "my_table")]
    [TestCase("already_bare", "already_bare")]
    [TestCase(null, null)]
    public void BareTableName_WithVariousInput_StripsOnlyTheDatabaseQualifier(string input, string expected)
    {
        Assert.That(TestUtilities.BareTableName(input), Is.EqualTo(expected));
    }

    [Test]
    public void BareTableName_WithGeneratedName_RoundTripsAgainstCreateTableName()
    {
        var qualified = TestUtilities.CreateTableName("round_trip", database: "test_secondary");

        Assert.That(TestUtilities.BareTableName(qualified), Is.EqualTo(qualified["test_secondary.".Length..]));
    }

    // The whole scheme is worthless if the server rejects the names it produces, so exercise a
    // generated name against a real server rather than trusting the character filter.
    [Test]
    public async Task CreateTableName_WithAwkwardPrefix_ProducesNameTheServerAccepts()
    {
        var table = CreateTableName("Map(String, Array(Nullable(DateTime64(3, 'UTC'))))");

        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (value Int32) ENGINE Memory");
        await connection.ExecuteStatementAsync($"INSERT INTO {table} VALUES (42)");

        Assert.That(await connection.ExecuteScalarAsync($"SELECT value FROM {table}"), Is.EqualTo(42));
    }

    // Names handed out by the fixture must be dropped on teardown; without that, randomization would
    // trade flakiness for unbounded table growth in the shared `test` database.
    [Test]
    public async Task CreateTableName_OnFixtureTearDown_DropsRegisteredTables()
    {
        var fixture = new TearDownProbeFixture();
        var table = fixture.CreateProbeTable();

        await fixture.CreateAsync(table);
        Assert.That(await TableExistsAsync(table), Is.True, "table should exist before teardown");

        fixture.Dispose();

        Assert.That(await TableExistsAsync(table), Is.False, "teardown should have dropped the table");
    }

    private async Task<bool> TableExistsAsync(string qualifiedName)
    {
        var parts = qualifiedName.Split('.');
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("db", parts[0]);
        parameters.AddParameter("name", parts[1]);

        var count = await client.ExecuteScalarAsync(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {name:String}",
            parameters);

        return Convert.ToUInt64(count, CultureInfo.InvariantCulture) > 0;
    }

    /// <summary>
    /// Stands in for a real fixture so the teardown drop can be observed without ending this one.
    /// </summary>
    private sealed class TearDownProbeFixture : AbstractConnectionTestFixture
    {
        public string CreateProbeTable() => CreateTableName("teardown_probe");

        public Task CreateAsync(string table) =>
            connection.ExecuteStatementAsync($"CREATE TABLE {table} (value Int32) ENGINE Memory");
    }
}
