using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Utility;

public class StringExtensionsTests
{
    [TestCase("table", ExpectedResult = "`table`")]
    [TestCase("my-table", ExpectedResult = "`my-table`")]
    [TestCase("db.table", ExpectedResult = "`db`.`table`")]
    [TestCase("db.my-table", ExpectedResult = "`db`.`my-table`")]
    [TestCase("my-db.my-table", ExpectedResult = "`my-db`.`my-table`")]
    [TestCase("`my-table`", ExpectedResult = "`my-table`")]
    [TestCase("`db`.`table`", ExpectedResult = "`db`.`table`")]
    [TestCase("db.`my-table`", ExpectedResult = "`db`.`my-table`")]
    [TestCase("`my.table`", ExpectedResult = "`my.table`")]
    [TestCase("`db`.`my.table`", ExpectedResult = "`db`.`my.table`")]
    [TestCase("db.back`tick", ExpectedResult = "`db`.`back\\`tick`")]
    [TestCase("`back\\`tick`.table", ExpectedResult = "`back\\`tick`.`table`")]
    [TestCase("\"my-table\"", ExpectedResult = "\"my-table\"")]
    [TestCase("db.\"my.table\"", ExpectedResult = "`db`.\"my.table\"")]
    [TestCase("", ExpectedResult = "")]
    [TestCase(null, ExpectedResult = null)]
    public string EncloseQualifiedName_Name_ShouldEncloseEveryPartOnce(string name) =>
        name.EncloseQualifiedName();

    /// <summary>
    /// The three insert paths enclose independently, and the schema cache key is built from the same
    /// name, so enclosing an enclosed name must not add another layer of quoting.
    /// </summary>
    [TestCase("table")]
    [TestCase("my-table")]
    [TestCase("db.my-table")]
    [TestCase("`db`.`my.table`")]
    [TestCase("\"my-table\"")]
    public void EncloseQualifiedName_AlreadyEnclosedName_ShouldBeIdempotent(string name)
    {
        var enclosed = name.EncloseQualifiedName();

        Assert.That(enclosed.EncloseQualifiedName(), Is.EqualTo(enclosed));
    }
}
