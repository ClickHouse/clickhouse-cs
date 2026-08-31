namespace ClickHouse.Driver.Tcp.Tests.Exceptions;

// Only what a live server cannot be made to produce on demand: an error code this client does not name.
// Codes a real query does raise are covered in ClickHouseTcpExceptionIntegrationTests.
[TestFixture]
public class ClickHouseTcpServerExceptionTests
{
    private static ClickHouseTcpServerException Raised(int code) =>
        new(code, "DB::Exception", "message", "stack trace");

    [Test]
    public void Code_ServerSentACodeThisClientDoesNotName_ReadsAsUnknownWithTheRawValueKept()
    {
        var exception = Raised(65000);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Code, Is.EqualTo(ClickHouseErrorCode.Unknown));
            Assert.That(exception.RawCode, Is.EqualTo(65000));
            Assert.That(exception.ErrorCode, Is.EqualTo(65000));
        });
    }

    // The shapes a live server does not send: a message without the prefix, one that only looks like it carries
    // it, and an empty name. That the real prefix is present at all is asserted in the integration suite.
    [TestCase("DB::Exception", "DB::Exception: it failed", "it failed")]
    [TestCase("DB::Exception", "it failed", "it failed")]
    [TestCase("DB::Exception", "DB::Exception", "DB::Exception")]
    [TestCase("DB::Exception", "DB::ExceptionX: it failed", "DB::ExceptionX: it failed")]
    [TestCase("DB::NetException", "DB::Exception: it failed", "DB::Exception: it failed")]
    [TestCase("", "DB::Exception: it failed", "DB::Exception: it failed")]
    [TestCase(null, "DB::Exception: it failed", "DB::Exception: it failed")]
    public void Message_ServerRepeatedTheNameAtTheHeadOfTheText_ReportsTheTextWithoutIt(string name, string sent, string expected)
    {
        var exception = new ClickHouseTcpServerException(60, name, sent, "stack trace");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo(expected));
            Assert.That(exception.Name, Is.EqualTo(name), "the class name is still reported, just not twice.");
        });
    }
}
