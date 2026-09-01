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
}
