namespace ClickHouse.Driver.Tcp.Tests.Exceptions;

// Only what a live server cannot be made to produce on demand: an unnamed error code, and each arm of the
// transient table. Codes a real query does raise are covered in ClickHouseTcpExceptionIntegrationTests.
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
            Assert.That(exception.IsTransient, Is.False, "an unnamed code cannot be judged, so it is not retried.");
        });
    }

    [TestCase(ClickHouseErrorCode.TimeoutExceeded)]
    [TestCase(ClickHouseErrorCode.TooManySimultaneousQueries)]
    [TestCase(ClickHouseErrorCode.NoFreeConnection)]
    [TestCase(ClickHouseErrorCode.SocketTimeout)]
    [TestCase(ClickHouseErrorCode.NetworkError)]
    [TestCase(ClickHouseErrorCode.TooManyParts)]
    [TestCase(ClickHouseErrorCode.AllConnectionTriesFailed)]
    [TestCase(ClickHouseErrorCode.ServerOverloaded)]
    [TestCase(ClickHouseErrorCode.KeeperException)]
    public void IsTransient_LoadOrContentionCode_IsTrue(ClickHouseErrorCode code)
        => Assert.That(Raised((int)code).IsTransient, Is.True);

    // MemoryLimitExceeded and TooSlow look temporary but repeat for the same query at the same size, so
    // retrying unchanged just fails again.
    [TestCase(ClickHouseErrorCode.MemoryLimitExceeded)]
    [TestCase(ClickHouseErrorCode.TooSlow)]
    [TestCase(ClickHouseErrorCode.UnknownTable)]
    [TestCase(ClickHouseErrorCode.SyntaxError)]
    [TestCase(ClickHouseErrorCode.AuthenticationFailed)]
    [TestCase(ClickHouseErrorCode.QueryWasCancelled)]
    public void IsTransient_DeterministicFailure_IsFalse(ClickHouseErrorCode code)
        => Assert.That(Raised((int)code).IsTransient, Is.False);
}
