using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Authentication;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Diagnostic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Client;

/// <summary>
/// <see cref="ClickHouseTcpClientOptions.CredentialsProvider"/> and <see cref="ClickHouseTcpClientOptions.BearerToken"/>
/// without a server: the factory against a socket server that only does the handshake, and the pool over connections
/// that carry credentials. JWT authentication needs ClickHouse Cloud; the password-rotation flow against a real server
/// is in <c>CredentialsProviderIntegrationTests</c>.
/// </summary>
[TestFixture]
public class CredentialsProviderTests
{
    private const int Hello = 0;
    private const int AuthenticationFailed = 516;
    private const string JwtMarker = " JWT AUTHENTICATION ";

    private static readonly CancellationToken None = CancellationToken.None;

    // Precedence: the provider, then BearerToken, then Username and Password. Only credentials from the provider are
    // tracked on the connection.
    private static IEnumerable<TestCaseData> HelloCases()
    {
        yield return new TestCaseData("static-token", false, null, JwtMarker, "static-token")
            .SetName("CreateAsync_BearerTokenOption_SendsTheJwtMarkerAndTheToken");
        yield return new TestCaseData("static-token", true, ClickHouseCredentials.CreateBearer("provided-token"), JwtMarker, "provided-token")
            .SetName("CreateAsync_ProviderReturnsABearerToken_SendsTheJwtMarkerAndTheProvidersToken");
        yield return new TestCaseData("static-token", true, ClickHouseCredentials.CreateBasic("bob", "new"), "bob", "new")
            .SetName("CreateAsync_ProviderReturnsBasicCredentials_SendsThemInsteadOfTheOptions");
        yield return new TestCaseData(null, true, null, "alice", "password")
            .SetName("CreateAsync_ProviderReturnsNull_SendsTheStaticCredentials");
    }

    [TestCaseSource(nameof(HelloCases))]
    public async Task CreateAsync_Credentials_SendsTheExpectedHello(
        string bearerToken,
        bool withProvider,
        ClickHouseCredentials provided,
        string expectedUsername,
        string expectedPassword)
    {
        await using var server = new HandshakeServer(Hello);
        ClickHouseTcpClientOptions options = Options(server) with
        {
            Username = "alice",
            Password = "password",
            BearerToken = bearerToken,
            CredentialsProvider = withProvider ? (_, _) => new(provided) : null,
        };
        using var factory = new TcpConnectionFactory(options);

        using ClickHouseTcpConnection connection = await factory.CreateAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(server.Hellos, Is.EqualTo(new[] { (expectedUsername, expectedPassword) }));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(connection.Credentials?.Credentials, Is.SameAs(provided));
        });
    }

    [Test]
    public async Task CreateAsync_HandshakeRejectsTheProvidersCredentials_ConnectsOnceMoreWithNewCredentials()
    {
        await using var server = new HandshakeServer(AuthenticationFailed, Hello);
        var reasons = new ConcurrentQueue<ClickHouseCredentialsRefreshReason>();
        using var logs = new CapturingLoggerFactory();
        ClickHouseTcpClientOptions options = Options(server) with
        {
            LoggerFactory = logs,
            CredentialsProvider = (request, _) =>
            {
                reasons.Enqueue(request.Reason);
                return new(ClickHouseCredentials.CreateBearer($"token-{reasons.Count}"));
            },
        };
        using var factory = new TcpConnectionFactory(options);

        using ClickHouseTcpConnection connection = await factory.CreateAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(server.Hellos, Is.EqualTo(new[] { (JwtMarker, "token-1"), (JwtMarker, "token-2") }));
            Assert.That(reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(connection.Credentials.Credentials.BearerToken, Is.EqualTo("token-2"));
            Assert.That(logs.Logger(ClickHouseTcpDiagnostics.ConnectionLogCategory).WithEventId(2004), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task CreateAsync_HandshakeRejectsTheNewCredentialsToo_ThrowsAfterOneMoreConnect()
    {
        await using var server = new HandshakeServer(AuthenticationFailed, AuthenticationFailed, Hello);
        int calls = 0;
        ClickHouseTcpClientOptions options = Options(server) with
        {
            CredentialsProvider = (_, _) =>
            {
                Interlocked.Increment(ref calls);
                return new(ClickHouseCredentials.CreateBearer("wrong"));
            },
        };
        using var factory = new TcpConnectionFactory(options);

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(async () => await factory.CreateAsync(None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.RawCode, Is.EqualTo(AuthenticationFailed));
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(server.Hellos, Has.Count.EqualTo(2));
        });
    }

    // No new connection when it would send the same credentials (the static ones), or when the error is not a
    // rejection of the credentials.
    [TestCase(AuthenticationFailed, ProviderResult.NoProvider, TestName = "CreateAsync_HandshakeRejectsTheStaticCredentials_DoesNotConnectAgain")]
    [TestCase(AuthenticationFailed, ProviderResult.Null, TestName = "CreateAsync_HandshakeRejectsTheStaticCredentialsTheProviderSelected_DoesNotConnectAgain")]
    [TestCase(192, ProviderResult.Bearer, TestName = "CreateAsync_HandshakeFailsWithAnotherErrorCode_DoesNotConnectAgain")]
    public async Task CreateAsync_HandshakeFails_DoesNotConnectAgain(int code, ProviderResult provider)
    {
        await using var server = new HandshakeServer(code, Hello);
        ClickHouseTcpClientOptions options = Options(server) with
        {
            CredentialsProvider = provider switch
            {
                ProviderResult.Null => (_, _) => new((ClickHouseCredentials)null),
                ProviderResult.Bearer => (_, _) => new(ClickHouseCredentials.CreateBearer("token")),
                _ => null,
            },
        };
        using var factory = new TcpConnectionFactory(options);

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(async () => await factory.CreateAsync(None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.RawCode, Is.EqualTo(code));
            Assert.That(server.Hellos, Has.Count.EqualTo(1));
        });
    }

    [TestCase(true, TestName = "CreateAsync_ProviderDoesNotAnswerWithinDialTimeout_ThrowsATimeoutThatNamesTheProvider")]
    [TestCase(false, TestName = "CreateAsync_ServerDoesNotAnswerAfterTheProvider_ThrowsATimeoutThatNamesTheConnect")]
    public async Task CreateAsync_DialTimeoutElapses_NamesTheStageThatTimedOut(bool providerHangs)
    {
        // Accepts and never replies, so a connect that gets past the provider waits for the server Hello.
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
        ClickHouseTcpClientOptions options = new()
        {
            Host = "127.0.0.1",
            Port = ((IPEndPoint)listener.LocalEndpoint).Port,
            DialTimeout = TimeSpan.FromMilliseconds(250),
            CredentialsProvider = async (_, cancellationToken) =>
            {
                if (providerHangs)
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }

                return ClickHouseCredentials.CreateBearer("token");
            },
        };

        try
        {
            using var factory = new TcpConnectionFactory(options);

            var thrown = Assert.ThrowsAsync<ClickHouseTcpConnectionException>(async () => await factory.CreateAsync(None));

            Assert.Multiple(() =>
            {
                Assert.That(thrown.InnerException, Is.TypeOf<TimeoutException>());
                Assert.That(thrown.Message, Does.Contain("DialTimeout"));
                Assert.That(thrown.Message, providerHangs ? Does.StartWith("Getting credentials from the CredentialsProvider") : Does.StartWith("Connecting to").And.Not.Contain("CredentialsProvider"));
            });
        }
        finally
        {
            listener.Stop();
            if (accepted.IsCompletedSuccessfully)
            {
                (await accepted).Dispose();
            }
        }
    }

    [Test]
    public async Task DisposeAsync_RentWaitsForTheProvider_ThrowsObjectDisposedAndCancelsTheProvider()
    {
        var providerCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var providerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ClickHouseTcpClientOptions options = new()
        {
            Host = "127.0.0.1",
            Port = 9,
            PoolTimeout = TimeSpan.FromSeconds(5),
            CredentialsProvider = async (_, cancellationToken) =>
            {
                using CancellationTokenRegistration registration = cancellationToken.Register(() => providerCancelled.TrySetResult());
                providerStarted.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken);
                return null;
            },
        };
        var pool = new ConnectionPool(options);
        ValueTask<IConnectionLease> rent = pool.RentAsync(None);
        await providerStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await pool.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await rent);

        // Disposing the pool disposes the factory, which cancels the provider call.
        await providerCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Test]
    public void CreateAsync_ProviderThrows_ThrowsTheProvidersException()
    {
        var failure = new InvalidOperationException("the token service is down");
        ClickHouseTcpClientOptions options = new()
        {
            Host = "127.0.0.1",
            Port = 9,
            CredentialsProvider = (_, _) => throw failure,
        };
        using var factory = new TcpConnectionFactory(options);

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await factory.CreateAsync(None));

        Assert.That(thrown, Is.SameAs(failure));
    }

    // Expiry at 60 s after the start: a connection is expired from 10 s before that.
    [TestCase(49, 1, TestName = "RentAsync_IdleConnectionWhoseCredentialsExpireInMoreThan10Seconds_ReusesIt")]
    [TestCase(50, 2, TestName = "RentAsync_IdleConnectionWhoseCredentialsExpireIn10Seconds_OpensANewOne")]
    public async Task RentAsync_IdleConnectionWithCredentialsExpiry(int secondsLater, int expectedConnections)
    {
        var time = new ControlledTimeProvider();
        var connections = new FakeConnectionFactory { Credentials = () => ExpiringCredentials(time, TimeSpan.FromSeconds(60)) };
        await using var pool = new ConnectionPool(new ClickHouseTcpClientOptions(), connections, time);
        await using (IConnectionLease first = await pool.RentAsync(None))
        {
        }

        time.Advance(TimeSpan.FromSeconds(secondsLater));
        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(connections.CreateCount, Is.EqualTo(expectedConnections));
            Assert.That(connections.Created[0].State, Is.EqualTo(expectedConnections == 1 ? TcpConnectionState.Ready : TcpConnectionState.Terminated));
        });
    }

    [Test]
    public async Task Sweep_IdleConnectionWhoseCredentialsExpired_ClosesIt()
    {
        var time = new ControlledTimeProvider();
        var connections = new FakeConnectionFactory { Credentials = () => ExpiringCredentials(time, TimeSpan.FromSeconds(60)) };
        await using var pool = new ConnectionPool(new ClickHouseTcpClientOptions(), connections, time);
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
        }

        time.Advance(TimeSpan.FromSeconds(50));
        pool.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero);
            Assert.That(connections.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated));
        });
    }

    // A connection without credentials from the provider is not affected by any expiry.
    [Test]
    public async Task Sweep_IdleConnectionWithStaticCredentials_KeepsIt()
    {
        var time = new ControlledTimeProvider();
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(new ClickHouseTcpClientOptions(), connections, time);
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
        }

        time.Advance(TimeSpan.FromSeconds(50));
        pool.Sweep();

        Assert.That(pool.IdleCount, Is.EqualTo(1));
    }

    [TestCase(AuthenticationFailed, true, true, 3008, TestName = "DisposeAsync_OperationFailedWith516_InvalidatesTheProvidersCredentials")]
    [TestCase(60, true, false, 3002, TestName = "DisposeAsync_OperationFailedWithAnotherError_KeepsTheProvidersCredentials")]
    [TestCase(AuthenticationFailed, false, false, 3002, TestName = "DisposeAsync_OperationFailedWith516OnStaticCredentials_OnlyDiscardsTheConnection")]
    public async Task DisposeAsync_OperationFailedWithAServerError(int code, bool fromProvider, bool expectInvalidated, int expectedEventId)
    {
        var reasons = new ConcurrentQueue<ClickHouseCredentialsRefreshReason>();
        using var cache = new CredentialsCache((request, _) =>
        {
            reasons.Enqueue(request.Reason);
            return new(ClickHouseCredentials.CreateBearer("token", DateTimeOffset.UtcNow.AddHours(1)));
        });
        ClickHouseCredentials used = await cache.GetAsync(None);
        using var logs = new CapturingLoggerFactory();
        var connections = new FakeConnectionFactory
        {
            Trailing = await ExceptionPacketAsync(code),
            Credentials = () => fromProvider ? new ConnectionCredentials(cache, used) : null,
        };
        var options = new ClickHouseTcpClientOptions { LoggerFactory = logs };
        await using var pool = new ConnectionPool(options, connections, new ControlledTimeProvider());

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
            {
                await foreach (Block block in lease.Connection.QueryAsync("SELECT 1", cancellationToken: None))
                {
                }
            });
        }

        ClickHouseCredentials next = await cache.GetAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero, "the failed operation terminated the connection");
            Assert.That(ReferenceEquals(next, used), Is.EqualTo(!expectInvalidated));
            Assert.That(reasons.Last(), Is.EqualTo(expectInvalidated ? ClickHouseCredentialsRefreshReason.Rejected : ClickHouseCredentialsRefreshReason.Initial));
            Assert.That(logs.Logger(ClickHouseTcpDiagnostics.PoolLogCategory).WithEventId(expectedEventId), Has.Count.EqualTo(1));
        });
    }

    public enum ProviderResult
    {
        NoProvider,
        Null,
        Bearer,
    }

    private static ClickHouseTcpClientOptions Options(HandshakeServer server)
        => new() { Host = "127.0.0.1", Port = server.Port, DialTimeout = TimeSpan.FromSeconds(10) };

    // Credentials with a known expiry, `lifetime` after the current time of `time`.
    private static ConnectionCredentials ExpiringCredentials(ControlledTimeProvider time, TimeSpan lifetime)
    {
        var cache = new CredentialsCache((_, _) => default);
        return new ConnectionCredentials(cache, ClickHouseCredentials.CreateBearer("token", time.GetUtcNow() + lifetime));
    }

    private static async Task<byte[]> ExceptionPacketAsync(int code)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(buffer))
        {
            writer.WriteVarUInt((ulong)ServerPacketType.Exception);
            writer.WriteInt32(code);
            writer.WriteString("DB::Exception");
            writer.WriteString("scripted failure");
            writer.WriteString("stack trace");
            writer.WriteBool(false); // has_nested
            await writer.FlushAsync(None);
        }

        return buffer.ToArray();
    }

    /// <summary>
    /// A native server that does only the handshake. For each connection it reads the ClientHello, records the
    /// Username and Password fields, and sends the next reply of its script: a server Hello (<see cref="Hello"/>), or
    /// an Exception with that error code. It keeps each connection open until it is disposed.
    /// </summary>
    private sealed class HandshakeServer : IAsyncDisposable
    {
        private readonly TcpListener listener = new(IPAddress.Loopback, 0);
        private readonly ConcurrentQueue<(string Username, string Password)> hellos = new();
        private readonly ConcurrentQueue<TcpClient> accepted = new();
        private readonly Task loop;

        internal HandshakeServer(params int[] replies)
        {
            listener.Start();
            loop = RunAsync(replies);
        }

        internal int Port => ((IPEndPoint)listener.LocalEndpoint).Port;

        internal IReadOnlyList<(string Username, string Password)> Hellos => hellos.ToArray();

        public async ValueTask DisposeAsync()
        {
            listener.Stop();
            await loop.ContinueWith(static _ => { }, TaskScheduler.Default);
            foreach (TcpClient client in accepted)
            {
                client.Dispose();
            }
        }

        private async Task RunAsync(int[] replies)
        {
            foreach (int reply in replies)
            {
                TcpClient client = await listener.AcceptTcpClientAsync();
                accepted.Enqueue(client);
                NetworkStream stream = client.GetStream();

                _ = await ReadVarUIntAsync(stream); // packet type
                _ = await ReadStringAsync(stream);  // client name
                _ = await ReadVarUIntAsync(stream); // version major
                _ = await ReadVarUIntAsync(stream); // version minor
                _ = await ReadVarUIntAsync(stream); // protocol revision
                _ = await ReadStringAsync(stream);  // database
                string username = await ReadStringAsync(stream);
                string password = await ReadStringAsync(stream);
                hellos.Enqueue((username, password));

                byte[] answer = reply == Hello
                    ? await FakeConnectionFactory.ServerHelloBytesAsync(None)
                    : await ExceptionPacketAsync(reply);
                await stream.WriteAsync(answer);
                await stream.FlushAsync();
            }
        }

        private static async Task<ulong> ReadVarUIntAsync(Stream stream)
        {
            ulong value = 0;
            var one = new byte[1];
            for (int shift = 0; ; shift += 7)
            {
                await stream.ReadExactlyAsync(one);
                value |= (ulong)(one[0] & 0x7F) << shift;
                if ((one[0] & 0x80) == 0)
                {
                    return value;
                }
            }
        }

        private static async Task<string> ReadStringAsync(Stream stream)
        {
            var bytes = new byte[(int)await ReadVarUIntAsync(stream)];
            await stream.ReadExactlyAsync(bytes);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
