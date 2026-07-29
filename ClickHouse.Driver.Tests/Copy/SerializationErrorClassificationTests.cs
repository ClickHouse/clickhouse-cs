using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// End-to-end coverage of how a failure during a binary insert is classified.
/// </summary>
/// <remarks>
/// Binary inserts serialize the payload directly into the HTTP request body, so a transport-level
/// write failure throws from inside <c>serializer.Serialize</c> and gets wrapped into a
/// <see cref="ClickHouseBulkCopySerializationException"/> with the failing row attached. The client
/// must unwrap those, otherwise a transport error is misreported as a serialization error and row
/// contents leak into the surfaced exception (and any logs built from it). Conversely a genuine
/// serialization fault must still surface as <see cref="ClickHouseBulkCopySerializationException"/>
/// even though the request is already in flight and the server will also fail the (now truncated)
/// insert.
/// <para>
/// Both insert paths are exercised (<c>object[]</c> via <see cref="InsertPath.ObjectArray"/> and POCO
/// via <see cref="InsertPath.Poco"/>) because each has its own serialization-error capture site.
/// </para>
/// </remarks>
[TestFixture]
public class SerializationErrorClassificationTests : AbstractConnectionTestFixture
{
    public enum InsertPath
    {
        ObjectArray,
        Poco,
    }

    public enum RequestStreamFault
    {
        Io,
        Disposed,
        Canceled,
    }

    // Below the ClickHouse DateTime lower bound (1970-01-01), so the client-side range check fails.
    private static readonly DateTime OutOfRangeValue = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static readonly string[] DateColumns = { "Id", "Value" };

    private static readonly string[] PayloadColumns = { "Id", "Payload" };

    // Supplying the schema up front skips the "SELECT ... WHERE 1=0" probe, so the only request the
    // client makes is the insert itself. That is what lets the fake endpoints below get away with
    // speaking no ClickHouse protocol at all, and lets the server-error test fail on the INSERT
    // rather than on the probe.
    private static readonly IReadOnlyDictionary<string, string> DateColumnTypes = new Dictionary<string, string>
    {
        ["Id"] = "UInt64",
        ["Value"] = "DateTime",
    };

    private static readonly IReadOnlyDictionary<string, string> PayloadColumnTypes = new Dictionary<string, string>
    {
        ["Id"] = "UInt64",
        ["Payload"] = "String",
    };

    private static readonly string PayloadText = new('x', 1024);

    // Enough payload (~16 MB uncompressed) that the client is guaranteed to still be writing the
    // request body when the peer resets or the caller cancels.
    private const int LargePayloadRowCount = 16 * 1024;

    // Only ever appears in the INSERT text sent to the fake endpoints below, which parse nothing, so
    // no such table is ever created. Named through the static helper anyway rather than hard-coded -
    // the fixture's tracking overload would only queue a pointless DROP for a table that never exists.
    private static readonly string FakeEndpointTable = TestUtilities.CreateTableName("serialization_error_fake_endpoint");

    private class DateRow
    {
        public ulong Id { get; set; }

        public DateTime Value { get; set; }
    }

    private class PayloadRow
    {
        public ulong Id { get; set; }

        public string Payload { get; set; }
    }

    [OneTimeSetUp]
    public void RegisterPocoTypes()
    {
        client.RegisterBinaryInsertType<DateRow>();
        client.RegisterBinaryInsertType<PayloadRow>();
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public async Task InsertBinaryAsync_WithOutOfRangeValueAfterValidRows_ThrowsSerializationExceptionWithFailingRow(InsertPath path)
    {
        var table = CreateTableName($"outofrange_{path}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (Id UInt64, Value DateTime) ENGINE = Memory");

        // Enough serializable rows precede the bad one (~240 KB uncompressed, far past the
        // connection's write buffer) that the server has really received payload by the time
        // serialization fails, so the insert dies mid-request rather than before it starts.
        var baseValue = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var rows = Enumerable
            .Range(0, 20_000)
            .Select(i => new DateRow { Id = (ulong)i, Value = baseValue.AddSeconds(i) })
            .Append(new DateRow { Id = ulong.MaxValue, Value = OutOfRangeValue })
            .ToList();

        var ex = Assert.CatchAsync<ClickHouseBulkCopySerializationException>(
            () => InsertDateRowsAsync(client, path, table, rows, Uncompressed()));

        Assert.Multiple(() =>
        {
            // The client-side fault wins over the server's "truncated body" error, which would
            // otherwise be all the caller sees.
            Assert.That(ex.InnerException, Is.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(ex.InnerException?.Message, Does.Contain("ClickHouse DateTime"));
            Assert.That(ex.Row, Is.Not.Null);
            Assert.That(ex.Row[1], Is.EqualTo(OutOfRangeValue));
        });

        // A truncated insert commits nothing, so none of the rows the server did receive land.
        var count = Convert.ToInt64(await client.ExecuteScalarAsync($"SELECT count() FROM {table}"));
        Assert.That(count, Is.Zero);
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public void InsertBinaryAsync_WhenServerRejectsInsert_SurfacesServerErrorUntouched(InsertPath path)
    {
        // Nothing fails client-side here, so no serialization error is captured and the server's error
        // must propagate as-is (the "no captured error" branch of the classification).
        //
        // The static helper, not the fixture's tracking overload: this table is deliberately never
        // created, so there is nothing for the fixture to drop.
        var table = TestUtilities.CreateTableName($"missing_{path}");
        var rows = new List<DateRow>
        {
            new() { Id = 1, Value = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        };

        var ex = Assert.CatchAsync<ClickHouseServerException>(
            () => InsertDateRowsAsync(client, path, table, rows, Uncompressed(DateColumnTypes)));

        // The server reports the unqualified name, so compare against that.
        Assert.That(ex.Message, Does.Contain(TestUtilities.BareTableName(table)));
    }

    // The shape a torn-down request stream surfaces as depends on where in the write it is torn down
    // and on HttpClient internals, so it is injected at the request-stream boundary here - exactly
    // where a real one originates - to pin each shape deterministically. The socket-level tests below
    // cover the same contract without injection, at the cost of not choosing the shape.
    [TestCase(InsertPath.ObjectArray, RequestStreamFault.Io)]
    [TestCase(InsertPath.ObjectArray, RequestStreamFault.Disposed)]
    [TestCase(InsertPath.ObjectArray, RequestStreamFault.Canceled)]
    [TestCase(InsertPath.Poco, RequestStreamFault.Io)]
    [TestCase(InsertPath.Poco, RequestStreamFault.Disposed)]
    [TestCase(InsertPath.Poco, RequestStreamFault.Canceled)]
    public void InsertBinaryAsync_WhenRequestStreamFailsMidPayload_SurfacesTransportErrorWithoutRow(
        InsertPath path, RequestStreamFault fault)
    {
        var injected = CreateFault(fault);

        // Faulting after the first KB puts the failure inside the serializer's row loop, i.e. where it
        // gets wrapped with the failing row.
        using var httpClient = new HttpClient(new FaultingRequestStreamHandler(injected, bytesBeforeFault: 1024));
        using var faultingClient = CreateClient(httpClient);

        var ex = Assert.CatchAsync(() => InsertPayloadRowsAsync(faultingClient, path, rowCount: 64));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.SameAs(injected));
            AssertNoSerializationErrorInChain(ex);
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public void InsertBinaryAsync_WhenRequestStreamFailsBeforeFirstRow_SurfacesTransportErrorUnwrapped(InsertPath path)
    {
        // The INSERT query line is written before the serializer's try block, so a failure there is not
        // wrapped in the first place; this guards against future wrapping being introduced around it,
        // not against the unwrapping being removed. A real peer cannot be made to trigger it reliably:
        // the query line is small enough to disappear into the socket send buffer, so the write
        // succeeds locally even against a dead connection.
        var injected = new IOException("proxy aborted the upload");

        using var httpClient = new HttpClient(new FaultingRequestStreamHandler(injected, bytesBeforeFault: 0));
        using var faultingClient = CreateClient(httpClient);

        var ex = Assert.CatchAsync(() => InsertPayloadRowsAsync(faultingClient, path, rowCount: 64));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.SameAs(injected));
            AssertNoSerializationErrorInChain(ex);
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public void InsertBinaryAsync_WhenPeerResetsConnectionMidUpload_SurfacesTransportErrorNotSerializationError(InsertPath path)
    {
        using var server = new UnresponsiveServer(UnresponsiveServer.Behavior.ResetMidUpload);
        using var abortedClient = CreateClient(server);

        var ex = Assert.CatchAsync(() => InsertPayloadRowsAsync(abortedClient, path, LargePayloadRowCount));

        Assert.Multiple(() =>
        {
            // The peer only resets after reading this much, so it proves the reset landed mid-payload
            // rather than before the request left the client.
            Assert.That(server.BytesReceived, Is.GreaterThanOrEqualTo(UnresponsiveServer.ObservedUploadBytes));
            AssertTransportFailure(ex);
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public void InsertBinaryAsync_WhenClientTimesOutMidUpload_SurfacesTransportErrorNotSerializationError(InsertPath path)
    {
        // The peer stops reading, so the write blocks and the client's own timeout tears the request
        // stream down under the serializer - the other way a real insert dies mid-payload.
        using var server = new UnresponsiveServer(UnresponsiveServer.Behavior.StallMidUpload);
        using var stalledClient = CreateClient(server, TimeSpan.FromSeconds(2));

        var ex = Assert.CatchAsync(() => InsertPayloadRowsAsync(stalledClient, path, LargePayloadRowCount));

        Assert.Multiple(() =>
        {
            Assert.That(server.BytesReceived, Is.GreaterThanOrEqualTo(UnresponsiveServer.ObservedUploadBytes));
            AssertTransportFailure(ex);
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public async Task InsertBinaryAsync_WhenCallerCancelsMidUpload_SurfacesCancellationWithoutHanging(InsertPath path)
    {
        // Weaker than it looks, deliberately: cancelling the caller's token also cancels the
        // Parallel.ForEachAsync that drives the batches, and that replaces whatever the batch body
        // threw with its own TaskCanceledException. So the classification cannot be observed through
        // this route no matter what the serializer saw - the timeout case above is what exercises it.
        // What this pins down is that a cancel mid-upload unwinds promptly as cancellation and never
        // as a serialization error.
        using var server = new UnresponsiveServer(UnresponsiveServer.Behavior.StallMidUpload);
        using var stalledClient = CreateClient(server);
        using var cts = new CancellationTokenSource();

        var insert = InsertPayloadRowsAsync(stalledClient, path, LargePayloadRowCount, cts.Token);
        try
        {
            // Cancel only once the upload is demonstrably in flight, so the token fires while the
            // serializer is writing rather than before the request starts.
            await server.WaitForUploadAsync(insert);
            cts.Cancel();

            var ex = Assert.CatchAsync(() => insert);

            Assert.Multiple(() =>
            {
                Assert.That(server.BytesReceived, Is.GreaterThanOrEqualTo(UnresponsiveServer.ObservedUploadBytes));
                AssertTransportFailure(ex);
            });
        }
        finally
        {
            // Never leave the insert unobserved: the fixture tears the client and the peer down on the
            // way out, which would fault it after the test has finished.
            await ObserveAsync(insert);
        }
    }

    private static Task InsertDateRowsAsync(
        ClickHouseClient target,
        InsertPath path,
        string table,
        IReadOnlyCollection<DateRow> rows,
        InsertOptions options = null)
    {
        return path switch
        {
            InsertPath.ObjectArray => target.InsertBinaryAsync(
                table,
                DateColumns,
                rows.Select(row => new object[] { row.Id, row.Value }),
                options),
            InsertPath.Poco => target.InsertBinaryAsync(table, rows, options),
            _ => throw new ArgumentOutOfRangeException(nameof(path), path, null),
        };
    }

    private static Task InsertPayloadRowsAsync(
        ClickHouseClient target,
        InsertPath path,
        int rowCount,
        CancellationToken cancellationToken = default)
    {
        var options = Uncompressed(PayloadColumnTypes);

        return path switch
        {
            InsertPath.ObjectArray => target.InsertBinaryAsync(
                FakeEndpointTable,
                PayloadColumns,
                Enumerable.Range(0, rowCount).Select(i => new object[] { (ulong)i, PayloadText }),
                options,
                cancellationToken),
            InsertPath.Poco => target.InsertBinaryAsync(
                FakeEndpointTable,
                Enumerable.Range(0, rowCount).Select(i => new PayloadRow { Id = (ulong)i, Payload = PayloadText }),
                options,
                cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(path), path, null),
        };
    }

    // Insert compression is disabled so rows reach the request stream verbatim, which keeps both the
    // injected fault offset and the byte counts the fake peers observe predictable.
    private static InsertOptions Uncompressed(IReadOnlyDictionary<string, string> columnTypes = null) =>
        new() { ColumnTypes = columnTypes, Compressor = null };

    private static ClickHouseClient CreateClient(HttpClient httpClient) =>
        Register(new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient }));

    // The timeout doubles as the guard against a hang: if the fake peer never gets to reset or stall,
    // the insert fails within it instead of blocking for the two-minute default.
    private static ClickHouseClient CreateClient(UnresponsiveServer server, TimeSpan? timeout = null)
    {
        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = IPAddress.Loopback.ToString(),
            Port = (ushort)server.Port,
        };

        var settings = new ClickHouseClientSettings(builder)
        {
            Timeout = timeout ?? TimeSpan.FromSeconds(30),
        };

        return Register(new ClickHouseClient(settings));
    }

    private static async Task ObserveAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The test has already asserted on this; swallowing here only keeps it from resurfacing as
            // an unobserved task exception.
        }
    }

    // POCO insert mappings are per-client, so every client used by the POCO path needs the registration.
    private static ClickHouseClient Register(ClickHouseClient target)
    {
        target.RegisterBinaryInsertType<PayloadRow>();
        return target;
    }

    private static Exception CreateFault(RequestStreamFault fault) => fault switch
    {
        RequestStreamFault.Io => new IOException("connection reset by peer"),
        // A request stream torn down mid-write surfaces as ObjectDisposedException, which the client
        // treats as a transport failure elsewhere too (see DrainAndDisposeAsync).
        RequestStreamFault.Disposed => new ObjectDisposedException("request stream"),
        RequestStreamFault.Canceled => new OperationCanceledException("request aborted"),
        _ => throw new ArgumentOutOfRangeException(nameof(fault), fault, null),
    };

    // A real tear-down can be caught by the request write or by the response read, and the OS and
    // HttpClient decide the shape, so only the family is asserted. What must hold either way is that
    // it is not a serialization error - that is what would expose the failing row.
    private static void AssertTransportFailure(Exception exception)
    {
        Assert.That(
            exception,
            Is.InstanceOf<IOException>()
                .Or.InstanceOf<ObjectDisposedException>()
                .Or.InstanceOf<OperationCanceledException>()
                .Or.InstanceOf<HttpRequestException>(),
            $"Unexpected exception for a transport failure: {exception}");

        AssertNoSerializationErrorInChain(exception);
    }

    private static void AssertNoSerializationErrorInChain(Exception exception)
    {
        Assert.That(exception, Is.Not.Null, "Expected the insert to fail.");

        for (var current = exception; current != null; current = current.InnerException)
        {
            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                    AssertNoSerializationErrorInChain(inner);
            }

            Assert.That(
                current,
                Is.Not.InstanceOf<ClickHouseBulkCopySerializationException>(),
                "A transport failure must not surface as a serialization error, which would expose the failing row.");
        }
    }

    /// <summary>
    /// Copies the request body into a stream that fails after a fixed number of bytes, reproducing a
    /// request stream that is torn down mid-write.
    /// </summary>
    private sealed class FaultingRequestStreamHandler : HttpMessageHandler
    {
        private readonly Exception fault;
        private readonly int bytesBeforeFault;

        public FaultingRequestStreamHandler(Exception fault, int bytesBeforeFault)
        {
            this.fault = fault;
            this.bytesBeforeFault = bytesBeforeFault;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await request.Content.CopyToAsync(new FaultingStream(fault, bytesBeforeFault), cancellationToken).ConfigureAwait(false);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };
        }
    }

    private sealed class FaultingStream : Stream
    {
        private readonly Exception fault;
        private readonly int bytesBeforeFault;
        private long written;

        public FaultingStream(Exception fault, int bytesBeforeFault)
        {
            this.fault = fault;
            this.bytesBeforeFault = bytesBeforeFault;
        }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => written;
            set => throw new NotSupportedException();
        }

        // Deliberately does not throw: BinaryWriter flushes while the serializer's stack unwinds, and
        // a throwing Flush would replace the exception under test.
        public override void Flush()
        {
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(new ReadOnlySpan<byte>(buffer, offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            written += buffer.Length;
            if (written > bytesBeforeFault)
                throw fault;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(new ReadOnlySpan<byte>(buffer, offset, count));
            return Task.CompletedTask;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return default;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }

    /// <summary>
    /// A loopback TCP endpoint that speaks no HTTP at all: it accepts the connection, reads the start
    /// of the request, and then either resets the connection or stops reading and never replies. The
    /// client's request-body write therefore fails (or blocks) for real, with no injection.
    /// </summary>
    private sealed class UnresponsiveServer : IDisposable
    {
        public enum Behavior
        {
            ResetMidUpload,
            StallMidUpload,
        }

        /// <summary>
        /// How much of the request the peer reads before turning hostile. Tests assert against it to
        /// show the failure landed in the payload rather than in the request headers.
        /// </summary>
        public const int ObservedUploadBytes = 8 * 1024;

        private readonly TcpListener listener;
        private readonly CancellationTokenSource shutdown = new();
        private readonly TaskCompletionSource<bool> uploadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Behavior behavior;
        private readonly Task acceptLoop;
        private Exception failure;
        private long bytesReceived;

        public UnresponsiveServer(Behavior behavior)
        {
            this.behavior = behavior;
            listener = new TcpListener(IPAddress.Loopback, 0);

            // Set before listening so accepted sockets inherit it: a small receive buffer means the
            // client's write blocks after a bounded amount of payload instead of relying on the whole
            // body outgrowing whatever the kernel autotunes to.
            listener.Server.ReceiveBufferSize = ObservedUploadBytes;
            listener.Start();
            Port = ((IPEndPoint)listener.LocalEndpoint).Port;
            acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public int Port { get; }

        public long BytesReceived => Interlocked.Read(ref bytesReceived);

        /// <summary>
        /// Waits until the client has actually pushed <see cref="ObservedUploadBytes"/> of request, so
        /// tests can act on observed upload progress instead of a timing guess. Fails fast if the
        /// insert dies first or the peer never sees the payload.
        /// </summary>
        public async Task WaitForUploadAsync(Task insert)
        {
            using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var timeout = Task.Delay(Timeout.Infinite, timeoutSource.Token);

            var completed = await Task.WhenAny(uploadStarted.Task, insert, timeout).ConfigureAwait(false);
            if (completed == timeout)
                Assert.Fail("Timed out waiting for the client to start uploading the insert payload.");

            // Surface the accept loop's own failure rather than letting the test fail later on a
            // confusing byte-count assertion.
            if (failure != null)
                Assert.Fail($"The fake peer failed before observing the upload: {failure}");

            if (completed == insert)
                Assert.Fail($"The insert finished before the peer observed {ObservedUploadBytes} bytes of payload.");
        }

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!shutdown.IsCancellationRequested)
                {
                    using var socket = await listener.AcceptSocketAsync().ConfigureAwait(false);
                    var buffer = new byte[ObservedUploadBytes];

                    while (Interlocked.Read(ref bytesReceived) < ObservedUploadBytes)
                    {
                        var read = await socket
                            .ReceiveAsync(new ArraySegment<byte>(buffer), SocketFlags.None)
                            .ConfigureAwait(false);
                        if (read == 0)
                            break;

                        Interlocked.Add(ref bytesReceived, read);
                    }

                    uploadStarted.TrySetResult(true);

                    if (behavior == Behavior.ResetMidUpload)
                    {
                        // Discard anything still queued and send an RST rather than a graceful FIN.
                        socket.LingerState = new LingerOption(true, 0);
                        socket.Dispose();
                    }
                    else
                    {
                        // Stop reading and hold the connection open: the client's write blocks once
                        // the socket buffers fill, which is what makes it die mid-payload.
                        await Task.Delay(Timeout.Infinite, shutdown.Token).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex) when (IsExpectedTeardown(ex))
            {
                // Listener disposed or the client vanished: nothing left to serve.
            }
            catch (Exception ex)
            {
                // A genuine helper bug (bad socket option, unexpected platform behaviour): remember it
                // so the test reports it instead of an inscrutable assertion failure downstream.
                failure = ex;
            }
            finally
            {
                uploadStarted.TrySetResult(false);
            }
        }

        private bool IsExpectedTeardown(Exception exception) =>
            shutdown.IsCancellationRequested || exception is SocketException or ObjectDisposedException;

        public void Dispose()
        {
            shutdown.Cancel();
            try
            {
                listener.Stop();
            }
            catch (SocketException)
            {
                // Already torn down.
            }

            // Let the loop unwind (releasing the accepted socket) before the token source it observes
            // goes away.
            acceptLoop.Wait(TimeSpan.FromSeconds(5));
            shutdown.Dispose();
        }
    }
}
