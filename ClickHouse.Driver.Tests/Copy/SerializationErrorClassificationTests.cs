using System;
using System.IO;
using System.Runtime.ExceptionServices;
using ClickHouse.Driver.Copy;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

// Once binary-insert serialization runs directly against the request-body stream, a transport-level
// write failure surfaces from inside the serializer and gets wrapped into a
// ClickHouseBulkCopySerializationException with the failing row attached. RethrowSerializationError
// must unwrap those so a transport error isn't misreported as a serialization error (and row contents
// aren't leaked into the surfaced exception/logs).
[TestFixture]
public class SerializationErrorClassificationTests
{
    [Test]
    public void RethrowSerializationError_WithNull_DoesNotThrow()
    {
        Assert.DoesNotThrow(() => ClickHouseClient.RethrowSerializationError(null));
    }

    [Test]
    public void RethrowSerializationError_WithGenuineSerializationFault_RethrowsWrapperWithRow()
    {
        var row = new object[] { 1UL, "not_a_number" };
        var wrapper = new ClickHouseBulkCopySerializationException(row, new FormatException("bad value"));
        var edi = ExceptionDispatchInfo.Capture(wrapper);

        var ex = Assert.Throws<ClickHouseBulkCopySerializationException>(
            () => ClickHouseClient.RethrowSerializationError(edi));

        Assert.That(ex, Is.SameAs(wrapper));
        Assert.That(ex.Row, Is.SameAs(row));
    }

    [Test]
    public void RethrowSerializationError_WhenWrappedInnerIsIOException_SurfacesTransportErrorWithoutRow()
    {
        var transport = new IOException("connection reset");
        var wrapper = new ClickHouseBulkCopySerializationException(new object[] { 1UL }, transport);
        var edi = ExceptionDispatchInfo.Capture(wrapper);

        var ex = Assert.Throws<IOException>(() => ClickHouseClient.RethrowSerializationError(edi));

        Assert.That(ex, Is.SameAs(transport));
    }

    [Test]
    public void RethrowSerializationError_WhenWrappedInnerIsCancellation_SurfacesCancellation()
    {
        var cancellation = new OperationCanceledException();
        var wrapper = new ClickHouseBulkCopySerializationException(new object[] { 1UL }, cancellation);
        var edi = ExceptionDispatchInfo.Capture(wrapper);

        Assert.Throws<OperationCanceledException>(() => ClickHouseClient.RethrowSerializationError(edi));
    }

    [Test]
    public void RethrowSerializationError_WhenWrappedInnerIsObjectDisposed_SurfacesTransportErrorWithoutRow()
    {
        // A request stream disposed mid-write surfaces as ObjectDisposedException, which the client
        // treats as a transport failure elsewhere; it must not be reported as a serialization error.
        var transport = new ObjectDisposedException("request stream");
        var wrapper = new ClickHouseBulkCopySerializationException(new object[] { 1UL }, transport);
        var edi = ExceptionDispatchInfo.Capture(wrapper);

        var ex = Assert.Throws<ObjectDisposedException>(() => ClickHouseClient.RethrowSerializationError(edi));

        Assert.That(ex, Is.SameAs(transport));
    }

    [Test]
    public void RethrowSerializationError_WithRawTransportException_RethrowsAsIs()
    {
        // A failure before the serializer's own try block (e.g. writing the query line) propagates
        // unwrapped; it should surface exactly as captured.
        var transport = new IOException("proxy abort");
        var edi = ExceptionDispatchInfo.Capture(transport);

        var ex = Assert.Throws<IOException>(() => ClickHouseClient.RethrowSerializationError(edi));

        Assert.That(ex, Is.SameAs(transport));
    }
}
