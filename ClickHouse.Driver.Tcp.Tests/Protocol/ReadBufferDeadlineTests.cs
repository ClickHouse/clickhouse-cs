using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ReadBufferDeadlineTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task ReadIntoAsync_TimeoutAfterSuccessfulRead_NextReadUsesFreshDeadline(bool cancelCaller)
    {
        using var caller = new CancellationTokenSource();
        var deadline = new IdleReadDeadline(TimeSpan.FromMilliseconds(200));
        deadline.Begin(caller.Token);
        using var stream = new DelayedContinuationStream(cancelCaller ? caller.Cancel : null);
        using var buffer = new ReadBuffer(stream, deadline: deadline);
        try
        {
            var first = new byte[1];
            Task read = buffer.ReadIntoAsync(first, caller.Token).AsTask();

            // The first read succeeds, but its continuation is held until the actual timer cancels its token.
            // This forces the completion/timeout race without relying on thread-pool scheduling.
            await stream.TimeoutObserved.WaitAsync(TimeSpan.FromSeconds(10));
            stream.ResumeContinuation();
            await read;
            Assert.That(first[0], Is.EqualTo(42));

            if (cancelCaller)
            {
                Task nextRead = buffer.ReadIntoAsync(new byte[1], caller.Token).AsTask();
                Assert.CatchAsync<OperationCanceledException>(async () =>
                    await nextRead.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            else
            {
                var second = new byte[1];
                await buffer.ReadIntoAsync(second, caller.Token);
                Assert.That(second[0], Is.EqualTo(43));

                // The replacement must still enforce ReadTimeout when the next transport read stalls.
                var timeout = Assert.ThrowsAsync<TimeoutException>(async () =>
                    await buffer.ReadIntoAsync(new byte[1], caller.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.That(timeout.Message, Does.Contain("ReadTimeout"));
            }
        }
        finally
        {
            deadline.End();
        }
    }

    // Completes the first read successfully and holds its await continuation until the test releases it.
    private sealed class DelayedContinuationStream : MemoryStream, IValueTaskSource<int>
    {
        private readonly Action cancelSecondRead;
        private readonly TaskCompletionSource timeoutObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private CancellationTokenRegistration registration;
        private Action<object> continuation;
        private object continuationState;
        private bool completed;
        private int reads;
        private int result;

        internal DelayedContinuationStream(Action cancelSecondRead)
            : base(new byte[] { 42, 43 })
        {
            this.cancelSecondRead = cancelSecondRead;
        }

        internal Task TimeoutObserved => timeoutObserved.Task;

        internal void ResumeContinuation() => continuation(continuationState);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (++reads == 1)
            {
                result = Read(buffer.Span);
                registration = cancellationToken.Register(() => timeoutObserved.TrySetResult());
                return new ValueTask<int>(this, 0);
            }

            if (reads == 2 && cancelSecondRead is null)
            {
                return base.ReadAsync(buffer, cancellationToken);
            }

            Task<int> pending = WaitForCancellationAsync(cancellationToken);
            if (reads == 2)
            {
                // Cancel only after the pending read has registered on its deadline token.
                cancelSecondRead();
                Assert.That(cancellationToken.IsCancellationRequested, Is.True, "Caller cancellation must reach the replacement read token synchronously.");
            }

            return new ValueTask<int>(pending);
        }

        public ValueTaskSourceStatus GetStatus(short token)
            => completed ? ValueTaskSourceStatus.Succeeded : ValueTaskSourceStatus.Pending;

        public int GetResult(short token) => result;

        public void OnCompleted(Action<object> callback, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            continuation = callback;
            continuationState = state;
            completed = true;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                registration.Dispose();
            }

            base.Dispose(disposing);
        }

        private static async Task<int> WaitForCancellationAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }
    }
}
