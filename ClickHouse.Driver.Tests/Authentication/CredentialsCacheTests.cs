using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Authentication;

namespace ClickHouse.Driver.Tests.Authentication;

public class CredentialsCacheTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private DateTimeOffset now;

    [SetUp]
    public void SetUp() => now = Start;

    /// <summary>
    /// A provider that records each call and returns the next credentials from <see cref="Next"/>.
    /// </summary>
    private sealed class FakeProvider
    {
        private readonly ConcurrentQueue<ClickHouseCredentialsRequest> requests = new();

        public Func<int, CancellationToken, Task<ClickHouseCredentials>> Next { get; set; }

        public ConcurrentBag<CancellationToken> Tokens { get; } = new();

        public IReadOnlyList<ClickHouseCredentialsRefreshReason> Reasons => requests.Select(r => r.Reason).ToList();

        public int Calls => requests.Count;

        public async ValueTask<ClickHouseCredentials> InvokeAsync(ClickHouseCredentialsRequest request, CancellationToken token)
        {
            requests.Enqueue(request);
            Tokens.Add(token);
            return await Next(requests.Count, token).ConfigureAwait(false);
        }
    }

    private CredentialsCache CreateCache(FakeProvider provider, Action<Exception> onBackgroundRefreshFailed = null)
        => new(provider.InvokeAsync, onBackgroundRefreshFailed, () => now);

    private ClickHouseCredentials Bearer(int n, TimeSpan lifetime) => ClickHouseCredentials.CreateBearer($"token-{n}", now + lifetime);

    [Test]
    public async Task GetAsync_CredentialsWithoutExpiry_CallsProviderEachTime()
    {
        var provider = new FakeProvider { Next = (n, _) => Task.FromResult(ClickHouseCredentials.CreateBasic("user", $"password-{n}")) };
        using var cache = CreateCache(provider);

        var first = await cache.GetAsync(CancellationToken.None);
        var second = await cache.GetAsync(CancellationToken.None);
        var third = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(new[] { first.Password, second.Password, third.Password }, Is.EqualTo(new[] { "password-1", "password-2", "password-3" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[]
            {
                ClickHouseCredentialsRefreshReason.Initial,
                ClickHouseCredentialsRefreshReason.Expiring,
                ClickHouseCredentialsRefreshReason.Expiring,
            }));
        });
    }

    [Test]
    public async Task GetAsync_JwtWithExpClaim_IsCachedUntilItExpires()
    {
        var exp = (now + TimeSpan.FromHours(1)).ToUnixTimeSeconds();
        var provider = new FakeProvider
        {
            Next = (n, _) => Task.FromResult(ClickHouseCredentials.CreateBearer(ClickHouseCredentialsTests.Jwt($"{{\"n\":{n},\"exp\":{exp}}}"))),
        };
        using var cache = CreateCache(provider);

        var first = await cache.GetAsync(CancellationToken.None);
        now += TimeSpan.FromMinutes(50);
        var second = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first));
            Assert.That(provider.Calls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetAsync_CredentialsExpiringSoon_ReturnsCachedAndStartsOneBackgroundRefresh()
    {
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var refreshStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new FakeProvider();
        provider.Next = async (n, _) =>
        {
            if (n == 1)
                return Bearer(1, TimeSpan.FromHours(1));
            refreshStarted.TrySetResult(true);
            await release.Task;
            return Bearer(n, TimeSpan.FromHours(1));
        };
        using var cache = CreateCache(provider);
        var original = await cache.GetAsync(CancellationToken.None);

        // 30 s before the expiry: inside the 60 s refresh window, outside the 10 s expired margin.
        now = original.EffectiveExpiresAt.Value - TimeSpan.FromSeconds(30);
        var duringRefresh = new[]
        {
            await cache.GetAsync(CancellationToken.None),
            await cache.GetAsync(CancellationToken.None),
            await cache.GetAsync(CancellationToken.None),
        };
        await refreshStarted.Task.WaitAsync(Timeout);
        release.SetResult(true);
        var refreshed = await WaitForAsync(cache, c => !ReferenceEquals(c, original));

        Assert.Multiple(() =>
        {
            Assert.That(duringRefresh, Is.All.SameAs(original));
            Assert.That(refreshed.BearerToken, Is.EqualTo("token-2"));
            Assert.That(provider.Calls, Is.EqualTo(2));
            Assert.That(provider.Reasons[1], Is.EqualTo(ClickHouseCredentialsRefreshReason.Expiring));
        });
    }

    [TestCase(15, false)]
    [TestCase(21, true)]
    public async Task GetAsync_ShortLifetime_RefreshWindowIsHalfTheLifetime(int secondsAfterFetch, bool expectRefresh)
    {
        var refreshed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new FakeProvider();
        provider.Next = (n, _) =>
        {
            if (n > 1)
                refreshed.TrySetResult(true);
            return Task.FromResult(Bearer(n, TimeSpan.FromSeconds(40)));
        };
        using var cache = CreateCache(provider);
        await cache.GetAsync(CancellationToken.None);

        // Lifetime 40 s: the refresh window is 20 s, so the refresh starts 20 s after the fetch.
        now += TimeSpan.FromSeconds(secondsAfterFetch);
        await cache.GetAsync(CancellationToken.None);

        if (expectRefresh)
        {
            await refreshed.Task.WaitAsync(Timeout);
        }
        else
        {
            // A refresh would run on another thread: give it the time to show.
            await Task.Delay(200);
        }

        Assert.That(provider.Calls, Is.EqualTo(expectRefresh ? 2 : 1));
    }

    [Test]
    public async Task GetAsync_CredentialsExpired_WaitsForNewCredentials()
    {
        var provider = new FakeProvider { Next = (n, _) => Task.FromResult(Bearer(n, TimeSpan.FromHours(1))) };
        using var cache = CreateCache(provider);
        var original = await cache.GetAsync(CancellationToken.None);

        // 5 s before the expiry is inside the 10 s expired margin.
        now = original.EffectiveExpiresAt.Value - TimeSpan.FromSeconds(5);
        var current = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(current.BearerToken, Is.EqualTo("token-2"));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Expiring }));
        });
    }

    [Test]
    public async Task GetAsync_FiftyParallelCallers_CallProviderOnce()
    {
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new FakeProvider();
        provider.Next = async (n, _) =>
        {
            await release.Task;
            return Bearer(n, TimeSpan.FromHours(1));
        };
        using var cache = CreateCache(provider);

        var entered = 0;
        var callers = Enumerable.Range(0, 50)
            .Select(_ => Task.Run(async () =>
            {
                // GetAsync decides synchronously whether to join the running call.
                var result = cache.GetAsync(CancellationToken.None);
                Interlocked.Increment(ref entered);
                return await result;
            }))
            .ToArray();
        while (Volatile.Read(ref entered) < 50)
        {
            await Task.Delay(5);
        }

        release.SetResult(true);
        var results = await Task.WhenAll(callers).WaitAsync(Timeout);

        Assert.Multiple(() =>
        {
            Assert.That(provider.Calls, Is.EqualTo(1));
            Assert.That(results, Is.All.SameAs(results[0]));
        });
    }

    [Test]
    public async Task GetAsync_BackgroundRefreshFails_KeepsCachedCredentialsAndWaitsBeforeTheNextTry()
    {
        var failure = new InvalidOperationException("identity provider down");
        var reported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new FakeProvider
        {
            Next = (n, _) => n == 1 ? Task.FromResult(Bearer(1, TimeSpan.FromHours(1))) : Task.FromException<ClickHouseCredentials>(failure),
        };
        using var cache = CreateCache(provider, ex => reported.TrySetResult(ex));
        var original = await cache.GetAsync(CancellationToken.None);
        now = original.EffectiveExpiresAt.Value - TimeSpan.FromSeconds(30);

        var duringFailure = await cache.GetAsync(CancellationToken.None);
        var reportedException = await reported.Task.WaitAsync(Timeout);
        var beforeBackoffEnds = await cache.GetAsync(CancellationToken.None);
        var callsBeforeBackoffEnds = provider.Calls;
        now += CredentialsCache.FailedRefreshBackoff;
        var afterBackoff = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(new[] { duringFailure, beforeBackoffEnds, afterBackoff }, Is.All.SameAs(original));
            Assert.That(reportedException, Is.SameAs(failure));
            Assert.That(callsBeforeBackoffEnds, Is.EqualTo(2));
        });
        await WaitForCallsAsync(provider, 3);
    }

    [Test]
    public void GetAsync_RefreshThatACallerWaitsForFails_ThrowsTheProviderException()
    {
        var failure = new InvalidOperationException("identity provider down");
        var provider = new FakeProvider { Next = (_, _) => Task.FromException<ClickHouseCredentials>(failure) };
        using var cache = CreateCache(provider);

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await cache.GetAsync(CancellationToken.None));

        Assert.That(thrown, Is.SameAs(failure));
    }

    [Test]
    public void GetAsync_ProviderThrowsSynchronously_ThrowsTheProviderException()
    {
        var failure = new InvalidOperationException("bad configuration");
        using var cache = new CredentialsCache((_, _) => throw failure, utcNow: () => now);

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await cache.GetAsync(CancellationToken.None));

        Assert.That(thrown, Is.SameAs(failure));
    }

    [Test]
    public async Task GetAsync_CancelledCaller_DoesNotCancelTheSharedRefresh()
    {
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new FakeProvider();
        provider.Next = async (n, _) =>
        {
            await release.Task;
            return Bearer(n, TimeSpan.FromHours(1));
        };
        using var cache = CreateCache(provider);
        using var cancelled = new CancellationTokenSource();

        var cancelledCaller = cache.GetAsync(cancelled.Token).AsTask();
        var otherCaller = cache.GetAsync(CancellationToken.None).AsTask();
        cancelled.Cancel();

        Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelledCaller);
        release.SetResult(true);
        var result = await otherCaller.WaitAsync(Timeout);

        Assert.Multiple(() =>
        {
            Assert.That(result.BearerToken, Is.EqualTo("token-1"));
            Assert.That(provider.Calls, Is.EqualTo(1));
            Assert.That(provider.Tokens.Single().IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public async Task GetAsync_ProviderBlocksBeforeItsFirstAwait_CancelledCallerStopsWaiting()
    {
        using var release = new ManualResetEventSlim();
        var provider = new FakeProvider();
        provider.Next = (n, _) =>
        {
            release.Wait(Timeout);
            return Task.FromResult(Bearer(n, TimeSpan.FromHours(1)));
        };
        using var cache = CreateCache(provider);
        using var cancelled = new CancellationTokenSource();

        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var caller = cache.GetAsync(cancelled.Token).AsTask();
            cancelled.Cancel();

            Assert.ThrowsAsync<TaskCanceledException>(async () => await caller.WaitAsync(Timeout));
            Assert.That(stopwatch.Elapsed, Is.LessThan(Timeout / 2));
        }
        finally
        {
            release.Set();
        }

        // The shared call goes on, and its result serves the next caller.
        var result = await cache.GetAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Multiple(() =>
        {
            Assert.That(result.BearerToken, Is.EqualTo("token-1"));
            Assert.That(provider.Calls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Invalidate_SameInstance_NextCallAsksWithReasonRejected()
    {
        var provider = new FakeProvider { Next = (n, _) => Task.FromResult(Bearer(n, TimeSpan.FromHours(1))) };
        using var cache = CreateCache(provider);
        var rejected = await cache.GetAsync(CancellationToken.None);

        // Equal values, other instance: not the credentials that the cache holds.
        cache.Invalidate(ClickHouseCredentials.CreateBearer(rejected.BearerToken, rejected.ExpiresAt));
        var stillCached = await cache.GetAsync(CancellationToken.None);
        cache.Invalidate(rejected);
        var replacement = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stillCached, Is.SameAs(rejected));
            Assert.That(replacement.BearerToken, Is.EqualTo("token-2"));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
        });
    }

    [Test]
    public async Task Invalidate_ManyRejectionsOfTheSameCredentials_CauseOneRefresh()
    {
        var provider = new FakeProvider { Next = (n, _) => Task.FromResult(Bearer(n, TimeSpan.FromHours(1))) };
        using var cache = CreateCache(provider);
        var rejected = await cache.GetAsync(CancellationToken.None);

        cache.Invalidate(rejected);
        var replacement = await cache.GetAsync(CancellationToken.None);
        cache.Invalidate(rejected);
        cache.Invalidate(rejected);
        var afterLateRejections = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(afterLateRejections, Is.SameAs(replacement));
            Assert.That(provider.Calls, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Invalidate_CredentialsWithoutExpiry_NextCallAsksWithReasonRejected()
    {
        var provider = new FakeProvider { Next = (n, _) => Task.FromResult(ClickHouseCredentials.CreateBasic("user", $"password-{n}")) };
        using var cache = CreateCache(provider);
        var rejected = await cache.GetAsync(CancellationToken.None);

        cache.Invalidate(rejected);
        await cache.GetAsync(CancellationToken.None);

        Assert.That(provider.Reasons[1], Is.EqualTo(ClickHouseCredentialsRefreshReason.Rejected));
    }

    [TestCase(516, true, TestName = "IsRejection_AuthenticationFailed_IsTrue")]
    [TestCase(194, true, TestName = "IsRejection_RequiredPassword_IsTrue")]
    [TestCase(767, false, TestName = "IsRejection_RequiredSecondFactor_IsFalse")]
    [TestCase(60, false, TestName = "IsRejection_UnknownTable_IsFalse")]
    [TestCase(-1, false, TestName = "IsRejection_NoErrorCode_IsFalse")]
    public void IsRejection(int errorCode, bool expected)
        => Assert.That(CredentialsCache.IsRejection(errorCode), Is.EqualTo(expected));

    [Test]
    public async Task Invalidate_OlderCredentialsWithoutExpiry_NextCallAsksWithReasonRejected()
    {
        var provider = new FakeProvider { Next = (n, _) => Task.FromResult(ClickHouseCredentials.CreateBasic("user", $"password-{n}")) };
        using var cache = CreateCache(provider);
        var first = await cache.GetAsync(CancellationToken.None);
        await cache.GetAsync(CancellationToken.None);

        // Each request has its own credentials without an expiry, so a rejection of the older one counts too.
        cache.Invalidate(first);
        await cache.GetAsync(CancellationToken.None);

        Assert.That(provider.Reasons[2], Is.EqualTo(ClickHouseCredentialsRefreshReason.Rejected));
    }

    [Test]
    public async Task GetAsync_RejectionWhileARefreshRuns_CallsProviderAgainAfterItWithReasonRejected()
    {
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var refreshStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = 0;
        var maxRunning = 0;
        var provider = new FakeProvider();
        provider.Next = async (n, _) =>
        {
            var nowRunning = Interlocked.Increment(ref running);
            InterlockedMax(ref maxRunning, nowRunning);
            try
            {
                if (n == 2)
                {
                    refreshStarted.TrySetResult(true);
                    await release.Task;
                }

                return Bearer(n, TimeSpan.FromHours(1));
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
        };
        using var cache = CreateCache(provider);
        var rejected = await cache.GetAsync(CancellationToken.None);
        now = rejected.EffectiveExpiresAt.Value - TimeSpan.FromSeconds(30);
        await cache.GetAsync(CancellationToken.None); // starts the background refresh (call 2)
        await refreshStarted.Task.WaitAsync(Timeout);

        cache.Invalidate(rejected);
        var retry = cache.GetAsync(CancellationToken.None).AsTask();
        release.SetResult(true);
        var replacement = await retry.WaitAsync(Timeout);
        var next = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(replacement.BearerToken, Is.EqualTo("token-3"));
            Assert.That(next, Is.SameAs(replacement));
            Assert.That(provider.Reasons, Is.EqualTo(new[]
            {
                ClickHouseCredentialsRefreshReason.Initial,
                ClickHouseCredentialsRefreshReason.Expiring,
                ClickHouseCredentialsRefreshReason.Rejected,
            }));
            Assert.That(maxRunning, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetAsync_RefreshThatStartedBeforeARejection_DoesNotKeepItsResult()
    {
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var refreshStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var refreshReturned = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        ClickHouseCredentials first = null;
        var provider = new FakeProvider();
        provider.Next = async (n, _) =>
        {
            if (n == 1)
                return first = Bearer(1, TimeSpan.FromHours(1));
            if (n == 2)
            {
                // A provider with its own cache: it gives the same credentials until it is told they were rejected.
                refreshStarted.TrySetResult(true);
                await release.Task;
                refreshReturned.TrySetResult(true);
                return first;
            }

            return Bearer(n, TimeSpan.FromHours(1));
        };
        using var cache = CreateCache(provider);
        await cache.GetAsync(CancellationToken.None);
        now = first.EffectiveExpiresAt.Value - TimeSpan.FromSeconds(30);
        await cache.GetAsync(CancellationToken.None); // starts the background refresh (call 2)
        await refreshStarted.Task.WaitAsync(Timeout);

        cache.Invalidate(first);
        release.SetResult(true);
        await refreshReturned.Task.WaitAsync(Timeout);
        await Task.Delay(100); // let call 2 finish
        var current = await cache.GetAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(current, Is.Not.SameAs(first));
            Assert.That(provider.Reasons.Last(), Is.EqualTo(ClickHouseCredentialsRefreshReason.Rejected));
        });
    }

    private sealed class RecordingSynchronizationContext : SynchronizationContext
    {
    }

    [Test]
    public async Task GetAsync_CallerHasSynchronizationContext_ProviderRunsWithoutIt()
    {
        SynchronizationContext seen = new RecordingSynchronizationContext();
        using var cache = new CredentialsCache(
            (_, _) =>
            {
                seen = SynchronizationContext.Current;
                return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer("token"));
            },
            utcNow: () => now);
        var callerContext = new RecordingSynchronizationContext();
        var original = SynchronizationContext.Current;

        ValueTask<ClickHouseCredentials> call;
        SynchronizationContext.SetSynchronizationContext(callerContext);
        try
        {
            call = cache.GetAsync(CancellationToken.None);
            Assert.That(SynchronizationContext.Current, Is.SameAs(callerContext));
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(original);
        }

        await call;
        Assert.That(seen, Is.Null);
    }

    [Test]
    public async Task GetAsync_ProviderReturnsNull_ReturnsNull()
    {
        var provider = new FakeProvider { Next = (_, _) => Task.FromResult<ClickHouseCredentials>(null) };
        using var cache = CreateCache(provider);

        Assert.That(await cache.GetAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public void Dispose_ProviderCallRuns_CancelsItsToken()
    {
        var provider = new FakeProvider();
        provider.Next = async (_, token) =>
        {
            await Task.Delay(System.Threading.Timeout.Infinite, token);
            return null;
        };
        var cache = CreateCache(provider);
        var waiting = cache.GetAsync(CancellationToken.None).AsTask();

        cache.Dispose();

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<TaskCanceledException>(async () => await waiting.WaitAsync(Timeout));
            Assert.Throws<ObjectDisposedException>(() => cache.GetAsync(CancellationToken.None));
        });
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    private static async Task<ClickHouseCredentials> WaitForAsync(CredentialsCache cache, Func<ClickHouseCredentials, bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (true)
        {
            var current = await cache.GetAsync(CancellationToken.None);
            if (condition(current) || DateTime.UtcNow > deadline)
                return current;
            await Task.Delay(10);
        }
    }

    private static async Task WaitForCallsAsync(FakeProvider provider, int calls)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (provider.Calls < calls && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        Assert.That(provider.Calls, Is.EqualTo(calls));
    }
}
