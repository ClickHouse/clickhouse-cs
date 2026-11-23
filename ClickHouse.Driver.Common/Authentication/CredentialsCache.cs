using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Authentication;

/// <summary>
/// The credentials of one client, from its <see cref="ClickHouseCredentialsProvider"/>. Both clients use
/// it: the HTTP client for each request, the native TCP client for each new connection.
/// </summary>
/// <remarks>
/// <para>
/// Credentials without a known expiry are not cached: each <see cref="GetAsync"/> calls the provider.
/// Credentials with a known expiry (<see cref="ClickHouseCredentials.EffectiveExpiresAt"/>) are cached.
/// They are <em>expired</em> from <see cref="ExpiredMargin"/> before the expiry: callers then wait for a
/// new value. They are <em>expiring</em> from the refresh window before the expiry (the smaller of
/// <see cref="MaxRefreshWindow"/> and half the lifetime): callers then get the cached value, and one
/// background refresh starts.
/// </para>
/// <para>
/// At most one provider call runs at a time. It gets a token that <see cref="Dispose"/> cancels; the
/// token of a caller cancels only the wait of that caller.
/// </para>
/// </remarks>
internal sealed class CredentialsCache : IDisposable
{
    /// <summary>
    /// The server error code for rejected credentials (<c>AUTHENTICATION_FAILED</c>).
    /// </summary>
    internal const int AuthenticationFailedErrorCode = 516;

    internal static readonly TimeSpan ExpiredMargin = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan MaxRefreshWindow = TimeSpan.FromSeconds(60);
    internal static readonly TimeSpan FailedRefreshBackoff = TimeSpan.FromSeconds(5);

    private readonly ClickHouseCredentialsProvider provider;
    private readonly Action<Exception> onBackgroundRefreshFailed;
    private readonly Func<DateTimeOffset> utcNow;
    private readonly CancellationTokenSource disposal = new();
    private readonly object gate = new();

    // All the fields below are guarded by `gate`.
    private ClickHouseCredentials cached;
    private DateTimeOffset cachedAt;
    private ClickHouseCredentials lastFetched;
    private Task<ClickHouseCredentials> refresh;
    private int refreshStartedAtRejection;
    private int rejections;
    private ClickHouseCredentialsRefreshReason nextReason = ClickHouseCredentialsRefreshReason.Initial;
    private DateTimeOffset noBackgroundRefreshBefore = DateTimeOffset.MinValue;
    private bool disposed;

    /// <param name="provider">The provider of the client.</param>
    /// <param name="onBackgroundRefreshFailed">
    /// Called when a background refresh fails; the cached credentials stay in use. The client logs a
    /// warning here. A failure that a caller waits for goes to that caller instead.
    /// </param>
    /// <param name="utcNow">The clock. Tests replace it.</param>
    internal CredentialsCache(
        ClickHouseCredentialsProvider provider,
        Action<Exception> onBackgroundRefreshFailed = null,
        Func<DateTimeOffset> utcNow = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        this.provider = provider;
        this.onBackgroundRefreshFailed = onBackgroundRefreshFailed;
        this.utcNow = utcNow ?? (static () => DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// Returns <see langword="true"/> when credentials with this effective expiry must no longer be used:
    /// the expiry is known, and it is <see cref="ExpiredMargin"/> away or less.
    /// </summary>
    internal static bool IsExpired(DateTimeOffset? effectiveExpiry, DateTimeOffset now)
        => effectiveExpiry.HasValue && now + ExpiredMargin >= effectiveExpiry.Value;

    /// <summary>
    /// Gets the credentials to use now. <see langword="null"/> means the provider returned
    /// <see langword="null"/>: use the static credentials of the client settings.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait of this caller only.</param>
    internal ValueTask<ClickHouseCredentials> GetAsync(CancellationToken cancellationToken)
    {
        ClickHouseCredentials usable = null;
        Task<ClickHouseCredentials> pending = null;
        Refresh started = null;

        lock (gate)
        {
            // ObjectDisposedException.ThrowIf is unavailable on net6.0 (a supported target), so throw manually.
#pragma warning disable CA1513
            if (disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
#pragma warning restore CA1513

            var now = utcNow();
            if (cached is not null && !IsExpired(cached.EffectiveExpiresAt, now))
            {
                usable = cached;
                if (refresh is null && now >= noBackgroundRefreshBefore && IsExpiring(cached.EffectiveExpiresAt.Value, cachedAt, now))
                {
                    started = BeginRefreshLocked(background: true);
                }
            }
            else
            {
                // A call that started before the last rejection can return the rejected credentials, so a
                // caller does not join it: a new call runs after it.
                if (refresh is null || refreshStartedAtRejection != rejections)
                {
                    started = BeginRefreshLocked(background: false);
                }

                pending = refresh;
            }
        }

        if (started is not null)
        {
            if (started.Background)
            {
                // Run the provider off the caller's thread: a provider that blocks before its first await
                // must not delay a request that has usable credentials.
                _ = Task.Run(() => RunRefreshAsync(started), CancellationToken.None);
            }
            else
            {
                _ = RunRefreshAsync(started);
            }
        }

        if (usable is not null)
        {
            return new ValueTask<ClickHouseCredentials>(usable);
        }

        return new ValueTask<ClickHouseCredentials>(
            cancellationToken.CanBeCanceled ? pending.WaitAsync(cancellationToken) : pending);
    }

    /// <summary>
    /// Tells the cache that the server rejected <paramref name="rejected"/>, which <see cref="GetAsync"/>
    /// returned. The next provider call has the reason <see cref="ClickHouseCredentialsRefreshReason.Rejected"/>,
    /// and the cache drops <paramref name="rejected"/> if it holds them.
    /// </summary>
    /// <remarks>
    /// Cached credentials (those with a known expiry) that a newer provider call already replaced are
    /// ignored, so many requests that fail with the same cached credentials cause one refresh. Credentials
    /// without a known expiry are new for each request, so each rejection of them counts.
    /// </remarks>
    internal void Invalidate(ClickHouseCredentials rejected)
    {
        if (rejected is null)
        {
            return;
        }

        lock (gate)
        {
            var isLast = ReferenceEquals(rejected, lastFetched);
            if (!isLast && rejected.EffectiveExpiresAt is not null)
            {
                return;
            }

            if (isLast)
            {
                lastFetched = null;
            }

            if (ReferenceEquals(cached, rejected))
            {
                cached = null;
            }

            nextReason = ClickHouseCredentialsRefreshReason.Rejected;
            rejections++;
        }
    }

    /// <summary>
    /// Cancels a provider call that runs, and makes <see cref="GetAsync"/> throw
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            cached = null;
        }

        // Not disposed on purpose: a provider call that still runs may use the token.
        disposal.Cancel();
    }

    private static bool IsExpiring(DateTimeOffset expiry, DateTimeOffset fetchedAt, DateTimeOffset now)
    {
        var halfLifetime = TimeSpan.FromTicks(Math.Max(0, (expiry - fetchedAt).Ticks / 2));
        var window = halfLifetime < MaxRefreshWindow ? halfLifetime : MaxRefreshWindow;
        return now + window >= expiry;
    }

    private Refresh BeginRefreshLocked(bool background)
    {
        var started = new Refresh(nextReason, rejections, background, previous: refresh);
        refresh = started.Completion.Task;
        refreshStartedAtRejection = rejections;
        return started;
    }

    private async Task RunRefreshAsync(Refresh run)
    {
        if (run.Previous is not null)
        {
            // One provider call at a time: wait for the older call, whatever its result.
            await Task.WhenAny(run.Previous).ConfigureAwait(false);
        }

        ClickHouseCredentials credentials;
        try
        {
            credentials = await InvokeProviderAsync(run.Reason).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            lock (gate)
            {
                if (ReferenceEquals(refresh, run.Completion.Task))
                {
                    refresh = null;
                }

                if (run.Background)
                {
                    noBackgroundRefreshBefore = utcNow() + FailedRefreshBackoff;
                }
            }

            run.Completion.TrySetException(ex);

            // The waiters can all have stopped waiting (a background refresh can have none). Read the
            // exception so that it does not reach TaskScheduler.UnobservedTaskException.
            _ = run.Completion.Task.Exception;
            if (run.Background && !disposal.IsCancellationRequested)
            {
                ReportBackgroundFailure(ex);
            }

            return;
        }

        lock (gate)
        {
            if (ReferenceEquals(refresh, run.Completion.Task))
            {
                refresh = null;
            }

            // After a rejection that came while this call ran, the result can be the rejected credentials:
            // give it to the waiters of this call, but do not keep it.
            if (run.StartedAtRejection == rejections)
            {
                lastFetched = credentials;
                nextReason = ClickHouseCredentialsRefreshReason.Expiring;
                if (!disposed && credentials?.EffectiveExpiresAt is not null)
                {
                    cached = credentials;
                    cachedAt = utcNow();
                }
                else
                {
                    cached = null;
                }
            }
        }

        run.Completion.TrySetResult(credentials);
    }

    private ValueTask<ClickHouseCredentials> InvokeProviderAsync(ClickHouseCredentialsRefreshReason reason)
    {
        // The provider is user code. Call it without the caller's SynchronizationContext, so that a provider
        // that awaits without ConfigureAwait(false) cannot deadlock a caller that blocks on the operation
        // (the synchronous ADO.NET methods).
        var context = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            return provider(new ClickHouseCredentialsRequest(reason), disposal.Token);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(context);
        }
    }

    private void ReportBackgroundFailure(Exception exception)
    {
        try
        {
            onBackgroundRefreshFailed?.Invoke(exception);
        }
#pragma warning disable CA1031 // Nothing waits for a background refresh: a failing logger must not fault it.
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    private sealed class Refresh
    {
        internal Refresh(ClickHouseCredentialsRefreshReason reason, int startedAtRejection, bool background, Task<ClickHouseCredentials> previous)
        {
            Reason = reason;
            StartedAtRejection = startedAtRejection;
            Background = background;
            Previous = previous;
        }

        // Waiters continue on their own threads, not inside RunRefreshAsync.
        internal TaskCompletionSource<ClickHouseCredentials> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ClickHouseCredentialsRefreshReason Reason { get; }

        internal int StartedAtRejection { get; }

        internal bool Background { get; }

        internal Task<ClickHouseCredentials> Previous { get; }
    }
}
