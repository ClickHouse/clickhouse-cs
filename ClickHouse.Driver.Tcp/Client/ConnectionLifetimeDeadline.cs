using System;
using System.Collections.Generic;
using System.Globalization;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Derives the server-side <c>max_execution_time</c> that keeps a query inside its connection's remaining life.
/// Without it a connection that reaches <c>MaxConnectionLifetime</c> mid-query leaves the pool a bad choice:
/// cut the query off and kill the transport, or keep a connection past the age at which it was to be retired.
/// Bounding the query instead makes the server end it first, with a normal error on a stream that stays aligned,
/// so the connection is still reusable.
/// </summary>
internal static class ConnectionLifetimeDeadline
{
    /// <summary>The setting that bounds a query's server-side execution time, in seconds.</summary>
    internal const string MaxExecutionTimeSetting = "max_execution_time";

    /// <summary>
    /// How far inside the connection's remaining life the query's deadline is set, leaving the server time to
    /// report the timeout and the client time to read it before the connection is retired.
    /// </summary>
    internal static readonly TimeSpan Margin = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The remaining life below which a connection is retired at checkout instead of being handed out. It has to
    /// exceed <see cref="Margin"/>: at or under the margin the derived deadline would be zero or negative, and to
    /// ClickHouse a <c>max_execution_time</c> of 0 means <i>no limit</i> — the naive subtraction would hand out
    /// unlimited execution exactly when the least was left.
    /// </summary>
    internal static readonly TimeSpan RetirementFloor = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Applies the derived deadline to an operation's settings, in place. With no lifetime limit configured
    /// (<paramref name="remainingLifetime"/> null) the settings are left alone.
    /// </summary>
    /// <param name="settings">The merged settings for one operation, modified in place.</param>
    /// <param name="remainingLifetime">The leased connection's remaining life, or null when it has no age limit.</param>
    internal static void Apply(Dictionary<string, string> settings, TimeSpan? remainingLifetime)
    {
        if (remainingLifetime is not { } remaining)
        {
            return;
        }

        // The pool retires a connection below the floor rather than leasing it, so the margin cannot bite here.
        double derived = Math.Floor((remaining - Margin).TotalSeconds);
        if (derived < 1)
        {
            return;
        }

        if (settings.TryGetValue(MaxExecutionTimeSetting, out string existing) && !Supersedes(derived, existing))
        {
            return;
        }

        settings[MaxExecutionTimeSetting] = derived.ToString("0", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Whether the derived deadline replaces a caller's own <c>max_execution_time</c>. A shorter caller limit is
    /// respected; a longer one (including <c>0</c>, which means no limit) is not, since it would outlive the
    /// connection carrying it.
    /// </summary>
    /// <param name="derived">The deadline derived from the connection's remaining life, in seconds.</param>
    /// <param name="callerValue">The caller's setting value, as it would go on the wire.</param>
    /// <returns>True to overwrite the caller's value.</returns>
    private static bool Supersedes(double derived, string callerValue)
    {
        // A value we cannot read is left alone: the server may accept a spelling we do not parse, and silently
        // rewriting it would be worse than leaving the connection to be retired by age.
        if (!double.TryParse(callerValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double caller))
        {
            return false;
        }

        return caller <= 0 || caller > derived;
    }
}
