using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Client;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// The precedence rules between the derived deadline and a caller's own max_execution_time. A round trip can show
// the setting arriving at the server (ConnectionPoolIntegrationTests does), but not which of the two values won
// and why, so the rules are pinned here.
[TestFixture]
public class ConnectionLifetimeDeadlineTests
{
    private const string Setting = "max_execution_time";

    [Test]
    public void Apply_NoLifetimeLimit_LeavesTheSettingsUntouched()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);

        ConnectionLifetimeDeadline.Apply(settings, remainingLifetime: null);

        Assert.That(settings, Is.Empty);
    }

    [Test]
    public void Apply_RemainingLifetime_SetsItLessTheMargin()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(600));

        Assert.That(settings[Setting], Is.EqualTo("595"));
    }

    [Test]
    public void Apply_FractionalRemainingLifetime_RoundsDownSoTheServerFinishesFirst()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(30.9));

        Assert.That(settings[Setting], Is.EqualTo("25"));
    }

    [Test]
    public void Apply_RemainingLifetimeInsideTheMargin_SetsNothingRatherThanAnUnlimitedValue()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);

        // The pool retires a connection before this, so it is a backstop: a value of 0 or less means *no limit*
        // to ClickHouse, which is the opposite of what is wanted here.
        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(4));

        Assert.That(settings, Is.Empty);
    }

    [Test]
    public void Apply_CallerAskedForLess_KeepsTheCallersLimit()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { [Setting] = "30" };

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(600));

        Assert.That(settings[Setting], Is.EqualTo("30"));
    }

    [Test]
    public void Apply_CallerAskedForMore_ClampsToTheConnectionsLife()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { [Setting] = "3600" };

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(600));

        Assert.That(settings[Setting], Is.EqualTo("595"), "a caller's limit must not outlive the connection carrying it");
    }

    [Test]
    public void Apply_CallerAskedForUnlimited_ClampsToTheConnectionsLife()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { [Setting] = "0" };

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(600));

        Assert.That(settings[Setting], Is.EqualTo("595"), "0 means no limit, which is the largest value of all");
    }

    [Test]
    public void Apply_CallerValueNotANumber_IsLeftForTheServerToJudge()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { [Setting] = "10 minutes" };

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(600));

        Assert.That(settings[Setting], Is.EqualTo("10 minutes"), "a spelling we cannot read must not be silently rewritten");
    }

    [Test]
    public void Apply_CallerUsedAFractionalValue_IsComparedNumerically()
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { [Setting] = "12.5" };

        ConnectionLifetimeDeadline.Apply(settings, TimeSpan.FromSeconds(600));

        Assert.That(settings[Setting], Is.EqualTo("12.5"));
    }
}
