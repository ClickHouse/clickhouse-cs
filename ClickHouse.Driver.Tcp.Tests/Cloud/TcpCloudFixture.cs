using System;

namespace ClickHouse.Driver.Tcp.Tests.Cloud;

/// <summary>
/// Points the Cloud tests at a real ClickHouse Cloud service over the secure native port. Configured entirely by
/// one environment variable holding a native connection string, so the same tests run locally against any
/// TLS-serving ClickHouse without a second code path.
///
/// <para>
/// Deliberately not a <c>SetUpFixture</c>, and deliberately in its own namespace: the <c>Integration</c> namespace
/// starts a ClickHouse container for every test under it, which a Cloud run neither needs nor can rely on having
/// Docker for.
/// </para>
/// </summary>
internal static class TcpCloudFixture
{
    /// <summary>The environment variable holding the native connection string, TLS included.</summary>
    internal const string ConnectionVariable = "CLICKHOUSE_TCP_CLOUD_CONNECTION";

    /// <summary>
    /// The options the Cloud tests connect with. Ignores the calling test when the variable is unset, so a
    /// developer running the suite locally is not failed by credentials they do not have.
    /// </summary>
    /// <returns>Options parsed from the connection string.</returns>
    internal static ClickHouseTcpClientOptions Options()
    {
        string connectionString = Environment.GetEnvironmentVariable(ConnectionVariable);
        if (string.IsNullOrEmpty(connectionString))
        {
            Assert.Ignore(
                $"Set {ConnectionVariable} to a native connection string with UseTls=true to run the Cloud tests, " +
                "for example 'Host=<service>.clickhouse.cloud;UseTls=true;Username=default;Password=<password>'.");
        }

        var options = ClickHouseTcpClientOptions.FromConnectionString(connectionString);

        // These tests prove both the TLS transport and validation through the host's public trust store. Letting
        // the connection string bypass validation or supply a private authority would make that proof vacuous.
        Assert.Multiple(() =>
        {
            Assert.That(
                options.UseTls,
                Is.True,
                $"{ConnectionVariable} must set UseTls=true; these tests exist to exercise the TLS transport.");
            Assert.That(
                options.TlsAllowInvalidCertificates,
                Is.False,
                $"{ConnectionVariable} must not set TlsAllowInvalidCertificates=true; these tests must " +
                "validate the service certificate.");
            Assert.That(
                options.TlsCaCertificatePath,
                Is.Null,
                $"{ConnectionVariable} must not set TlsCaCertificatePath; these tests must use the host's " +
                "public trust store.");
        });

        return options;
    }

    /// <summary>A client against the Cloud service.</summary>
    /// <returns>A client built from <see cref="Options"/>.</returns>
    internal static ClickHouseTcpClient CreateClient() => new(Options());
}
