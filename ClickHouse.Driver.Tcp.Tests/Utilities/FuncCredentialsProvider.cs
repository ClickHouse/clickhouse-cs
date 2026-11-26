using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>An <see cref="IClickHouseCredentialsProvider"/> that runs a function, for tests.</summary>
internal sealed class FuncCredentialsProvider : IClickHouseCredentialsProvider
{
    private readonly Func<ClickHouseCredentialsRequest, CancellationToken, ValueTask<ClickHouseCredentials>> getCredentials;

    public FuncCredentialsProvider(Func<ClickHouseCredentialsRequest, CancellationToken, ValueTask<ClickHouseCredentials>> getCredentials)
        => this.getCredentials = getCredentials;

    public ValueTask<ClickHouseCredentials> GetCredentialsAsync(ClickHouseCredentialsRequest request, CancellationToken cancellationToken)
        => getCredentials(request, cancellationToken);
}
