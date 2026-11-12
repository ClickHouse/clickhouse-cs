using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>Shorthands for the tests that do not use replica tags.</summary>
internal static class ConnectionSourceExtensions
{
    /// <summary>Rents a connection with the source's default replica tag.</summary>
    /// <param name="source">The source to rent from.</param>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing a connection.</param>
    /// <returns>A lease over a ready connection.</returns>
    internal static ValueTask<IConnectionLease> RentAsync(this IConnectionSource source, CancellationToken cancellationToken)
        => source.RentAsync(replicaTag: null, cancellationToken);
}
