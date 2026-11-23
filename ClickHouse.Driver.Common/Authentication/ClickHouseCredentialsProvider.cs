using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver;

/// <summary>
/// Gets the credentials that a client authenticates with. Use it for tokens that expire (refresh
/// them here) and for passwords that rotate.
/// </summary>
/// <param name="request">Why the client asks for credentials.</param>
/// <param name="cancellationToken">
/// Cancelled when the client is disposed. A caller that stops waiting does not cancel the call.
/// </param>
/// <returns>
/// The credentials, or <see langword="null"/> to use the static credentials of the client settings.
/// </returns>
/// <remarks>
/// <para>
/// A client calls the provider at most once at a time. It caches credentials that have a known expiry
/// (<see cref="ClickHouseCredentials.ExpiresAt"/>, or the <c>exp</c> claim of a JWT) and asks for new
/// ones before they expire. It calls the provider again each time for credentials without a known
/// expiry.
/// </para>
/// <para>
/// An exception from the provider goes to the operation that waits for the credentials. When the client
/// refreshes cached credentials in the background, it logs a failure as a warning and uses the cached
/// credentials until they expire.
/// </para>
/// <para>
/// The client does not limit how long a call takes, and the operations that need new credentials wait
/// for it. Give the provider its own timeout.
/// </para>
/// </remarks>
public delegate ValueTask<ClickHouseCredentials> ClickHouseCredentialsProvider(
    ClickHouseCredentialsRequest request,
    CancellationToken cancellationToken);
