namespace ClickHouse.Driver;

/// <summary>
/// The request that a client passes to a <see cref="ClickHouseCredentialsProvider"/>.
/// </summary>
public sealed class ClickHouseCredentialsRequest
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCredentialsRequest"/> class, for example to
    /// test a provider.
    /// </summary>
    /// <param name="reason">Why the client asks for credentials.</param>
    public ClickHouseCredentialsRequest(ClickHouseCredentialsRefreshReason reason)
    {
        Reason = reason;
    }

    /// <summary>
    /// Gets the reason why the client asks for credentials.
    /// </summary>
    public ClickHouseCredentialsRefreshReason Reason { get; }

    /// <inheritdoc />
    public override string ToString() => $"{nameof(ClickHouseCredentialsRequest)} {{ Reason = {Reason} }}";
}
