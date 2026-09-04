namespace ClickHouse.Driver.Examples;

/// <summary>
/// Which ClickHouse interface an example talks to. The two use different ports, so an example can
/// fail for no reason other than the other one's port being closed.
/// </summary>
public enum ExampleTransport
{
    /// <summary>The HTTP interface, port 8123 by default.</summary>
    Http,

    /// <summary>The native protocol, port 9000 by default.</summary>
    Tcp,
}
