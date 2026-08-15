namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Which idle connection the pool hands out next. Both policies are equally correct — a connection is checked
/// for age and liveness whichever end it comes from — so the choice is about how work spreads over the pool.
/// </summary>
public enum ClickHouseTcpPoolReusePolicy
{
    /// <summary>
    /// Hand out the most recently returned connection. Concentrates traffic on a small hot set, letting the rest
    /// go idle and be closed, so a pool sized for peak load costs little off-peak. The default.
    /// </summary>
    Lifo,

    /// <summary>
    /// Hand out the least recently returned connection. Spreads traffic evenly, so every connection is exercised
    /// often enough that a server-side or network-side drop is found by use rather than at the next checkout.
    /// </summary>
    Fifo,
}
