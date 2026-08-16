namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Which idle connection the pool hands out next. Both policies are equally correct, because a connection is checked
/// for age, idleness and liveness whichever end it comes from. The choice is about how work spreads over the pool.
/// </summary>
public enum ClickHouseTcpPoolReusePolicy
{
    /// <summary>
    /// Hand out the most recently returned connection. Concentrates traffic on a small hot set, letting the rest
    /// go idle and be closed, so a pool sized for peak load costs little off-peak. The default.
    /// </summary>
    Lifo,

    /// <summary>
    /// Hand out the least recently returned connection. Spreads traffic evenly, so under steady load every connection
    /// is used again inside its idle window and fewer are retired for idleness. The cost is keeping the whole pool
    /// warm rather than letting the surplus go.
    /// </summary>
    Fifo,
}
