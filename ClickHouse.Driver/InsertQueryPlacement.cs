namespace ClickHouse.Driver;

/// <summary>
/// Specifies where a binary insert sends its <c>INSERT INTO ... FORMAT ...</c> statement.
/// </summary>
public enum InsertQueryPlacement
{
    /// <summary>
    /// The statement is written as the first line of the request body, ahead of the rows. The body
    /// carries the whole request, so no part of it is subject to a URL length limit.
    /// </summary>
    Body = 0,

    /// <summary>
    /// The statement is sent as the <c>query</c> URL parameter and the body carries only rows. This
    /// makes the statement readable by proxies, gateways and access logs, which cannot see into a
    /// compressed body. The URL grows with the statement, so a long column list can exceed the
    /// server's <c>max_uri_size</c> (1 MiB by default), which the server rejects with
    /// <c>400 Bad Request</c>, or a lower limit of an intermediary.
    /// </summary>
    Url = 1,
}
