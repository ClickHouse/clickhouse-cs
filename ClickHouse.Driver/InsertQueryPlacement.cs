namespace ClickHouse.Driver;

/// <summary>
/// Specifies where a binary insert sends its <c>INSERT INTO ... FORMAT ...</c> statement.
/// </summary>
public enum InsertQueryPlacement
{
    /// <summary>
    /// The statement is written as the first line of the request body, ahead of the rows. The statement
    /// and rows are not subject to a URL length limit; other request options can still appear in the URL.
    /// </summary>
    Body = 0,

    /// <summary>
    /// The statement is sent as the <c>query</c> URL parameter and the body carries only rows. This
    /// makes the statement available to routing and logging that inspect only the URL, without decoding
    /// and inspecting the request body. The effective URL limit is the lowest imposed by the .NET runtime,
    /// an intermediary, and the server. On .NET 6 through .NET 9, the complete encoded URI cannot exceed
    /// 65,519 characters; the server's <c>http_max_uri_size</c> is 1 MiB by default.
    /// </summary>
    Url = 1,
}
