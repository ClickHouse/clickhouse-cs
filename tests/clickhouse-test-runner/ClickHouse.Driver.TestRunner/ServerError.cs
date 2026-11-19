using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.TestRunner;

/// <summary>An error that the server reported, as either client gives it.</summary>
internal sealed record ServerError(int Code, string Message)
{
    /// <summary>
    /// Returns the server error in <paramref name="e"/>, or null when <paramref name="e"/> is not a server
    /// error (a client or transport failure).
    /// </summary>
    public static ServerError? From(Exception e)
    {
        switch (e)
        {
            case ClickHouseServerException http:
                return new ServerError(http.ErrorCode, http.Message);
            case ClickHouseTcpServerException tcp:
                return new ServerError(tcp.RawCode, tcp.Message);
            case AggregateException { InnerExceptions.Count: 1 } a:
                return From(a.InnerException!);
            default:
                return null;
        }
    }

    /// <summary>
    /// Writes the error the way <c>clickhouse-client</c> does, so that a reader can find the code and the
    /// name: <c>Code: 60. DB::Exception: ... (UNKNOWN_TABLE)</c>.
    /// </summary>
    public string Format()
    {
        var msg = Message.Trim();
        if (msg.StartsWith("Code: ", StringComparison.Ordinal))
        {
            return msg;
        }
        var name = ErrorCodes.NameOf(Code);
        if (name != null && !msg.Contains("(" + name + ")", StringComparison.Ordinal))
        {
            msg += " (" + name + ")";
        }
        return $"Code: {Code}. DB::Exception: {msg}";
    }
}
