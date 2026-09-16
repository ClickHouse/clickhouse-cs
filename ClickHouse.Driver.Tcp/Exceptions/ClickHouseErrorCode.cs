namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The ClickHouse error codes worth branching on, for <see cref="ClickHouseTcpServerException.Code"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is a selection, not the whole set: a 26.7 server names about 660 codes, and most of them describe
/// an internal condition no client can act on. A code outside this list reads as <see cref="Unknown"/>,
/// and <see cref="ClickHouseTcpServerException.RawCode"/> always carries the number the server sent.
/// </para>
/// <para>
/// Values are the server's own numbers, so a code can be compared against
/// <c>SELECT errorCodeToName(n)</c> or the <c>system.errors</c> table.
/// </para>
/// </remarks>
public enum ClickHouseErrorCode
{
    /// <summary>The server sent a code this client does not name. Read <see cref="ClickHouseTcpServerException.RawCode"/>.</summary>
    Unknown = -1,

    /// <summary>The operation is not supported.</summary>
    UnsupportedMethod = 1,

    /// <summary>A value could not be parsed from its text form.</summary>
    CannotParseText = 6,

    /// <summary>The same column name appears twice.</summary>
    DuplicateColumn = 15,

    /// <summary>The table has no column of that name.</summary>
    NoSuchColumnInTable = 16,

    /// <summary>
    /// A quoted string was expected and something else was found. What a query parameter named after a server
    /// setting produces, because the server reads the name as that setting and then parses its value as one.
    /// </summary>
    CannotParseQuotedString = 26,

    /// <summary>The input did not match the expected format.</summary>
    CannotParseInputAssertionFailed = 27,

    /// <summary>The data ended earlier than the format required.</summary>
    AttemptToReadAfterEof = 32,

    /// <summary>Fewer bytes arrived than the format required.</summary>
    CannotReadAllData = 33,

    /// <summary>A function was called with the wrong number of arguments.</summary>
    NumberOfArgumentsDoesntMatch = 42,

    /// <summary>A function was called with an argument of the wrong type.</summary>
    IllegalTypeOfArgument = 43,

    /// <summary>A column cannot be used where the query uses it.</summary>
    IllegalColumn = 44,

    /// <summary>No function of that name exists.</summary>
    UnknownFunction = 46,

    /// <summary>An identifier in the query resolves to nothing.</summary>
    UnknownIdentifier = 47,

    /// <summary>The server recognises the request but does not implement it.</summary>
    NotImplemented = 48,

    /// <summary>The server hit one of its own invariants. Worth reporting upstream.</summary>
    LogicalError = 49,

    /// <summary>No data type of that name exists.</summary>
    UnknownType = 50,

    /// <summary>A value's type does not match the type expected there.</summary>
    TypeMismatch = 53,

    /// <summary>A table of that name already exists.</summary>
    TableAlreadyExists = 57,

    /// <summary>No table of that name exists.</summary>
    UnknownTable = 60,

    /// <summary>The query could not be parsed.</summary>
    SyntaxError = 62,

    /// <summary>A value cannot be converted to the target type.</summary>
    CannotConvertType = 70,

    /// <summary>No database of that name exists.</summary>
    UnknownDatabase = 81,

    /// <summary>A database of that name already exists.</summary>
    DatabaseAlreadyExists = 82,

    /// <summary>The server could not make sense of a packet this client sent.</summary>
    UnknownPacketFromClient = 99,

    /// <summary>The server reported an unexpected packet on a connection it owns.</summary>
    UnexpectedPacketFromServer = 102,

    /// <summary>No setting of that name exists.</summary>
    UnknownSetting = 115,

    /// <summary>The query exceeded its row limit.</summary>
    TooManyRows = 158,

    /// <summary>The query exceeded <c>max_execution_time</c> or another server-side deadline.</summary>
    TimeoutExceeded = 159,

    /// <summary>The query was killed for running below <c>min_execution_speed</c>.</summary>
    TooSlow = 160,

    /// <summary>The user or the session may not perform writes.</summary>
    ReadOnly = 164,

    /// <summary>No user of that name exists.</summary>
    UnknownUser = 192,

    /// <summary>The password is wrong.</summary>
    WrongPassword = 193,

    /// <summary>The user requires a password and none was given.</summary>
    RequiredPassword = 194,

    /// <summary>The user may not connect from this address.</summary>
    IpAddressNotAllowed = 195,

    /// <summary>The server is already running <c>max_concurrent_queries</c>.</summary>
    TooManySimultaneousQueries = 202,

    /// <summary>The server has no free connection in an internal pool.</summary>
    NoFreeConnection = 203,

    /// <summary>A socket the server owns timed out.</summary>
    SocketTimeout = 209,

    /// <summary>The server hit a network failure of its own, often talking to a replica.</summary>
    NetworkError = 210,

    /// <summary>The connection reached the HTTP port instead of the native one.</summary>
    ClientHasConnectedToWrongPort = 217,

    /// <summary>The table was dropped while the query ran.</summary>
    TableIsDropped = 218,

    /// <summary>The operation was aborted.</summary>
    Aborted = 236,

    /// <summary>The query exceeded <c>max_memory_usage</c> or a server-wide memory limit.</summary>
    MemoryLimitExceeded = 241,

    /// <summary>The table does not accept writes.</summary>
    TableIsReadOnly = 242,

    /// <summary>The partition has more parts than <c>parts_to_throw_insert</c> allows; merges are behind.</summary>
    TooManyParts = 252,

    /// <summary>The server could not reach any replica.</summary>
    AllConnectionTriesFailed = 279,

    /// <summary>The query exceeded its byte limit.</summary>
    TooManyBytes = 307,

    /// <summary>A value does not fit the target data type.</summary>
    ValueIsOutOfRangeOfDataType = 321,

    /// <summary>A null was written into a column that is not nullable.</summary>
    CannotInsertNullInOrdinaryColumn = 349,

    /// <summary>No session of that id exists, or it has expired.</summary>
    SessionNotFound = 372,

    /// <summary>Another request is already using the session.</summary>
    SessionIsLocked = 373,

    /// <summary>The query was killed, by <c>KILL QUERY</c> or by a client cancellation.</summary>
    QueryWasCancelled = 394,

    /// <summary>The user lacks the privilege the statement needs.</summary>
    AccessDenied = 497,

    /// <summary>Authentication failed. The message says no more than that, deliberately.</summary>
    AuthenticationFailed = 516,

    /// <summary>The server shed the request because it is overloaded.</summary>
    ServerOverloaded = 745,

    /// <summary>The server could not complete an operation against Keeper or ZooKeeper.</summary>
    KeeperException = 999,
}
