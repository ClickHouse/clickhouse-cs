namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// One client of this repository, as the shim uses it: one session for the whole invocation.
/// </summary>
internal interface IBackend : IAsyncDisposable
{
    /// <summary>Gets a value indicating whether the session state (SET, USE, temporary tables) was lost, so
    /// that the runner must open the session again and run the USE and SET statements again.</summary>
    bool SessionLost { get; }

    Task OpenAsync();

    /// <summary>Opens a new session after <see cref="SessionLost"/>.</summary>
    Task ReopenAsync();

    /// <summary>Runs a statement that returns no result set.</summary>
    Task ExecuteAsync(string sql, QueryContext context);

    /// <summary>Runs a statement that returns a result set and writes the rows.</summary>
    Task QueryAsync(string sql, OutputFormat format, ResultWriter writer, QueryContext context);

    /// <summary>Records the new current database of the session after a successful USE.</summary>
    void UseDatabase(string database);
}

/// <summary>The settings and query parameters that go with one statement.</summary>
internal sealed record QueryContext(IReadOnlyDictionary<string, string> Settings, IReadOnlyDictionary<string, string> Parameters);
