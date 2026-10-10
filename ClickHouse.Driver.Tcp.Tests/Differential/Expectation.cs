using System;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The row range of a facet run.</summary>
internal enum Rows
{
    /// <summary>All rows, <c>[0, RowCount)</c>.</summary>
    All,

    /// <summary>
    /// The tail: a read window or a write slice with a start above zero. For a case of two rows or more it is
    /// <c>[RowCount / 2, RowCount)</c>. For a case of one row it is row 1 of a column that has a row before the case's row.
    /// </summary>
    Tail,
}

/// <summary>An outcome that a declared outcome or a stated outcome expects from an arm.</summary>
internal sealed class Expectation
{
    private readonly Func<Outcome, Rows, int, string> verify;

    private Expectation(string description, Func<Outcome, Rows, int, string> verify)
    {
        Description = description;
        this.verify = verify;
    }

    /// <summary>A description of the expected outcome, for failure messages.</summary>
    public string Description { get; }

    /// <summary>These values, for all rows. The tail must be the same values from the first row of the case that the tail reads.</summary>
    /// <param name="values">The values, boxed, one for each row.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Values(params object[] values)
        => new(
            $"the values [{string.Join(", ", values.Select(ValueComparer.Describe))}]",
            (actual, rows, tailStart) =>
            {
                object[] expected = rows == Rows.All ? values : values[tailStart..];
                return Outcome.Difference(Outcome.OfValues(actual.ValueType ?? typeof(object), expected), actual);
            });

    /// <summary>
    /// A failure with an exception of this type whose message contains this text, for all rows and for the tail.
    /// When the tail fails with other text (for example at another row) or gives values, use
    /// <see cref="ForRows"/> to give the tail an expectation of its own.
    /// </summary>
    /// <typeparam name="TException">The exception type.</typeparam>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails<TException>(params string[] messageParts)
        where TException : Exception
        => Fails(typeof(TException), messageParts);

    /// <summary>
    /// A failure with an exception of this type whose message contains this text, for all rows and for the tail.
    /// When the tail fails with other text (for example at another row) or gives values, use
    /// <see cref="ForRows"/> to give the tail an expectation of its own.
    /// </summary>
    /// <param name="exceptionType">The exception type.</param>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails(Type exceptionType, params string[] messageParts)
        => new(
            $"a failure with {exceptionType.Name} that contains {Quote(messageParts)}",
            (actual, _, _) => ExceptionDifference(actual, OutcomeKind.Failed, exceptionType, messageParts));

    /// <summary>
    /// One expectation for all rows and another for the tail. Each part checks its own range only. A
    /// <see cref="Values"/> part lists the values of all rows, as it does alone.
    /// </summary>
    /// <param name="all">The expectation for all rows.</param>
    /// <param name="tail">The expectation for the tail.</param>
    /// <returns>The expectation.</returns>
    public static Expectation ForRows(Expectation all, Expectation tail)
        => new(
            $"{all.Description} for all rows, and {tail.Description} for the tail",
            (actual, rows, tailStart) => (rows == Rows.All ? all : tail).Verify(actual, rows, tailStart));

    /// <summary>Checks an outcome against the expectation.</summary>
    /// <param name="actual">The outcome.</param>
    /// <param name="rows">The row range of the outcome.</param>
    /// <param name="tailStart">The first row of the case that the tail covers.</param>
    /// <returns>Null when the outcome meets the expectation, otherwise why not.</returns>
    public string Verify(Outcome actual, Rows rows, int tailStart)
        => verify(actual, rows, tailStart);

    /// <inheritdoc/>
    public override string ToString() => Description;

    private static string Quote(string[] parts) => string.Join(" and ", parts.Select(part => $"\"{part}\""));

    private static string ExceptionDifference(Outcome actual, OutcomeKind kind, Type exceptionType, string[] messageParts)
    {
        if (actual.Kind != kind)
        {
            return $"the kind is {actual.Kind}, not {kind}";
        }

        if (actual.ExceptionType != exceptionType)
        {
            return $"the exception is {actual.ExceptionType.Name}, not {exceptionType.Name}";
        }

        string missing = messageParts.FirstOrDefault(part => !actual.Message.Contains(part, StringComparison.Ordinal));
        return missing is null ? null : $"the message \"{actual.Message}\" does not contain \"{missing}\"";
    }
}
