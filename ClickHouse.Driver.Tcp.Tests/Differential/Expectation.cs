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

/// <summary>An outcome that a stated outcome expects from the baseline.</summary>
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
    /// </summary>
    /// <typeparam name="TException">The exception type.</typeparam>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails<TException>(params string[] messageParts)
        where TException : Exception
        => Fails(typeof(TException), messageParts);

    /// <summary>
    /// A failure with an exception of this type whose message contains this text, for all rows and for the tail.
    /// </summary>
    /// <param name="exceptionType">The exception type.</param>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails(Type exceptionType, params string[] messageParts)
        => new(
            $"a failure with {exceptionType.Name} that contains {Quote(messageParts)}",
            (actual, _, _) => FailureDifference(actual, exceptionType, messageParts));

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

    private static string FailureDifference(Outcome actual, Type exceptionType, string[] messageParts)
    {
        if (actual.Kind != OutcomeKind.Failed)
        {
            return $"the kind is {actual.Kind}, not {OutcomeKind.Failed}";
        }

        if (actual.ExceptionType != exceptionType)
        {
            return $"the exception is {actual.ExceptionType.Name}, not {exceptionType.Name}";
        }

        string missing = messageParts.FirstOrDefault(part => !actual.Message.Contains(part, StringComparison.Ordinal));
        return missing is null ? null : $"the message \"{actual.Message}\" does not contain \"{missing}\"";
    }
}
