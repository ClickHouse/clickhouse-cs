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

/// <summary>The reference outcomes of the other facets of a case, for an <see cref="Expectation"/> that refers to them.</summary>
internal interface IReferenceOutcomes
{
    /// <summary>The reference outcome of a read facet, or null when the case has no such facet or the tier no reference.</summary>
    /// <param name="tier"><see cref="Tier.ReadAs"/>, <see cref="Tier.Poco"/> or <see cref="Tier.CanRead"/>.</param>
    /// <param name="target">The read target.</param>
    /// <param name="rows">The row range.</param>
    /// <returns>The outcome.</returns>
    Outcome Read(Tier tier, Type target, Rows rows);

    /// <summary>The reference outcome of a write facet, or null when the case has no such facet or the tier no reference.</summary>
    /// <param name="tier"><see cref="Tier.Write"/> or <see cref="Tier.CanWrite"/>.</param>
    /// <param name="inputLabel">The label of the write input.</param>
    /// <param name="rows">The row range.</param>
    /// <returns>The outcome.</returns>
    Outcome Write(Tier tier, string inputLabel, Rows rows);
}

/// <summary>The outcome that a deliberate change expects from the candidates.</summary>
internal sealed class Expectation
{
    private readonly Func<Outcome, Rows, int, IReferenceOutcomes, string> verify;

    private Expectation(string description, Func<Outcome, Rows, int, IReferenceOutcomes, string> verify)
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
            (actual, rows, tailStart, _) =>
            {
                object[] expected = rows == Rows.All ? values : values[tailStart..];
                return Outcome.Difference(Outcome.OfValues(actual.ValueType ?? typeof(object), expected), actual);
            });

    /// <summary>The reference outcome of another read facet of the case, for the same rows. Value types may differ.</summary>
    /// <param name="tier"><see cref="Tier.ReadAs"/> or <see cref="Tier.Poco"/>.</param>
    /// <param name="target">The read target of the other facet.</param>
    /// <returns>The expectation.</returns>
    public static Expectation SameAs(Tier tier, Type target)
        => new(
            $"the reference outcome of {tier}<{TypeNames.Of(target)}>",
            (actual, rows, _, references) =>
            {
                Outcome expected = references.Read(tier, target, rows);
                if (expected is null)
                {
                    return $"the case has no reference outcome for {tier}<{TypeNames.Of(target)}>";
                }

                if (expected.Kind == OutcomeKind.Values && actual.Kind == OutcomeKind.Values)
                {
                    expected = Outcome.OfValues(actual.ValueType, expected.Values);
                }

                return Outcome.Difference(expected, actual);
            });

    /// <summary>The reference outcome of another write facet of the case, for the same rows.</summary>
    /// <param name="inputLabel">The label of the other write input.</param>
    /// <returns>The expectation.</returns>
    public static Expectation SameAsWrite(string inputLabel)
        => new(
            $"the reference outcome of Write[{inputLabel}]",
            (actual, rows, _, references) =>
            {
                Outcome expected = references.Write(Tier.Write, inputLabel, rows);
                return expected is null
                    ? $"the case has no reference outcome for Write[{inputLabel}]"
                    : Outcome.Difference(expected, actual);
            });

    /// <summary>These bytes for all rows, and these bytes for the tail.</summary>
    /// <param name="all">The bytes of the write of all rows.</param>
    /// <param name="tail">The bytes of the write of the tail.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Bytes(byte[] all, byte[] tail)
        => new(
            $"{all.Length} bytes for all rows and {tail.Length} bytes for the tail",
            (actual, rows, _, _) => Outcome.Difference(Outcome.OfBytes(rows == Rows.All ? all : tail), actual));

    /// <summary>This answer.</summary>
    /// <param name="answer">The answer of <c>CanRead</c> or <c>CanWrite</c>.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Answer(bool answer)
        => new($"the answer {answer}", (actual, _, _, _) => Outcome.Difference(Outcome.OfAnswer(answer), actual));

    /// <summary>A refusal with an exception of this type whose message contains this text, for every row range.</summary>
    /// <typeparam name="TException">The exception type.</typeparam>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Refused<TException>(params string[] messageParts)
        where TException : Exception
        => Refused(typeof(TException), messageParts);

    /// <summary>A refusal with an exception of this type whose message contains this text, for every row range.</summary>
    /// <param name="exceptionType">The exception type.</param>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Refused(Type exceptionType, params string[] messageParts)
        => new(
            $"a refusal with {exceptionType.Name} that contains {Quote(messageParts)}",
            (actual, _, _, _) => ExceptionDifference(actual, OutcomeKind.Refused, exceptionType, messageParts));

    /// <summary>
    /// A failure with an exception of this type whose message contains this text, for all rows. The tail is not
    /// checked, because it may not contain the row that fails.
    /// </summary>
    /// <typeparam name="TException">The exception type.</typeparam>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails<TException>(params string[] messageParts)
        where TException : Exception
        => Fails(typeof(TException), messageParts);

    /// <summary>
    /// A failure with an exception of this type whose message contains this text, for all rows. The tail is not
    /// checked, because it may not contain the row that fails.
    /// </summary>
    /// <param name="exceptionType">The exception type.</param>
    /// <param name="messageParts">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails(Type exceptionType, params string[] messageParts)
        => new(
            $"a failure with {exceptionType.Name} that contains {Quote(messageParts)}",
            (actual, rows, _, _) => rows == Rows.All ? ExceptionDifference(actual, OutcomeKind.Failed, exceptionType, messageParts) : null);

    /// <summary>Checks an outcome against the expectation.</summary>
    /// <param name="actual">The outcome.</param>
    /// <param name="rows">The row range of the outcome.</param>
    /// <param name="tailStart">The first row of the case that the tail covers.</param>
    /// <param name="references">The reference outcomes of the case.</param>
    /// <returns>Null when the outcome meets the expectation, otherwise why not.</returns>
    public string Verify(Outcome actual, Rows rows, int tailStart, IReferenceOutcomes references)
        => verify(actual, rows, tailStart, references);

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
