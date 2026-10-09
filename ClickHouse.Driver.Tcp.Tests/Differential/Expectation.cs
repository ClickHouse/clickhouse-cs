using System;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The row range of a facet run.</summary>
internal enum Rows
{
    /// <summary>All rows, <c>[0, RowCount)</c>.</summary>
    All,

    /// <summary>The tail, <c>[RowCount / 2, RowCount)</c>: a read window or a write slice with a start above zero.</summary>
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

    /// <summary>These values, for all rows. The tail must be the same values from row <c>RowCount / 2</c>.</summary>
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
    /// <param name="messagePart">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Refused<TException>(string messagePart)
        where TException : Exception
        => new(
            $"a refusal with {typeof(TException).Name} that contains \"{messagePart}\"",
            (actual, _, _, _) => ExceptionDifference(actual, OutcomeKind.Refused, typeof(TException), messagePart));

    /// <summary>
    /// A failure with an exception of this type whose message contains this text, for all rows. The tail is not
    /// checked, because it may not contain the row that fails.
    /// </summary>
    /// <typeparam name="TException">The exception type.</typeparam>
    /// <param name="messagePart">Text that the message must contain.</param>
    /// <returns>The expectation.</returns>
    public static Expectation Fails<TException>(string messagePart)
        where TException : Exception
        => new(
            $"a failure with {typeof(TException).Name} that contains \"{messagePart}\"",
            (actual, rows, _, _) => rows == Rows.All ? ExceptionDifference(actual, OutcomeKind.Failed, typeof(TException), messagePart) : null);

    /// <summary>Checks an outcome against the expectation.</summary>
    /// <param name="actual">The outcome.</param>
    /// <param name="rows">The row range of the outcome.</param>
    /// <param name="tailStart">The first row of the tail.</param>
    /// <param name="references">The reference outcomes of the case.</param>
    /// <returns>Null when the outcome meets the expectation, otherwise why not.</returns>
    public string Verify(Outcome actual, Rows rows, int tailStart, IReferenceOutcomes references)
        => verify(actual, rows, tailStart, references);

    /// <inheritdoc/>
    public override string ToString() => Description;

    private static string ExceptionDifference(Outcome actual, OutcomeKind kind, Type exceptionType, string messagePart)
    {
        if (actual.Kind != kind)
        {
            return $"the kind is {actual.Kind}, not {kind}";
        }

        if (actual.ExceptionType != exceptionType)
        {
            return $"the exception is {actual.ExceptionType.Name}, not {exceptionType.Name}";
        }

        return actual.Message.Contains(messagePart, StringComparison.Ordinal)
            ? null
            : $"the message \"{actual.Message}\" does not contain \"{messagePart}\"";
    }
}
