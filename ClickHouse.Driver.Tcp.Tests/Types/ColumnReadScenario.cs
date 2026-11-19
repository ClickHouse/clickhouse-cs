using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Canonical values of one ClickHouse type, read as one CLR type, and the outcome of that reading: the values it
/// gives, or the exception it throws on a row. The differential tests check each scenario against
/// <c>Block.ReadAs&lt;T&gt;</c>, for all the rows and for a tail.
/// </summary>
public sealed class ColumnReadScenario
{
    private ColumnReadScenario(string name, string columnType, Array values, Type target, Array expected, Type exceptionType, string[] messageParts)
    {
        Name = name;
        ColumnType = columnType;
        Values = values;
        Target = target;
        Expected = expected;
        ExceptionType = exceptionType;
        MessageParts = messageParts;
    }

    /// <summary>The name of the scenario, unique in the case list.</summary>
    public string Name { get; }

    /// <summary>The ClickHouse type of the column.</summary>
    public string ColumnType { get; }

    /// <summary>The values of the column, in the canonical CLR type of <see cref="ColumnType"/>.</summary>
    public Array Values { get; }

    /// <summary>The CLR type to read the values as.</summary>
    public Type Target { get; }

    /// <summary>The values that the reading gives, one for each value, or null when it throws.</summary>
    public Array Expected { get; }

    /// <summary>The exception that the reading throws on each value, or null when it gives values.</summary>
    public Type ExceptionType { get; }

    /// <summary>Text that the message of <see cref="ExceptionType"/> contains.</summary>
    public IReadOnlyList<string> MessageParts { get; }

    /// <summary>A reading that gives values.</summary>
    /// <typeparam name="TSource">The canonical CLR type of the column type.</typeparam>
    /// <typeparam name="TTarget">The CLR type to read the values as.</typeparam>
    /// <param name="name">The name of the scenario.</param>
    /// <param name="columnType">The ClickHouse type of the column.</param>
    /// <param name="values">The values of the column.</param>
    /// <param name="expected">The values that the reading gives, one for each value.</param>
    /// <returns>The scenario.</returns>
    public static ColumnReadScenario Reads<TSource, TTarget>(string name, string columnType, TSource[] values, params TTarget[] expected)
        => values.Length == expected.Length
            ? new(name, columnType, values, typeof(TTarget), expected, exceptionType: null, Array.Empty<string>())
            : throw new ArgumentException($"Scenario '{name}' has {values.Length} values and {expected.Length} expected values.", nameof(expected));

    /// <summary>A reading that throws on the value.</summary>
    /// <typeparam name="TSource">The canonical CLR type of the column type.</typeparam>
    /// <typeparam name="TTarget">The CLR type to read the value as.</typeparam>
    /// <typeparam name="TException">The exception that the reading throws.</typeparam>
    /// <param name="name">The name of the scenario.</param>
    /// <param name="columnType">The ClickHouse type of the column.</param>
    /// <param name="value">The value of the column.</param>
    /// <param name="messageParts">Text that the message contains.</param>
    /// <returns>The scenario.</returns>
    public static ColumnReadScenario Throws<TSource, TTarget, TException>(string name, string columnType, TSource value, params string[] messageParts)
        where TException : Exception
        => new(name, columnType, new[] { value }, typeof(TTarget), expected: null, typeof(TException), messageParts);

    /// <inheritdoc/>
    public override string ToString() => Name;
}
