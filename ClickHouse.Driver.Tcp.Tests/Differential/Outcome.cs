using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The kind of result that an implementation gives for one facet and one row range.</summary>
internal enum OutcomeKind
{
    /// <summary>A read gave values.</summary>
    Values,

    /// <summary>A write gave bytes.</summary>
    Bytes,

    /// <summary><c>CanRead</c> or <c>CanWrite</c> gave an answer.</summary>
    Answer,

    /// <summary>The implementation refused the facet before it read or wrote a value.</summary>
    Refused,

    /// <summary>The implementation accepted the facet, then threw on a value.</summary>
    Failed,

    /// <summary>The facet has no input, for example a read back whose reference read was refused.</summary>
    Unavailable,
}

/// <summary>
/// Thrown by an implementation under test to refuse a facet with a message that the differential tests do not
/// compare. A refusal of any other type is compared by its type and its message.
/// </summary>
internal sealed class ArmRefusal : Exception
{
    public ArmRefusal(string message)
        : base(message)
    {
    }
}

/// <summary>Thrown by an implementation under test when it finds that the implementation broke one of its own rules.</summary>
internal sealed class ArmInvariantException : Exception
{
    public ArmInvariantException(string message)
        : base(message)
    {
    }
}

/// <summary>The result that an implementation gives for one facet and one row range.</summary>
internal sealed class Outcome
{
    private const int MaxDescriptionLength = 400;

    private Outcome(OutcomeKind kind, Type valueType = null, object[] values = null, byte[] bytes = null, bool answer = false, Type exceptionType = null, string message = null)
    {
        Kind = kind;
        ValueType = valueType;
        Values = values;
        Bytes = bytes;
        Answer = answer;
        ExceptionType = exceptionType;
        Message = message;
    }

    public OutcomeKind Kind { get; }

    /// <summary>The CLR type of the values, for <see cref="OutcomeKind.Values"/>.</summary>
    public Type ValueType { get; }

    /// <summary>The values, boxed, for <see cref="OutcomeKind.Values"/>.</summary>
    public object[] Values { get; }

    /// <summary>The bytes, for <see cref="OutcomeKind.Bytes"/>.</summary>
    public byte[] Bytes { get; }

    /// <summary>The answer, for <see cref="OutcomeKind.Answer"/>.</summary>
    public bool Answer { get; }

    /// <summary>The exception type, for <see cref="OutcomeKind.Refused"/> and <see cref="OutcomeKind.Failed"/>.</summary>
    public Type ExceptionType { get; }

    /// <summary>The exception message, or the reason for <see cref="OutcomeKind.Unavailable"/>.</summary>
    public string Message { get; }

    /// <summary>Whether the implementation gave values, bytes or an answer.</summary>
    public bool IsResult => Kind is OutcomeKind.Values or OutcomeKind.Bytes or OutcomeKind.Answer;

    public static Outcome OfValues<T>(T[] values)
    {
        var boxed = new object[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            boxed[i] = values[i];
        }

        return new Outcome(OutcomeKind.Values, valueType: typeof(T), values: boxed);
    }

    public static Outcome OfValues(Type valueType, object[] values) => new(OutcomeKind.Values, valueType: valueType, values: values);

    public static Outcome OfBytes(byte[] bytes) => new(OutcomeKind.Bytes, bytes: bytes);

    public static Outcome OfAnswer(bool answer) => new(OutcomeKind.Answer, answer: answer);

    public static Outcome Refusal(Exception exception) => new(OutcomeKind.Refused, exceptionType: exception.GetType(), message: exception.Message);

    public static Outcome Failure(Exception exception) => new(OutcomeKind.Failed, exceptionType: exception.GetType(), message: exception.Message);

    public static Outcome Unavailable(string reason) => new(OutcomeKind.Unavailable, message: reason);

    /// <summary>The values from row <paramref name="start"/> to the end.</summary>
    /// <param name="start">The first row to keep.</param>
    /// <returns>The values of those rows.</returns>
    public Outcome Tail(int start)
        => Kind == OutcomeKind.Values
            ? new Outcome(OutcomeKind.Values, valueType: ValueType, values: Values[start..])
            : throw new InvalidOperationException($"Only values have a tail; this outcome is {Kind}.");

    /// <summary>The values as an array of their own CLR type.</summary>
    /// <typeparam name="T">The CLR type of the values.</typeparam>
    /// <returns>The values.</returns>
    public T[] ValuesAs<T>()
    {
        var values = new T[Values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = (T)Values[i];
        }

        return values;
    }

    /// <summary>Compares two outcomes.</summary>
    /// <param name="expected">The outcome to compare with.</param>
    /// <param name="actual">The outcome to compare.</param>
    /// <returns>Null when the outcomes are the same, otherwise the first difference.</returns>
    public static string Difference(Outcome expected, Outcome actual)
    {
        if (expected.Kind != actual.Kind)
        {
            return $"the kind is {actual.Kind}, not {expected.Kind}";
        }

        switch (expected.Kind)
        {
            case OutcomeKind.Values:
                if (expected.ValueType != actual.ValueType)
                {
                    return $"the values are {TypeNames.Of(actual.ValueType)}, not {TypeNames.Of(expected.ValueType)}";
                }

                if (expected.Values.Length != actual.Values.Length)
                {
                    return $"{actual.Values.Length} values, not {expected.Values.Length}";
                }

                for (int row = 0; row < expected.Values.Length; row++)
                {
                    string difference = ValueComparer.Difference(expected.Values[row], actual.Values[row]);
                    if (difference is not null)
                    {
                        return $"value {row}: {difference}";
                    }
                }

                return null;

            case OutcomeKind.Bytes:
                return expected.Bytes.AsSpan().SequenceEqual(actual.Bytes) ? null : DescribeByteDifference(expected.Bytes, actual.Bytes);

            case OutcomeKind.Answer:
                return expected.Answer == actual.Answer ? null : $"the answer is {actual.Answer}, not {expected.Answer}";

            case OutcomeKind.Refused:
                // An ArmRefusal carries a message that is not part of the comparison.
                if (expected.ExceptionType == typeof(ArmRefusal) || actual.ExceptionType == typeof(ArmRefusal))
                {
                    return null;
                }

                return DescribeExceptionDifference(expected, actual);

            case OutcomeKind.Failed:
                return DescribeExceptionDifference(expected, actual);

            default:
                return null;
        }
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        string text = Kind switch
        {
            OutcomeKind.Values => $"{Values.Length} values of {TypeNames.Of(ValueType)}: [{string.Join(", ", Values.Select(ValueComparer.Describe))}]",
            OutcomeKind.Bytes => $"{Bytes.Length} bytes: {Convert.ToHexString(Bytes)}",
            OutcomeKind.Answer => $"answer {Answer}",
            OutcomeKind.Refused => $"refused with {ExceptionType.Name}: {Message}",
            OutcomeKind.Failed => $"failed with {ExceptionType.Name}: {Message}",
            _ => $"unavailable: {Message}",
        };

        return text.Length <= MaxDescriptionLength ? text : text[..MaxDescriptionLength] + " ...";
    }

    private static string DescribeExceptionDifference(Outcome expected, Outcome actual)
    {
        if (expected.ExceptionType != actual.ExceptionType)
        {
            return $"the exception is {actual.ExceptionType.Name}, not {expected.ExceptionType.Name}";
        }

        return string.Equals(expected.Message, actual.Message, StringComparison.Ordinal)
            ? null
            : $"the message is \"{actual.Message}\", not \"{expected.Message}\"";
    }

    private static string DescribeByteDifference(byte[] expected, byte[] actual)
    {
        int length = Math.Min(expected.Length, actual.Length);
        int at = 0;
        while (at < length && expected[at] == actual[at])
        {
            at++;
        }

        return $"{actual.Length} bytes, not {expected.Length}; the first difference is at byte {at}";
    }
}

/// <summary>
/// Compares two values strictly: the same runtime type, the same bits for floating-point values, the same
/// <see cref="DateTime.Kind"/> and offset, the same decimal scale, and the same content for arrays, tuples and
/// key-value pairs.
/// </summary>
internal static class ValueComparer
{
    /// <summary>Compares two values.</summary>
    /// <param name="expected">The value to compare with.</param>
    /// <param name="actual">The value to compare.</param>
    /// <returns>Null when the values are the same, otherwise the first difference.</returns>
    public static string Difference(object expected, object actual)
    {
        if (expected is null || actual is null)
        {
            return expected is null && actual is null ? null : $"{Describe(actual)}, not {Describe(expected)}";
        }

        Type type = expected.GetType();
        if (type != actual.GetType())
        {
            return $"{Describe(actual)} of {TypeNames.Of(actual.GetType())}, not {Describe(expected)} of {TypeNames.Of(type)}";
        }

        if (expected is Array expectedArray)
        {
            var actualArray = (Array)actual;
            if (expectedArray.Length != actualArray.Length)
            {
                return $"an array of {actualArray.Length} elements, not {expectedArray.Length}";
            }

            for (int i = 0; i < expectedArray.Length; i++)
            {
                string difference = Difference(expectedArray.GetValue(i), actualArray.GetValue(i));
                if (difference is not null)
                {
                    return $"element {i}: {difference}";
                }
            }

            return null;
        }

        if (expected is ITuple expectedTuple)
        {
            var actualTuple = (ITuple)actual;
            for (int i = 0; i < expectedTuple.Length; i++)
            {
                string difference = Difference(expectedTuple[i], actualTuple[i]);
                if (difference is not null)
                {
                    return $"item {i + 1}: {difference}";
                }
            }

            return null;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
        {
            string difference = Difference(type.GetProperty("Key").GetValue(expected), type.GetProperty("Key").GetValue(actual));
            if (difference is not null)
            {
                return $"key: {difference}";
            }

            difference = Difference(type.GetProperty("Value").GetValue(expected), type.GetProperty("Value").GetValue(actual));
            return difference is null ? null : $"value: {difference}";
        }

        return SameScalar(expected, actual) ? null : $"{Describe(actual)}, not {Describe(expected)}";
    }

    /// <summary>A short description of a value, for a failure message.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The description.</returns>
    public static string Describe(object value) => value switch
    {
        null => "null",
        string text => Quote(text),
        byte[] bytes => "0x" + Convert.ToHexString(bytes),
        float number => $"{number.ToString("R", CultureInfo.InvariantCulture)} (0x{BitConverter.SingleToInt32Bits(number):X8})",
        double number => $"{number.ToString("R", CultureInfo.InvariantCulture)} (0x{BitConverter.DoubleToInt64Bits(number):X16})",
        decimal number => number.ToString(CultureInfo.InvariantCulture),
        DateTime time => $"{time.ToString("O", CultureInfo.InvariantCulture)} ({time.Kind})",
        DateTimeOffset time => time.ToString("O", CultureInfo.InvariantCulture),
        Array array => "[" + string.Join(", ", array.Cast<object>().Select(Describe)) + "]",
        ITuple tuple => "(" + string.Join(", ", Enumerable.Range(0, tuple.Length).Select(i => Describe(tuple[i]))) + ")",
        _ when value.GetType().IsGenericType && value.GetType().GetGenericTypeDefinition() == typeof(KeyValuePair<,>)
            => $"{Describe(value.GetType().GetProperty("Key").GetValue(value))}: {Describe(value.GetType().GetProperty("Value").GetValue(value))}",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };

    private static bool SameScalar(object expected, object actual) => expected switch
    {
        float number => BitConverter.SingleToInt32Bits(number) == BitConverter.SingleToInt32Bits((float)actual),
        double number => BitConverter.DoubleToInt64Bits(number) == BitConverter.DoubleToInt64Bits((double)actual),
        decimal number => decimal.GetBits(number).AsSpan().SequenceEqual(decimal.GetBits((decimal)actual)),
        DateTime time => time.Ticks == ((DateTime)actual).Ticks && time.Kind == ((DateTime)actual).Kind,
        DateTimeOffset time => time.UtcTicks == ((DateTimeOffset)actual).UtcTicks && time.Offset == ((DateTimeOffset)actual).Offset,
        ClickHouseTcpDecimal number => number.Mantissa == ((ClickHouseTcpDecimal)actual).Mantissa && number.Scale == ((ClickHouseTcpDecimal)actual).Scale,
        string text => string.Equals(text, (string)actual, StringComparison.Ordinal),
        IPAddress address => address.Equals(actual),
        _ => expected.Equals(actual),
    };

    private static string Quote(string text)
    {
        var builder = new StringBuilder("\"");
        foreach (char c in text)
        {
            builder.Append(c switch
            {
                '"' => "\\\"",
                '\\' => "\\\\",
                _ when char.IsControl(c) || char.IsSurrogate(c) => $"\\u{(int)c:X4}",
                _ => c.ToString(),
            });
        }

        return builder.Append('"').ToString();
    }
}

/// <summary>Short, readable names for CLR types, for facet names and failure messages.</summary>
internal static class TypeNames
{
    private static readonly Dictionary<Type, string> Keywords = new()
    {
        [typeof(bool)] = "bool",
        [typeof(byte)] = "byte",
        [typeof(sbyte)] = "sbyte",
        [typeof(short)] = "short",
        [typeof(ushort)] = "ushort",
        [typeof(int)] = "int",
        [typeof(uint)] = "uint",
        [typeof(long)] = "long",
        [typeof(ulong)] = "ulong",
        [typeof(float)] = "float",
        [typeof(double)] = "double",
        [typeof(decimal)] = "decimal",
        [typeof(string)] = "string",
        [typeof(object)] = "object",
        [typeof(char)] = "char",
    };

    /// <summary>The name of a type in C# spelling, without namespaces.</summary>
    /// <param name="type">The type.</param>
    /// <returns>The name, for example <c>KeyValuePair&lt;string, DateTime?&gt;[]</c>.</returns>
    public static string Of(Type type)
    {
        if (Keywords.TryGetValue(type, out string keyword))
        {
            return keyword;
        }

        if (type.IsArray)
        {
            return Of(type.GetElementType()) + "[" + new string(',', type.GetArrayRank() - 1) + "]";
        }

        Type underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
        {
            return Of(underlying) + "?";
        }

        if (!type.IsGenericType)
        {
            return type.Name;
        }

        Type[] arguments = type.GetGenericArguments();
        string definition = type.GetGenericTypeDefinition().FullName ?? type.Name;
        if (definition.StartsWith("System.ValueTuple`", StringComparison.Ordinal) && arguments.Length > 1)
        {
            return "(" + string.Join(", ", arguments.Select(Of)) + ")";
        }

        string name = type.Name[..type.Name.IndexOf('`')];
        return name + "<" + string.Join(", ", arguments.Select(Of)) + ">";
    }
}
