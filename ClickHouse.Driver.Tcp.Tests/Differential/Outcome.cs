using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The kind of result of a read or a write of a range of rows in one tier.</summary>
internal enum OutcomeKind
{
    /// <summary>A read gave values.</summary>
    Values,

    /// <summary>A write gave bytes.</summary>
    Bytes,

    /// <summary>The tier refused the column before it read or wrote a value.</summary>
    Refused,

    /// <summary>The tier took the column, then threw on a value.</summary>
    Failed,

    /// <summary>The write has no input: a read back whose read gave no values.</summary>
    Unavailable,
}

/// <summary>A refusal whose message the differential tests do not compare with the refusal of another tier.</summary>
internal sealed class ArmRefusal(string message) : Exception(message);

/// <summary>Thrown by a tier runner that finds that the tier broke one of its own rules.</summary>
internal sealed class ArmInvariantException(string message) : Exception(message);

/// <summary>The result of a read or a write of a range of rows in one tier.</summary>
/// <param name="Kind">The kind of the result.</param>
/// <param name="ValueType">The CLR type of the values, for <see cref="OutcomeKind.Values"/>.</param>
/// <param name="Values">The values, boxed, for <see cref="OutcomeKind.Values"/>.</param>
/// <param name="Bytes">The bytes, for <see cref="OutcomeKind.Bytes"/>.</param>
/// <param name="ExceptionType">The exception type, for <see cref="OutcomeKind.Refused"/> and <see cref="OutcomeKind.Failed"/>.</param>
/// <param name="Message">The exception message, or the reason for <see cref="OutcomeKind.Unavailable"/>.</param>
internal sealed record Outcome(OutcomeKind Kind, Type ValueType = null, object[] Values = null, byte[] Bytes = null, Type ExceptionType = null, string Message = null)
{
    public static Outcome OfValues<T>(T[] values) => new(OutcomeKind.Values, typeof(T), Array.ConvertAll(values, value => (object)value));

    public static Outcome OfValues(Type valueType, object[] values) => new(OutcomeKind.Values, valueType, values);

    public static Outcome OfBytes(byte[] bytes) => new(OutcomeKind.Bytes, Bytes: bytes);

    public static Outcome Refusal(Exception exception) => new(OutcomeKind.Refused, ExceptionType: exception.GetType(), Message: exception.Message);

    public static Outcome Failure(Exception exception) => new(OutcomeKind.Failed, ExceptionType: exception.GetType(), Message: exception.Message);

    public static Outcome Unavailable(string reason) => new(OutcomeKind.Unavailable, Message: reason);

    /// <summary>The values from row <paramref name="start"/> to the end.</summary>
    public Outcome Tail(int start) => OfValues(ValueType, Values[start..]);

    /// <summary>The values as an array of their own CLR type.</summary>
    public T[] ValuesAs<T>() => Array.ConvertAll(Values, value => (T)value);

    /// <summary>Compares two outcomes: values strictly, bytes, and the type and message of a refusal or a failure.</summary>
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
            case OutcomeKind.Values when expected.ValueType != actual.ValueType:
                return $"the values are {TypeNames.Of(actual.ValueType)}, not {TypeNames.Of(expected.ValueType)}";

            case OutcomeKind.Values when expected.Values.Length != actual.Values.Length:
                return $"{actual.Values.Length} values, not {expected.Values.Length}";

            case OutcomeKind.Values:
                return Enumerable.Range(0, expected.Values.Length)
                    .Select(row => ValueComparer.Difference(expected.Values[row], actual.Values[row]) is string difference ? $"value {row}: {difference}" : null)
                    .FirstOrDefault(difference => difference is not null);

            case OutcomeKind.Bytes:
                int at = 0;
                while (at < Math.Min(expected.Bytes.Length, actual.Bytes.Length) && expected.Bytes[at] == actual.Bytes[at])
                {
                    at++;
                }

                return at == expected.Bytes.Length && at == actual.Bytes.Length ? null : $"{actual.Bytes.Length} bytes, not {expected.Bytes.Length}; the first difference is at byte {at}";

            case OutcomeKind.Refused when expected.ExceptionType == typeof(ArmRefusal) || actual.ExceptionType == typeof(ArmRefusal):
                return null;

            case OutcomeKind.Refused or OutcomeKind.Failed:
                return ExceptionDifference(expected, actual);

            default:
                return null;
        }
    }

    /// <summary>
    /// Compares the failure of a read or a write of several rows with the failures of its rows alone. When several rows
    /// fail, which one a tier names is not specified, so the failure must be the failure of one of its rows: the same
    /// exception type and message, which names the row of that value where it names a row.
    /// </summary>
    /// <param name="actual">The outcome of the read or the write.</param>
    /// <param name="rowFailures">The failures of the rows of the range, each read or written alone.</param>
    /// <returns>Null when the outcome is one of the failures, otherwise why not.</returns>
    public static string FailureDifference(Outcome actual, IEnumerable<Outcome> rowFailures)
    {
        if (actual.Kind != OutcomeKind.Failed)
        {
            return $"the kind is {actual.Kind}, not {OutcomeKind.Failed}";
        }

        List<Outcome> rows = rowFailures.ToList();
        return rows.Exists(row => ExceptionDifference(row, actual) is null) ? null : $"the failure is the failure of none of its rows ({string.Join("; ", rows)})";
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        string text = Kind switch
        {
            OutcomeKind.Values => $"{Values.Length} values of {TypeNames.Of(ValueType)}: [{string.Join(", ", Values.Select(ValueComparer.Describe))}]",
            OutcomeKind.Bytes => $"{Bytes.Length} bytes: {Convert.ToHexString(Bytes)}",
            OutcomeKind.Refused => $"refused with {ExceptionType.Name}: {Message}",
            OutcomeKind.Failed => $"failed with {ExceptionType.Name}: {Message}",
            _ => $"unavailable: {Message}",
        };

        return text.Length <= 400 ? text : text[..400] + " ...";
    }

    private static string ExceptionDifference(Outcome expected, Outcome actual)
        => expected.ExceptionType != actual.ExceptionType ? $"the exception is {actual.ExceptionType.Name}, not {expected.ExceptionType.Name}"
            : string.Equals(expected.Message, actual.Message, StringComparison.Ordinal) ? null
            : $"the message is \"{actual.Message}\", not \"{expected.Message}\"";
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

        if (expected is Array array && array.Length != ((Array)actual).Length)
        {
            return $"an array of {((Array)actual).Length} elements, not {array.Length}";
        }

        (string Name, object Expected, object Actual)[] parts = expected switch
        {
            Array elements => Enumerable.Range(0, elements.Length).Select(i => ($"element {i}", elements.GetValue(i), ((Array)actual).GetValue(i))).ToArray(),
            ITuple tuple => Enumerable.Range(0, tuple.Length).Select(i => ($"item {i + 1}", tuple[i], ((ITuple)actual)[i])).ToArray(),
            _ when IsPair(type) => new[] { ("key", Part(expected, "Key"), Part(actual, "Key")), ("value", Part(expected, "Value"), Part(actual, "Value")) },
            _ => null,
        };

        return parts is null
            ? (SameScalar(expected, actual) ? null : $"{Describe(actual)}, not {Describe(expected)}")
            : parts.Select(p => Difference(p.Expected, p.Actual) is string difference ? $"{p.Name}: {difference}" : null).FirstOrDefault(d => d is not null);
    }

    /// <summary>A short description of a value, for a failure message.</summary>
    public static string Describe(object value) => value switch
    {
        null => "null",
        string text => Quote(text),
        byte[] bytes => "0x" + Convert.ToHexString(bytes),
        float number => $"{number.ToString("R", CultureInfo.InvariantCulture)} (0x{BitConverter.SingleToInt32Bits(number):X8})",
        double number => $"{number.ToString("R", CultureInfo.InvariantCulture)} (0x{BitConverter.DoubleToInt64Bits(number):X16})",
        DateTime time => $"{time.ToString("O", CultureInfo.InvariantCulture)} ({time.Kind})",
        DateTimeOffset or TimeOnly or DateOnly => ((IFormattable)value).ToString("O", CultureInfo.InvariantCulture),
        Array array => "[" + string.Join(", ", array.Cast<object>().Select(Describe)) + "]",
        ITuple tuple => "(" + string.Join(", ", Enumerable.Range(0, tuple.Length).Select(i => Describe(tuple[i]))) + ")",
        _ when IsPair(value.GetType()) => $"{Describe(Part(value, "Key"))}: {Describe(Part(value, "Value"))}",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };

    private static bool IsPair(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);

    private static object Part(object pair, string name) => pair.GetType().GetProperty(name).GetValue(pair);

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
            builder.Append(c is '"' or '\\' ? $"\\{c}" : char.IsControl(c) || char.IsSurrogate(c) ? $"\\u{(int)c:X4}" : c.ToString());
        }

        return builder.Append('"').ToString();
    }
}

/// <summary>Short, readable names for CLR types, for failure messages.</summary>
internal static class TypeNames
{
    private static readonly Dictionary<Type, string> Keywords = new()
    {
        [typeof(bool)] = "bool", [typeof(byte)] = "byte", [typeof(sbyte)] = "sbyte", [typeof(short)] = "short", [typeof(ushort)] = "ushort",
        [typeof(int)] = "int", [typeof(uint)] = "uint", [typeof(long)] = "long", [typeof(ulong)] = "ulong", [typeof(float)] = "float",
        [typeof(double)] = "double", [typeof(decimal)] = "decimal", [typeof(string)] = "string", [typeof(object)] = "object", [typeof(char)] = "char",
    };

    /// <summary>The name of a type in C# spelling, without namespaces, for example <c>KeyValuePair&lt;string, DateTime?&gt;[]</c>.</summary>
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

        if (Nullable.GetUnderlyingType(type) is Type underlying)
        {
            return Of(underlying) + "?";
        }

        if (!type.IsGenericType)
        {
            return type.Name;
        }

        Type[] arguments = type.GetGenericArguments();
        bool tuple = (type.GetGenericTypeDefinition().FullName ?? type.Name).StartsWith("System.ValueTuple`", StringComparison.Ordinal) && arguments.Length > 1;
        string names = string.Join(", ", arguments.Select(Of));
        return tuple ? $"({names})" : $"{type.Name[..type.Name.IndexOf('`')]}<{names}>";
    }
}
