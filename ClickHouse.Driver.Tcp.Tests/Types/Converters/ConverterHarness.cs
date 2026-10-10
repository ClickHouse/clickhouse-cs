using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Runs a derived converter and another path on the same input, so a test can compare them: reads through
/// <see cref="Block.ReadAs{T}(string)"/>, <see cref="BoundReader{T}.Fill"/> and a compiled
/// <see cref="ColumnReader.Emit"/>; writes through <see cref="IColumnCodec.WriteColumn(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/>
/// and <see cref="ColumnWriter{T}.Write"/>.
/// </summary>
internal static class ConverterHarness
{
    /// <summary>The session timezone of every test context: not UTC, so a DateTime with no timezone shows it.</summary>
    public const string SessionTimezone = "America/New_York";

    public static readonly ResolveContext Context = new() { ServerTimezone = SessionTimezone };

    public static IColumnCodec Codec(string type) => ColumnCodecRegistry.Default.Resolve(type, Context);

    /// <summary>Writes <paramref name="source"/> through the codec of <paramref name="type"/> and decodes it again.</summary>
    public static async Task<IColumn> DecodeAsync(string type, IColumn source)
    {
        IColumnCodec codec = Codec(type);
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteFull(w, source));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        if (source.RowCount > 0)
        {
            await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        }

        return await codec.ReadColumnAsync(reader, source.Name, type, source.RowCount, CodecTestHarness.None);
    }

    /// <summary>
    /// Decodes <paramref name="rows"/> rows of <paramref name="type"/> from <paramref name="bytes"/> as a query reads a
    /// column: the state prefix when there are rows, then the body. It fails the test when bytes are left over.
    /// </summary>
    public static async Task<IColumn> ReadBackAsync(string type, byte[] bytes, int rows, string name = "c")
    {
        IColumnCodec codec = Codec(type);
        using var stream = new System.IO.MemoryStream(bytes);
        using var reader = new ClickHouseBinaryReader(stream);
        if (rows > 0)
        {
            await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        }

        IColumn column = await codec.ReadColumnAsync(reader, name, type, rows, CodecTestHarness.None);
        Exception end = await CatchAsync(async () => await reader.ReadByteAsync(CodecTestHarness.None));
        Assert.That(end?.InnerException, Is.InstanceOf<System.IO.EndOfStreamException>(), $"{type}: the read leaves bytes after the column");
        return column;
    }

    /// <summary>A decoded <c>Nothing</c> column: the codec reads one byte for each row and keeps no value.</summary>
    public static async Task<IColumn> DecodeNothingAsync(int rows)
    {
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(new byte[rows]);
        return await Codec("Nothing").ReadColumnAsync(reader, "c", "Nothing", rows, CodecTestHarness.None);
    }

    /// <summary>The read of <see cref="Block.ReadAs{T}(string)"/>: the column itself, or the view of its derived reader.</summary>
    public static T[] ReadAs<T>(IColumn column, int start, int count)
        => ColumnCodecRegistry.Default.Projections.ReadAs<T>(column, Context).Values.Slice(start, count).ToArray();

    public static T[] ReadFill<T>(ColumnReader<T> reader, IColumn column, int start, int count)
    {
        var values = new T[count];
        reader.Bind(column).Fill(start, values);
        return values;
    }

    /// <summary>
    /// Compiles <see cref="ColumnReader.Emit"/> into a loop, as a POCO scatter does: the setup one time, then one
    /// expression for each row.
    /// </summary>
    public static T[] ReadEmit<T>(ColumnReader<T> reader, IColumn column, int start, int count)
    {
        ParameterExpression columnParameter = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression startParameter = Expression.Parameter(typeof(int), "start");
        ParameterExpression destination = Expression.Parameter(typeof(T[]), "destination");
        ParameterExpression i = Expression.Variable(typeof(int), "i");
        ParameterExpression row = Expression.Variable(typeof(int), "row");

        var scope = new EmitScope();
        Expression value = reader.Emit(columnParameter, row, scope);
        Assert.That(value.Type, Is.EqualTo(typeof(T)), "Emit must give an expression of the reader's value type.");

        LabelTarget done = Expression.Label("done");
        var body = new List<Expression>(scope.Setup)
        {
            Expression.Assign(i, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(i, Expression.ArrayLength(destination)),
                    Expression.Block(
                        Expression.Assign(row, Expression.Add(startParameter, i)),
                        Expression.Assign(Expression.ArrayAccess(destination, i), value),
                        Expression.PostIncrementAssign(i)),
                    Expression.Break(done)),
                done),
        };

        var loop = Expression.Lambda<Action<IColumn, int, T[]>>(
            Expression.Block(scope.Locals.Concat(new[] { i, row }), body),
            columnParameter,
            startParameter,
            destination).Compile();

        var values = new T[count];
        loop(column, start, values);
        return values;
    }

    /// <summary>The current write of rows [start, start + length) of a column of <paramref name="values"/>.</summary>
    public static Task<byte[]> WriteOldAsync<T>(string type, T[] values, int start, int length)
        => WriteOldAsync(type, new ArrayColumn<T>("c", type, values), start, length);

    public static Task<byte[]> WriteOldAsync(string type, IColumn column, int start, int length)
    {
        IColumnCodec codec = Codec(type);
        return CodecTestHarness.WriteAsync(w =>
        {
            IColumnWriteState state = codec.BeginWrite(column, start, length);
            try
            {
                codec.WriteStatePrefix(w, column, start, length, state);
                codec.WriteColumn(w, column, start, length, state);
            }
            finally
            {
                state?.Dispose();
            }
        });
    }

    /// <summary>
    /// The derived write of rows [start, start + length) of <paramref name="values"/>, from one span, for a column with
    /// the name of the column of <see cref="WriteOldAsync{T}(string, T[], int, int)"/>.
    /// </summary>
    public static Task<byte[]> WriteNewAsync<T>(ColumnWriter<T> writer, T[] values, int start, int length)
        => CodecTestHarness.WriteAsync(w => WriteAll(writer, w, ValueSource<T>.Of(values.AsSpan(start, length), start, "c")));

    /// <summary>The derived write of the segments, in order.</summary>
    public static Task<byte[]> WriteSegmentsAsync<T>(ColumnWriter<T> writer, T[][] segments)
        => CodecTestHarness.WriteAsync(w => WriteAll(writer, w, ValueSource<T>.OfSegments(segments)));

    public static void WriteAll<T>(ColumnWriter<T> writer, ClickHouseBinaryWriter output, ValueSource<T> source)
    {
        IColumnWriteState state = writer.Begin(source);
        try
        {
            writer.WritePrefix(output, source, state);
            writer.Write(output, source, state);
        }
        finally
        {
            state?.Dispose();
        }
    }

    /// <summary>Compares two values strictly: offsets, kinds, decimal scales, float bits and array contents too.</summary>
    public static bool StrictEquals(object expected, object actual) => (expected, actual) switch
    {
        (null, null) => true,
        (null, _) or (_, null) => false,
        (DateTimeOffset x, DateTimeOffset y) => x.EqualsExact(y),
        (DateTime x, DateTime y) => x.Ticks == y.Ticks && x.Kind == y.Kind,
        (byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
        (float x, float y) => BitConverter.SingleToUInt32Bits(x) == BitConverter.SingleToUInt32Bits(y),
        (double x, double y) => BitConverter.DoubleToUInt64Bits(x) == BitConverter.DoubleToUInt64Bits(y),
        (decimal x, decimal y) => decimal.GetBits(x).SequenceEqual(decimal.GetBits(y)),
        _ => expected.GetType() == actual.GetType() && expected.Equals(actual),
    };

    public static void AssertSameValues<T>(T[] expected, T[] actual, string path)
    {
        Assert.That(actual, Has.Length.EqualTo(expected.Length), $"{path}: row count");
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.That(StrictEquals(expected[i], actual[i]), Is.True, $"{path}: row {i} is {Describe(actual[i])}, expected {Describe(expected[i])}.");
        }
    }

    /// <summary>Runs <paramref name="action"/> and gives what it threw, or null.</summary>
    public static Exception Catch(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    public static async Task<Exception> CatchAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>Asserts that two failures are the same to a caller: type, message and parameter name.</summary>
    public static void AssertSameFailure(Exception expected, Exception actual, string path)
    {
        Assert.That(expected, Is.Not.Null, $"{path}: the path compared with must fail for this case.");
        Assert.That(actual, Is.Not.Null, $"{path}: the converter did not fail, the path compared with threw {expected?.GetType()}: {expected?.Message}");
        Assert.Multiple(() =>
        {
            Assert.That(actual.GetType(), Is.EqualTo(expected.GetType()), $"{path}: exception type");
            Assert.That(actual.Message, Is.EqualTo(expected.Message), $"{path}: message");
            Assert.That((actual as ArgumentException)?.ParamName, Is.EqualTo((expected as ArgumentException)?.ParamName), $"{path}: parameter name");
        });
    }

    /// <summary>
    /// Asserts that a failure is the pinned one: the name of the exception type, the parameter name of an
    /// <see cref="ArgumentException"/> (null for another exception), and the message.
    /// </summary>
    public static void AssertFailure(Exception actual, string exception, string parameter, string message, string path)
    {
        Assert.That(actual, Is.Not.Null, $"{path}: the write did not fail.");
        Assert.Multiple(() =>
        {
            Assert.That(actual.GetType().Name, Is.EqualTo(exception), $"{path}: exception type");
            Assert.That((actual as ArgumentException)?.ParamName, Is.EqualTo(parameter), $"{path}: parameter name");
            Assert.That(actual.Message, Is.EqualTo(message), $"{path}: message");
        });
    }

    /// <summary>
    /// The cases of <paramref name="cases"/> with the pinned error of each one appended to its arguments. A case is named by
    /// its first argument and its position in the list.
    /// </summary>
    public static IEnumerable<TestCaseData> WithErrors(IEnumerable<TestCaseData> cases, (string Exception, string Parameter, string Message)[] errors)
    {
        TestCaseData[] list = cases.ToArray();
        if (list.Length != errors.Length)
        {
            throw new InvalidOperationException($"{list.Length} cases have {errors.Length} pinned errors; pin one error for each case.");
        }

        for (int i = 0; i < list.Length; i++)
        {
            (string exception, string parameter, string message) = errors[i];
            yield return new TestCaseData(list[i].Arguments.Concat(new object[] { exception, parameter, message }).ToArray())
                .SetArgDisplayNames(list[i].Arguments[0]?.ToString(), $"case {i}");
        }
    }

    /// <summary>Calls a generic method of <paramref name="owner"/> closed over <paramref name="typeArguments"/>.</summary>
    public static object InvokeGeneric(Type owner, string method, Type[] typeArguments, params object[] arguments)
    {
        MethodInfo definition = owner.GetMethod(method, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"{owner} has no method {method}.");
        try
        {
            return definition.MakeGenericMethod(typeArguments).Invoke(null, arguments);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    private static string Describe(object value) => value switch
    {
        null => "null",
        byte[] bytes => $"[{Convert.ToHexString(bytes)}]",
        DateTimeOffset offset => offset.ToString("o"),
        DateTime dateTime => $"{dateTime:o} ({dateTime.Kind})",
        _ => $"{value} ({value.GetType().Name})",
    };
}
