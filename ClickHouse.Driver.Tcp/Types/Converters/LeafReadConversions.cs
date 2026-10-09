using System;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Text;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The conversion of one read leaf: from the value that the decoded column stores to the CLR type. A struct, so
/// the JIT can inline it into the loop of the reader.
/// </summary>
/// <typeparam name="TCanon">The value that the decoded column stores (its <see cref="IColumn{T}"/> type).</typeparam>
/// <typeparam name="T">The CLR type that the leaf gives.</typeparam>
internal interface IReadConversion<TCanon, T>
{
    /// <summary>Converts one stored value.</summary>
    /// <param name="value">The stored value.</param>
    /// <returns>The CLR value.</returns>
    T Convert(TCanon value);

    /// <summary>The same conversion as an expression, for <see cref="ColumnReader.Emit"/>.</summary>
    /// <param name="value">An expression of type <typeparamref name="TCanon"/>. The result uses it one time.</param>
    /// <returns>An expression of type <typeparamref name="T"/>.</returns>
    Expression Emit(Expression value);
}

/// <summary>The conversion of one read leaf whose decoded column stores a run of bytes for each row.</summary>
/// <typeparam name="T">The CLR type that the leaf gives.</typeparam>
internal interface IBytesReadConversion<T>
{
    /// <summary>Converts the bytes of one row.</summary>
    /// <param name="value">The bytes, borrowed from the column.</param>
    /// <returns>The CLR value, which does not borrow from the column.</returns>
    T Convert(ReadOnlySpan<byte> value);
}

/// <summary>Gives the stored value with no change.</summary>
/// <typeparam name="T">The stored type, which is also the CLR type.</typeparam>
internal readonly struct Identity<T> : IReadConversion<T, T>
{
    /// <inheritdoc/>
    public T Convert(T value) => value;

    /// <inheritdoc/>
    public Expression Emit(Expression value) => value;
}

/// <summary><c>DateTime</c> seconds as a <see cref="DateTimeOffset"/> in the timezone of the column.</summary>
internal readonly struct DateTimeAsOffset : IReadConversion<uint, DateTimeOffset>
{
    private readonly ResolvedTimeZone timeZone;

    public DateTimeAsOffset(ResolvedTimeZone timeZone) => this.timeZone = timeZone;

    /// <inheritdoc/>
    public DateTimeOffset Convert(uint value) => ColumnValueProjections.DateTimeToOffset(value, timeZone);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTimeToOffset), value, timeZone);
}

/// <summary><c>DateTime</c> seconds as a <see cref="DateTime"/> (see <see cref="ColumnValueProjections.PresentAsDateTime"/>).</summary>
internal readonly struct DateTimeAsDateTime : IReadConversion<uint, DateTime>
{
    private readonly ResolvedTimeZone timeZone;

    public DateTimeAsDateTime(ResolvedTimeZone timeZone) => this.timeZone = timeZone;

    /// <inheritdoc/>
    public DateTime Convert(uint value) => ColumnValueProjections.DateTimeToDateTime(value, timeZone);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTimeToDateTime), value, timeZone);
}

/// <summary>A <c>DateTime64</c> count as a <see cref="DateTimeOffset"/> in the timezone of the column.</summary>
internal readonly struct DateTime64AsOffset : IReadConversion<long, DateTimeOffset>
{
    private readonly int scale;
    private readonly ResolvedTimeZone timeZone;

    public DateTime64AsOffset(int scale, ResolvedTimeZone timeZone)
    {
        this.scale = scale;
        this.timeZone = timeZone;
    }

    /// <inheritdoc/>
    public DateTimeOffset Convert(long value) => ColumnValueProjections.DateTime64ToOffset(value, scale, timeZone);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTime64ToOffset), value, scale, timeZone);
}

/// <summary>A <c>DateTime64</c> count as a <see cref="DateTime"/> (see <see cref="ColumnValueProjections.PresentAsDateTime"/>).</summary>
internal readonly struct DateTime64AsDateTime : IReadConversion<long, DateTime>
{
    private readonly int scale;
    private readonly ResolvedTimeZone timeZone;

    public DateTime64AsDateTime(int scale, ResolvedTimeZone timeZone)
    {
        this.scale = scale;
        this.timeZone = timeZone;
    }

    /// <inheritdoc/>
    public DateTime Convert(long value) => ColumnValueProjections.DateTime64ToDateTime(value, scale, timeZone);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTime64ToDateTime), value, scale, timeZone);
}

/// <summary><c>Time</c> seconds as a <see cref="TimeSpan"/>.</summary>
internal readonly struct TimeAsTimeSpan : IReadConversion<int, TimeSpan>
{
    /// <inheritdoc/>
    public TimeSpan Convert(int value) => ColumnValueProjections.TimeToTimeSpan(value);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.TimeToTimeSpan), value);
}

/// <summary><c>Time</c> seconds as a <see cref="TimeOnly"/>. A value that is not a time of day throws.</summary>
internal readonly struct TimeAsTimeOnly : IReadConversion<int, TimeOnly>
{
    /// <inheritdoc/>
    public TimeOnly Convert(int value) => ColumnValueProjections.TimeToTimeOnly(value);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.TimeToTimeOnly), value);
}

/// <summary>A <c>Time64</c> count as a <see cref="TimeSpan"/>.</summary>
internal readonly struct Time64AsTimeSpan : IReadConversion<long, TimeSpan>
{
    private readonly int scale;

    public Time64AsTimeSpan(int scale) => this.scale = scale;

    /// <inheritdoc/>
    public TimeSpan Convert(long value) => ColumnValueProjections.Time64ToTimeSpan(value, scale);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.Time64ToTimeSpan), value, scale);
}

/// <summary>A <c>Time64</c> count as a <see cref="TimeOnly"/>. A value that is not a time of day throws.</summary>
internal readonly struct Time64AsTimeOnly : IReadConversion<long, TimeOnly>
{
    private readonly int scale;

    public Time64AsTimeOnly(int scale) => this.scale = scale;

    /// <inheritdoc/>
    public TimeOnly Convert(long value) => ColumnValueProjections.Time64ToTimeOnly(value, scale);

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => ColumnValueProjections.Call(nameof(ColumnValueProjections.Time64ToTimeOnly), value, scale);
}

/// <summary>An <c>Enum8</c> or <c>Enum16</c> ordinal as its declared label.</summary>
/// <typeparam name="T">The stored ordinal type (<see cref="sbyte"/> or <see cref="short"/>).</typeparam>
internal readonly struct EnumAsLabel<T> : IReadConversion<T, string>
    where T : unmanaged, IBinaryInteger<T>
{
    private static readonly MethodInfo LabelMethod =
        typeof(EnumMemberTable).GetMethod(nameof(EnumMemberTable.Label), BindingFlags.Public | BindingFlags.Instance);

    private readonly EnumMemberTable members;

    public EnumAsLabel(EnumMemberTable members) => this.members = members;

    /// <inheritdoc/>
    public string Convert(T value) => members.Label(long.CreateTruncating(value));

    /// <inheritdoc/>
    public Expression Emit(Expression value)
        => Expression.Call(Expression.Constant(members), LabelMethod, Expression.Convert(value, typeof(long)));
}

/// <summary>Decodes the bytes of one row as UTF-8. A byte sequence that UTF-8 cannot express becomes U+FFFD.</summary>
internal readonly struct Utf8Text : IBytesReadConversion<string>
{
    /// <inheritdoc/>
    public string Convert(ReadOnlySpan<byte> value) => Encoding.UTF8.GetString(value);
}

/// <summary>Copies the bytes of one row into an array that the caller owns.</summary>
internal readonly struct CopiedBytes : IBytesReadConversion<byte[]>
{
    /// <inheritdoc/>
    public byte[] Convert(ReadOnlySpan<byte> value) => value.ToArray();
}
