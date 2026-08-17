using System;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The read-side value conversions shared by the date/time and time codecs' <see cref="IColumnCodec.TryProjectRead"/>
/// and by the columns' own <c>GetDateTimeOffset</c>/<c>GetTimeSpan</c> accessors, so a raw wire count has exactly
/// one calendar reading no matter which surface asks for it.
///
/// <para>
/// Every method is <see langword="static"/> and takes the column's scale and timezone as plain arguments, so a
/// codec can inline a call to one into a compiled projection expression (passing them as captured constants)
/// without a delegate hop.
/// </para>
/// </summary>
internal static class ColumnValueProjections
{
    // The .NET tick scale: one tick is 100 ns, i.e. 10^-7 s.
    private const int DotNetTickScale = 7;

    private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

    /// <summary>
    /// Presents an instant as a <see cref="DateTime"/>: a zero offset yields <see cref="DateTimeKind.Utc"/>, and any
    /// other offset yields the wall clock in the column's timezone as <see cref="DateTimeKind.Unspecified"/>.
    ///
    /// <para>
    /// This matches the HTTP driver's <c>AbstractDateTimeType.ToDateTime</c> for a column whose type names a
    /// timezone. For a <b>bare</b> <c>DateTime</c>/<c>DateTime64</c> the two clients deliberately differ: HTTP
    /// presents it in UTC because its type object carries no zone, whereas this client resolves the session
    /// timezone (<see cref="DateTimeZones.Resolve"/>) and so agrees with what the server itself would display. That
    /// is the chosen behavior, not an oversight — do not "fix" it toward HTTP: it keeps the whole TCP read path
    /// honoring <c>session_timezone</c>. The cost is that a bare column read through the two clients can differ by
    /// the session offset.
    /// </para>
    /// </summary>
    /// <param name="presented">The instant, already converted into the column's timezone.</param>
    /// <returns>The <see cref="DateTime"/> reading of <paramref name="presented"/>.</returns>
    public static DateTime PresentAsDateTime(DateTimeOffset presented) => presented.Offset == TimeSpan.Zero
        ? presented.UtcDateTime
        : presented.DateTime;

    /// <summary>
    /// Projects a <c>DateTime</c> column's raw epoch-second count onto the .NET calendar. The wire value is a UTC
    /// instant; the timezone only decides the offset it is presented with, resolved from the instant so both
    /// daylight-saving transitions and historical base-offset changes are honored.
    /// </summary>
    /// <param name="seconds">The raw epoch-second count.</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant, presented in <paramref name="timeZone"/>.</returns>
    public static DateTimeOffset DateTimeToOffset(uint seconds, TimeZoneInfo timeZone)
        => TimeZoneInfo.ConvertTime(DateTimeOffset.FromUnixTimeSeconds(seconds), timeZone);

    /// <summary>
    /// <see cref="DateTimeToOffset"/> presented as a <see cref="DateTime"/> (see
    /// <see cref="PresentAsDateTime"/> for the <see cref="DateTimeKind"/> rule).
    /// </summary>
    /// <param name="seconds">The raw epoch-second count.</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant as a <see cref="DateTime"/>.</returns>
    public static DateTime DateTimeToDateTime(uint seconds, TimeZoneInfo timeZone)
        => PresentAsDateTime(DateTimeToOffset(seconds, timeZone));

    /// <summary>
    /// Projects a <c>DateTime64</c> column's raw count at <paramref name="scale"/> onto the .NET calendar and
    /// presents it in the column's timezone. Sub-100 ns digits at scale 8/9 are truncated toward zero; the exact
    /// value stays in the column's raw values.
    /// </summary>
    /// <param name="count">The raw count at <c>10^-<paramref name="scale"/></c> seconds since the epoch.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant, presented in <paramref name="timeZone"/>.</returns>
    /// <exception cref="OverflowException"><paramref name="count"/> is decodable but outside the range a
    /// <see cref="DateTimeOffset"/> can present.</exception>
    public static DateTimeOffset DateTime64ToOffset(long count, int scale, TimeZoneInfo timeZone)
    {
        // Projecting the count onto the .NET calendar overflows for counts outside DateTimeOffset's range even
        // though the raw count itself is decodable. Surface that as an actionable message pointing at the raw
        // values rather than a bare arithmetic exception.
        try
        {
            long dotNetTicks = FixedPointScaling.ShiftDecimalPlaces(count, DotNetTickScale - scale);
            var utc = new DateTimeOffset(UnixEpochTicks + dotNetTicks, TimeSpan.Zero);
            return TimeZoneInfo.ConvertTime(utc, timeZone);
        }
        catch (Exception ex) when (ex is OverflowException or ArgumentOutOfRangeException)
        {
            throw new OverflowException(
                $"A DateTime64 count of {count} at scale {scale} is outside the range presentable as a DateTimeOffset in timezone '{timeZone.Id}'; read the raw count via Values instead.",
                ex);
        }
    }

    /// <summary>
    /// <see cref="DateTime64ToOffset"/> presented as a <see cref="DateTime"/> (see
    /// <see cref="PresentAsDateTime"/> for the <see cref="DateTimeKind"/> rule).
    /// </summary>
    /// <param name="count">The raw count at <c>10^-<paramref name="scale"/></c> seconds since the epoch.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant as a <see cref="DateTime"/>.</returns>
    /// <exception cref="OverflowException"><paramref name="count"/> is decodable but outside the range a
    /// <see cref="DateTimeOffset"/> can present.</exception>
    public static DateTime DateTime64ToDateTime(long count, int scale, TimeZoneInfo timeZone)
        => PresentAsDateTime(DateTime64ToOffset(count, scale, timeZone));

    /// <summary>
    /// Projects a <c>Time</c> column's raw second count to a <see cref="TimeSpan"/>. Exact — <c>Time</c> is a
    /// whole-second duration.
    /// </summary>
    /// <param name="seconds">The raw signed second count.</param>
    /// <returns>The duration.</returns>
    public static TimeSpan TimeToTimeSpan(int seconds) => TimeSpan.FromTicks(seconds * TimeSpan.TicksPerSecond);

    /// <summary>
    /// Projects a <c>Time64</c> column's raw count at <paramref name="scale"/> to a <see cref="TimeSpan"/>.
    /// Sub-100 ns digits at scale 8/9 are truncated toward zero; the exact value stays in the column's raw values.
    /// </summary>
    /// <param name="count">The raw signed count at <c>10^-<paramref name="scale"/></c> seconds.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <returns>The duration.</returns>
    public static TimeSpan Time64ToTimeSpan(long count, int scale)
        => TimeSpan.FromTicks(FixedPointScaling.ShiftDecimalPlaces(count, DotNetTickScale - scale));

    /// <summary>
    /// Confirms the expression a codec was handed to project really yields its canonical
    /// <see cref="IColumnCodec.ElementType"/>. A mismatch here would otherwise surface much later as an opaque
    /// expression-tree or cast failure, so it is caught where the caller's mistake is.
    /// </summary>
    /// <param name="value">The expression to check.</param>
    /// <param name="elementType">The codec's canonical element type.</param>
    /// <param name="typeName">The codec's type name, for the message.</param>
    /// <exception cref="ArgumentException"><paramref name="value"/> is not of type <paramref name="elementType"/>.</exception>
    public static void RequireSourceType(Expression value, Type elementType, string typeName)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Type != elementType)
        {
            throw new ArgumentException(
                $"The '{typeName}' codec projects from its element type {elementType}, but was given an expression of type {value.Type}.",
                nameof(value));
        }
    }

    /// <summary>
    /// Lifts an inner codec's projection over a source that can be absent: an absent row yields
    /// <c>default(targetType)</c> — null for both a <see cref="Nullable{T}"/> and a reference type — and a present one
    /// is projected and re-wrapped. Shared by the transparent wrappers (<c>Nullable</c>,
    /// <c>LowCardinality(Nullable(T))</c>), whose surfaces differ but whose lifting rule does not.
    ///
    /// <para>
    /// The source shape and the target shape are read independently, each from the type in front of it: whether the
    /// source needs unwrapping is decided by <paramref name="value"/>'s own type, and whether the result needs
    /// wrapping by <paramref name="targetType"/>. Neither is inferred from the other. They happen to agree for every
    /// type pair that exists today — every codec's readings share their value/reference-ness with its
    /// <see cref="IColumnCodec.ElementType"/> — but a codec that broke that (a <c>FixedString(16)</c> read as a
    /// <see cref="Guid"/>, say) would otherwise silently turn a null row into <c>default(Guid)</c>.
    /// </para>
    /// </summary>
    /// <param name="value">An expression of the wrapper's surface element type: either
    /// <c>Nullable&lt;inner element&gt;</c> or, for a reference-typed inner, the inner element type itself.</param>
    /// <param name="inner">The inner codec, asked to project the present value.</param>
    /// <param name="innerTarget">The inner codec's own spelling of the target type.</param>
    /// <param name="targetType">The surfaced target type, which must be able to hold an absent row.</param>
    /// <param name="projected">An expression of type <paramref name="targetType"/>, or null when the inner offers no
    /// projection to <paramref name="innerTarget"/>.</param>
    /// <returns>Whether the inner offered the projection.</returns>
    public static bool TryLiftOverAbsent(Expression value, IColumnCodec inner, Type innerTarget, Type targetType, out Expression projected)
    {
        // Bind the source to a local first: it is spliced in twice (the presence test and the value) and may be an
        // arbitrary expression — a span access or a method call — that must not be evaluated twice.
        ParameterExpression source = Expression.Variable(value.Type, "surfaceValue");

        // A value-typed surface arrives as Nullable<U>, so presence is HasValue and the inner is handed Value. A
        // reference-typed one is its own inner type already, so presence is a null test and it passes straight through.
        // ReferenceNotEqual, not NotEqual: the latter binds a user-defined op_Inequality when the type declares one
        // (String does), which would put a call per row where a reference comparison belongs and would break outright
        // on an operator that dereferences its arguments or does not return bool.
        bool wrapped = Nullable.GetUnderlyingType(value.Type) is not null;
        Expression isPresent = wrapped
            ? Expression.Property(source, "HasValue")
            : Expression.ReferenceNotEqual(source, Expression.Constant(null, value.Type));
        Expression present = wrapped ? Expression.Property(source, "Value") : source;

        if (!inner.TryProjectRead(present, innerTarget, out Expression innerProjection))
        {
            projected = null;
            return false;
        }

        projected = Expression.Block(
            new[] { source },
            Expression.Assign(source, value),
            Expression.Condition(
                isPresent,
                innerProjection.Type == targetType ? innerProjection : Expression.Convert(innerProjection, targetType),
                Expression.Default(targetType)));
        return true;
    }

    /// <summary>
    /// Builds a call to one of this class's projection methods, with the column's per-column state (scale,
    /// timezone) passed as constants — the shape every codec's <see cref="IColumnCodec.TryProjectRead"/> returns.
    /// </summary>
    /// <param name="method">The projection method name on this class.</param>
    /// <param name="value">The expression yielding the raw value; becomes the first argument.</param>
    /// <param name="constants">The remaining arguments, embedded as constants.</param>
    /// <returns>The call expression.</returns>
    public static Expression Call(string method, Expression value, params object[] constants)
    {
        MethodInfo target = typeof(ColumnValueProjections).GetMethod(method, BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"No projection method '{method}'.");

        ParameterInfo[] parameters = target.GetParameters();
        if (parameters.Length != constants.Length + 1)
        {
            // Caught here rather than as an Expression.Call argument error, which would name the reflected method
            // instead of the codec that miscalled it.
            throw new InvalidOperationException(
                $"Projection method '{method}' takes {parameters.Length} arguments, but was given {constants.Length + 1}.");
        }

        var arguments = new Expression[constants.Length + 1];
        arguments[0] = value;
        for (int i = 0; i < constants.Length; i++)
        {
            // Typed against the parameter rather than inferred from the value, so a null constant still binds and an
            // implicitly-convertible one is not pinned to its own type.
            arguments[i + 1] = Expression.Constant(constants[i], parameters[i + 1].ParameterType);
        }

        return Expression.Call(target, arguments);
    }
}
