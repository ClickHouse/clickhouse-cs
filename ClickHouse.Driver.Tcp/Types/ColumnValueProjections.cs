using System;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The read-side conversions shared by the date/time codecs' <see cref="IColumnCodec.TryProjectRead"/> and by the
/// columns' own <c>GetDateTimeOffset</c>/<c>GetTimeSpan</c>, so a raw wire count has one calendar reading whichever
/// surface asks. Static, taking scale and timezone as plain arguments, so a codec can inline a call with them as
/// constants instead of paying a delegate hop.
/// </summary>
internal static class ColumnValueProjections
{
    // The .NET tick scale: one tick is 100 ns, i.e. 10^-7 s.
    private const int DotNetTickScale = 7;

    private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

    /// <summary>The exclusive upper bound of a time of day.</summary>
    private static readonly TimeSpan OneDay = TimeSpan.FromDays(1);

    private const long SecondsPerDay = 24 * 60 * 60;

    /// <summary>
    /// Presents an instant as a <see cref="DateTime"/>: a zero offset yields <see cref="DateTimeKind.Utc"/>, any
    /// other offset the wall clock in the column's timezone as <see cref="DateTimeKind.Unspecified"/>.
    /// <para>
    /// Deliberate divergence from HTTP, do not "fix": on a bare <c>DateTime</c>/<c>DateTime64</c> HTTP presents UTC,
    /// while this client resolves <c>session_timezone</c> and so matches what the server displays. The cost is that
    /// one column read through the two clients can differ by the session offset.
    /// </para>
    /// </summary>
    /// <param name="presented">The instant, already in the column's timezone.</param>
    /// <returns>The <see cref="DateTime"/> reading.</returns>
    public static DateTime PresentAsDateTime(DateTimeOffset presented) => presented.Offset == TimeSpan.Zero
        ? presented.UtcDateTime
        : presented.DateTime;

    /// <summary>
    /// Projects a <c>DateTime</c> column's raw epoch-second count onto the .NET calendar. The wire value is a UTC
    /// instant; the timezone only decides the presented offset, resolved from the instant so that both daylight-saving
    /// transitions and historical base-offset changes are honored.
    /// </summary>
    /// <param name="seconds">The raw epoch-second count.</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant, presented in <paramref name="timeZone"/>.</returns>
    /// <exception cref="FormatException">The column's timezone is one this platform cannot represent.</exception>
    /// <remarks>
    /// Takes the resolved zone, not the zone: a codec embeds this call in an expression tree before any row
    /// exists, and asking for the zone there would refuse the reading rather than the value. The dereference
    /// costs a null check against a timezone conversion.
    /// </remarks>
    public static DateTimeOffset DateTimeToOffset(uint seconds, ResolvedTimeZone timeZone)
        => TimeZoneInfo.ConvertTime(DateTimeOffset.FromUnixTimeSeconds(seconds), timeZone.Value);

    /// <summary><see cref="DateTimeToOffset"/> as a <see cref="DateTime"/>; see <see cref="PresentAsDateTime"/>.</summary>
    /// <param name="seconds">The raw epoch-second count.</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant as a <see cref="DateTime"/>.</returns>
    /// <exception cref="FormatException">The column's timezone is one this platform cannot represent.</exception>
    public static DateTime DateTimeToDateTime(uint seconds, ResolvedTimeZone timeZone)
        => PresentAsDateTime(DateTimeToOffset(seconds, timeZone));

    /// <summary>
    /// Projects a <c>DateTime64</c> count at <paramref name="scale"/> onto the .NET calendar, presented in the
    /// column's timezone. Scale 8/9 sub-100 ns digits truncate toward zero; the exact value stays in the raw values.
    /// </summary>
    /// <param name="count">The raw count at <c>10^-<paramref name="scale"/></c> seconds since the epoch.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant, presented in <paramref name="timeZone"/>.</returns>
    /// <exception cref="OverflowException">The count is decodable but outside <see cref="DateTimeOffset"/>'s range.</exception>
    public static DateTimeOffset DateTime64ToOffset(long count, int scale, ResolvedTimeZone timeZone)
    {
        // Resolved once, outside the try, since the conversion below needs it either way.
        TimeZoneInfo zone = timeZone.Value;

        // A count can be decodable yet outside DateTimeOffset's range. Point at the raw values rather than let a bare
        // arithmetic exception surface.
        try
        {
            long dotNetTicks = FixedPointScaling.ShiftDecimalPlaces(count, DotNetTickScale - scale);
            var utc = new DateTimeOffset(UnixEpochTicks + dotNetTicks, TimeSpan.Zero);
            return TimeZoneInfo.ConvertTime(utc, zone);
        }
        catch (Exception ex) when (ex is OverflowException or ArgumentOutOfRangeException)
        {
            throw new OverflowException(
                $"A DateTime64 count of {count} at scale {scale} is outside the range presentable as a DateTimeOffset in timezone '{zone.Id}'; read the raw count via Values instead.",
                ex);
        }
    }

    /// <summary><see cref="DateTime64ToOffset"/> as a <see cref="DateTime"/>; see <see cref="PresentAsDateTime"/>.</summary>
    /// <param name="count">The raw count at <c>10^-<paramref name="scale"/></c> seconds since the epoch.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <param name="timeZone">The column's timezone.</param>
    /// <returns>The instant as a <see cref="DateTime"/>.</returns>
    /// <exception cref="OverflowException">The count is decodable but outside <see cref="DateTimeOffset"/>'s range.</exception>
    public static DateTime DateTime64ToDateTime(long count, int scale, ResolvedTimeZone timeZone)
        => PresentAsDateTime(DateTime64ToOffset(count, scale, timeZone));

    /// <summary>Projects a <c>Time</c> column's raw second count to a <see cref="TimeSpan"/>. Exact — whole seconds.</summary>
    /// <param name="seconds">The raw signed second count.</param>
    /// <returns>The duration.</returns>
    public static TimeSpan TimeToTimeSpan(int seconds) => TimeSpan.FromTicks(seconds * TimeSpan.TicksPerSecond);

    /// <summary>
    /// Projects a <c>Time64</c> count at <paramref name="scale"/> to a <see cref="TimeSpan"/>. Scale 8/9 sub-100 ns
    /// digits truncate toward zero; the exact value stays in the raw values.
    /// </summary>
    /// <param name="count">The raw signed count at <c>10^-<paramref name="scale"/></c> seconds.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <returns>The duration.</returns>
    public static TimeSpan Time64ToTimeSpan(long count, int scale)
        => TimeSpan.FromTicks(FixedPointScaling.ShiftDecimalPlaces(count, DotNetTickScale - scale));

    /// <summary>Projects a <c>Time</c> column's raw second count to a <see cref="TimeOnly"/>.</summary>
    /// <param name="seconds">The raw signed second count.</param>
    /// <returns>The time of day.</returns>
    /// <exception cref="InvalidOperationException">The value is not a time of day.</exception>
    public static TimeOnly TimeToTimeOnly(int seconds) => AsTimeOfDay(TimeToTimeSpan(seconds), "Time");

    /// <summary>Projects a <c>Time64</c> count at <paramref name="scale"/> to a <see cref="TimeOnly"/>.</summary>
    /// <param name="count">The raw signed count at <c>10^-<paramref name="scale"/></c> seconds.</param>
    /// <param name="scale">The column's fractional-second scale (0–9).</param>
    /// <returns>The time of day, with sub-100 ns digits truncated toward zero.</returns>
    /// <exception cref="InvalidOperationException">The value is not a time of day.</exception>
    public static TimeOnly Time64ToTimeOnly(long count, int scale)
    {
        // Checked on the raw count rather than on the TimeSpan: at scale 8 or 9 the shift to 100 ns ticks
        // truncates toward zero, so a negative count finer than one tick reaches zero and passes a check made
        // after it. -1 at scale 9 would read as midnight.
        if (count < 0 || count >= SecondsPerDay * FixedPointScaling.Pow10(scale))
        {
            decimal seconds = (decimal)count / FixedPointScaling.Pow10(scale);
            throw NotATimeOfDay("Time64", $"{seconds.ToString(CultureInfo.InvariantCulture)} s");
        }

        return TimeOnly.FromTimeSpan(Time64ToTimeSpan(count, scale));
    }

    /// <summary>
    /// Confirms a codec was handed an expression of its canonical <see cref="IColumnCodec.ElementType"/>. Catches the
    /// caller's mistake here, instead of as an opaque expression-tree failure much later.
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

    // The narrowing a TimeOnly read is: a Time column holds a signed duration of up to 999 hours, and only the
    // part of that range which is a time of day has a TimeOnly. Refused rather than wrapped, a duration reduced
    // modulo a day being a different value.
    private static TimeOnly AsTimeOfDay(TimeSpan value, string typeName)
    {
        if (value < TimeSpan.Zero || value >= OneDay)
        {
            throw NotATimeOfDay(typeName, value.ToString());
        }

        return TimeOnly.FromTimeSpan(value);
    }

    private static InvalidOperationException NotATimeOfDay(string typeName, string value)
        => new($"A {typeName} column value of {value} is not a time of day, so it has no TimeOnly. Read the column as a TimeSpan.");

    /// <summary>
    /// Lifts an inner codec's projection over a source that can be absent: an absent row yields
    /// <c>default(targetType)</c> — null for both a <see cref="Nullable{T}"/> and a reference type. Shared by the
    /// transparent wrappers (<c>Nullable</c>, <c>LowCardinality(Nullable(T))</c>), whose surfaces differ but whose
    /// lifting rule does not.
    /// <para>
    /// Source and target shapes are read independently, each from the type in front of it, never one from the other.
    /// They agree for every pair that exists today, but a reference-typed element with a value-typed reading
    /// (<c>FixedString(16)</c> as a <see cref="Guid"/>) would otherwise turn a null row into <c>default(Guid)</c>.
    /// </para>
    /// </summary>
    /// <param name="value">An expression of the wrapper's surface element type.</param>
    /// <param name="inner">The inner codec, asked to project the present value.</param>
    /// <param name="innerTarget">The inner codec's own spelling of the target type.</param>
    /// <param name="targetType">The surfaced target type, which must be able to hold an absent row.</param>
    /// <param name="projected">An expression of type <paramref name="targetType"/>, or null when the inner declines.</param>
    /// <returns>Whether the inner offered the projection.</returns>
    public static bool TryLiftOverAbsent(Expression value, IColumnCodec inner, Type innerTarget, Type targetType, out Expression projected)
    {
        // Spliced in twice (the presence test and the value), and may be a span access or a call, so bind it once.
        ParameterExpression source = Expression.Variable(value.Type, "surfaceValue");

        // A value-typed surface arrives as Nullable<U>; a reference-typed one is its own inner type already.
        // ReferenceNotEqual, not NotEqual: the latter binds a user-defined op_Inequality where the type declares one
        // (String does), costing a call per row and breaking on an operator that dereferences its arguments.
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
    /// Builds a call to one of this class's projection methods, with the per-column state (scale, timezone) as
    /// constants — the shape every codec's <see cref="IColumnCodec.TryProjectRead"/> returns.
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
            // Named here because Expression.Call's own error names the reflected method, not the codec that miscalled it.
            throw new InvalidOperationException(
                $"Projection method '{method}' takes {parameters.Length} arguments, but was given {constants.Length + 1}.");
        }

        var arguments = new Expression[constants.Length + 1];
        arguments[0] = value;
        for (int i = 0; i < constants.Length; i++)
        {
            // Typed against the parameter, so a null constant still binds and a convertible one is not pinned.
            arguments[i + 1] = Expression.Constant(constants[i], parameters[i + 1].ParameterType);
        }

        return Expression.Call(target, arguments);
    }
}
