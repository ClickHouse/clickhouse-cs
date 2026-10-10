using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The read rules of decision D6, which apply to the whole column type (the root) after its own readings: a CLR enum
/// from its ordinal, a cast that the CLR allows (for example to <see cref="object"/>), and a nullable column read as a
/// value type that cannot hold NULL, which throws at the first NULL. They are the rules of POCO mapping, so every read
/// tier accepts the same readings.
/// </summary>
internal static class ReadRules
{
    private static readonly HashSet<Type> ArrayInterfaces = new()
    {
        typeof(IEnumerable<>),
        typeof(IReadOnlyCollection<>),
        typeof(IReadOnlyList<>),
        typeof(ICollection<>),
        typeof(IList<>),
    };

    /// <summary>
    /// Whether a value of <paramref name="from"/> converts to <paramref name="to"/> with no conversion of its own: a CLR
    /// enum from its ordinal (<see cref="IsEnumOrdinal"/>), or a cast that the CLR allows (<see cref="IsAssignable"/>).
    /// Numeric widening is not a rule.
    /// </summary>
    /// <param name="from">The canonical CLR type of the column, or the value type under its <see cref="Nullable{T}"/>.</param>
    /// <param name="to">The CLR type to read as.</param>
    /// <returns>Whether the rule applies.</returns>
    public static bool CanConvert(Type from, Type to) => IsEnumOrdinal(from, to) || IsAssignable(from, to);

    /// <summary>
    /// Whether <paramref name="to"/> is a CLR enum whose underlying type is <paramref name="from"/>. The conversion is the
    /// unchecked cast of C#: an ordinal that the enum does not name gives that ordinal.
    /// </summary>
    /// <param name="from">The ordinal type.</param>
    /// <param name="to">The enum type.</param>
    /// <returns>Whether the rule applies.</returns>
    public static bool IsEnumOrdinal(Type from, Type to) => to.IsEnum && Enum.GetUnderlyingType(to) == from;

    /// <summary>
    /// Whether the CLR casts a <paramref name="from"/> to a <paramref name="to"/> (<see cref="Type.IsAssignableFrom"/>)
    /// and the cast keeps the value: a reference keeps its object, and a value type is boxed. An array cast that gives
    /// the elements another meaning is refused (<see cref="ReinterpretsElements"/>). The cast rule of the writes uses this
    /// rule too (<see cref="WriteRules.CastTargets"/>), so it decides those array casts in both directions.
    /// </summary>
    /// <param name="from">The source type.</param>
    /// <param name="to">The target type.</param>
    /// <returns>Whether the rule applies.</returns>
    public static bool IsAssignable(Type from, Type to) => to.IsAssignableFrom(from) && !ReinterpretsElements(from, to);

    /// <summary>
    /// Whether the CLR casts an array <paramref name="from"/> to <paramref name="to"/> only because it reads the
    /// elements as another type of the same size, and that type gives each element another meaning: an integer type of
    /// the other sign (the <see cref="uint"/> 3000000000 reads as the <see cref="int"/> -1294967296), or an enum whose
    /// underlying type is not the other element type, also in jagged arrays and in the generic collection interfaces of
    /// an array. An enum and its underlying type keep the integer value of each element, so an array cast between them
    /// does not count.
    /// </summary>
    /// <param name="from">The source type.</param>
    /// <param name="to">The target type.</param>
    /// <returns>Whether the cast gives the elements another meaning.</returns>
    public static bool ReinterpretsElements(Type from, Type to)
    {
        if (!from.IsArray || !to.IsAssignableFrom(from))
        {
            return false;
        }

        Type fromElement = from.GetElementType();
        Type toElement = to.IsArray
            ? to.GetElementType()
            : to.IsGenericType && ArrayInterfaces.Contains(to.GetGenericTypeDefinition()) ? to.GetGenericArguments()[0] : null;
        if (toElement is null || toElement == fromElement)
        {
            return false;
        }

        if (!fromElement.IsValueType)
        {
            return ReinterpretsElements(fromElement, toElement);
        }

        // A value-type element has no reference conversion, so the CLR allows the cast only between types of one size.
        return !IsEnumOrdinal(fromElement, toElement) && !IsEnumOrdinal(toElement, fromElement);
    }
}

/// <summary>A CLR enum read from the ordinal that the child reads (<see cref="ReadRules.IsEnumOrdinal"/>).</summary>
/// <typeparam name="TOrdinal">The ordinal type, which is the underlying type of <typeparamref name="TEnum"/>.</typeparam>
/// <typeparam name="TEnum">The enum type.</typeparam>
internal sealed class EnumReader<TOrdinal, TEnum> : ColumnReader<TEnum>
    where TOrdinal : unmanaged
    where TEnum : unmanaged, Enum
{
    private readonly ColumnReader<TOrdinal> inner;

    /// <summary>Initializes the reader over the reader of the ordinal.</summary>
    /// <param name="inner">Reads the ordinals.</param>
    public EnumReader(ColumnReader<TOrdinal> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<TEnum> Bind(IColumn column) => new Bound(inner.Bind(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => Expression.Convert(inner.Emit(column, row, scope), typeof(TEnum));

    private sealed class Bound : BoundReader<TEnum>
    {
        private readonly BoundReader<TOrdinal> inner;

        public Bound(BoundReader<TOrdinal> inner) => this.inner = inner;

        // An enum has the layout of its underlying type, so the child fills the destination as ordinals.
        public override void Fill(int start, Span<TEnum> destination) => inner.Fill(start, MemoryMarshal.Cast<TEnum, TOrdinal>(destination));
    }
}

/// <summary>
/// <c>TEnum?</c> read from the <c>TOrdinal?</c> that the child reads: null stays null, and an ordinal becomes the enum
/// (<see cref="ReadRules.IsEnumOrdinal"/>).
/// </summary>
/// <typeparam name="TOrdinal">The ordinal type, which is the underlying type of <typeparamref name="TEnum"/>.</typeparam>
/// <typeparam name="TEnum">The enum type.</typeparam>
internal sealed class NullableEnumReader<TOrdinal, TEnum> : ColumnReader<TEnum?>
    where TOrdinal : unmanaged
    where TEnum : unmanaged, Enum
{
    private readonly ColumnReader<TOrdinal?> inner;

    /// <summary>Initializes the reader over the reader of the nullable ordinal.</summary>
    /// <param name="inner">Reads the nullable ordinals.</param>
    public NullableEnumReader(ColumnReader<TOrdinal?> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<TEnum?> Bind(IColumn column) => new Bound(inner.Bind(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => Expression.Convert(inner.Emit(column, row, scope), typeof(TEnum?));

    private sealed class Bound : BoundReader<TEnum?>, INullableBound
    {
        private readonly BoundReader<TOrdinal?> inner;

        public Bound(BoundReader<TOrdinal?> inner) => this.inner = inner;

        // Nullable<TEnum> and Nullable<TOrdinal> have one layout, so the child fills the destination as ordinals.
        public override void Fill(int start, Span<TEnum?> destination)
            => inner.Fill(start, MemoryMarshal.CreateSpan(ref Unsafe.As<TEnum?, TOrdinal?>(ref MemoryMarshal.GetReference(destination)), destination.Length));

        public int FindNull(int start, int length) => NullableRead.FindNull(inner, start, length);
    }
}

/// <summary>
/// A cast that the CLR allows (<see cref="ReadRules.IsAssignable"/>), from the canonical value that the child reads: a
/// reference keeps its object, and a value type is boxed.
/// </summary>
/// <typeparam name="TSource">The CLR type that the child reads.</typeparam>
/// <typeparam name="TTarget">The CLR type to read as.</typeparam>
internal sealed class AssignReader<TSource, TTarget> : ColumnReader<TTarget>
{
    private readonly ColumnReader<TSource> inner;

    /// <summary>Initializes the reader over the reader of the source type.</summary>
    /// <param name="inner">Reads the source values.</param>
    public AssignReader(ColumnReader<TSource> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<TTarget> Bind(IColumn column) => new Bound(inner.Bind(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => Expression.Convert(inner.Emit(column, row, scope), typeof(TTarget));

    private sealed class Bound : BoundReader<TTarget>
    {
        private readonly BoundReader<TSource> inner;

        public Bound(BoundReader<TSource> inner) => this.inner = inner;

        public override void Fill(int start, Span<TTarget> destination)
        {
            if (destination.IsEmpty)
            {
                inner.Fill(start, Span<TSource>.Empty);
                return;
            }

            TSource[] scratch = ArrayPool<TSource>.Shared.Rent(destination.Length);
            try
            {
                Span<TSource> values = scratch.AsSpan(0, destination.Length);
                inner.Fill(start, values);
                for (int i = 0; i < values.Length; i++)
                {
                    destination[i] = (TTarget)(object)values[i];
                }
            }
            finally
            {
                ArrayPool<TSource>.Shared.Return(scratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TSource>());
            }
        }
    }
}

/// <summary><c>T?</c> read from the <typeparamref name="T"/> that the child reads, for a column that has no NULL.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class AsNullableReader<T> : ColumnReader<T?>
    where T : struct
{
    private readonly ColumnReader<T> inner;

    /// <summary>Initializes the reader over the reader of the value type.</summary>
    /// <param name="inner">Reads the values.</param>
    public AsNullableReader(ColumnReader<T> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T?> Bind(IColumn column) => new Bound(inner.Bind(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => Expression.Convert(inner.Emit(column, row, scope), typeof(T?));

    private sealed class Bound : BoundReader<T?>
    {
        private readonly BoundReader<T> inner;

        public Bound(BoundReader<T> inner) => this.inner = inner;

        public override void Fill(int start, Span<T?> destination)
        {
            if (destination.IsEmpty)
            {
                inner.Fill(start, Span<T>.Empty);
                return;
            }

            T[] scratch = ArrayPool<T>.Shared.Rent(destination.Length);
            try
            {
                Span<T> values = scratch.AsSpan(0, destination.Length);
                inner.Fill(start, values);
                for (int i = 0; i < values.Length; i++)
                {
                    destination[i] = values[i];
                }
            }
            finally
            {
                ArrayPool<T>.Shared.Return(scratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            }
        }
    }
}

/// <summary>
/// A nullable column read as a value type <typeparamref name="T"/>, which cannot hold NULL: the child reads <c>T?</c>, and
/// a NULL row throws <see cref="NullValueException"/>. A bulk read gives the rows before the first NULL to the child,
/// then throws, so a NULL fails the read before a later row can fail it.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class NonNullReader<T> : ColumnReader<T>
    where T : struct
{
    private static readonly MethodInfo NullAtMethod =
        typeof(NullValueException).GetMethod(nameof(NullValueException.At), BindingFlags.Public | BindingFlags.Static);

    private readonly ColumnReader<T?> inner;

    /// <summary>Initializes the reader over the reader of the nullable type.</summary>
    /// <param name="inner">Reads <c>T?</c>, with null for a NULL row.</param>
    public NonNullReader(ColumnReader<T?> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => new Bound(inner.Bind(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression value = Expression.Variable(typeof(T?), "nullable");
        return Expression.Block(
            new[] { value },
            Expression.Assign(value, inner.Emit(column, row, scope)),
            Expression.Condition(
                Expression.Property(value, nameof(Nullable<T>.HasValue)),
                Expression.Property(value, nameof(Nullable<T>.Value)),
                Expression.Throw(Expression.Call(NullAtMethod, row), typeof(T))));
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly BoundReader<T?> inner;

        public Bound(BoundReader<T?> inner) => this.inner = inner;

        public override void Fill(int start, Span<T> destination)
        {
            int firstNull = NullableRead.FindNull(inner, start, destination.Length);
            int present = firstNull < 0 ? destination.Length : firstNull - start;
            if (present > 0)
            {
                T?[] scratch = ArrayPool<T?>.Shared.Rent(present);
                try
                {
                    Span<T?> values = scratch.AsSpan(0, present);
                    inner.Fill(start, values);
                    for (int i = 0; i < values.Length; i++)
                    {
                        destination[i] = values[i] ?? throw NullValueException.At(start + i);
                    }
                }
                finally
                {
                    ArrayPool<T?>.Shared.Return(scratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
                }
            }

            if (firstNull >= 0)
            {
                throw NullValueException.At(firstNull);
            }
        }
    }
}

/// <summary>The search for a NULL row that the readers of a nullable reading share.</summary>
internal static class NullableRead
{
    /// <summary>
    /// The first NULL row of a range, when <paramref name="bound"/> can find it without a conversion
    /// (<see cref="INullableBound"/>); else -1, and the caller finds a NULL in the values that it reads.
    /// </summary>
    /// <typeparam name="T">The CLR type of a value.</typeparam>
    /// <param name="bound">The bound reader.</param>
    /// <param name="start">The zero-based first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The row, or -1.</returns>
    public static int FindNull<T>(BoundReader<T> bound, int start, int length)
        => bound is INullableBound nullable ? nullable.FindNull(start, length) : -1;
}

/// <summary>
/// A NULL row of a column that is read as a type that cannot hold NULL. Each read tier catches it and throws its own
/// message, which names the column, its type and the row in the terms of that tier.
/// </summary>
internal sealed class NullValueException : InvalidOperationException
{
    private NullValueException(int row)
        : base($"Row {row} of the column has NULL, and the target type cannot hold NULL.") => Row = row;

    /// <summary>The zero-based row of the column that has NULL.</summary>
    public int Row { get; }

    /// <summary>The exception for a NULL at <paramref name="row"/>.</summary>
    /// <param name="row">The zero-based row of the column.</param>
    /// <returns>The exception, for the caller to throw.</returns>
    public static NullValueException At(int row) => new(row);
}
