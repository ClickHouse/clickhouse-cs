using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The write rules of decision D6, which apply to the whole column type (the root) after its own writes refuse the CLR
/// type: a CLR enum is written as its ordinal, a value is written as a type that it casts to with no conversion (for
/// example <see cref="object"/>), a value type is written into a type that is written from its nullable type, and a
/// nullable value type is written into a type that cannot hold NULL, which throws at the first NULL. They are the rules
/// of POCO mapping (<c>InsertRowsAsync&lt;T&gt;</c>), so every write tier accepts the same CLR types.
/// </summary>
internal static class WriteRules
{
    // The integer types of each size. The CLR reads an array of one of them, or of an enum over one of them, as an array
    // of any other of the same size.
    private static readonly Type[][] SameSizeIntegers =
    {
        new[] { typeof(sbyte), typeof(byte) },
        new[] { typeof(short), typeof(ushort) },
        new[] { typeof(int), typeof(uint) },
        new[] { typeof(long), typeof(ulong) },
    };

    /// <summary>
    /// The types other than <paramref name="source"/> that a value of <paramref name="source"/> casts to with no
    /// conversion, where a column type can be written from them, in the order that the rule tries them: the base classes
    /// of a class; for an array, the arrays whose elements the CLR reads in place of the source elements (an integer
    /// type of the same size, an enum and its integer type, a base class of a reference element, also in jagged arrays);
    /// then <see cref="object"/>. Interfaces are not in the list: no column type is written from one.
    /// </summary>
    /// <remarks>
    /// Each type is one that the cast rule of the reads accepts (<see cref="ReadRules.IsAssignable"/>), so the array casts
    /// that read elements as another type (<see cref="ReadRules.ReinterpretsElements"/>) are in one place for both
    /// directions.
    /// </remarks>
    /// <param name="source">The CLR type of the values. Not a nullable value type: the rules unwrap it first.</param>
    /// <returns>The types.</returns>
    public static IReadOnlyList<Type> CastTargets(Type source)
    {
        var targets = new List<Type>();
        AddCastTargets(source, targets);
        targets.RemoveAll(target => target == source || !ReadRules.IsAssignable(source, target));
        return targets;
    }

    /// <summary>The integer type under a CLR enum, or null for a type that is not an enum.</summary>
    /// <param name="type">The type.</param>
    /// <returns>The ordinal type, or null.</returns>
    public static Type OrdinalOf(Type type) => type.IsEnum ? Enum.GetUnderlyingType(type) : null;

    /// <summary>The message of a NULL that a type which cannot hold NULL is given, in the words of the row inserts.</summary>
    /// <param name="column">The name of the column.</param>
    /// <param name="columnType">The ClickHouse type of the column.</param>
    /// <param name="row">The zero-based row of the insert.</param>
    /// <returns>The exception to throw.</returns>
    public static InvalidOperationException NullNotWritable(string column, string columnType, long row)
        => new(
            $"Column '{column}' ({columnType}) is null at row {row} of the insert, but it cannot hold null. " +
            "Make the column Nullable(...), or leave out the rows with no value.");

    private static void AddCastTargets(Type source, List<Type> targets)
    {
        if (source.IsSZArray)
        {
            Type element = source.GetElementType();
            foreach (Type elementTarget in ElementCastTargets(element))
            {
                targets.Add(elementTarget.MakeArrayType());
            }
        }

        if (!source.IsValueType)
        {
            for (Type baseType = source.BaseType; baseType is not null && baseType != typeof(object); baseType = baseType.BaseType)
            {
                targets.Add(baseType);
            }
        }

        targets.Add(typeof(object));
    }

    // The types that the CLR reads an element of an array of this type as, in an array cast.
    private static IEnumerable<Type> ElementCastTargets(Type element)
    {
        if (!element.IsValueType)
        {
            var targets = new List<Type>();
            AddCastTargets(element, targets);
            return targets;
        }

        Type integer = OrdinalOf(element) ?? element;
        foreach (Type[] size in SameSizeIntegers)
        {
            if (Array.IndexOf(size, integer) >= 0)
            {
                return size;
            }
        }

        return Array.Empty<Type>();
    }
}

/// <summary>
/// A CLR enum written as its ordinal (<see cref="WriteRules.OrdinalOf"/>) by the writer of the ordinal type. An enum has
/// the layout of its ordinal type, so the values are read as ordinals with no copy.
/// </summary>
/// <typeparam name="TEnum">The enum type.</typeparam>
/// <typeparam name="TOrdinal">The ordinal type, which is the underlying type of <typeparamref name="TEnum"/>.</typeparam>
internal sealed class EnumWriter<TEnum, TOrdinal> : ColumnWriter<TEnum>
    where TEnum : unmanaged, Enum
    where TOrdinal : unmanaged
{
    private readonly ColumnWriter<TOrdinal> inner;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of the ordinals.</param>
    public EnumWriter(ColumnWriter<TOrdinal> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<TEnum> values)
        => ReinterpretedValues.Begin(values, inner, static span => MemoryMarshal.Cast<TEnum, TOrdinal>(span));

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<TEnum> values, IColumnWriteState state)
        => ReinterpretedValues.WritePrefix(writer, values, state, inner, static span => MemoryMarshal.Cast<TEnum, TOrdinal>(span));

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<TEnum> values, IColumnWriteState state)
        => ReinterpretedValues.Write(writer, values, state, inner, static span => MemoryMarshal.Cast<TEnum, TOrdinal>(span));
}

/// <summary>
/// <c>TEnum?</c> written as the nullable ordinal by the writer of <c>TOrdinal?</c>: null stays null. A nullable enum has
/// the layout of its nullable ordinal type, so the values are read as nullable ordinals with no copy.
/// </summary>
/// <typeparam name="TEnum">The enum type.</typeparam>
/// <typeparam name="TOrdinal">The ordinal type, which is the underlying type of <typeparamref name="TEnum"/>.</typeparam>
internal sealed class NullableEnumWriter<TEnum, TOrdinal> : ColumnWriter<TEnum?>
    where TEnum : unmanaged, Enum
    where TOrdinal : unmanaged
{
    private readonly ColumnWriter<TOrdinal?> inner;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of the nullable ordinals.</param>
    public NullableEnumWriter(ColumnWriter<TOrdinal?> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<TEnum?> values) => ReinterpretedValues.Begin(values, inner, Ordinals);

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<TEnum?> values, IColumnWriteState state)
        => ReinterpretedValues.WritePrefix(writer, values, state, inner, Ordinals);

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<TEnum?> values, IColumnWriteState state)
        => ReinterpretedValues.Write(writer, values, state, inner, Ordinals);

    private static ReadOnlySpan<TOrdinal?> Ordinals(ReadOnlySpan<TEnum?> values)
        => MemoryMarshal.CreateReadOnlySpan(ref Unsafe.As<TEnum?, TOrdinal?>(ref MemoryMarshal.GetReference(values)), values.Length);
}

/// <summary>
/// The types of a cast rule, for a tier that chooses the CLR type of its values from the type that a rule writes them
/// as (<see cref="AssignWriter{TSource, TTarget}"/>).
/// </summary>
internal interface ICastWriter
{
    /// <summary>The CLR type that the rule writes the values as.</summary>
    Type TargetType { get; }
}

/// <summary>
/// A value written as a type that it casts to with no conversion (<see cref="WriteRules.CastTargets"/>), by the writer
/// of that type: a reference keeps its object, and a value type is boxed.
/// </summary>
/// <remarks>
/// A cast between two reference types keeps each object, so the span of <typeparamref name="TSource"/> references is
/// read as a span of <typeparamref name="TTarget"/> references, with no copy. A value type is boxed into a pooled
/// buffer.
/// </remarks>
/// <typeparam name="TSource">The CLR type of the values.</typeparam>
/// <typeparam name="TTarget">The CLR type that the inner writer writes, which <typeparamref name="TSource"/> casts to.</typeparam>
internal sealed class AssignWriter<TSource, TTarget> : ColumnWriter<TSource>, ICastWriter
{
    // Both reference types: the references are read in place.
    private static readonly bool InPlace = !typeof(TSource).IsValueType && !typeof(TTarget).IsValueType;

    private readonly ColumnWriter<TTarget> inner;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of <typeparamref name="TTarget"/>.</param>
    public AssignWriter(ColumnWriter<TTarget> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public Type TargetType => typeof(TTarget);

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<TSource> values)
        => InPlace ? ReinterpretedValues.Begin(values, inner, Targets) : CopiedValues<TSource, TTarget>.Begin(values, inner, Cast);

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state)
    {
        if (InPlace)
        {
            ReinterpretedValues.WritePrefix(writer, values, state, inner, Targets);
        }
        else
        {
            CopiedValues<TSource, TTarget>.WritePrefix(writer, values, state, inner);
        }
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state)
    {
        if (InPlace)
        {
            ReinterpretedValues.Write(writer, values, state, inner, Targets);
        }
        else
        {
            CopiedValues<TSource, TTarget>.Write(writer, values, state, inner);
        }
    }

    // The cast of C#: a reference keeps its object, a value type is boxed, a null nullable value is null.
    private static TTarget Cast(TSource value) => (TTarget)(object)value;

    private static ReadOnlySpan<TTarget> Targets(ReadOnlySpan<TSource> values)
        => MemoryMarshal.CreateReadOnlySpan(ref Unsafe.As<TSource, TTarget>(ref MemoryMarshal.GetReference(values)), values.Length);
}

/// <summary>
/// A value type written by the writer of its nullable type, into a type that holds NULL. No value is NULL. A
/// <c>Nullable(X)</c> writer gets a null map of zeros and writes X from the same values, with no copy; another writer
/// gets the values copied into a pooled buffer of <c>T?</c>.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class LiftWriter<T> : ColumnWriter<T>
    where T : struct
{
    private readonly ColumnWriter<T?> inner;

    // The writer of X when the inner writer is the one of Nullable(X), else null.
    private readonly ColumnWriter<T> present;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of <c>T?</c>.</param>
    public LiftWriter(ColumnWriter<T?> inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        present = (inner as NullableValueWriter<T>)?.Inner;
    }

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<T> values)
        => present is not null ? present.Begin(values) : CopiedValues<T, T?>.Begin(values, inner, static value => value);

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        if (present is not null)
        {
            present.WritePrefix(writer, values, state);
        }
        else
        {
            CopiedValues<T, T?>.WritePrefix(writer, values, state, inner);
        }
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        if (present is null)
        {
            CopiedValues<T, T?>.Write(writer, values, state, inner);
            return;
        }

        // The null map: a position that the source marks (a NULL of an enclosing type) is NULL, every other one is not.
        if (values.HasAbsent)
        {
            foreach (byte mark in values.Absent)
            {
                writer.WriteByte(mark != 0 ? (byte)1 : (byte)0);
            }
        }
        else
        {
            LeafBytes.WriteZeros(writer, values.Count);
        }

        present.Write(writer, values, state);
    }
}

/// <summary>
/// A nullable value type written by the writer of its value type, into a type that cannot hold NULL. The first NULL, in
/// the order of the values, stops the write before the writer of the value type starts.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class NonNullWriter<T> : ColumnWriter<T?>
    where T : struct
{
    private readonly ColumnWriter<T> inner;
    private readonly string columnType;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of <typeparamref name="T"/>.</param>
    /// <param name="columnType">The ClickHouse type of the column, for the message of a NULL.</param>
    public NonNullWriter(ColumnWriter<T> inner, string columnType)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        this.columnType = columnType;
    }

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">A value is null. The message names the column, its type and the row.</exception>
    public override IColumnWriteState Begin(ValueSource<T?> values)
    {
        ReadOnlySpan<byte> absent = values.Absent;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            foreach (T? value in values.Run(r))
            {
                // A position that the source marks has no value to read.
                if (!value.HasValue && !(values.HasAbsent && absent[position] != 0))
                {
                    throw WriteRules.NullNotWritable(values.Column, columnType, (long)values.FirstRow + position);
                }

                position++;
            }
        }

        return CopiedValues<T?, T>.Begin(values, inner, static value => value.GetValueOrDefault());
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T?> values, IColumnWriteState state)
        => CopiedValues<T?, T>.WritePrefix(writer, values, state, inner);

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T?> values, IColumnWriteState state)
        => CopiedValues<T?, T>.Write(writer, values, state, inner);
}

/// <summary>A conversion of a span of values to a span of another type over the same memory.</summary>
/// <typeparam name="TSource">The CLR type of the values.</typeparam>
/// <typeparam name="TTarget">The CLR type that the span is read as.</typeparam>
/// <param name="values">The values.</param>
/// <returns>The same memory as values of <typeparamref name="TTarget"/>.</returns>
internal delegate ReadOnlySpan<TTarget> Reinterpret<TSource, TTarget>(ReadOnlySpan<TSource> values);

/// <summary>
/// The three phases of a rule writer that gives the inner writer its values as another type over the same memory. A
/// source over one span is read in place. A segmented source is copied run by run into a pooled buffer, so the inner
/// writer gets one span: its segments are arrays of the source type, which are no arrays of the other type.
/// </summary>
internal static class ReinterpretedValues
{
    /// <summary>Begins the inner write.</summary>
    /// <typeparam name="TSource">The CLR type of the values.</typeparam>
    /// <typeparam name="TTarget">The CLR type of the inner writer.</typeparam>
    /// <param name="values">The values.</param>
    /// <param name="inner">The inner writer.</param>
    /// <param name="reinterpret">Reads a span of values as the other type.</param>
    /// <returns>The state of the write.</returns>
    public static IColumnWriteState Begin<TSource, TTarget>(ValueSource<TSource> values, ColumnWriter<TTarget> inner, Reinterpret<TSource, TTarget> reinterpret)
    {
        if (!values.IsSegmented)
        {
            return inner.Begin(InPlace(values, reinterpret));
        }

        var state = new CopyState<TTarget>(values.Count);
        try
        {
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                ReadOnlySpan<TTarget> run = reinterpret(values.Run(r));
                run.CopyTo(state.Buffer.AsSpan(position));
                position += run.Length;
            }

            state.Inner = inner.Begin(state.Child(values));
            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <summary>Writes the inner prefix.</summary>
    /// <typeparam name="TSource">The CLR type of the values.</typeparam>
    /// <typeparam name="TTarget">The CLR type of the inner writer.</typeparam>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values that <see cref="Begin"/> was given.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    /// <param name="inner">The inner writer.</param>
    /// <param name="reinterpret">Reads a span of values as the other type.</param>
    public static void WritePrefix<TSource, TTarget>(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state, ColumnWriter<TTarget> inner, Reinterpret<TSource, TTarget> reinterpret)
    {
        if (values.IsSegmented)
        {
            var own = (CopyState<TTarget>)state;
            inner.WritePrefix(writer, own.Child(values), own.Inner);
        }
        else
        {
            inner.WritePrefix(writer, InPlace(values, reinterpret), state);
        }
    }

    /// <summary>Writes the inner body.</summary>
    /// <typeparam name="TSource">The CLR type of the values.</typeparam>
    /// <typeparam name="TTarget">The CLR type of the inner writer.</typeparam>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values that <see cref="Begin"/> was given.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    /// <param name="inner">The inner writer.</param>
    /// <param name="reinterpret">Reads a span of values as the other type.</param>
    public static void Write<TSource, TTarget>(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state, ColumnWriter<TTarget> inner, Reinterpret<TSource, TTarget> reinterpret)
    {
        if (values.IsSegmented)
        {
            var own = (CopyState<TTarget>)state;
            inner.Write(writer, own.Child(values), own.Inner);
        }
        else
        {
            inner.Write(writer, InPlace(values, reinterpret), state);
        }
    }

    // The span read as the other type, with the rows, the column name and the marks of the source.
    private static ValueSource<TTarget> InPlace<TSource, TTarget>(ValueSource<TSource> values, Reinterpret<TSource, TTarget> reinterpret)
    {
        ValueSource<TTarget> target = ValueSource<TTarget>.Of(reinterpret(values.Span), values.FirstRow, values.Column);
        return values.HasAbsent ? target.WithAbsent(values.Absent) : target;
    }
}

/// <summary>
/// The three phases of a rule writer that converts each value into a pooled buffer of the type of the inner writer. The
/// buffer lives in the state of the write, so the prefix and the body read the same values.
/// </summary>
/// <typeparam name="TSource">The CLR type of the values.</typeparam>
/// <typeparam name="TTarget">The CLR type of the inner writer.</typeparam>
internal static class CopiedValues<TSource, TTarget>
{
    /// <summary>Converts the values and begins the inner write.</summary>
    /// <param name="values">The values.</param>
    /// <param name="inner">The inner writer.</param>
    /// <param name="convert">Converts one value.</param>
    /// <returns>The state of the write.</returns>
    public static IColumnWriteState Begin(ValueSource<TSource> values, ColumnWriter<TTarget> inner, Func<TSource, TTarget> convert)
    {
        var state = new CopyState<TTarget>(values.Count);
        try
        {
            Span<TTarget> buffer = state.Buffer.AsSpan(0, values.Count);
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (TSource value in values.Run(r))
                {
                    buffer[position++] = convert(value);
                }
            }

            state.Inner = inner.Begin(state.Child(values));
            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <summary>Writes the inner prefix.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values that <see cref="Begin"/> was given.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    /// <param name="inner">The inner writer.</param>
    public static void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state, ColumnWriter<TTarget> inner)
    {
        var own = (CopyState<TTarget>)state;
        inner.WritePrefix(writer, own.Child(values), own.Inner);
    }

    /// <summary>Writes the inner body.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values that <see cref="Begin"/> was given.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    /// <param name="inner">The inner writer.</param>
    public static void Write(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state, ColumnWriter<TTarget> inner)
    {
        var own = (CopyState<TTarget>)state;
        inner.Write(writer, own.Child(values), own.Inner);
    }
}

/// <summary>The pooled values of a rule writer for its inner writer, and the state of the inner write.</summary>
/// <typeparam name="T">The CLR type of the inner writer.</typeparam>
internal sealed class CopyState<T> : IColumnWriteState
{
    /// <summary>Rents the buffer.</summary>
    /// <param name="count">The number of values.</param>
    public CopyState(int count) => Buffer = WriteBuffers.Rent<T>(count);

    /// <summary>The values for the inner writer.</summary>
    public T[] Buffer { get; private set; }

    /// <summary>The state of the inner write.</summary>
    public IColumnWriteState Inner { get; set; }

    /// <summary>
    /// The source of the inner writer: the buffer, with the rows, the column name and the marks of the source. A
    /// segmented source has no rows of its own, so the inner source starts at row 0, as the source does.
    /// </summary>
    /// <typeparam name="TSource">The CLR type of the values of the source.</typeparam>
    /// <param name="values">The source.</param>
    /// <returns>The source of the inner writer.</returns>
    public ValueSource<T> Child<TSource>(ValueSource<TSource> values)
    {
        ValueSource<T> child = ValueSource<T>.Of(Buffer.AsSpan(0, values.Count), values.FirstRow, values.Column);
        return values.HasAbsent ? child.WithAbsent(values.Absent) : child;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Inner?.Dispose();
        Inner = null;
        WriteBuffers.Return(Buffer);
        Buffer = null;
    }
}
