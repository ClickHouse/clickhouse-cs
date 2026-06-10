using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.ADO.Readers;

/// <summary>
/// Dispatches read-value conversions by CLR type. Register a per-type transform with
/// <see cref="For{T}"/>; values whose runtime type is not registered pass through unchanged.
/// Dispatch is by CLR type only — to distinguish columns by their ClickHouse-side type
/// (e.g., <c>DateTime</c> vs <c>DateTime('UTC')</c>, both surface as <see cref="System.DateTime"/>)
/// implement <see cref="IReadValueConverter"/> directly.
/// </summary>
public sealed class DictionaryReadValueConverter : IReadValueConverter
{
    private readonly Dictionary<Type, Entry> mappings = new Dictionary<Type, Entry>();

    /// <summary>
    /// Registers a converter for values whose runtime CLR type is exactly <typeparamref name="T"/>.
    /// Replaces any previous registration for the same type. Not thread-safe; call only during setup.
    /// </summary>
    /// <typeparam name="T">The runtime CLR type to match. Nullable column values surface as the
    /// underlying type (e.g., a <c>Nullable(DateTime)</c> non-null value matches <c>For&lt;DateTime&gt;</c>).</typeparam>
    /// <param name="converter">The transform to apply. Must return a value of the same type.</param>
    /// <returns>This instance, for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="converter"/> is null.</exception>
    public DictionaryReadValueConverter For<T>(Func<T, T> converter)
    {
        if (converter == null)
            throw new ArgumentNullException(nameof(converter));

        Func<object, object> boxed = v => converter((T)v);
        mappings[typeof(T)] = new Entry(converter, boxed);
        return this;
    }

    /// <inheritdoc/>
    public object ConvertValue(object value, string columnName, string clickHouseType)
    {
        if (value == null || value is DBNull)
            return value;

        return mappings.TryGetValue(value.GetType(), out var entry)
            ? entry.Boxed(value)
            : value;
    }

    /// <inheritdoc/>
    public T ConvertValue<T>(T value, string columnName, string clickHouseType)
    {
        if (value == null || value is DBNull)
            return value;

        // GetFieldValue<object>(i) goes through this generic path with T == object and
        // the value boxed to its runtime type. Dispatch by the runtime type so the result
        // matches GetValue(i), which also dispatches by value.GetType().
        if (typeof(T) == typeof(object))
        {
            return mappings.TryGetValue(value.GetType(), out var objEntry)
                ? (T)objEntry.Boxed(value)
                : value;
        }

        if (mappings.TryGetValue(typeof(T), out var entry) && entry.Typed is Func<T, T> typed)
            return typed(value);

        // Nullable<U> columns surface non-null cells as the underlying U, so GetFieldValue<U?>
        // must honour a For<U>() registration too — staying consistent with GetValue, which
        // dispatches by the (already-unwrapped) runtime type. Apply via the boxed delegate since
        // the registered Func<U, U> cannot be invoked directly as Func<U?, U?>.
        var underlying = Nullable.GetUnderlyingType(typeof(T));
        if (underlying != null && mappings.TryGetValue(underlying, out var nullableEntry))
            return (T)nullableEntry.Boxed(value);

        return value;
    }

    private readonly struct Entry
    {
        public Entry(Delegate typed, Func<object, object> boxed)
        {
            Typed = typed;
            Boxed = boxed;
        }

        public Delegate Typed { get; }

        public Func<object, object> Boxed { get; }
    }
}
