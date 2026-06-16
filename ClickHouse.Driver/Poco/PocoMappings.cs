using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Pre-computed mapping for a registered POCO type used during binary insert.
/// </summary>
internal abstract class PocoInsertMapping
{
    /// <summary>The properties mapped on the insert side.</summary>
    internal PocoPropertyInfo[] Properties { get; init; }

    /// <summary>
    /// Pre-built column name → ClickHouse type dictionary, or null if not all properties have
    /// explicit types. When non-null, the schema probe can be skipped.
    /// </summary>
    internal IReadOnlyDictionary<string, string> ColumnTypes { get; init; }
}

/// <summary>
/// Generic insert mapping that holds typed property getters, avoiding a per-row cast in the
/// serialization hot loop.
/// </summary>
internal sealed class PocoInsertMapping<T> : PocoInsertMapping
    where T : class
{
    /// <summary>Compiled getters; ordered to match <see cref="PocoInsertMapping.Properties"/>.</summary>
    internal Func<T, object>[] Getters { get; init; }
}

/// <summary>
/// Non-generic marker for a read mapping so the registry can store mappings keyed by
/// <see cref="Type"/> in a non-generic dictionary; the actual data lives on the generic subclass.
/// </summary>
internal abstract class PocoReadMapping
{
}

/// <summary>
/// Generic read mapping that holds one entry per mapped column name. Each entry carries both
/// the property metadata (for assignability checks and error messages) and the compiled setter,
/// so MapTo&lt;T&gt; looks up a column in a single dictionary hit with no further indirection.
/// </summary>
internal sealed class PocoReadMapping<T> : PocoReadMapping
    where T : class
{
    /// <summary>
    /// Lookup from mapped column name (case-sensitive, <see cref="StringComparer.Ordinal"/>) to
    /// the resolved column binding.
    /// </summary>
    internal IReadOnlyDictionary<string, ColumnBinding<T>> Bindings { get; init; }

    /// <summary>Compiled parameterless-constructor invoker for materializing new instances.</summary>
    internal Func<T> Constructor { get; init; }
}

/// <summary>
/// One resolved column-to-property binding: the property metadata used during assignability
/// validation and error messages, and the compiled setter that writes the column value into an
/// instance of <typeparamref name="T"/>.
/// </summary>
internal readonly struct ColumnBinding<T>
{
    internal PocoPropertyInfo PropInfo { get; init; }

    internal Action<T, object> Setter { get; init; }
}
