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
/// Pre-computed mapping for a registered POCO type used to materialize query results.
/// </summary>
internal abstract class PocoReadMapping
{
    /// <summary>The properties mapped on the read side.</summary>
    internal PocoPropertyInfo[] Properties { get; init; }

    /// <summary>
    /// Lookup from mapped column name (case-sensitive, <see cref="StringComparer.Ordinal"/>) to
    /// the property's index into <see cref="Properties"/>.
    /// </summary>
    internal IReadOnlyDictionary<string, int> ColumnNameToPropertyIndex { get; init; }
}

/// <summary>
/// Generic read mapping that holds typed property setters and a constructor delegate, avoiding
/// per-row reflection during materialization.
/// </summary>
internal sealed class PocoReadMapping<T> : PocoReadMapping
    where T : class
{
    /// <summary>Compiled setters; ordered to match <see cref="PocoReadMapping.Properties"/>.</summary>
    internal Action<T, object>[] Setters { get; init; }

    /// <summary>Compiled parameterless-constructor invoker for materializing new instances.</summary>
    internal Func<T> Constructor { get; init; }
}
