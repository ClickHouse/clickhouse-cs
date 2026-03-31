using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Copy;

/// <summary>
/// Pre-computed mapping for a registered POCO type, including property metadata
/// and an optional column types dictionary for schema probe skipping.
/// </summary>
internal abstract class PocoTypeMapping
{
    /// <summary>
    /// The mapped properties for this type.
    /// </summary>
    internal BinaryInsertPropertyInfo[] Properties { get; init; }

    /// <summary>
    /// Pre-built column name → ClickHouse type dictionary, or null if not all properties have explicit types.
    /// When non-null, the schema probe can be skipped.
    /// </summary>
    internal IReadOnlyDictionary<string, string> ColumnTypes { get; init; }
}

/// <summary>
/// Generic mapping that holds typed property getters, avoiding a per-row cast in the serialization hot loop.
/// </summary>
internal sealed class PocoTypeMapping<T> : PocoTypeMapping
    where T : class
{
    /// <summary>
    /// Compiled getters that extract each property value from a <typeparamref name="T"/> instance.
    /// Ordered to match <see cref="PocoTypeMapping.Properties"/>.
    /// </summary>
    internal Func<T, object>[] Getters { get; init; }
}
