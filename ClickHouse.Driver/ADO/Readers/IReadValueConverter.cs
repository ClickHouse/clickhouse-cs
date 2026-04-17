namespace ClickHouse.Driver.ADO.Readers;

/// <summary>
/// Transforms values returned by the data reader after deserialization, without changing the CLR type.
/// Sits between the internal type readers and user-facing GetValue/GetFieldValue calls,
/// allowing same-type transformations (e.g., setting DateTime.Kind, trimming strings, normalizing values).
/// The converter must not change the runtime type of values — column metadata (GetFieldType, GetSchemaTable)
/// is not affected by the converter and must remain consistent with the values returned.
/// Return the value unchanged to pass through, or return a transformed value of the same type.
/// </summary>
/// <remarks>
/// Implementations must be thread-safe, as they may be called concurrently from multiple readers.
/// </remarks>
public interface IReadValueConverter
{
    /// <summary>
    /// Converts a deserialized value before it is returned by <see cref="ClickHouseDataReader.GetValue"/>.
    /// </summary>
    /// <param name="value">The deserialized value (may be null or DBNull).</param>
    /// <param name="columnName">The column name.</param>
    /// <param name="clickHouseType">The ClickHouse-side type name exactly as reported by the server (e.g., <c>"DateTime64(3, 'UTC')"</c>, <c>"Nullable(String)"</c>, <c>"LowCardinality(String)"</c>, <c>"Array(Int32)"</c>). Stable across client settings; parse if you need structured info.</param>
    /// <returns>The converted value.</returns>
    object ConvertValue(object value, string columnName, string clickHouseType);

    /// <summary>
    /// Converts a deserialized value before it is returned by <see cref="ClickHouseDataReader.GetFieldValue{T}"/>.
    /// For zero-boxing implementations, use <c>typeof(T)</c> checks with <c>Unsafe.As</c>
    /// to transform value types without boxing.
    /// </summary>
    /// <typeparam name="T">The target type requested by the caller.</typeparam>
    /// <param name="value">The deserialized value, already cast to <typeparamref name="T"/>.</param>
    /// <param name="columnName">The column name.</param>
    /// <param name="clickHouseType">The ClickHouse-side type name exactly as reported by the server (e.g., <c>"DateTime64(3, 'UTC')"</c>, <c>"Nullable(String)"</c>, <c>"LowCardinality(String)"</c>, <c>"Array(Int32)"</c>). Stable across client settings; parse if you need structured info.</param>
    /// <returns>The converted value.</returns>
    T ConvertValue<T>(T value, string columnName, string clickHouseType);
}
