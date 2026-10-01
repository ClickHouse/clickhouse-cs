using System;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Shared POCO column-to-property assignment rules, used by both the boxed <c>MapTo&lt;T&gt;</c> path and the
/// box-free read fast path's per-column boxed fallback, so the two produce identical validation and errors.
/// </summary>
internal static class PocoColumnAssignment
{
    /// <summary>
    /// Whether a column of the given ClickHouse type can be assigned to <paramref name="propInfo"/> under the
    /// strict read rules: exact/assignable framework type (nullable-unwrapped). Polymorphic columns
    /// (FrameworkType == object, e.g. Variant/Dynamic/JSON/Object) are permitted here and validated per-row.
    /// </summary>
    public static bool IsAssignable(PocoPropertyInfo propInfo, ClickHouseType columnType)
    {
        var colFrameworkType = columnType.FrameworkType;
        var unwrapped = Nullable.GetUnderlyingType(colFrameworkType) ?? colFrameworkType;

        if (unwrapped == typeof(object))
            return true;

        return propInfo.PropertyType.IsAssignableFrom(unwrapped)
            || (propInfo.NullableUnderlyingType != null && propInfo.NullableUnderlyingType.IsAssignableFrom(unwrapped));
    }

    public static string BuildAssignmentErrorMessage(
        Type targetType,
        PocoPropertyInfo propInfo,
        string columnName,
        string clickHouseType,
        Type returnedType)
    {
        var returnedDescription = returnedType is null ? "null" : returnedType.FullName;
        return
            $"Cannot map ClickHouse column '{columnName}' ({clickHouseType}) to property " +
            $"{targetType.Name}.{propInfo.PropertyName} ({propInfo.PropertyType.FullName}). " +
            $"The reader returned {returnedDescription}, which is not assignable to {propInfo.PropertyType.FullName}.";
    }
}
