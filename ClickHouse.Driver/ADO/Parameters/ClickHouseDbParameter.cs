using System;
using System.Data;
using System.Data.Common;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Represents a parameter for a <see cref="ClickHouseCommand"/>.
/// </summary>
/// <remarks>
/// Parameters are substituted using ClickHouse's native parameter syntax: <c>{name:Type}</c>.
/// </remarks>
public class ClickHouseDbParameter : DbParameter
{
    /// <inheritdoc/>
    public override DbType DbType { get; set; }

    /// <summary>
    /// Gets or sets an explicit ClickHouse type name (e.g., "UInt64", "Nullable(String)").
    /// When set, this overrides automatic type inference from <see cref="Value"/>.
    /// </summary>
    public string ClickHouseType { get; set; }

    /// <summary>
    /// Gets the parameter direction. Always returns <see cref="ParameterDirection.Input"/>
    /// as ClickHouse only supports input parameters.
    /// </summary>
    public override ParameterDirection Direction { get => ParameterDirection.Input; set { } }

    /// <inheritdoc/>
    public override bool IsNullable { get; set; }

    /// <summary>
    /// Gets or sets the parameter name (without the @ prefix).
    /// </summary>
    public override string ParameterName { get; set; }

    /// <inheritdoc/>
    public override int Size { get; set; }

    /// <inheritdoc/>
    public override string SourceColumn { get; set; }

    /// <inheritdoc/>
    public override bool SourceColumnNullMapping { get; set; }

    /// <summary>
    /// Gets or sets the parameter value. The type is automatically inferred for SQL generation.
    /// </summary>
    public override object Value { get; set; }

    /// <inheritdoc/>
    public override void ResetDbType() { }

    /// <inheritdoc/>
    public override string ToString() => $"{ParameterName}:{Value}";

    /// <summary>
    /// Gets the ClickHouse parameter placeholder string to embed in SQL queries.
    /// Format: <c>{name:Type}</c> (e.g., <c>{id:Int32}</c>).
    /// </summary>
    public string QueryForm
    {
        get
        {
            if (ClickHouseType != null)
                return $"{{{ParameterName}:{ClickHouseType}}}";

            if (Value is decimal d)
            {
                var parts = decimal.GetBits(d);
                int scale = (parts[3] >> 16) & 0x7F;
                return $"{{{ParameterName}:Decimal128({scale})}}";
            }

            var chType = TypeConverter.ToClickHouseType(Value?.GetType() ?? typeof(DBNull)).ToString();
            return $"{{{ParameterName}:{chType}}}";
        }
    }
}
