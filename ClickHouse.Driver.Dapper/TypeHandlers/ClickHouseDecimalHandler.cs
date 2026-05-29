using System;
using System.Data;
using System.Globalization;
using ClickHouse.Driver.Numerics;
using Dapper;

namespace ClickHouse.Driver.Dapper.TypeHandlers;

internal sealed class ClickHouseDecimalHandler : SqlMapper.TypeHandler<ClickHouseDecimal>
{
    public override void SetValue(IDbDataParameter parameter, ClickHouseDecimal value)
    {
        parameter.Value = value.ToString(CultureInfo.InvariantCulture);
    }

    public override ClickHouseDecimal Parse(object value) => value switch
    {
        ClickHouseDecimal chd => chd,
        IConvertible ic => Convert.ToDecimal(ic, CultureInfo.InvariantCulture),
        _ => throw new ArgumentException($"Cannot convert {value?.GetType().Name ?? "null"} to ClickHouseDecimal", nameof(value)),
    };
}
