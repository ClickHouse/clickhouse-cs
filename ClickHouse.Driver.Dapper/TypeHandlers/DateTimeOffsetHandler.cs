using System;
using System.Data;
using System.Globalization;
using Dapper;

namespace ClickHouse.Driver.Dapper.TypeHandlers;

internal sealed class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
{
    public override void SetValue(IDbDataParameter parameter, DateTimeOffset value)
    {
        parameter.Value = value.UtcDateTime;
    }

    public override DateTimeOffset Parse(object value) => value switch
    {
        DateTimeOffset dto => dto,
        DateTime dt => new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt),
        string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture),
        _ => throw new ArgumentException($"Cannot convert {value?.GetType().Name ?? "null"} to DateTimeOffset", nameof(value)),
    };
}
