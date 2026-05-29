using System;
using System.Data;
using System.Runtime.CompilerServices;
using Dapper;

namespace ClickHouse.Driver.Dapper.TypeHandlers;

internal sealed class ITupleHandler : SqlMapper.TypeHandler<ITuple>
{
    public override void SetValue(IDbDataParameter parameter, ITuple value)
    {
        parameter.Value = value;
    }

    public override ITuple Parse(object value) =>
        value as ITuple ?? throw new ArgumentException($"Cannot convert {value?.GetType().Name ?? "null"} to ITuple", nameof(value));
}
