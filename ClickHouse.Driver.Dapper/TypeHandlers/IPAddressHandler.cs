using System;
using System.Data;
using System.Net;
using Dapper;

namespace ClickHouse.Driver.Dapper.TypeHandlers;

internal sealed class IPAddressHandler : SqlMapper.TypeHandler<IPAddress>
{
    public override void SetValue(IDbDataParameter parameter, IPAddress value)
    {
        parameter.Value = value;
    }

    public override IPAddress Parse(object value) => value switch
    {
        IPAddress ip => ip,
        string s => IPAddress.Parse(s),
        _ => throw new ArgumentException($"Cannot convert {value?.GetType().Name ?? "null"} to IPAddress", nameof(value)),
    };
}
