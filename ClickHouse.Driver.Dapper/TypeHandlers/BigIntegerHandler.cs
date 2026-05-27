using System;
using System.Data;
using System.Globalization;
using System.Numerics;
using Dapper;

namespace ClickHouse.Driver.Dapper.TypeHandlers;

internal sealed class BigIntegerHandler : SqlMapper.TypeHandler<BigInteger>
{
    public override void SetValue(IDbDataParameter parameter, BigInteger value)
    {
        parameter.Value = value;
    }

    public override BigInteger Parse(object value) => value switch
    {
        BigInteger bi => bi,
        long l => new BigInteger(l),
        ulong ul => new BigInteger(ul),
        int i => new BigInteger(i),
        uint ui => new BigInteger(ui),
        decimal d => new BigInteger(d),
        string s => BigInteger.Parse(s, CultureInfo.InvariantCulture),
        _ => throw new ArgumentException($"Cannot convert {value?.GetType().Name ?? "null"} to BigInteger", nameof(value)),
    };
}
