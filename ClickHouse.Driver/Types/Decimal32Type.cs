﻿using System;
using System.Globalization;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class Decimal32Type : DecimalType
{
    public Decimal32Type()
    {
        Precision = 9;
    }

    public override string Name => "Decimal32";

    public override int Size => 4;

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new Decimal32Type
        {
            Scale = int.Parse(node.SingleChild.Value, CultureInfo.InvariantCulture),
            UseBigDecimal = settings.useBigDecimal,
        };
    }

    public override string ToString() => $"{Name}({Scale})";
}
