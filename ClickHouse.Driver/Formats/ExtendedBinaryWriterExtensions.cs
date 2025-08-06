#if NET462
using System;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Formats;

internal static class ExtendedBinaryWriterExtensions
{
    public static void WriteTuple(this ExtendedBinaryWriter writer, object value, ClickHouseType[] underlyingTypes)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        var type = value.GetType();
        var length = TupleHelper.GetTupleLength(type);
        if (length != underlyingTypes.Length)
            throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));

        var properties = TupleHelper.GetTuplePropertiesWithRest(type);

        for (var i = 0; i < underlyingTypes.Length; i++)
        {
            var property = properties[i];
            if (property == null)
                throw new ArgumentException($"Property for index {i} not found on tuple type {type}", nameof(value));

            var itemValue = property.GetValue(value);

            // Rest returns Tuple<T8>
            if (i == 7 && property.Name == "Rest" && itemValue != null && TupleHelper.IsTupleType(itemValue.GetType()))
            {
                var restProperties = TupleHelper.GetTupleProperties(itemValue.GetType());
                if (restProperties.Length > 0)
                    itemValue = restProperties[0].GetValue(itemValue);
            }

            underlyingTypes[i].Write(writer, itemValue);
        }
    }
}
#endif
