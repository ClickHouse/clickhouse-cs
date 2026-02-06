using ClickHouse.Driver.ADO.Parameters;

namespace ClickHouse.Driver.Utility;

public static class ClickHouseParameterCollectionExtensions
{
    public static void AddParameter(this ClickHouseParameterCollection parameters, string parameterName, object value)
    {
        parameters.Add(new ClickHouseDbParameter { ParameterName = parameterName, Value = value });
    }
}
