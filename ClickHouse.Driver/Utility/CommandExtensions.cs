using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;

namespace ClickHouse.Driver.Utility;

public static class CommandExtensions
{
    /// <summary>
    /// Add parameter to a command without specifying the ClickHouse type. The type will be inferred.
    /// </summary>
    /// <param name="command">The command to add the parameter to</param>
    /// <param name="parameterName">Parameter name (without curly braces). This should match the placeholder name used in the SQL command text (e.g., "userId" for {userId:UInt64})</param>
    /// <param name="parameterValue">Parameter value to bind. The ClickHouse type will be automatically inferred from the .NET type</param>
    /// <returns>The created ClickHouseDbParameter that was added to the command</returns>
    public static ClickHouseDbParameter AddParameter(this ClickHouseCommand command, string parameterName, object parameterValue)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = parameterName;
        parameter.Value = parameterValue;
        command.Parameters.Add(parameter);
        return parameter;
    }

    /// <summary>
    /// Add parameter to a command, including a specific ClickHouse type.
    /// </summary>
    /// <param name="command">The command to add the parameter to</param>
    /// <param name="parameterName">Parameter name (without curly braces). This should match the placeholder name used in the SQL command text (e.g., "userId" for {userId:UInt64})</param>
    /// <param name="clickHouseType">The explicit ClickHouse type of the parameter (e.g., "UInt64", "String", "DateTime", "Array(Int32)")</param>
    /// <param name="parameterValue">Parameter value to bind. The value should be compatible with the specified ClickHouse type</param>
    /// <returns>The created ClickHouseDbParameter that was added to the command</returns>
    public static ClickHouseDbParameter AddParameter(this ClickHouseCommand command, string parameterName, string clickHouseType, object parameterValue)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = parameterName;
        parameter.ClickHouseType = clickHouseType;
        parameter.Value = parameterValue;
        command.Parameters.Add(parameter);
        return parameter;
    }
}
