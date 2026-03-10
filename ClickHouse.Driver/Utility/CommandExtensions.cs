using System;
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
    /// <remarks>
    /// This overload is obsolete because the type can now be specified directly in the SQL query
    /// using ClickHouse's native parameter syntax: <c>{parameterName:Type}</c>.
    /// Use <see cref="AddParameterWithTypeOverride"/> if you need to explicitly override the SQL type hint.
    /// </remarks>
    [Obsolete("Parameter type is now parsed from the query and does not need to be specified. Use AddParameter(parameterName, value) instead. Use AddParameterWithTypeOverride() if you need to override the SQL type hint.")]
    public static ClickHouseDbParameter AddParameter(this ClickHouseCommand command, string parameterName, string clickHouseType, object parameterValue)
    {
        return AddParameterWithTypeOverride(command, parameterName, clickHouseType, parameterValue);
    }

    /// <summary>
    /// Add parameter to a command with an explicit ClickHouse type that overrides any type hint in the SQL query.
    /// </summary>
    /// <param name="command">The command to add the parameter to</param>
    /// <param name="parameterName">Parameter name (without curly braces). This should match the placeholder name used in the SQL command text (e.g., "userId" for {userId:UInt64})</param>
    /// <param name="clickHouseType">The explicit ClickHouse type of the parameter (e.g., "UInt64", "String", "DateTime", "Array(Int32)"). This takes precedence over any type specified in the SQL query.</param>
    /// <param name="parameterValue">Parameter value to bind. The value should be compatible with the specified ClickHouse type</param>
    /// <returns>The created ClickHouseDbParameter that was added to the command</returns>
    /// <remarks>
    /// Use this method when you need to override the type hint specified in the SQL query.
    /// For example, if your SQL contains <c>{dt:DateTime}</c> but you want to format as <c>DateTime64</c>,
    /// you can use this method to explicitly set the type.
    /// </remarks>
    public static ClickHouseDbParameter AddParameterWithTypeOverride(this ClickHouseCommand command, string parameterName, string clickHouseType, object parameterValue)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = parameterName;
        parameter.ClickHouseType = clickHouseType;
        parameter.Value = parameterValue;
        command.Parameters.Add(parameter);
        return parameter;
    }
}
