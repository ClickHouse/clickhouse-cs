using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;
using System.Text.Json.Nodes;
using NUnit.Framework.Constraints;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Represents the test environment type.
/// </summary>
public enum TestEnv
{
    /// <summary>
    /// Local single-node ClickHouse instance with full capabilities (default).
    /// Typically a Docker-based setup with access storage enabled.
    /// </summary>
    LocalSingleNode,

    /// <summary>
    /// Local ClickHouse cluster.
    /// </summary>
    LocalCluster,

    /// <summary>
    /// ClickHouse Cloud.
    /// </summary>
    Cloud,

    /// <summary>
    /// Local quick-setup ClickHouse instance with limited capabilities.
    /// Uses "curl https://clickhouse.com/ | sh" style installation.
    /// Does not support user/role management (no access storage).
    /// </summary>
    LocalQuickSetup
}

public static class TestUtilities
{
    public static readonly Feature SupportedFeatures;
    public static readonly Version ServerVersion;

    /// <summary>
    /// Gets the test environment type.
    /// Set via CLICKHOUSE_TEST_ENVIRONMENT environment variable.
    /// Possible values: "local_single_node" (default), "local_cluster", "cloud", "local_quick_setup".
    /// </summary>
    public static readonly TestEnv TestEnvironment;

    static TestUtilities()
    {
        // Initialize test environment
        TestEnvironment = GetClickHouseTestEnvironment();

        var versionString = Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION");
        if (string.IsNullOrEmpty(versionString) || versionString == "latest" || versionString == "head")
        {
            // If there's no version string in the env, get it from the server
            var conn = TestUtilities.GetTestClickHouseConnection();
            var reader = conn.ExecuteReaderAsync("SELECT version()").Result;
            reader.Read();
            versionString = reader.GetString(0);
        }

        try
        {
            ServerVersion = Version.Parse(versionString.Split(':').Last().Trim());
            SupportedFeatures = ClickHouseFeatureMap.GetFeatureFlags(ServerVersion);
        }
        catch
        {
            SupportedFeatures = Feature.All;
        }
    }

    /// <summary>
    /// Equality assertion with special handling for certain object types
    /// </summary>
    /// <param name="expected"></param>
    /// <param name="result"></param>
    public static void AssertEqual(object expected, object result)
    {
        if (expected is JsonNode)
        {
            // Necessary because the ordering of the fields is not guaranteed to be the same
            Assert.That(result, Is.EqualTo(expected).Using<JsonObject, JsonObject>(JsonNode.DeepEquals));
        }
        else if (expected is ITuple expectedTuple && result is ITuple resultTuple)
        {
            // Handle Tuple vs ValueTuple comparison element-by-element via ITuple interface
            Assert.That(resultTuple.Length, Is.EqualTo(expectedTuple.Length), "Tuple length mismatch");
            for (int i = 0; i < expectedTuple.Length; i++)
                AssertEqual(expectedTuple[i], resultTuple[i]);
        }
        else
        {
            Assert.That(result, Is.EqualTo(expected).UsingPropertiesComparer());
        }
    }

    public static ClickHouseClientSettings GetTestClickHouseClientSettings(bool compression = true, bool session = false, bool customDecimals = true, string password = null, bool useFormDataParameters = false, JsonReadMode jsonReadMode = JsonReadMode.Binary, JsonWriteMode jsonWriteMode = JsonWriteMode.String)
    {
        var builder = GetConnectionStringBuilder();
        builder.Compression = compression;
        builder.UseSession = session;
        builder.UseCustomDecimals = customDecimals;
        builder.JsonReadMode = jsonReadMode;
        builder.JsonWriteMode = jsonWriteMode;

        if (password is not null)
        {
            builder.Password = password;
        }
        builder["set_session_timeout"] = 1; // Expire sessions quickly after test
        builder["set_allow_experimental_geo_types"] = 1; // Allow support for geo types
        builder["set_flatten_nested"] = 0; // Nested should be a single column, see https://clickhouse.com/docs/en/operations/settings/settings#flatten-nested

        if (SupportedFeatures.HasFlag(Feature.Map))
        {
            builder["set_allow_experimental_map_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Variant))
        {
            builder["set_allow_experimental_variant_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Json))
        {
            builder["set_allow_experimental_json_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Dynamic))
        {
            builder["set_allow_experimental_dynamic_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Time))
        {
            builder["set_enable_time_time64_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Geometry))
        {
            // Revisit this if the Geometry type is updated to not require this setting in the future
            // it could cause problems by hiding other issues
            builder["set_allow_suspicious_variant_types"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.QBit) && TestEnvironment != TestEnv.Cloud)
        {
            // Cloud doesn't want us changing this option
            builder["set_allow_experimental_qbit_type"] = 1;
        }

        return new ClickHouseClientSettings(builder)
        {
            UseFormDataParameters = useFormDataParameters
        };
    }

    public static ClickHouseClient GetTestClickHouseClient(bool compression = true, bool session = false, bool customDecimals = true, string password = null, bool useFormDataParameters = false, JsonReadMode jsonReadMode = JsonReadMode.Binary, JsonWriteMode jsonWriteMode = JsonWriteMode.String)
    {
        var settings = GetTestClickHouseClientSettings(compression, session, customDecimals, password, useFormDataParameters, jsonReadMode, jsonWriteMode);
        return new ClickHouseClient(settings);
    }

    /// <summary>
    /// Utility method to allow to redirect ClickHouse connections to different machine, in case of Windows development environment
    /// </summary>
    /// <returns></returns>
    public static ClickHouseConnection GetTestClickHouseConnection(bool compression = true, bool session = false, bool customDecimals = true, string password = null, bool useFormDataParameters = false, JsonReadMode jsonReadMode = JsonReadMode.Binary, JsonWriteMode jsonWriteMode = JsonWriteMode.String)
    {
        // Construct from settings so the connection owns its internal ClickHouseClient.
        // Using client.CreateConnection() would set ownsClient=false, leaking the client on dispose.
        var settings = GetTestClickHouseClientSettings(compression, session, customDecimals, password, useFormDataParameters, jsonReadMode, jsonWriteMode);
        var conn = new ClickHouseConnection(settings);
        conn.Open();
        return conn;
    }

    public static ClickHouseConnectionStringBuilder GetConnectionStringBuilder()
    {
        // Connection string must be provided pointing to a test ClickHouse server
        var devConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ??
            throw new InvalidOperationException("Must set CLICKHOUSE_CONNECTION environment variable pointing at ClickHouse server");

        return new ClickHouseConnectionStringBuilder(devConnectionString);
    }

    /// <summary>
    /// Parses the CLICKHOUSE_TEST_ENVIRONMENT environment variable to determine the test environment.
    /// </summary>
    public static TestEnv GetClickHouseTestEnvironment()
    {
        var value = Environment.GetEnvironmentVariable("CLICKHOUSE_TEST_ENVIRONMENT");
        return value switch
        {
            "cloud" => TestEnv.Cloud,
            "local_cluster" => TestEnv.LocalCluster,
            "local_quick_setup" => TestEnv.LocalQuickSetup,
            "local_single_node" or null or "" => TestEnv.LocalSingleNode,
            _ => throw new InvalidOperationException(
                $"Unexpected CLICKHOUSE_TEST_ENVIRONMENT value: '{value}'. " +
                "Possible options: 'local_single_node', 'local_cluster', 'local_quick_setup', 'cloud'. " +
                "You can keep it unset to fall back to 'local_single_node'.")
        };
    }

    public static object[] GetEnsureSingleRow(this DbDataReader reader)
    {
        ClassicAssert.IsTrue(reader.HasRows, "Reader expected to have rows");
        ClassicAssert.IsTrue(reader.Read(), "Failed to read first row");

        var data = reader.GetFieldValues();

        ClassicAssert.IsFalse(reader.Read(), "Unexpected extra row: " + string.Join(",", reader.GetFieldValues()));

        return data;
    }

    public static Type[] GetFieldTypes(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetFieldType).ToArray();

    public static string[] GetFieldNames(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();

    public static object[] GetFieldValues(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetValue).ToArray();

    public static void AssertHasFieldCount(this DbDataReader reader, int expectedCount) => Assert.That(reader.FieldCount, Is.EqualTo(expectedCount));
}
