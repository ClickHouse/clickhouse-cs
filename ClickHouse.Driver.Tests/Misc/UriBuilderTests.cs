using System;
using System.Collections.Generic;
using System.Web;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Misc;

public class UriBuilderTests
{
    [Test]
    public void ShouldSetUriParametersCorrectly()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            ConnectionQueryStringParameters = new Dictionary<string, object> { { "a", 1 }, { "b", "c" } },
            CommandQueryStringParameters = new Dictionary<string, object> { { "c", 1 }, { "d", "c" } },
            UseCompression = false,
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.Multiple(() =>
        {
            Assert.That(result.Host, Is.EqualTo("some.server"));
            Assert.That(result.Port, Is.EqualTo(123));
            Assert.That(@params.Get("database"), Is.EqualTo("DATABASE"));
            Assert.That(@params.Get("query"), Is.EqualTo("SELECT 1"));
            Assert.That(@params.Get("a"), Is.EqualTo("1"));
            Assert.That(@params.Get("b"), Is.EqualTo("c"));
            Assert.That(@params.Get("c"), Is.EqualTo("1"));
            Assert.That(@params.Get("d"), Is.EqualTo("c"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("SESSION"));
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("false"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("QUERY"));
            Assert.That(@params.Get("param_sqlParameterName"), Is.EqualTo("sqlParameterValue"));
        });
    }

    [Test]
    public void CommandQueryStringParametersShouldOverrideConnectionParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            ConnectionQueryStringParameters = new Dictionary<string, object> { { "a", 1 } },
            CommandQueryStringParameters = new Dictionary<string, object> { { "a", 2 } },
        };

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.That(@params.Get("a"), Is.EqualTo("2"));
    }

    [Test]
    public void ConnectionQueryStringParametersShouldOverrideCommonParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            UseCompression = false,
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
            ConnectionQueryStringParameters = new Dictionary<string, object>
            {
                { "database", "overrided" },
                { "enable_http_compression", "overrided" },
                { "query", "overrided" },
                { "session_id", "overrided" },
                { "query_id", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.Multiple(() =>
        {
            Assert.That(@params.Get("database"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("overrided"));
        });
    }

    [Test]
    public void ConnectionQueryStringParametersShouldOverrideSqlQueryParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            ConnectionQueryStringParameters = new Dictionary<string, object>
            {
                { "param_sqlParameterName", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.That(@params.Get("param_sqlParameterName"), Is.EqualTo("overrided"));
    }

    [Test]
    public void CommandQueryStringParametersShouldOverrideCommonParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            UseCompression = false,
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
            CommandQueryStringParameters = new Dictionary<string, object>
            {
                { "database", "overrided" },
                { "enable_http_compression", "overrided" },
                { "query", "overrided" },
                { "session_id", "overrided" },
                { "query_id", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.Multiple(() =>
        {
            Assert.That(@params.Get("database"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("overrided"));
        });
    }

    [Test]
    public void CommandQueryStringParametersShouldOverrideSqlQueryParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            CommandQueryStringParameters = new Dictionary<string, object>
            {
                { "param_sqlParameterName", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.That(@params.Get("param_sqlParameterName"), Is.EqualTo("overrided"));
    }

    [Test]
    [TestCase("http://localhost:8123", "http://localhost:8123/")]
    [TestCase("http://localhost:8123/", "http://localhost:8123/")]
    [TestCase("http://localhost/clickhouse", "http://localhost:80/clickhouse")]
    [TestCase("https://some.server:8443/path", "https://some.server:8443/path")]
    // IPv6 literals: Uri.Host keeps the brackets (unlike DnsSafeHost), so the manual composition
    // matches UriBuilder byte-for-byte.
    [TestCase("http://[::1]:8123", "http://[::1]:8123/")]
    [TestCase("http://[2001:db8::1]:9000/path", "http://[2001:db8::1]:9000/path")]
    public void ToString_ShouldComposeUriFromBaseParts_PreservingPathAndPort(string baseUri, string expectedPrefix)
    {
        var builder = new ClickHouseUriBuilder(new Uri(baseUri));

        var result = builder.ToString();

        // The base part (scheme://host:port/path) must be reproduced verbatim, and the query
        // must be separated by a single '?'. This matches the previous UriBuilder-based output.
        Assert.That(result, Does.StartWith(expectedPrefix + "?"));
    }

    [Test]
    public void ToString_WithNoCustomSettings_ShouldEmitAllParametersWithoutDuplicates()
    {
        // Exercises the fast path (no connection/command query-string parameters): base settings,
        // SQL parameters, JSON modes, max_execution_time and roles must all appear exactly once.
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
            UseCompression = true,
            JsonReadMode = JsonReadMode.String,
            JsonWriteMode = JsonWriteMode.Binary,
            MaxExecutionTime = TimeSpan.FromSeconds(30),
            ConnectionRoles = new[] { "admin", "reader" },
        };
        builder.AddSqlQueryParameter("p1", "v1");
        builder.AddSqlQueryParameter("p2", "v2");

        var uri = builder.ToString();
        var @params = HttpUtility.ParseQueryString(new Uri(uri).Query);

        Assert.Multiple(() =>
        {
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("true"));
            Assert.That(@params.Get("default_format"), Is.EqualTo("RowBinaryWithNamesAndTypes"));
            Assert.That(@params.Get("database"), Is.EqualTo("DATABASE"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("SESSION"));
            Assert.That(@params.Get("query"), Is.EqualTo("SELECT 1"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("QUERY"));
            Assert.That(@params.Get("output_format_binary_write_json_as_string"), Is.EqualTo("1"));
            Assert.That(@params.Get("input_format_binary_read_json_as_string"), Is.EqualTo("0"));
            Assert.That(@params.Get("param_p1"), Is.EqualTo("v1"));
            Assert.That(@params.Get("param_p2"), Is.EqualTo("v2"));
            Assert.That(@params.Get("max_execution_time"), Is.EqualTo("30"));
            Assert.That(@params.GetValues("role"), Is.EqualTo(new[] { "admin", "reader" }));
            // Exactly one '?' separates path and query; no key is emitted twice
            // (ParseQueryString would fold duplicates into a comma-joined value).
            Assert.That(uri.Split('?'), Has.Length.EqualTo(2));
            foreach (var key in @params.AllKeys)
                Assert.That(@params.GetValues(key), Has.Length.EqualTo(key == "role" ? 2 : 1), $"key '{key}' duplicated");
        });
    }

    [Test]
    public void ToString_WithJsonModeNone_ShouldOmitJsonFormatSettings()
    {
        // JsonReadMode/JsonWriteMode of None must skip the format settings entirely
        // (for readonly connections or older servers) on the fast path.
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            JsonReadMode = JsonReadMode.None,
            JsonWriteMode = JsonWriteMode.None,
        };

        var @params = HttpUtility.ParseQueryString(new Uri(builder.ToString()).Query);

        Assert.Multiple(() =>
        {
            Assert.That(@params.AllKeys, Does.Not.Contain("output_format_binary_write_json_as_string"));
            Assert.That(@params.AllKeys, Does.Not.Contain("input_format_binary_read_json_as_string"));
        });
    }

    [Test]
    public void ToString_WithCustomSettingsAndMaxExecutionTime_ShouldEmitBothOnDeduplicationPath()
    {
        // Exercises the slow (deduplication) path together with max_execution_time, ensuring both
        // the custom setting and the max_execution_time key survive.
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            CommandQueryStringParameters = new Dictionary<string, object> { { "max_threads", 4 } },
            MaxExecutionTime = TimeSpan.FromSeconds(45),
        };

        var @params = HttpUtility.ParseQueryString(new Uri(builder.ToString()).Query);

        Assert.Multiple(() =>
        {
            Assert.That(@params.Get("max_threads"), Is.EqualTo("4"));
            Assert.That(@params.Get("max_execution_time"), Is.EqualTo("45"));
        });
    }

    [Test]
    public void ToString_WithEmptyDatabaseAndSession_ShouldOmitThoseParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "",
            SessionId = null,
        };

        var @params = HttpUtility.ParseQueryString(new Uri(builder.ToString()).Query);

        Assert.Multiple(() =>
        {
            Assert.That(@params.AllKeys, Does.Not.Contain("database"));
            Assert.That(@params.AllKeys, Does.Not.Contain("session_id"));
            Assert.That(@params.AllKeys, Does.Not.Contain("query"));
        });
    }

    [Test]
    [TestCase("Çay", "%c3%87ay")]
    public void ShouldEncodeUnicodeCharactersCorrectly(string input, string expected)
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://a.b:123"))
        {
            CommandQueryStringParameters = new Dictionary<string, object>
            {
                { "param_input", input },
            },
        };

        Assert.That(builder.ToString(), Contains.Substring(expected));
    }
    
    [Test]
    public void UriBuilder_ShouldIncludeSingleRole()
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"))
        {
            ConnectionRoles = new[] { "admin" }
        };

        var uri = uriBuilder.ToString();

        Assert.That(uri, Does.Contain("role=admin"));
    }

    [Test]
    public void UriBuilder_ShouldIncludeMultipleRoles()
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"))
        {
            ConnectionRoles = new[] { "admin", "reader" }
        };

        var uri = uriBuilder.ToString();

        Assert.That(uri, Does.Contain("role=admin"));
        Assert.That(uri, Does.Contain("role=reader"));
    }

    [Test]
    [TestCase((string)null)]
    [TestCase("")]
    public void UriBuilder_ShouldGenerateQueryIdWhenNullOrEmpty(string queryId)
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"))
        {
            QueryId = queryId,
        };

        var uri = uriBuilder.ToString();
        var @params = HttpUtility.ParseQueryString(new Uri(uri).Query);

        var generatedQueryId = @params.Get("query_id");
        Assert.That(generatedQueryId, Is.Not.Null.And.Not.Empty);
        Assert.That(Guid.TryParse(generatedQueryId, out _), Is.True, "Auto-generated query_id should be a valid GUID");
    }

    [Test]
    public void UriBuilder_ShouldPreserveProvidedQueryId()
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"))
        {
            QueryId = "my-custom-query-id"
        };

        var uri = uriBuilder.ToString();
        var @params = HttpUtility.ParseQueryString(new Uri(uri).Query);

        Assert.That(@params.Get("query_id"), Is.EqualTo("my-custom-query-id"));
    }

    [Test]
    public void UriBuilder_GetEffectiveQueryId_ShouldReturnSameValueOnRepeatedCalls()
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"));

        var queryId1 = uriBuilder.GetEffectiveQueryId();
        var queryId2 = uriBuilder.GetEffectiveQueryId();

        Assert.That(queryId1, Is.EqualTo(queryId2), "GetEffectiveQueryId should return the same cached value");
        Assert.That(Guid.TryParse(queryId1, out _), Is.True, "Auto-generated query_id should be a valid GUID");
    }

    [Test]
    public void UriBuilder_GetEffectiveQueryId_ShouldReturnProvidedQueryId()
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"))
        {
            QueryId = "my-custom-query-id"
        };

        Assert.That(uriBuilder.GetEffectiveQueryId(), Is.EqualTo("my-custom-query-id"));
    }

    [Test]
    public void UriBuilder_DifferentInstances_ShouldGenerateUniqueQueryIds()
    {
        var uriBuilder1 = new ClickHouseUriBuilder(new Uri("http://localhost:8123"));
        var uriBuilder2 = new ClickHouseUriBuilder(new Uri("http://localhost:8123"));

        var queryId1 = uriBuilder1.GetEffectiveQueryId();
        var queryId2 = uriBuilder2.GetEffectiveQueryId();

        Assert.That(queryId1, Is.Not.EqualTo(queryId2), "Different instances should have different auto-generated query_ids");
    }

    [Test]
    public void UriBuilder_SettingQueryId_ShouldClearCachedEffectiveQueryId()
    {
        var uriBuilder = new ClickHouseUriBuilder(new Uri("http://localhost:8123"));

        // First call generates and caches a GUID
        var autoGenerated = uriBuilder.GetEffectiveQueryId();
        Assert.That(Guid.TryParse(autoGenerated, out _), Is.True);

        // Setting QueryId should clear the cache
        uriBuilder.QueryId = "my-custom-id";

        // Now GetEffectiveQueryId should return the custom ID, not the cached GUID
        Assert.That(uriBuilder.GetEffectiveQueryId(), Is.EqualTo("my-custom-id"));
    }
}
