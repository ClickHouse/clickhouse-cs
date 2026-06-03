using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class ApplicationInfoTests : AbstractConnectionTestFixture
{
    private static string UserAgentString(ClickHouseClient c)
    {
        var headers = new HttpRequestMessage().Headers;
        c.AddDefaultHttpHeaders(headers);
        return headers.UserAgent.ToString();
    }

    [Test]
    public void AddDefaultHttpHeaders_WithoutApplicationInfo_EmitsSystemOnlyToken()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        using var c = new ClickHouseClient(settings);

        var headers = new HttpRequestMessage().Headers;
        c.AddDefaultHttpHeaders(headers);
        var ua = headers.UserAgent.ToString();

        Assert.That(headers.UserAgent.Count, Is.EqualTo(2));
        Assert.That(ua, Does.Not.Contain("()"));
        // System tags must still appear in the merged comment token even when ApplicationInfo is empty.
        Assert.That(ua, Does.Contain("platform:"));
        Assert.That(ua, Does.Contain("os:"));
        Assert.That(ua, Does.Contain("arch:"));
    }

    [Test]
    public void AddDefaultHttpHeaders_WithApplicationInfo_AppendsCommentToken()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["app"] = "TestApp",
                ["ver"] = "1.0",
            },
        };
        using var c = new ClickHouseClient(settings);

        var ua = UserAgentString(c);

        Assert.That(ua, Does.Contain("app:TestApp; ver:1.0)"));
        Assert.That(
            ua.IndexOf("ClickHouse.Driver/", StringComparison.Ordinal),
            Is.LessThan(ua.IndexOf("app:TestApp", StringComparison.Ordinal)));
    }

    [Test]
    public void ApplicationInfo_ValuesAreSanitized()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["raw"] = "a(b)c\\d;e,f:g\th i",
            },
        };
        using var c = new ClickHouseClient(settings);

        var ua = UserAgentString(c);

        Assert.That(ua, Does.Contain("raw:a|b|c|d|e|f|g|h i"));
        Assert.That(ua, Does.Not.Contain("a(b)"));
    }

    [Test]
    public void ApplicationInfo_EmptyValueEntriesAreSkipped()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["app"] = "Real",
                ["empty"] = "",
            },
        };
        using var c = new ClickHouseClient(settings);

        var ua = UserAgentString(c);

        Assert.That(ua, Does.Contain("app:Real"));
        Assert.That(ua, Does.Not.Contain("empty:"));
    }

    [Test]
    public void ApplicationInfo_AllEmptyValues_EmitsSystemOnlyToken()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["a"] = "",
                ["b"] = "",
            },
        };
        using var c = new ClickHouseClient(settings);

        var headers = new HttpRequestMessage().Headers;
        c.AddDefaultHttpHeaders(headers);
        var ua = headers.UserAgent.ToString();

        Assert.That(headers.UserAgent.Count, Is.EqualTo(2));
        Assert.That(ua, Does.Contain("platform:"));
        Assert.That(ua, Does.Not.Contain("a:"));
        Assert.That(ua, Does.Not.Contain("b:"));
    }

    [Test]
    public void ApplicationInfo_InvalidKey_ThrowsOnClientConstruction()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["bad key"] = "value",
            },
        };

        var ex = Assert.Throws<ArgumentException>(() => new ClickHouseClient(settings));
        Assert.That(ex.Message, Does.Contain("User-Agent tag key"));
    }

    [Test]
    public void ApplicationInfo_EmptyKey_ThrowsOnClientConstruction()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                [""] = "value",
            },
        };

        Assert.Throws<ArgumentException>(() => new ClickHouseClient(settings));
    }

    [Test]
    public void ApplicationInfo_NullDictionary_DoesNotThrow()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = null,
        };

        Assert.DoesNotThrow(() => new ClickHouseClient(settings));

        using var c = new ClickHouseClient(settings);
        var headers = new HttpRequestMessage().Headers;
        c.AddDefaultHttpHeaders(headers);
        Assert.That(headers.UserAgent.Count, Is.EqualTo(2));
    }

    [Test]
    public void ApplicationInfo_CopyConstructor_WithNullSource_ProducesEmptyDict()
    {
        var original = new ClickHouseClientSettings { ApplicationInfo = null };

        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.ApplicationInfo, Is.Not.Null);
        Assert.That(copy.ApplicationInfo, Is.Empty);
    }

    [Test]
    public void ApplicationInfo_OverlongKey_ThrowsOnClientConstruction()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                [new string('a', 33)] = "value",
            },
        };

        Assert.Throws<ArgumentException>(() => new ClickHouseClient(settings));
    }

    [Test]
    public void ApplicationInfo_NonAsciiValue_IsUrlEncoded()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["app"] = "caf\u00e9", // "café"
            },
        };
        using var c = new ClickHouseClient(settings);

        var ua = UserAgentString(c);

        Assert.That(ua, Does.Contain("app:caf%C3%A9"));
    }

    [Test]
    public void ApplicationInfo_ControlCharsInValue_AreReplacedWithPipe()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["ctl"] = "a\nb\rc\u0001d\u007Fe",
            },
        };
        using var c = new ClickHouseClient(settings);

        var ua = UserAgentString(c);

        Assert.That(ua, Does.Contain("ctl:a|b|c|d|e"));
    }

    [Test]
    public void ApplicationInfo_ValueLongerThan256Chars_IsTruncated()
    {
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var longValue = new string('x', 500);
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["app"] = longValue,
            },
        };
        using var c = new ClickHouseClient(settings);

        var ua = UserAgentString(c);

        var start = ua.IndexOf("app:", StringComparison.Ordinal);
        Assert.That(start, Is.GreaterThanOrEqualTo(0));
        var end = ua.IndexOf(')', start);
        var emittedValue = ua.Substring(start + "app:".Length, end - (start + "app:".Length));
        Assert.That(emittedValue.Length, Is.EqualTo(256));
        Assert.That(emittedValue, Is.EqualTo(new string('x', 256)));
    }

    [Test]
    public async Task ApplicationInfo_AppearsInSystemQueryLog()
    {
        var marker = $"appinfo_{Guid.NewGuid():N}";
        var baseSettings = TestUtilities.GetTestClickHouseClientSettings();
        var settings = new ClickHouseClientSettings(baseSettings)
        {
            ApplicationInfo = new Dictionary<string, string>
            {
                ["app"] = marker,
                ["ver"] = "1.0",
            },
        };
        using var tagged = new ClickHouseClient(settings);

        await tagged.ExecuteScalarAsync($"SELECT 1 /* {marker} */");

        // SYSTEM FLUSH LOGS is synchronous and forces the query_log buffer to disk,
        // so the row is visible immediately afterwards — no polling/sleep needed.
        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

        object scalar = await client.ExecuteScalarAsync(
            $"SELECT http_user_agent FROM system.query_log " +
            $"WHERE query LIKE '%{marker}%' AND type = 'QueryFinish' " +
            $"ORDER BY event_time DESC LIMIT 1");

        Assert.That(scalar, Is.Not.Null, "Expected a query_log row for the marker but found none.");
        Assert.That(scalar, Is.Not.EqualTo(DBNull.Value), "Expected a non-null http_user_agent.");
        var ua = (string)scalar;
        Assert.That(ua, Does.Contain($"app:{marker}"));
        Assert.That(ua, Does.Contain("ver:1.0"));
    }
}
