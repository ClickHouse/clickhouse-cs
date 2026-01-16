using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public class DefaultSettingsTests
{
    [Test]
    public void DefaultSettingsShouldMatch()
    {
        var builder = new ClickHouseConnectionStringBuilder();
        Assert.Multiple(() =>
        {
            Assert.That(builder.UseCustomDecimals, Is.EqualTo(true));
            Assert.That(builder.Compression, Is.EqualTo(true));
            Assert.That(builder.UseSession, Is.EqualTo(false));
        });
    }
}
