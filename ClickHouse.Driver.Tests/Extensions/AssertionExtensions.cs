using System;
using System.Globalization;

namespace ClickHouse.Driver.Tests.Extensions;

internal static class AssertionExtensions
{
    private const double DefaultEpsilon = 1e-7;

    public static void AssertFloatingPointEquals(this string actualResult, object expectedValue, double epsilon = DefaultEpsilon)
    {
        switch (expectedValue)
        {
            case float @float:
                float.Parse(actualResult, CultureInfo.InvariantCulture).AssertFloatingPointEquals(@float, (float)epsilon);
                break;
            case double @double:
                double.Parse(actualResult, CultureInfo.InvariantCulture).AssertFloatingPointEquals(@double, epsilon);
                break;
            default:
                var expected = Convert.ToString(expectedValue, CultureInfo.InvariantCulture);
                Assert.That(actualResult, Is.EqualTo(expected));
                break;
        }
    }

    public static void AssertFloatingPointEquals(this double actual, double expected, double epsilon = DefaultEpsilon)
    {
        Assert.That(Math.Abs(actual - expected), Is.LessThan(epsilon),
            $"Expected: {expected}, Actual: {actual}");
    }

    public static void AssertFloatingPointEquals(this float actual, float expected, float epsilon = (float)DefaultEpsilon)
    {
        Assert.That(Math.Abs(actual - expected), Is.LessThan(epsilon),
            $"Expected: {expected}, Actual: {actual}");
    }
}
