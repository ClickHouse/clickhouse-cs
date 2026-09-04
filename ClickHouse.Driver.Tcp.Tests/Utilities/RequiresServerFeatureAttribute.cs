using System;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Skips a test when the server under test predates the feature it needs, instead of letting it fail.
/// </summary>
/// <remarks>
/// Use this on whole tests. For one <c>TestCaseSource</c> case, guard its <c>yield return</c> with
/// <see cref="TcpServerFeatures.Has(TcpFeature)"/>.
/// </remarks>
/// <param name="feature">The feature the test needs.</param>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RequiresServerFeatureAttribute(TcpFeature feature) : NUnitAttribute, IApplyToTest
{
    /// <summary>Marks the test as ignored when the server does not have the feature.</summary>
    /// <param name="test">The test being built.</param>
    public void ApplyToTest(Test test)
    {
        if (test.RunState == RunState.NotRunnable || TcpServerFeatures.Has(feature))
        {
            return;
        }

        test.RunState = RunState.Ignored;
        test.Properties.Set(
            PropertyNames.SkipReason,
            $"Needs server feature {feature}; {TcpServerFeatures.Version?.ToString() ?? "the server"} predates it.");
    }
}
