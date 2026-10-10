using System;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Declares the writes that the write rules of D6 (<see cref="WriteRules"/>) change in the columnar tier: a column whose
/// values are the reading of a D6 read rule (<see cref="ReadConverterRegistration.D6Changes"/>) is written back through
/// the matching write rule. <c>InsertAsync</c> writes the values that a column read as <c>T?</c> gives into the column
/// type that cannot hold NULL, and <c>ClickHouseTcpTypes.CanWrite</c> says true for each of them. The arms that run these
/// facets are the client's (<see cref="ClientArms.Write"/>, <see cref="ClientArms.CanWrite"/>) and the converter arms
/// of <see cref="LeafConverterRegistration"/> and <see cref="WriteConverterRegistration"/>.
/// </summary>
internal sealed class WriteRulesRegistration : IDifferentialRegistration
{
    private const string D6Reason =
        "D6: one set of write rules for every write tier. InsertAsync and CanWrite take the CLR types that InsertRowsAsync<T> takes.";

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        foreach ((string caseId, Type target, bool failsAtNull) in ReadConverterRegistration.D6Changes)
        {
            string input = $"read back as {TypeNames.Of(target)}";

            // A reading that fails at a NULL has no values to write back, so only the answer changes.
            if (!failsAtNull)
            {
                registry.DeclareChange(caseId, Tier.Write, input, Expectation.SameAsWrite("canonical"), D6Reason);
            }

            registry.DeclareChange(caseId, Tier.CanWrite, input, Expectation.Answer(true), D6Reason);
        }
    }
}
