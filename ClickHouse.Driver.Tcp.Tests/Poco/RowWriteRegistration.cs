using System.Linq;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Runs the client's row inserts in the differential tests: the POCO write plan of <c>InsertRowsAsync&lt;T&gt;</c> in both
/// gather tiers and the untyped rows of <c>InsertRowsAsync(object[])</c>, for every write facet, against the old row
/// inserts (<see cref="LegacyRowWrite"/>), and their answers. <c>ClickHouseTcpTypes.CanWrite</c> runs in the answer
/// tiers of both, so it gives the answer of the row inserts. It also declares the writes that the row inserts take now
/// and the old ones refused.
/// </summary>
internal sealed class RowWriteRegistration : IDifferentialRegistration
{
    /// <summary>
    /// The writes of the converter layer (decision D7) that reach the row inserts with the converter derivation: (case,
    /// input, reason). Both row inserts write the bytes of the case's source column, and both answers are true.
    /// </summary>
    internal static readonly (string CaseId, string Input, string Reason)[] D7Changes =
        WriteConverterRegistration.D7Changes
            .Append(("ColumnReadProjection: FixedString(4)", "read back as string", "FixedString(N) is written from string: UTF-8, zero bytes up to N (decision D7)."))
            .ToArray();

    /// <summary>
    /// The writes that the POCO write plan takes through the nullable rules of D6 for a CLR type that the column type is
    /// written from but that is not one of the codec's preferred write types. The old plan applied its rules only to the
    /// preferred types. Each value read back fails at a NULL, so only the answer changes.
    /// </summary>
    internal static readonly (string CaseId, string Input)[] PocoNullableRuleChanges =
    {
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", "read back as DateTime"),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", "read back as DateTimeOffset"),
    };

    /// <summary>
    /// The untyped values that the old untyped insert refused because their CLR type is not one of the codec's preferred
    /// write types (ClickHouse/integrations#800), and that the converter derivation writes: (case, input, whether the
    /// input has values to write). The insert writes the bytes of the columnar insert of the same values.
    /// </summary>
    internal static readonly (string CaseId, string Input, bool Writes)[] Issue800Changes =
    {
        ("ColumnReadProjection: Array(Array(String))", "read back as byte[][][]", true),
        ("ColumnReadProjection: Array(DateTime('UTC'))", "read back as DateTime[]", true),
        ("ColumnReadProjection: Array(String)", "read back as byte[][]", true),
        ("ColumnReadProjection: LowCardinality(DateTime('UTC'))", "read back as DateTime", true),
        ("ColumnReadProjection: LowCardinality(DateTime('UTC'))", "read back as DateTimeOffset", true),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", "read back as DateTime", false),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", "read back as DateTime?", true),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", "read back as DateTimeOffset", false),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", "read back as DateTimeOffset?", true),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime))", "read back as DateTime?", true),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime))", "read back as DateTimeOffset?", true),
        ("ColumnReadProjection: Map(String, DateTime('UTC'))", "read back as KeyValuePair<string, DateTime>[]", true),
        ("ColumnReadProjection: Map(String, String)", "read back as KeyValuePair<byte[], byte[]>[]", true),
        ("ColumnReadProjection: Map(UInt8, String)", "read back as KeyValuePair<byte, byte[]>[]", true),
        ("ColumnReadProjection: Tuple(DateTime('UTC'), Time)", "read back as (DateTime, TimeSpan)", true),
        ("ColumnReadProjection: Tuple(String)", "read back as ValueTuple<byte[]>", true),
        ("ColumnReadProjection: Tuple(UInt8, String)", "read back as (byte, byte[])", true),
        ("ColumnReadScenario: LowCardinality(DateTime('Europe/Berlin')) as DateTimeOffset", "read back as DateTimeOffset", true),
        ("ColumnReadScenario: LowCardinality(Nullable(DateTime('UTC'))) as DateTimeOffset?", "read back as DateTimeOffset?", true),
        ("CompositeLiftMatrix: Array(Array(Array(DateTime64(3, 'UTC'))))", "read back as DateTime[][][]", true),
        ("CompositeLiftMatrix: Array(Array(DateTime('UTC')))", "read back as DateTime[][]", true),
        ("CompositeLiftMatrix: Array(Array(Nullable(Time64(3))))", "read back as TimeSpan?[][]", true),
        ("CompositeLiftMatrix: Array(Array(Tuple(DateTime('UTC'), String)))", "read back as (DateTime, string)[][]", true),
        ("CompositeLiftMatrix: Array(DateTime('UTC'))", "read back as DateTime[]", true),
        ("CompositeLiftMatrix: Array(LowCardinality(DateTime('UTC')))", "read back as DateTime[]", true),
        ("CompositeLiftMatrix: Array(Map(String, DateTime('UTC')))", "read back as KeyValuePair<string, DateTime>[][]", true),
        ("CompositeLiftMatrix: Array(Nullable(DateTime('UTC')))", "read back as DateTime?[]", true),
        ("CompositeLiftMatrix: Array(Time64(3))", "read back as TimeSpan[]", true),
        ("CompositeLiftMatrix: Array(Tuple(DateTime('UTC'), String))", "read back as (DateTime, string)[]", true),
        ("CompositeLiftMatrix: Map(DateTime('UTC'), Time64(3))", "read back as KeyValuePair<DateTime, TimeSpan>[]", true),
        ("CompositeLiftMatrix: Map(String, Array(DateTime('UTC')))", "read back as KeyValuePair<string, DateTime[]>[]", true),
        ("CompositeLiftMatrix: Map(String, DateTime('UTC'))", "read back as KeyValuePair<string, DateTime>[]", true),
        ("CompositeLiftMatrix: Map(String, LowCardinality(Nullable(DateTime('UTC'))))", "read back as KeyValuePair<string, DateTime?>[]", true),
        ("CompositeLiftMatrix: Map(String, Map(String, DateTime('UTC')))", "read back as KeyValuePair<string, KeyValuePair<string, DateTime>[]>[]", true),
        ("CompositeLiftMatrix: Map(Tuple(DateTime('UTC'), String), Array(Time))", "read back as KeyValuePair<(DateTime, string), TimeSpan[]>[]", true),
        ("CompositeLiftMatrix: Tuple(Array(Array(Time)), Time64(6))", "read back as (TimeSpan[][], TimeSpan)", true),
        ("CompositeLiftMatrix: Tuple(Array(DateTime('UTC')), String)", "read back as (DateTime[], string)", true),
        ("CompositeLiftMatrix: Tuple(DateTime('UTC'), DateTime64(3, 'UTC'), Time)", "read back as (DateTime, long, TimeSpan)", true),
        ("CompositeLiftMatrix: Tuple(DateTime('UTC'), DateTime64(3, 'UTC'), Time, Time64(3), Int32, String, Nullable(DateTime('UTC')))", "read back as (DateTime, DateTime, TimeSpan, TimeSpan, int, string, DateTime?)", true),
        ("CompositeLiftMatrix: Tuple(DateTime('UTC'), String)", "read back as (DateTime, string)", true),
        ("CompositeLiftMatrix: Tuple(Nullable(DateTime('UTC')), String)", "read back as (DateTime?, string)", true),
        ("InsertRoundTrip: Array(Enum8('a' = -1, 'b' = 127)) from labels [2 rows]", "insert", true),
    };

    private const string NullableRuleReason =
        "D6: a value type is written into a type that is written from its nullable type, for every CLR type that the type is written from, not only for the codec's preferred write types.";

    private const string Issue800Reason =
        "ClickHouse/integrations#800: an untyped column is written from every CLR type that the column type is written from, not only from the codec's preferred write types.";

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.AddForEveryFacet(ClientArms.PocoWrite);
        registry.AddForEveryFacet(new RowWriteArms.ClientPocoArm("Client.PocoWrite: Delegate", PocoGatherTier.Delegate));
        registry.AddForEveryFacet(ClientArms.PocoCanWrite);
        registry.AddForEveryFacet(new ClientArms.FunctionAnswerArm("Client.CanWrite: PocoCanWrite", Tier.PocoCanWrite, ClickHouseTcpTypes.CanWrite));
        registry.AddForEveryFacet(ClientArms.UntypedWrite);
        registry.AddForEveryFacet(ClientArms.UntypedCanWrite);
        registry.AddForEveryFacet(new ClientArms.FunctionAnswerArm("Client.CanWrite: UntypedCanWrite", Tier.UntypedCanWrite, ClickHouseTcpTypes.CanWrite));

        foreach ((string caseId, string input, string reason) in D7Changes)
        {
            foreach ((Tier write, Tier answer) in new[] { (Tier.PocoWrite, Tier.PocoCanWrite), (Tier.UntypedWrite, Tier.UntypedCanWrite) })
            {
                registry.DeclareChange(caseId, write, input, Expectation.SameAsWrite("canonical"), reason);
                registry.DeclareChange(caseId, answer, input, Expectation.Answer(true), reason);
            }
        }

        foreach ((string caseId, string input) in PocoNullableRuleChanges)
        {
            registry.DeclareChange(caseId, Tier.PocoCanWrite, input, Expectation.Answer(true), NullableRuleReason);
        }

        foreach ((string caseId, string input, bool writes) in Issue800Changes)
        {
            if (writes)
            {
                // The values of a built input are the column itself; a read-back input holds the values of the canonical one.
                string same = input.StartsWith("read back as ", System.StringComparison.Ordinal) ? "canonical" : input;
                registry.DeclareChange(caseId, Tier.UntypedWrite, input, Expectation.SameAsWrite(same), Issue800Reason);
            }

            registry.DeclareChange(caseId, Tier.UntypedCanWrite, input, Expectation.Answer(true), Issue800Reason);
        }
    }
}
