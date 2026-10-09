using System;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The placement of a value in a <c>Variant</c> (<see cref="VariantWriter"/>): by the canonical CLR type of an
/// alternative first, as the current codec places it, then by the derivation of each alternative for the value's CLR
/// type. A tie goes to the alternative that claims the value.
/// </summary>
[TestFixture]
public class VariantWriterTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    /// <summary>
    /// A value whose CLR type is the canonical type of an alternative goes there, also when another alternative is
    /// written from that type too: the bytes are those of the current codec.
    /// </summary>
    [TestCase("Variant(String, FixedString(3))", "abc")]
    [TestCase("Variant(Enum8('a' = 1), String)", "a")]
    [TestCase("Variant(DateTime('UTC'), UInt32)", 5u)]
    [TestCase("Variant(Decimal(9, 2), String)", "1.5")]
    public async Task Write_ValueOfTheCanonicalTypeOfAnAlternative_GoesWhereTheCurrentCodecPutsIt(string type, object value)
    {
        object[] values = { value, null, value };
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        Exception current = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteOldAsync(type, values, 0, values.Length));
        if (current is not null)
        {
            // The current codec refuses a canonical collision that no value claims; the writer refuses it the same way.
            Exception actual = await ConverterHarness.CatchAsync(() => ConverterHarness.WriteNewAsync(writer, values, 0, values.Length));
            ConverterHarness.AssertSameFailure(current, actual, type);
            return;
        }

        byte[] expected = await ConverterHarness.WriteOldAsync(type, values, 0, values.Length);
        Assert.That(Convert.ToHexString(await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length)), Is.EqualTo(Convert.ToHexString(expected)));
    }

    /// <summary>
    /// A value whose CLR type is the canonical type of no alternative goes to the alternative that is written from its
    /// type: the bytes are those of the same values given in the canonical type of that alternative.
    /// </summary>
    [Test]
    public async Task Write_ValueThatOneAlternativeIsWrittenFrom_GoesToThatAlternative()
    {
        const string type = "Variant(DateTime('UTC'), String, UInt64)";
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        object[] derived = { new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero), new byte[] { 0x61, 0x62 }, null, 7UL, "x" };
        object[] canonical = { 1718452800u, "ab", null, 7UL, "x" };

        byte[] expected = await ConverterHarness.WriteOldAsync(type, canonical, 0, canonical.Length);
        byte[] actual = await ConverterHarness.WriteNewAsync(writer, derived, 0, derived.Length);

        Assert.That(Convert.ToHexString(actual), Is.EqualTo(Convert.ToHexString(expected)));
    }

    /// <summary>
    /// A leaf alternative writes the values of two CLR types in row order, so <see cref="T:byte[]"/> that UTF-8 cannot
    /// spell stays as it is next to text, and a slice from row 2 starts after earlier values.
    /// </summary>
    [Test]
    public async Task Write_LeafAlternativeFromTwoClrTypes_WritesTheValuesInRowOrder()
    {
        const string type = "Variant(String, UInt64)";
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        object[] values = { "a", new byte[] { 0xFF }, 1UL, "b", new byte[] { 0x63 }, "d" };

        byte[] actual = await ConverterHarness.WriteNewAsync(writer, values, 2, values.Length - 2);

        // Mode 0; the discriminators of rows 2 to 5 (UInt64 = 1, String = 0); the strings "b", "c", "d"; the UInt64 1.
        string expected = "0000000000000000" + "01000000" + "0162" + "0163" + "0164" + "0100000000000000";
        Assert.That(Convert.ToHexString(actual), Is.EqualTo(expected));
    }

    /// <summary>A composite alternative takes the values of one CLR type in one write, and refuses two.</summary>
    [Test]
    public void Write_CompositeAlternativeFromTwoClrTypes_IsRefused()
    {
        const string type = "Variant(Array(String), UInt64)";
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        object[] values = { new[] { "a" }, new[] { new byte[] { 0x62 } } };

        Exception failure = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(writer, values, 0, values.Length).GetAwaiter().GetResult());

        Assert.That(failure, Is.TypeOf<ArgumentException>());
        Assert.That(
            failure.Message,
            Is.EqualTo("Variant 'Variant(Array(String), UInt64)' cannot write the values of its alternative 'Array(String)' from more than one CLR type in one block " +
                "(System.String[] and System.Byte[][]). Give the values of that alternative as one CLR type."));
    }

    /// <summary>
    /// A value that two alternatives are written from, and that neither claims, is refused: it does not say which one
    /// it means.
    /// </summary>
    [Test]
    public void Write_ValueThatTwoAlternativesAreWrittenFrom_IsRefused()
    {
        const string type = "Variant(DateTime('UTC'), DateTime64(3, 'UTC'))";
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        object[] values = { DateTimeOffset.UnixEpoch };

        Exception failure = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(writer, values, 0, values.Length).GetAwaiter().GetResult());

        Assert.That(failure, Is.TypeOf<ArgumentException>());
        Assert.That(
            failure.Message,
            Is.EqualTo("Variant 'Variant(DateTime('UTC'), DateTime64(3, 'UTC'))' cannot place a value of CLR type 'System.DateTimeOffset': the alternatives " +
                "'DateTime('UTC')', 'DateTime64(3, 'UTC')' are all written from that type, and the value does not say which of them is meant."));
    }

    /// <summary>A value that two alternatives are written from goes to the one alternative that claims it.</summary>
    [Test]
    public async Task Write_ValueThatOneOfTwoAlternativesClaims_GoesToThatAlternative()
    {
        // IPv4 and IPv6 both have the canonical type IPAddress, so the address family decides.
        const string type = "Variant(IPv4, IPv6, UInt8)";
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        object[] values = { IPAddress.Parse("::1"), IPAddress.Parse("1.2.3.4"), (byte)7 };

        byte[] expected = await ConverterHarness.WriteOldAsync(type, values, 0, values.Length);
        byte[] actual = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);

        Assert.That(Convert.ToHexString(actual), Is.EqualTo(Convert.ToHexString(expected)));
    }

    /// <summary>
    /// An alternative that no value selects still writes its prefix: a LowCardinality alternative writes its version, as
    /// the current codec does.
    /// </summary>
    [Test]
    public async Task Write_AlternativeThatNoValueSelects_WritesItsPrefix()
    {
        const string type = "Variant(LowCardinality(String), UInt64)";
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);
        object[] values = { 1UL, null, 2UL };

        byte[] expected = await ConverterHarness.WriteOldAsync(type, values, 0, values.Length);
        byte[] actual = await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length);

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(actual), Is.EqualTo(Convert.ToHexString(expected)));
            Assert.That(Convert.ToHexString(actual), Does.StartWith("0000000000000000" + "0100000000000000"), "mode 0, then the LowCardinality version 1");
        });
    }

    /// <summary>
    /// A Variant of many alternatives writes the bytes of the current codec, for all rows and for a slice from row 3: the
    /// values of each alternative stay in row order when the writer groups them.
    /// </summary>
    [Test]
    public async Task Write_WideVariant_GivesTheBytesOfTheCurrentWrite()
    {
        string type = WideVariant(64);
        object[] values = WideValues(97);
        ColumnWriter<object> writer = Derivation.Writer<object>(type, ConverterHarness.Context);

        foreach (int start in new[] { 0, 3 })
        {
            byte[] expected = await ConverterHarness.WriteOldAsync(type, values, start, values.Length - start);
            byte[] actual = await ConverterHarness.WriteNewAsync(writer, values, start, values.Length - start);
            Assert.That(Convert.ToHexString(actual), Is.EqualTo(Convert.ToHexString(expected)), $"rows [{start}, {values.Length})");
        }
    }

    /// <summary>
    /// The scratch of a write holds one entry for each value, whatever the number of alternatives. The test takes the
    /// pooled buffers of the size of the scratch out of the pool first, so each buffer that the write rents is a new
    /// allocation: a Variant of 64 alternatives and 4,000 values then allocates about three buffers of 4,000 entries, not
    /// two for each alternative.
    /// </summary>
    [Test]
    public void Write_WideVariant_AllocatesScratchForTheValuesOnly()
    {
        ColumnWriter<object> writer = Derivation.Writer<object>(WideVariant(64), ConverterHarness.Context);
        object[] values = WideValues(4000);
        using var output = new ClickHouse.Driver.Tcp.Protocol.ClickHouseBinaryWriter(System.IO.Stream.Null);
        ConverterHarness.WriteAll(writer, output, ValueSource<object>.Of(values));

        // More buffers than the shared pool keeps of one size: a partition for each processor, at most 32 buffers each.
        int drain = (64 * Environment.ProcessorCount) + 16;
        var objects = new object[drain][];
        var children = new VariantChild[drain][];
        for (int i = 0; i < drain; i++)
        {
            objects[i] = System.Buffers.ArrayPool<object>.Shared.Rent(values.Length);
            children[i] = System.Buffers.ArrayPool<VariantChild>.Shared.Rent(values.Length);
        }

        long allocated;
        try
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            ConverterHarness.WriteAll(writer, output, ValueSource<object>.Of(values));
            allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        }
        finally
        {
            for (int i = 0; i < drain; i++)
            {
                System.Buffers.ArrayPool<object>.Shared.Return(objects[i], clearArray: true);
                System.Buffers.ArrayPool<VariantChild>.Shared.Return(children[i], clearArray: true);
            }
        }

        // Two buffers of 4,096 references for each of the 64 alternatives would be 4 MiB.
        Assert.That(allocated, Is.LessThan(512 * 1024), "bytes allocated by one write");
    }

    /// <summary>A Variant with an alternative that is written from nothing (<c>Nothing</c>) is refused before a write.</summary>
    [Test]
    public void Derive_VariantWithAnAlternativeThatIsNotWritten_IsRefused()
    {
        Derivation derivation = Derivation.Derive("Variant(Nothing, String)", ConverterHarness.Context, typeof(object), ConversionDirection.Write);

        Assert.That(derivation.Refusal, Is.EqualTo("Values cannot be written to a ClickHouse Nothing column. It is inside the column type 'Variant(Nothing, String)'."));
    }

    /// <summary>
    /// A value that more than one alternative takes, and that none of them claims, is refused, both for alternatives that
    /// share a canonical type and for alternatives that the derivation writes from the value's type.
    /// </summary>
    [Test]
    public void Write_ValueThatNoCandidateClaims_IsRefused()
    {
        ColumnWriter<int> canonicalInt = Derivation.Writer<int>("Int32", ConverterHarness.Context);
        ColumnWriter<long> canonicalLong = Derivation.Writer<long>("Int64", ConverterHarness.Context);

        var colliding = new VariantWriter(
            "Variant(A, B)",
            new IColumnCodec[] { new ClaimingNothing("A", typeof(int)), new ClaimingNothing("B", typeof(int)) },
            new VariantChild[] { new VariantChild<int>(canonicalInt), new VariantChild<int>(canonicalInt) },
            static (_, _) => null);
        var derived = new VariantWriter(
            "Variant(C, D)",
            new IColumnCodec[] { new ClaimingNothing("C", typeof(long)), new ClaimingNothing("D", typeof(long)) },
            new VariantChild[] { new VariantChild<long>(canonicalLong), new VariantChild<long>(canonicalLong) },
            (_, type) => type == typeof(int) ? new VariantChild<int>(canonicalInt) : null);

        Exception collision = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(colliding, new object[] { 1 }, 0, 1).GetAwaiter().GetResult());
        Exception tie = ConverterHarness.Catch(() => ConverterHarness.WriteNewAsync(derived, new object[] { 1 }, 0, 1).GetAwaiter().GetResult());

        Assert.Multiple(() =>
        {
            Assert.That(collision.Message, Is.EqualTo("Variant 'Variant(A, B)' cannot place a value of CLR type 'System.Int32': it surfaces the type of the alternatives 'A', 'B', but matches none of them."));
            Assert.That(tie.Message, Is.EqualTo("Variant 'Variant(C, D)' cannot place a value of CLR type 'System.Int32': the alternatives 'C', 'D' are written from that type, but none of them claims the value."));
        });
    }

    /// <summary>A leaf writer is flat, so two writes of it make one body; a composite writer is not.</summary>
    [TestCase("String", typeof(byte[]), true)]
    [TestCase("FixedString(2)", typeof(string), true)]
    [TestCase("DateTime('UTC')", typeof(DateTimeOffset), true)]
    [TestCase("Array(String)", typeof(string[]), false)]
    [TestCase("LowCardinality(String)", typeof(string), false)]
    [TestCase("Nullable(String)", typeof(string), false)]
    [TestCase("Tuple(Int32, String)", typeof((int, string)), false)]
    [TestCase("QBit(Float32, 2)", typeof(float[]), false)]
    public void IsFlat_OfAWriter_IsTrueForALeafOnly(string type, Type clrType, bool flat)
    {
        var writer = (ColumnWriter)Derivation.Derive(type, ConverterHarness.Context, clrType, ConversionDirection.Write).Converter;

        Assert.That(writer.IsFlat, Is.EqualTo(flat));
    }

    // An alternative that claims no value, so a tie between two of them cannot resolve.
    private sealed class ClaimingNothing : IColumnCodec
    {
        public ClaimingNothing(string typeName, Type elementType)
        {
            TypeName = typeName;
            ElementType = elementType;
        }

        public string TypeName { get; }

        public Type ElementType { get; }

        public object NullPlaceholder => null;

        public bool ClaimsValue(object value) => false;

        public System.Threading.Tasks.ValueTask<IColumn> ReadColumnAsync(ClickHouse.Driver.Tcp.Protocol.ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, System.Threading.CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public bool CanWrite(IColumn column) => false;

        public void WriteColumn(ClickHouse.Driver.Tcp.Protocol.ClickHouseBinaryWriter writer, IColumn column, int start, int length) => throw new NotSupportedException();
    }

    // Variant(UInt8, Array(UInt8), Array(Array(UInt8)), ...): alternatives of distinct canonical CLR types.
    private static string WideVariant(int alternatives)
    {
        var names = new string[alternatives];
        names[0] = "UInt8";
        for (int i = 1; i < alternatives; i++)
        {
            names[i] = $"Array({names[i - 1]})";
        }

        return $"Variant({string.Join(", ", names)})";
    }

    // Values of the first three alternatives, and NULL, in an order that interleaves them.
    private static object[] WideValues(int count)
    {
        var values = new object[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (i % 4) switch
            {
                0 => (byte)i,
                1 => new[] { (byte)i, (byte)(i + 1) },
                2 => new[] { new[] { (byte)i } },
                _ => null,
            };
        }

        return values;
    }
}
