using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Covers <see cref="ConverterDerivation.Derive"/>: the refusals and their messages, the exceptions for a type
/// that does not parse or is not supported, the aliases, and the cache (its key, its identity, and concurrent use).
/// </summary>
[TestFixture]
public class ConverterDerivationTests
{
    private static ConverterDerivation Fresh() => new(ColumnCodecRegistry.Default);

    [Test]
    public void Derive_ReadAsATypeTheLeafDoesNotOffer_RefusesAndNamesTheReadings()
    {
        Derivation derivation = Fresh().Derive("DateTime", ConverterHarness.Context, typeof(int), ConversionDirection.Read);

        Assert.Multiple(() =>
        {
            Assert.That(derivation.Succeeded, Is.False);
            Assert.That(derivation.Converter, Is.Null);
            Assert.That(
                derivation.Refusal,
                Is.EqualTo("'DateTime' cannot be read as System.Int32. It reads as: System.UInt32, System.DateTimeOffset, System.DateTime."));
        });
    }

    [Test]
    public void Derive_WriteFromATypeTheLeafDoesNotTake_RefusesAndNamesTheWriteTypes()
    {
        Derivation derivation = Fresh().Derive("FixedString(4)", ConverterHarness.Context, typeof(Guid), ConversionDirection.Write);

        Assert.That(
            derivation.Refusal,
            Is.EqualTo("'FixedString(4)' cannot be written from System.Guid. It is written from: System.Byte[], System.String."));
    }

    [Test]
    public void Derive_WriteToNothing_RefusesWithTheCodecMessage()
    {
        Derivation derivation = Fresh().Derive("Nothing", ConverterHarness.Context, typeof(object), ConversionDirection.Write);

        Assert.That(derivation.Refusal, Is.EqualTo("Values cannot be written to a ClickHouse Nothing column."));
    }

    /// <summary>The CLR type of <c>Decimal(P, S)</c> follows the precision, and a refusal names the one that applies.</summary>
    [TestCase("Decimal(18, 2)", typeof(ClickHouseTcpDecimal), "System.Decimal")]
    [TestCase("Decimal(19, 2)", typeof(decimal), "ClickHouse.Driver.Tcp.ClickHouseTcpDecimal")]
    [TestCase("Decimal32(2)", typeof(ClickHouseTcpDecimal), "System.Decimal")]
    public void Derive_DecimalAsTheTypeOfTheOtherWidth_RefusesAndNamesTheTypeOfThisWidth(string type, Type clrType, string offered)
    {
        Derivation read = Fresh().Derive(type, ConverterHarness.Context, clrType, ConversionDirection.Read);
        Derivation write = Fresh().Derive(type, ConverterHarness.Context, clrType, ConversionDirection.Write);

        Assert.Multiple(() =>
        {
            Assert.That(read.Refusal, Does.EndWith($"It reads as: {offered}."));
            Assert.That(write.Refusal, Does.EndWith($"It is written from: {offered}."));
        });
    }

    /// <summary>The bare <c>Enum</c> takes its width from its ordinals, and so does its CLR ordinal type.</summary>
    [TestCase("Enum('a' = 1)", typeof(sbyte), typeof(short))]
    [TestCase("Enum('a' = 1000)", typeof(short), typeof(sbyte))]
    public void Derive_BareEnum_OffersTheOrdinalTypeOfItsWidth(string type, Type offered, Type other)
    {
        ConverterDerivation derivation = Fresh();

        Assert.Multiple(() =>
        {
            Assert.That(derivation.Derive(type, ConverterHarness.Context, offered, ConversionDirection.Read).Succeeded, Is.True);
            Assert.That(derivation.Derive(type, ConverterHarness.Context, offered, ConversionDirection.Write).Succeeded, Is.True);
            Assert.That(derivation.Derive(type, ConverterHarness.Context, other, ConversionDirection.Read).Succeeded, Is.False);
            Assert.That(derivation.Derive(type, ConverterHarness.Context, other, ConversionDirection.Write).Succeeded, Is.False);
        });
    }

    /// <summary>A refusal inside a type that wraps a leaf also names the column type, as a codec resolution does.</summary>
    [Test]
    public void Derive_RefusalInsideAnAggregateFunctionValue_NamesTheColumnType()
    {
        const string type = "SimpleAggregateFunction(sum, UInt64)";
        Derivation refused = Fresh().Derive(type, ConverterHarness.Context, typeof(string), ConversionDirection.Read);
        Derivation derived = Fresh().Derive(type, ConverterHarness.Context, typeof(ulong), ConversionDirection.Write);

        Assert.Multiple(() =>
        {
            Assert.That(
                refused.Refusal,
                Is.EqualTo($"'UInt64' cannot be read as System.String. It reads as: System.UInt64. It is inside the column type '{type}'."));
            Assert.That(derived.Converter, Is.InstanceOf<ColumnWriter<ulong>>());
        });
    }

    /// <summary>A composite has no converter yet, so it is refused, not thrown.</summary>
    [TestCase("Nullable(Int32)")]
    [TestCase("Array(String)")]
    [TestCase("LowCardinality(String)")]
    [TestCase("Tuple(Int32, String)")]
    [TestCase("Map(String, Int32)")]
    [TestCase("Variant(Int32, String)")]
    [TestCase("Point")]
    public void Derive_Composite_RefusesWithNoConverter(string type)
    {
        Derivation derivation = Fresh().Derive(type, ConverterHarness.Context, typeof(object), ConversionDirection.Read);

        Assert.That(derivation.Refusal, Is.EqualTo($"'{type}' has no converter for System.Object."));
    }

    /// <summary>A type that does not parse, or that the client does not support, throws as a codec resolution does.</summary>
    [TestCase("Array(", typeof(FormatException))]
    [TestCase("FixedString(0)", typeof(FormatException))]
    [TestCase("DateTime64(10)", typeof(FormatException))]
    [TestCase("NoSuchType", typeof(NotSupportedException))]
    [TestCase("Array(NoSuchType)", typeof(NotSupportedException))]
    [TestCase("AggregateFunction(uniq, UInt64)", typeof(NotSupportedException))]
    public void Derive_TypeThatDoesNotResolve_ThrowsAsTheRegistryDoes(string type, Type exceptionType)
    {
        Exception expected = ConverterHarness.Catch(() => ColumnCodecRegistry.Default.Resolve(type, ConverterHarness.Context));
        Exception actual = ConverterHarness.Catch(() => Fresh().Derive(type, ConverterHarness.Context, typeof(object), ConversionDirection.Read));

        Assert.Multiple(() =>
        {
            Assert.That(expected, Is.InstanceOf(exceptionType));
            Assert.That(actual?.GetType(), Is.EqualTo(expected.GetType()));
            Assert.That(actual?.Message, Is.EqualTo(expected.Message));
        });
    }

    [Test]
    public void Derive_NullArguments_Throw()
    {
        ConverterDerivation derivation = Fresh();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => derivation.Derive(null, ConverterHarness.Context, typeof(int), ConversionDirection.Read));
            Assert.Throws<ArgumentNullException>(() => derivation.Derive("Int32", ConverterHarness.Context, null, ConversionDirection.Read));
        });
    }

    /// <summary>An alias or another spelling of a leaf finds the same leaf as the registry does.</summary>
    [TestCase("boolean", typeof(bool))]
    [TestCase("TINYINT", typeof(sbyte))]
    [TestCase("int", typeof(int))]
    [TestCase("DateTime32", typeof(DateTimeOffset))]
    [TestCase("TIMESTAMP('UTC')", typeof(DateTime))]
    [TestCase("BINARY(3)", typeof(string))]
    [TestCase("BLOB", typeof(byte[]))]
    [TestCase("INET6", typeof(IPAddress))]
    [TestCase("NUMERIC(10, 2)", typeof(decimal))]
    public void Derive_AliasOfALeaf_DerivesTheLeaf(string type, Type clrType)
    {
        Derivation derivation = Fresh().Derive(type, ConverterHarness.Context, clrType, ConversionDirection.Read);

        Assert.That(derivation.Converter, Is.InstanceOf<ColumnReader>().And.Property(nameof(ColumnReader.ValueType)).EqualTo(clrType));
    }

    [Test]
    public void ReaderAndWriter_Refused_ThrowInvalidCastWithTheRefusal()
    {
        ConverterDerivation derivation = Fresh();

        Assert.Multiple(() =>
        {
            var read = Assert.Throws<InvalidCastException>(() => derivation.Reader<Guid>("String", ConverterHarness.Context));
            Assert.That(read.Message, Is.EqualTo(derivation.Derive("String", ConverterHarness.Context, typeof(Guid), ConversionDirection.Read).Refusal));

            var write = Assert.Throws<InvalidCastException>(() => derivation.Writer<Guid>("String", ConverterHarness.Context));
            Assert.That(write.Message, Is.EqualTo(derivation.Derive("String", ConverterHarness.Context, typeof(Guid), ConversionDirection.Write).Refusal));
        });
    }

    [Test]
    public void Derive_SameKey_GivesTheCachedTree()
    {
        ConverterDerivation derivation = Fresh();
        Derivation first = derivation.Derive("DateTime", ConverterHarness.Context, typeof(DateTimeOffset), ConversionDirection.Read);
        Derivation refused = derivation.Derive("DateTime", ConverterHarness.Context, typeof(Guid), ConversionDirection.Read);

        Assert.Multiple(() =>
        {
            Assert.That(derivation.Derive("DateTime", ConverterHarness.Context, typeof(DateTimeOffset), ConversionDirection.Read), Is.SameAs(first));
            Assert.That(derivation.Derive("DateTime", ConverterHarness.Context, typeof(Guid), ConversionDirection.Read), Is.SameAs(refused));
        });
    }

    /// <summary>
    /// The session timezone is part of the key: a <c>DateTime</c> with no timezone reads in it, so two sessions get
    /// two trees, and each tree reads in its own session.
    /// </summary>
    [Test]
    public async Task Derive_DifferentSessionTimezones_DeriveSeparateTrees()
    {
        ConverterDerivation derivation = Fresh();
        var tokyo = new ResolveContext { ServerTimezone = "Asia/Tokyo" };
        var lima = new ResolveContext { ServerTimezone = "America/Lima" };
        ColumnReader<DateTimeOffset> inTokyo = derivation.Reader<DateTimeOffset>("DateTime", tokyo);
        ColumnReader<DateTimeOffset> inLima = derivation.Reader<DateTimeOffset>("DateTime", lima);
        using IColumn column = await ConverterHarness.DecodeAsync("DateTime", new ArrayColumn<uint>("c", "DateTime", new uint[] { 0 }));

        Assert.Multiple(() =>
        {
            Assert.That(inTokyo, Is.Not.SameAs(inLima));
            Assert.That(ConverterHarness.ReadFill(inTokyo, column, 0, 1)[0].Offset, Is.EqualTo(TimeSpan.FromHours(9)));
            Assert.That(ConverterHarness.ReadFill(inLima, column, 0, 1)[0].Offset, Is.EqualTo(TimeSpan.FromHours(-5)));
        });
    }

    [Test]
    public void Derive_ReadAndWrite_AreCachedSeparately()
    {
        ConverterDerivation derivation = Fresh();
        object reader = derivation.Derive("Int32", ConverterHarness.Context, typeof(int), ConversionDirection.Read).Converter;
        object writer = derivation.Derive("Int32", ConverterHarness.Context, typeof(int), ConversionDirection.Write).Converter;

        Assert.Multiple(() =>
        {
            Assert.That(reader, Is.InstanceOf<ColumnReader<int>>());
            Assert.That(writer, Is.InstanceOf<ColumnWriter<int>>());
        });
    }

    /// <summary>
    /// Concurrent callers on one key all get the same tree, and the tree reads correctly on every thread: a tree holds
    /// no per-column state.
    /// </summary>
    [Test]
    public async Task Derive_ConcurrentCallers_ShareOneTreeThatEveryThreadCanUse()
    {
        ConverterDerivation derivation = Fresh();
        using IColumn column = await ConverterHarness.DecodeAsync("DateTime('UTC')", new ArrayColumn<uint>("c", "DateTime('UTC')", new uint[] { 1, 2, 3 }));
        DateTimeOffset[] expected = { DateTimeOffset.FromUnixTimeSeconds(1), DateTimeOffset.FromUnixTimeSeconds(2), DateTimeOffset.FromUnixTimeSeconds(3) };

        var trees = new object[64];
        var reads = new DateTimeOffset[64][];
        Parallel.For(0, trees.Length, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i =>
        {
            Derivation derived = derivation.Derive("DateTime('UTC')", ConverterHarness.Context, typeof(DateTimeOffset), ConversionDirection.Read);
            trees[i] = derived.Converter;
            reads[i] = ConverterHarness.ReadFill((ColumnReader<DateTimeOffset>)derived.Converter, column, 0, 3);
        });

        Assert.Multiple(() =>
        {
            Assert.That(trees.Distinct().Count(), Is.EqualTo(1));
            Assert.That(reads, Has.All.EqualTo(expected));
        });
    }

    /// <summary>The cache has a limit. Past it, a derivation is still correct, and it is not kept.</summary>
    [Test]
    public void Derive_PastTheCacheLimit_StillDerives()
    {
        ConverterDerivation derivation = Fresh();
        for (int i = 0; i < 1100; i++)
        {
            derivation.Derive($"FixedString({i + 1})", ConverterHarness.Context, typeof(byte[]), ConversionDirection.Read);
        }

        Derivation late = derivation.Derive("FixedString(5000)", ConverterHarness.Context, typeof(string), ConversionDirection.Read);
        Derivation again = derivation.Derive("FixedString(5000)", ConverterHarness.Context, typeof(string), ConversionDirection.Read);

        Assert.Multiple(() =>
        {
            Assert.That(late.Succeeded, Is.True);
            Assert.That(again.Succeeded, Is.True);
            Assert.That(again, Is.Not.SameAs(late), "past the limit a derivation is not cached");
        });
    }

    [Test]
    public void OfAndRefused_Null_Throw()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => Derivation.Of(null));
            Assert.Throws<ArgumentNullException>(() => Derivation.Refused(null));
        });
    }
}
