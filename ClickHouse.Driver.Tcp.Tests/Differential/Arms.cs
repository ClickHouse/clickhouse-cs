using System;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>Reads rows <c>[start, start + count)</c> of a bound column.</summary>
/// <typeparam name="T">The CLR type to read the values as.</typeparam>
/// <param name="start">The first row to read.</param>
/// <param name="count">The number of rows to read.</param>
/// <returns>The values, one for each row.</returns>
internal delegate T[] RowReader<T>(int start, int count);

/// <summary>Writes rows <c>[start, start + length)</c> of a bound column: <c>BeginWrite</c>, the state prefix and the body.</summary>
/// <param name="writer">The writer to encode into.</param>
/// <param name="start">The first row to write.</param>
/// <param name="length">The number of rows to write.</param>
internal delegate void SliceWriter(ClickHouseBinaryWriter writer, int start, int length);

/// <summary>An implementation that the differential tests run for the facets of one tier.</summary>
internal abstract class Arm
{
    protected Arm(string name, Tier tier)
    {
        Name = name;
        Tier = tier;
    }

    /// <summary>The name of the implementation, unique in a registry. Failure messages use it.</summary>
    public string Name { get; }

    /// <summary>The tier whose facets the implementation runs, and whose reference it is compared with.</summary>
    public Tier Tier { get; }

    /// <summary>Whether the implementation runs a facet of its tier. The default runs every facet.</summary>
    /// <param name="facet">A facet of <see cref="Tier"/>.</param>
    /// <returns>Whether to run the facet.</returns>
    public virtual bool Covers(Facet facet) => true;

    /// <inheritdoc/>
    public override string ToString() => Name;
}

/// <summary>A read implementation, for <see cref="Tier.ReadAs"/> or <see cref="Tier.Poco"/>.</summary>
internal abstract class ReadArm : Arm
{
    protected ReadArm(string name, Tier tier)
        : base(name, tier is Tier.ReadAs or Tier.Poco ? tier : throw new ArgumentOutOfRangeException(nameof(tier), tier, "A read arm runs ReadAs or Poco facets."))
    {
    }

    /// <summary>
    /// Binds the implementation to the only column of <paramref name="block"/>, read as <typeparamref name="T"/>.
    /// To refuse the reading, throw here. To fail on a value, throw from the returned reader.
    /// </summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="block">A block with one column, called <c>value</c>. The block is decoded for this call only.</param>
    /// <returns>A reader of the column's rows.</returns>
    public abstract RowReader<T> Bind<T>(Block block);
}

/// <summary>A write implementation, for <see cref="Tier.Write"/>, <see cref="Tier.PocoWrite"/> or <see cref="Tier.UntypedWrite"/>.</summary>
internal abstract class WriteArm : Arm
{
    protected WriteArm(string name, Tier tier = Tier.Write)
        : base(name, Facet.WritesBytes(tier) ? tier : throw new ArgumentOutOfRangeException(nameof(tier), tier, "A write arm runs Write, PocoWrite or UntypedWrite facets."))
    {
    }

    /// <summary>
    /// Binds the implementation to <paramref name="column"/>, written as <paramref name="columnType"/>. To refuse
    /// the column, throw here. To fail on a value, throw from the returned writer.
    /// </summary>
    /// <typeparam name="T">The CLR element type of the column.</typeparam>
    /// <param name="column">The column to write. It is built for this call only.</param>
    /// <param name="columnType">The ClickHouse type to write the column as.</param>
    /// <param name="context">The context to resolve the type with.</param>
    /// <returns>A writer of the column's rows.</returns>
    public abstract SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context);
}

/// <summary>
/// A yes-or-no implementation, for <see cref="Tier.CanRead"/>, <see cref="Tier.CanWrite"/>, <see cref="Tier.PocoCanWrite"/> or
/// <see cref="Tier.UntypedCanWrite"/>.
/// </summary>
internal abstract class AnswerArm : Arm
{
    protected AnswerArm(string name, Tier tier)
        : base(name, tier is Tier.CanRead or Tier.CanWrite or Tier.PocoCanWrite or Tier.UntypedCanWrite ? tier : throw new ArgumentOutOfRangeException(nameof(tier), tier, "An answer arm runs CanRead or a write answer tier."))
    {
    }

    /// <summary>Answers the question of the tier.</summary>
    /// <param name="columnType">The ClickHouse type.</param>
    /// <param name="elementType">The read target, or the CLR element type of the write input.</param>
    /// <returns>The answer.</returns>
    public abstract bool Answer(string columnType, Type elementType);
}

/// <summary>
/// An arm that runs another arm under a different name, for example to register the reference as a candidate.
/// </summary>
internal static class RenamedArm
{
    public static ReadArm Of(string name, ReadArm arm) => new Read(name, arm);

    public static WriteArm Of(string name, WriteArm arm) => new Write(name, arm);

    public static AnswerArm Of(string name, AnswerArm arm) => new Answering(name, arm);

    private sealed class Read : ReadArm
    {
        private readonly ReadArm arm;

        public Read(string name, ReadArm arm)
            : base(name, arm.Tier) => this.arm = arm;

        public override bool Covers(Facet facet) => arm.Covers(facet);

        public override RowReader<T> Bind<T>(Block block) => arm.Bind<T>(block);
    }

    private sealed class Write : WriteArm
    {
        private readonly WriteArm arm;

        public Write(string name, WriteArm arm)
            : base(name, arm.Tier) => this.arm = arm;

        public override bool Covers(Facet facet) => arm.Covers(facet);

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
            => arm.Bind(column, columnType, context);
    }

    private sealed class Answering : AnswerArm
    {
        private readonly AnswerArm arm;

        public Answering(string name, AnswerArm arm)
            : base(name, arm.Tier) => this.arm = arm;

        public override bool Covers(Facet facet) => arm.Covers(facet);

        public override bool Answer(string columnType, Type elementType) => arm.Answer(columnType, elementType);
    }
}
