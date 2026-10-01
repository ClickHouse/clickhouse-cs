namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// The block-info values that precede a block's columns: whether the block holds overflow rows and its
/// two-level aggregation bucket number. This is a plain data holder — its field-id-tagged wire form is read
/// and written by <see cref="BlockReader"/> and <see cref="BlockWriter"/>, keeping all block serialization in
/// one place.
/// </summary>
internal readonly struct BlockInfo
{
    /// <summary>Field id for <c>is_overflows</c> in the tagged wire layout.</summary>
    public const ulong IsOverflowsFieldId = 1;

    /// <summary>Field id for <c>bucket_number</c> in the tagged wire layout.</summary>
    public const ulong BucketNumberFieldId = 2;

    /// <summary>Field id that terminates the tagged field list.</summary>
    public const ulong TerminatorFieldId = 0;

    /// <summary>The standard info for an outgoing block: not an overflow block, no aggregation bucket.</summary>
    public static readonly BlockInfo Default = new(isOverflows: false, bucketNumber: -1);

    /// <summary>Initializes a new instance of the <see cref="BlockInfo"/> struct.</summary>
    /// <param name="isOverflows">Whether the block holds overflow rows from a GROUP BY with limit.</param>
    /// <param name="bucketNumber">The two-level aggregation bucket number, or -1 for none.</param>
    public BlockInfo(bool isOverflows, int bucketNumber)
    {
        IsOverflows = isOverflows;
        BucketNumber = bucketNumber;
    }

    /// <summary>Whether the block holds overflow rows.</summary>
    public bool IsOverflows { get; }

    /// <summary>The two-level aggregation bucket number, or -1 when not bucketed.</summary>
    public int BucketNumber { get; }
}
