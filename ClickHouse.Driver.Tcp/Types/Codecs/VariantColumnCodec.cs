using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The wire constants for the variant serialization-state prefix.</summary>
internal static class VariantWire
{
    /// <summary>
    /// The discriminators-mode value in the state prefix selecting the BASIC layout, where every row's
    /// discriminator is written literally (as opposed to COMPACT run-length granules).
    /// </summary>
    public const ulong BasicDiscriminatorsMode = 0;
}

/// <summary>
/// A codec for the ClickHouse <c>Variant(T1, ..., Tn)</c> column — a discriminated union where each row holds a
/// value of exactly one alternative type, or NULL. The wire layout is columnar: a serialization-state prefix
/// (a <c>UInt64</c> discriminators mode), then one <c>UInt8</c> discriminator per row, then a dense run per
/// alternative type holding the values of the rows that selected it (in row order). NULL is the reserved
/// discriminator <c>255</c> and consumes no value from any run; the alternatives are therefore never themselves
/// <c>Nullable</c>. The server canonicalizes the alternatives (sorted by name) before sending the type string,
/// so the declared order already is the discriminator order — this codec does not reorder it.
///
/// <para>
/// Only the BASIC discriminators mode (every row's discriminator written literally) is supported; the server
/// uses it by default over the native protocol. A COMPACT (run-length) prefix is rejected.
/// </para>
///
/// <para>
/// On the write path a dense <see cref="VariantColumn"/> is serialized straight from its discriminator stream and
/// per-type child columns with no copy. A flat <c>IColumn&lt;object&gt;</c> — a caller's column or what an
/// <c>Array(Variant(...))</c> flattens into — is scattered by each value's runtime CLR type into per-type
/// buffers, which boxes; this is the ergonomic, not the hot, path.
/// </para>
/// </summary>
internal sealed class VariantColumnCodec : IColumnCodec
{
    // The discriminator is a single byte and 255 marks NULL, so at most 255 alternatives (indices 0..254) can be
    // addressed under the BASIC layout.
    private const int MaxTypes = 255;

    private readonly IColumnCodec[] children;
    private readonly Func<string, string, object[], int, IColumn>[] childFlatBuilders;
    private readonly Dictionary<Type, int> discriminatorByClrType;
    private readonly bool allChildrenWritable;

    private VariantColumnCodec(string typeName, IColumnCodec[] children)
    {
        TypeName = typeName;
        this.children = children;

        int typeCount = children.Length;
        childFlatBuilders = new Func<string, string, object[], int, IColumn>[typeCount];
        MethodInfo builderTemplate = typeof(VariantColumnCodec).GetMethod(nameof(BuildFlatColumn), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildFlatColumn)}' was not found.");

        // Map each alternative's writable CLR types to its discriminator, so the ergonomic write path can pick a
        // row's alternative from the runtime type of its value. The canonical element type is registered first;
        // if two alternatives claim the same CLR type the lower discriminator wins (the server does not allow
        // duplicate alternative types, so this is only a defensive tie-break).
        discriminatorByClrType = new Dictionary<Type, int>();
        bool writable = true;
        for (int i = 0; i < typeCount; i++)
        {
            childFlatBuilders[i] = (Func<string, string, object[], int, IColumn>)builderTemplate
                .MakeGenericMethod(children[i].ElementType)
                .CreateDelegate(typeof(Func<string, string, object[], int, IColumn>));

            discriminatorByClrType.TryAdd(children[i].ElementType, i);
            foreach (Type writeType in children[i].WritableElementTypes)
            {
                discriminatorByClrType.TryAdd(writeType, i);
            }

            // Probe writability with an empty child column so a Variant over a non-writable alternative (e.g.
            // Nothing) is rejected up front rather than mid-write.
            IColumn probe = childFlatBuilders[i](string.Empty, children[i].TypeName, Array.Empty<object>(), 0);
            writable &= children[i].CanWrite(probe);
        }

        allChildrenWritable = writable;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(object);

    // A Variant is never nested inside Nullable (the server rejects Nullable(Variant(...))), so this placeholder
    // is a formality the interface requires and is never written.
    /// <inheritdoc/>
    public object NullPlaceholder => null;

    /// <summary>Builds a <c>Variant(...)</c> codec, resolving each alternative's codec through the registry.</summary>
    /// <param name="node">The parsed <c>Variant</c> node; its arguments are the alternative types in discriminator order.</param>
    /// <param name="context">The resolution context, forwarded to each alternative codec's factory.</param>
    /// <param name="registry">The registry used to resolve the alternative codecs.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The variant has no alternatives, or an alternative is <c>Nullable</c>.</exception>
    /// <exception cref="NotSupportedException">The variant has more alternatives than the BASIC layout can address.</exception>
    public static VariantColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count == 0)
        {
            throw new FormatException($"Variant type '{node}' must have at least one alternative type argument.");
        }

        if (node.Arguments.Count > MaxTypes)
        {
            throw new NotSupportedException(
                $"Variant type '{node}' has {node.Arguments.Count} alternatives; the discriminator is one byte, so at most {MaxTypes} are addressable.");
        }

        var childCodecs = new IColumnCodec[node.Arguments.Count];
        for (int i = 0; i < childCodecs.Length; i++)
        {
            TypeNode argument = node.Arguments[i];
            if (string.Equals(argument.Name, "Nullable", StringComparison.Ordinal))
            {
                throw new FormatException(
                    $"Variant alternative '{argument}' must not be Nullable; a Variant carries NULL through its discriminator, not a nullable alternative.");
            }

            childCodecs[i] = registry.ResolveNode(argument, in context);
        }

        return new VariantColumnCodec(node.ToString(), childCodecs);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong mode = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        if (mode != VariantWire.BasicDiscriminatorsMode)
        {
            throw new NotSupportedException(
                $"Variant column '{TypeName}' uses discriminators mode {mode}; this client only supports BASIC (0).");
        }

        foreach (IColumnCodec child in children)
        {
            await child.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            // A zero-row block carries no discriminators and no per-type values; still read each child at zero
            // rows so a codec that expects the call sees it, and surface an empty column.
            var emptyChildren = new IColumn[children.Length];
            int built = 0;
            try
            {
                for (int i = 0; i < children.Length; i++)
                {
                    emptyChildren[i] = await children[i].ReadColumnAsync(reader, columnName, children[i].TypeName, 0, cancellationToken).ConfigureAwait(false);
                    built = i + 1;
                }
            }
            catch
            {
                for (int i = 0; i < built; i++)
                {
                    emptyChildren[i].Dispose();
                }

                throw;
            }

            return new VariantColumn(columnName, columnType, Array.Empty<byte>(), emptyChildren, 0, pooledDiscriminators: false, ownsColumns: true);
        }

        byte[] discriminators = ArrayPool<byte>.Shared.Rent(rowCount);
        var typeColumns = new IColumn[children.Length];
        int read = 0;
        try
        {
            await reader.ReadBytesAsync(discriminators.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);

            var counts = new int[children.Length];
            for (int row = 0; row < rowCount; row++)
            {
                byte d = discriminators[row];
                if (d == VariantColumn.NullDiscriminator)
                {
                    continue;
                }

                if (d >= children.Length)
                {
                    throw new FormatException(
                        $"Variant column '{columnName}' ({columnType}) has discriminator {d} at row {row}, but the type declares only {children.Length} alternative(s).");
                }

                counts[d]++;
            }

            for (int i = 0; i < children.Length; i++)
            {
                typeColumns[i] = await children[i].ReadColumnAsync(reader, columnName, children[i].TypeName, counts[i], cancellationToken).ConfigureAwait(false);
                read = i + 1;
            }

            return new VariantColumn(columnName, columnType, discriminators, typeColumns, rowCount, pooledDiscriminators: true, ownsColumns: true);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(discriminators);
            for (int i = 0; i < read; i++)
            {
                typeColumns[i].Dispose();
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public long MeasureRowBytes(IColumn column, int row)
    {
        // Every row costs its one discriminator byte; a NULL row costs nothing more, and a present row also costs
        // its value in the selected alternative's encoding.
        if (column is IVariantColumn dense && dense.TypeCount == children.Length)
        {
            byte d = dense.Discriminators[row];
            if (d == VariantColumn.NullDiscriminator)
            {
                return 1;
            }

            IColumn typeColumn = dense.GetTypeColumn(d);
            return 1 + children[d].MeasureRowBytes(typeColumn, dense.LocalIndices[row]);
        }

        object value = column.GetValue(row);
        if (value is null)
        {
            return 1;
        }

        int discriminator = DiscriminatorFor(value.GetType());
        var single = new[] { value };
        IColumn probe = childFlatBuilders[discriminator](column.Name, children[discriminator].TypeName, single, 1);
        return 1 + children[discriminator].MeasureRowBytes(probe, 0);
    }

    /// <inheritdoc/>
    // A dense variant column is only writable when its alternatives match this codec's; a bare IColumn<object> is
    // scattered by runtime CLR type. A variant column of a different arity is rejected here rather than silently
    // re-scattered (which could reorder its discriminators).
    public bool CanWrite(IColumn column)
        => allChildrenWritable && (column is IVariantColumn dense ? dense.TypeCount == children.Length : column is IColumn<object>);

    /// <inheritdoc/>
    // The prefix is a fixed mode word followed by the alternatives' own prefixes; every alternative supported
    // today has a data-independent prefix, so the outer column/slice is forwarded unchanged and ignored.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        writer.WriteUInt64(VariantWire.BasicDiscriminatorsMode);
        foreach (IColumnCodec child in children)
        {
            child.WriteStatePrefix(writer, column, start, length);
        }
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is IVariantColumn dense && dense.TypeCount == children.Length)
        {
            WriteDense(writer, dense, start, length);
            return;
        }

        WriteScattered(writer, column, start, length);
    }

    // The dense path: the discriminators and per-type child columns already exist, so write the slice's
    // discriminators verbatim, then each child's slice. A child's slice is the contiguous run of its values whose
    // originating rows fall in [start, start + length) — found by counting that type's discriminators before and
    // within the slice, since values are stored in row order.
    private void WriteDense(ClickHouseBinaryWriter writer, IVariantColumn dense, int start, int length)
    {
        ReadOnlySpan<byte> discriminators = dense.Discriminators;
        writer.WriteBytes(discriminators.Slice(start, length));

        // Each type's values sit contiguously in its child column in row order, so writing this slice needs, per
        // type, the count of its values before the slice (the child-column start offset) and within it (the length
        // to write). Both come from a single pass over the slice: the precomputed LocalIndices give each row its
        // index within its type's child column, so the first in-slice row of a given discriminator already carries
        // that type's before-slice count. This avoids rescanning [0, start) per slice, which would make a
        // multi-block insert quadratic. within[d] == 0 flags the first in-slice occurrence, and an absent type
        // keeps before/within 0 (writing nothing) — so both spans must start zeroed.
        ReadOnlySpan<int> localIndices = dense.LocalIndices;
        Span<int> before = stackalloc int[children.Length];
        Span<int> within = stackalloc int[children.Length];

        // Explicitly zero the stack spans. stackalloc is zeroed today only because the compiler emits `.locals
        // init`; a future `[SkipLocalsInit]` would silently drop that and leave the counters holding garbage, so
        // do not depend on it — the Clear is a cheap memset that keeps this correct regardless.
        before.Clear();
        within.Clear();

        for (int i = start; i < start + length; i++)
        {
            byte d = discriminators[i];
            if (d == VariantColumn.NullDiscriminator)
            {
                continue;
            }

            if (within[d] == 0)
            {
                before[d] = localIndices[i];
            }

            within[d]++;
        }

        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteColumn(writer, dense.GetTypeColumn(i), before[i], within[i]);
        }
    }

    // The ergonomic path: scatter a flat column of boxed values into per-type buffers by each value's runtime CLR
    // type, writing the discriminator stream as it goes, then hand each type's bucket to its child codec. Mirrors
    // the tuple codec's flat write, bucketing by discriminator instead of distributing across parallel columns.
    private void WriteScattered(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        int typeCount = children.Length;
        byte[] discriminators = ArrayPool<byte>.Shared.Rent(length);
        var buckets = new object[typeCount][];
        var filled = new int[typeCount];
        for (int i = 0; i < typeCount; i++)
        {
            buckets[i] = ArrayPool<object>.Shared.Rent(length);
        }

        try
        {
            for (int row = 0; row < length; row++)
            {
                object value = column.GetValue(start + row);
                if (value is null)
                {
                    discriminators[row] = VariantColumn.NullDiscriminator;
                    continue;
                }

                int discriminator = DiscriminatorFor(value.GetType());
                discriminators[row] = (byte)discriminator;
                buckets[discriminator][filled[discriminator]++] = value;
            }

            writer.WriteBytes(discriminators.AsSpan(0, length));

            for (int i = 0; i < typeCount; i++)
            {
                IColumn childColumn = childFlatBuilders[i](column.Name, children[i].TypeName, buckets[i], filled[i]);
                children[i].WriteColumn(writer, childColumn, 0, filled[i]);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(discriminators);
            for (int i = 0; i < typeCount; i++)
            {
                ArrayPool<object>.Shared.Return(buckets[i], clearArray: true);
            }
        }
    }

    // Resolves the discriminator for a value's runtime CLR type, or throws if no alternative accepts it.
    private int DiscriminatorFor(Type clrType)
    {
        if (discriminatorByClrType.TryGetValue(clrType, out int discriminator))
        {
            return discriminator;
        }

        throw new ArgumentException(
            $"Variant '{TypeName}' has no alternative for a value of CLR type '{clrType}'. Supported CLR types: {string.Join(", ", discriminatorByClrType.Keys)}.");
    }

    // Builds a flat typed column from boxed values — the ergonomic write path's per-type projection.
    private static IColumn BuildFlatColumn<T>(string name, string typeName, object[] boxed, int count)
    {
        var values = new T[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (T)boxed[i];
        }

        return new ArrayColumn<T>(name, typeName, values);
    }
}
