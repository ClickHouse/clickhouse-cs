using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>Builds typed key strategies for LowCardinality dictionary writes.</summary>
internal static class LowCardinalityKeys
{
    /// <summary>Uses the source value itself as the dictionary key.</summary>
    public static object Identity<T>() => IdentityCache<T>.Instance;

    /// <summary>Projects each source value once to the key used for dictionary lookup.</summary>
    public static object Projected<TSource, TKey>(Func<TSource, TKey> project)
        => new ProjectedKeyWriter<TSource, TKey>(project);

    /// <summary>Uses byte content rather than array identity as the dictionary key.</summary>
    public static object Bytes()
        => Projected<byte[], ByteArrayKey>(static value => new ByteArrayKey(value));

    private static class IdentityCache<T>
    {
        public static readonly object Instance = new IdentityKeyWriter<T>();
    }

    private sealed class IdentityKeyWriter<T> : ILowCardinalityKeyWriter<T>
    {
        public void Write(
            IColumnCodec inner,
            Protocol.ClickHouseBinaryWriter writer,
            IColumn<T> values,
            T placeholder,
            IColumn source,
            ILowCardinalityNullMap nullMap,
            int start,
            int length)
            => LowCardinalityValueWriter.Write<T, T, IdentityKeySelector<T>>(
                inner, default, writer, values, placeholder, source, nullMap, start, length);
    }

    private sealed class ProjectedKeyWriter<TSource, TKey> : ILowCardinalityKeyWriter<TSource>
    {
        private readonly Func<TSource, TKey> project;

        public ProjectedKeyWriter(Func<TSource, TKey> project) => this.project = project;

        public void Write(
            IColumnCodec inner,
            Protocol.ClickHouseBinaryWriter writer,
            IColumn<TSource> values,
            TSource placeholder,
            IColumn source,
            ILowCardinalityNullMap nullMap,
            int start,
            int length)
            => LowCardinalityValueWriter.Write<TSource, TKey, ProjectedKeySelector<TSource, TKey>>(
                inner, new ProjectedKeySelector<TSource, TKey>(project), writer, values, placeholder, source, nullMap, start, length);
    }

    private readonly struct IdentityKeySelector<T> : ILowCardinalityKeySelector<T, T>
    {
        public T Select(T value) => value;
    }

    private readonly struct ProjectedKeySelector<TSource, TKey> : ILowCardinalityKeySelector<TSource, TKey>
    {
        private readonly Func<TSource, TKey> project;

        public ProjectedKeySelector(Func<TSource, TKey> project) => this.project = project;

        public TKey Select(TSource value) => project(value);
    }

    private readonly struct ByteArrayKey : IEquatable<ByteArrayKey>
    {
        private readonly byte[] bytes;

        public ByteArrayKey(byte[] bytes) => this.bytes = bytes;

        public bool Equals(ByteArrayKey other)
            => ReferenceEquals(bytes, other.bytes)
                || (bytes is not null && other.bytes is not null && bytes.AsSpan().SequenceEqual(other.bytes));

        public override bool Equals(object obj) => obj is ByteArrayKey other && Equals(other);

        public override int GetHashCode()
        {
            if (bytes is null)
            {
                return 0;
            }

            var hash = default(HashCode);
            hash.AddBytes(bytes);
            return hash.ToHashCode();
        }
    }
}
