using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Builds the comparers codecs return from <see cref="IColumnCodec.WireEqualityComparer"/>: equal exactly when
/// the codec encodes to the same bytes.
/// </summary>
internal static class WireEquality
{
    /// <summary>CLR equality, for a type whose equality already agrees with its encoding.</summary>
    /// <typeparam name="T">The write type.</typeparam>
    /// <returns>The default comparer.</returns>
    public static object Default<T>() => EqualityComparer<T>.Default;

    /// <summary>
    /// Equality on what <paramref name="project"/> returns. Pass the same conversion the write path uses, so the
    /// two cannot drift: the comparer then agrees with the encoding by construction.
    /// </summary>
    /// <typeparam name="TSource">The write type.</typeparam>
    /// <typeparam name="TKey">The converted form the codec encodes.</typeparam>
    /// <param name="project">The write path's conversion.</param>
    /// <returns>The comparer.</returns>
    public static object Projected<TSource, TKey>(Func<TSource, TKey> project) => new ProjectedComparer<TSource, TKey>(project);

    /// <summary>Equality on byte content, for a codec that writes a caller's array verbatim.</summary>
    /// <returns>The comparer.</returns>
    public static object Bytes() => ByteContentComparer.Instance;

    private sealed class ProjectedComparer<TSource, TKey> : IEqualityComparer<TSource>
    {
        private readonly Func<TSource, TKey> project;

        public ProjectedComparer(Func<TSource, TKey> project) => this.project = project;

        public bool Equals(TSource x, TSource y) => EqualityComparer<TKey>.Default.Equals(project(x), project(y));

        public int GetHashCode(TSource value) => EqualityComparer<TKey>.Default.GetHashCode(project(value));
    }

    private sealed class ByteContentComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteContentComparer Instance = new();

        public bool Equals(byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] value)
        {
            var hash = default(HashCode);
            hash.AddBytes(value);
            return hash.ToHashCode();
        }
    }
}
