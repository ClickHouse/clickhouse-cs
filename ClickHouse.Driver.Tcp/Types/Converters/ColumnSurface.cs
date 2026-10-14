using System;
using System.Reflection;
using System.Text;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// Gives a decoded column as the interface or class that a reader needs, or throws a message that names the
/// column. A reader is derived from the ClickHouse type, so a column of that type that a codec decoded always has
/// the surface. A column that a caller built can have the type name without the decoded shape.
/// </summary>
internal static class ColumnSurface
{
    private static readonly MethodInfo OfDefinition =
        typeof(ColumnSurface).GetMethod(nameof(Of), BindingFlags.Public | BindingFlags.Static);

    /// <summary>Gives <paramref name="column"/> as <typeparamref name="TSurface"/>.</summary>
    /// <typeparam name="TSurface">The column interface or class that the reader needs.</typeparam>
    /// <param name="column">The decoded column.</param>
    /// <returns>The same column, as <typeparamref name="TSurface"/>.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="column"/> is not a <typeparamref name="TSurface"/>.</exception>
    public static TSurface Of<TSurface>(IColumn column)
        where TSurface : class
    {
        ArgumentNullException.ThrowIfNull(column);
        return column as TSurface
            ?? throw new InvalidOperationException(
                $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which does not expose " +
                $"{Describe(typeof(TSurface))}, so a converter cannot reach its values.");
    }

    /// <summary>The method <see cref="Of{TSurface}"/>, closed over <typeparamref name="TSurface"/>.</summary>
    /// <typeparam name="TSurface">The column interface or class that the reader needs.</typeparam>
    /// <returns>The closed method.</returns>
    internal static MethodInfo OfMethod<TSurface>()
        where TSurface : class
        => SurfaceMethod<TSurface>.Instance;

    // Writes a generic type as C# does, for example IColumn<UInt32>, so the message names the surface plainly.
    private static string Describe(Type type)
    {
        if (!type.IsGenericType)
        {
            return type.Name;
        }

        var builder = new StringBuilder(type.Name, 0, type.Name.IndexOf('`'), 32);
        builder.Append('<');
        Type[] arguments = type.GenericTypeArguments;
        for (int i = 0; i < arguments.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(Describe(arguments[i]));
        }

        return builder.Append('>').ToString();
    }

    // The closed method for each surface, made one time.
    private static class SurfaceMethod<TSurface>
        where TSurface : class
    {
        public static readonly MethodInfo Instance = OfDefinition.MakeGenericMethod(typeof(TSurface));
    }
}
