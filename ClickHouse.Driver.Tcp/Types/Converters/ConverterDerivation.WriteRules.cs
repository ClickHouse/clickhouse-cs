using System;
using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The write rules of D6.</summary>
internal sealed partial class ConverterDerivation
{
    /// <summary>
    /// The write rules of D6 (<see cref="WriteRules"/>), for a root whose own writes refused <paramref name="clrType"/>.
    /// Each rule writes the values as another CLR type W through the root's own write of W. They are the rules of POCO
    /// mapping: for <c>T?</c>, the nullable ordinal of an enum (null stays null), then <c>T</c> or the ordinal of an enum,
    /// which throws at the first NULL, then a cast that keeps the value (null stays null); for any other type, the
    /// ordinal of an enum, then a cast, then the nullable type of a value type or of its ordinal.
    /// </summary>
    /// <param name="type">The column type, as the caller gave it, for the message of a NULL.</param>
    /// <param name="root">The parsed column type.</param>
    /// <param name="context">The resolution context.</param>
    /// <param name="clrType">The CLR type of the values.</param>
    /// <param name="refused">The refusal of the column type's own writes, which this gives when no rule applies.</param>
    /// <returns>The writer of a rule, or <paramref name="refused"/>.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveByWriteRules(string type, TypeNode root, in ResolveContext context, Type clrType, Derivation refused)
    {
        if (!IsValueTypeArgument(clrType))
        {
            return refused;
        }

        Type value = Nullable.GetUnderlyingType(clrType);
        if (value is not null)
        {
            Type ordinal = WriteRules.OrdinalOf(value);
            if (ordinal is not null && TryDeriveWrite(root, in context, typeof(Nullable<>).MakeGenericType(ordinal), out object nullableOrdinals))
            {
                return Derivation.Of(Activator.CreateInstance(typeof(NullableEnumWriter<,>).MakeGenericType(value, ordinal), nullableOrdinals));
            }

            if (TryDeriveWrite(root, in context, value, out object values))
            {
                return Derivation.Of(NonNull(value, values, type));
            }

            if (ordinal is not null && TryDeriveWrite(root, in context, ordinal, out object ordinals))
            {
                return Derivation.Of(NonNull(value, EnumOrdinals(value, ordinal, ordinals), type));
            }

            return TryDeriveCast(root, in context, clrType, value, out object cast) ? Derivation.Of(cast) : refused;
        }

        Type enumOrdinal = WriteRules.OrdinalOf(clrType);
        if (enumOrdinal is not null && TryDeriveWrite(root, in context, enumOrdinal, out object enumOrdinals))
        {
            return Derivation.Of(EnumOrdinals(clrType, enumOrdinal, enumOrdinals));
        }

        if (TryDeriveCast(root, in context, clrType, clrType, out object assigned))
        {
            return Derivation.Of(assigned);
        }

        if (!clrType.IsValueType)
        {
            return refused;
        }

        if (TryDeriveWrite(root, in context, typeof(Nullable<>).MakeGenericType(clrType), out object lifted))
        {
            return Derivation.Of(Lift(clrType, lifted));
        }

        if (enumOrdinal is not null && TryDeriveWrite(root, in context, typeof(Nullable<>).MakeGenericType(enumOrdinal), out object liftedOrdinals))
        {
            return Derivation.Of(EnumOrdinals(clrType, enumOrdinal, Lift(enumOrdinal, liftedOrdinals)));
        }

        return refused;
    }

    // A cast of the values (source) to a type that the root is written from: the first of WriteRules.CastTargets of the
    // value type (castFrom, which is source or the value type under it).
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private bool TryDeriveCast(TypeNode root, in ResolveContext context, Type source, Type castFrom, out object writer)
    {
        foreach (Type target in WriteRules.CastTargets(castFrom))
        {
            if (TryDeriveWrite(root, in context, target, out object inner))
            {
                writer = Activator.CreateInstance(typeof(AssignWriter<,>).MakeGenericType(source, target), inner);
                return true;
            }
        }

        writer = null;
        return false;
    }

    // The root's own write of a CLR type, with no rule.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private bool TryDeriveWrite(TypeNode root, in ResolveContext context, Type clrType, out object writer)
    {
        Derivation derived = DeriveNode(root, root, in context, clrType, ConversionDirection.Write);
        writer = derived.Converter;
        return derived.Succeeded;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static object EnumOrdinals(Type enumType, Type ordinal, object ordinals)
        => Activator.CreateInstance(typeof(EnumWriter<,>).MakeGenericType(enumType, ordinal), ordinals);

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static object Lift(Type value, object nullables)
        => Activator.CreateInstance(typeof(LiftWriter<>).MakeGenericType(value), nullables);

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static object NonNull(Type value, object values, string type)
        => Activator.CreateInstance(typeof(NonNullWriter<>).MakeGenericType(value), values, type);
}
