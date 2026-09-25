using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The per-type half of a POCO mapping: which properties map to which column names, and how a column name coming
/// off the wire finds its property. Built once per POCO type and shared by the query and insert plans, since
/// nothing here depends on the columns a particular query or table happens to have.
///
/// <para>
/// Matching is tiered, most specific first: the exact column name, then ignoring case, then ignoring case and
/// underscores — so a <c>user_id</c> column reaches a <c>UserId</c> property with no attribute needed. A column
/// that reaches more than one property at the tier it lands on is an error rather than a silent pick; a column that
/// reaches none is simply not mapped, which the query path skips and the insert path reports.
/// </para>
/// </summary>
internal abstract class PocoTypeDescriptor
{
    private readonly Dictionary<string, PocoMember> byExactName;
    private readonly Dictionary<string, PocoMember[]> byCaseInsensitiveName;
    private readonly Dictionary<string, PocoMember[]> byUnderscoreFreeName;

    /// <summary>Builds the name lookups over an already-discovered member set.</summary>
    /// <param name="pocoType">The POCO type, for diagnostics.</param>
    /// <param name="members">The mapped members.</param>
    /// <exception cref="InvalidOperationException">Two members map to the same column name.</exception>
    protected PocoTypeDescriptor(Type pocoType, PocoMember[] members)
    {
        PocoType = pocoType;

        // Wrapped rather than handed over: the descriptor is cached and shared across threads, so no consumer
        // should be able to reach the backing array.
        Members = Array.AsReadOnly(members);

        byExactName = new Dictionary<string, PocoMember>(members.Length, StringComparer.Ordinal);
        foreach (PocoMember member in members)
        {
            if (!byExactName.TryAdd(member.ColumnName, member))
            {
                // Unlike the looser tiers, an exact collision can never be resolved by which column name arrives,
                // so it fails the type outright rather than only the columns that would hit it.
                throw new InvalidOperationException(
                    $"Type '{pocoType.Name}' maps both '{byExactName[member.ColumnName].MemberName}' and '{member.MemberName}' to column '{member.ColumnName}'. " +
                    $"Point one of them at a different column with [ClickHouseTcpColumn(Name = \"...\")], or exclude it with [ClickHouseTcpNotMapped].");
            }
        }

        byCaseInsensitiveName = GroupByKey(members, static member => member.ColumnName);
        byUnderscoreFreeName = GroupByKey(members, static member => WithoutUnderscores(member.ColumnName));
    }

    /// <summary>The POCO type this describes.</summary>
    public Type PocoType { get; }

    /// <summary>
    /// The mapped members. Never empty, and in no contracted order — reflection does not promise one, and for an
    /// inherited property it reports the derived type first.
    /// </summary>
    public IReadOnlyList<PocoMember> Members { get; }

    /// <summary>
    /// Names the columns this type maps to, for a plan reporting that a shape and a type do not meet.
    /// </summary>
    /// <returns>The column names, as a phrase.</returns>
    public string DescribeMappedColumns()
    {
        var names = new string[Members.Count];
        for (int i = 0; i < names.Length; i++)
        {
            names[i] = Members[i].ColumnName;
        }

        return $"properties mapping to {string.Join(", ", names)}";
    }

    /// <summary>
    /// Finds the member a column maps to, through the tiers described on this class.
    /// </summary>
    /// <param name="columnName">The column name as the server sent it.</param>
    /// <param name="member">The matched member, or null when the column maps to no member.</param>
    /// <returns>Whether a member matched.</returns>
    /// <exception cref="InvalidOperationException">The column reaches several members at the tier it lands on.</exception>
    public bool TryMatchColumn(string columnName, out PocoMember member)
    {
        if (byExactName.TryGetValue(columnName, out member))
        {
            return true;
        }

        if (TryMatchTier(byCaseInsensitiveName, columnName, columnName, "differing only in case", out member))
        {
            return true;
        }

        return TryMatchTier(byUnderscoreFreeName, WithoutUnderscores(columnName), columnName, "differing only in case and underscores", out member);
    }

    /// <summary>
    /// Reflects over a POCO type and resolves its mapped members: public instance properties, minus indexers and
    /// anything carrying <see cref="ClickHouseTcpNotMappedAttribute"/>, each paired with the column name it matches
    /// on.
    /// </summary>
    /// <param name="pocoType">The POCO type.</param>
    /// <returns>The mapped members.</returns>
    /// <exception cref="InvalidOperationException">Nothing is left to map, a
    /// <see cref="ClickHouseTcpColumnAttribute"/> carries a blank name, or one name is declared by two unrelated
    /// interfaces.</exception>
    protected static PocoMember[] DiscoverMembers(Type pocoType)
    {
        List<PropertyInfo> visible = VisibleProperties(pocoType);

        // Indexers are dropped first because they all report the same name, so they would otherwise collapse into
        // one another in the shadowing pass below.
        var candidates = new List<PropertyInfo>(visible.Count);
        foreach (PropertyInfo property in visible)
        {
            if (property.GetIndexParameters().Length == 0)
            {
                candidates.Add(property);
            }
        }

        List<PropertyInfo> distinct = KeepMostDerived(candidates);

        var members = new List<PocoMember>(distinct.Count);
        foreach (PropertyInfo property in distinct)
        {
            if (property.GetCustomAttribute<ClickHouseTcpNotMappedAttribute>() is not null)
            {
                continue;
            }

            members.Add(new PocoMember(property, ResolveColumnName(pocoType, property)));
        }

        if (members.Count == 0)
        {
            string reason = visible.Count == 0 ? "it has no public instance properties"
                : distinct.Count == 0 ? "its only public instance properties are indexers, which are never mapped"
                : $"every mappable property ({distinct.Count}) carries [ClickHouseTcpNotMapped]";
            throw new InvalidOperationException($"Type '{pocoType.Name}' has no properties to map to ClickHouse columns: {reason}.");
        }

        return members.ToArray();
    }

    // GetProperties walks a class's base types, but an interface has no base type: what an interface inherits sits
    // on the interfaces it extends. An interface is a legal insert source, so collect those too. GetInterfaces
    // returns the whole closure, each interface once, so a diamond yields no duplicates.
    private static List<PropertyInfo> VisibleProperties(Type pocoType)
    {
        const BindingFlags Mapped = BindingFlags.Public | BindingFlags.Instance;
        var properties = new List<PropertyInfo>(pocoType.GetProperties(Mapped));

        if (pocoType.IsInterface)
        {
            foreach (Type inherited in pocoType.GetInterfaces())
            {
                properties.AddRange(inherited.GetProperties(Mapped));
            }
        }

        return properties;
    }

    // A property declared with `new` is reported once per declaring type, so it would look like two properties
    // competing for one column. Keep the most derived declaration, which is the one the compiler binds. Reflection
    // does not contract an order, so neither does this; internal to let that be pinned directly.
    internal static List<PropertyInfo> KeepMostDerived(List<PropertyInfo> properties)
    {
        var kept = new List<PropertyInfo>(properties.Count);
        var ordinalOf = new Dictionary<string, int>(properties.Count, StringComparer.Ordinal);

        foreach (PropertyInfo property in properties)
        {
            if (ordinalOf.TryGetValue(property.Name, out int at))
            {
                PropertyInfo previous = kept[at];
                if (previous.DeclaringType.IsAssignableFrom(property.DeclaringType))
                {
                    kept[at] = property;
                }
                else if (!property.DeclaringType.IsAssignableFrom(previous.DeclaringType))
                {
                    // Only an interface can reach here, by extending two interfaces that each declare the name: a
                    // class hierarchy is a single chain, so one declaration always wins. C# itself needs a cast to
                    // read such a property, so there is no declaration to prefer.
                    throw new InvalidOperationException(
                        $"Property '{property.Name}' is declared by both '{previous.DeclaringType.Name}' and '{property.DeclaringType.Name}', and neither inherits the other, so the column it maps to is undecided. " +
                        $"Map a type that declares the property once.");
                }

                continue;
            }

            ordinalOf[property.Name] = kept.Count;
            kept.Add(property);
        }

        return kept;
    }

    private static string ResolveColumnName(Type pocoType, PropertyInfo property)
    {
        string name = property.GetCustomAttribute<ClickHouseTcpColumnAttribute>()?.Name;
        if (name is null)
        {
            return property.Name;
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException(
                $"Property '{pocoType.Name}.{property.Name}' has a [ClickHouseTcpColumn] name that is empty or whitespace. " +
                $"Give it a column name, or leave Name unset to match on the property name.");
        }

        return name;
    }

    // Groups the members by a derived key, case-insensitively. Buckets hold every member that shares the key, so
    // the lookup can tell "one match" from "ambiguous" and name the candidates either way.
    private static Dictionary<string, PocoMember[]> GroupByKey(PocoMember[] members, Func<PocoMember, string> keyOf)
    {
        var buckets = new Dictionary<string, List<PocoMember>>(members.Length, StringComparer.OrdinalIgnoreCase);
        foreach (PocoMember member in members)
        {
            string key = keyOf(member);
            if (!buckets.TryGetValue(key, out List<PocoMember> bucket))
            {
                buckets[key] = bucket = new List<PocoMember>(1);
            }

            bucket.Add(member);
        }

        var grouped = new Dictionary<string, PocoMember[]>(buckets.Count, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, List<PocoMember>> bucket in buckets)
        {
            grouped[bucket.Key] = bucket.Value.ToArray();
        }

        return grouped;
    }

    private static string WithoutUnderscores(string name)
        => name.IndexOf('_') < 0 ? name : name.Replace("_", string.Empty, StringComparison.Ordinal);

    private bool TryMatchTier(
        Dictionary<string, PocoMember[]> lookup,
        string key,
        string columnName,
        string tier,
        out PocoMember member)
    {
        member = null;
        if (!lookup.TryGetValue(key, out PocoMember[] candidates))
        {
            return false;
        }

        if (candidates.Length > 1)
        {
            // Reached only when no member's column name matched exactly — an exact match would have won the tier
            // above — so the caller has to say which one it means.
            var names = new string[candidates.Length];
            for (int i = 0; i < candidates.Length; i++)
            {
                names[i] = candidates[i].MemberName;
            }

            throw new InvalidOperationException(
                $"Column '{columnName}' matches {candidates.Length} properties of '{PocoType.Name}' {tier} ({string.Join(", ", names)}), and none of them exactly. " +
                $"Put [ClickHouseTcpColumn(Name = \"{columnName}\")] on the one it should map to.");
        }

        member = candidates[0];
        return true;
    }
}

/// <summary>
/// The typed <see cref="PocoTypeDescriptor"/>, adding the one piece of the per-type mapping that needs
/// <typeparamref name="T"/> itself: the compiled constructor a query materializes rows with.
/// </summary>
/// <typeparam name="T">The POCO type.</typeparam>
internal sealed class PocoTypeDescriptor<T> : PocoTypeDescriptor
    where T : class
{
    private readonly Func<T> activator;
    private readonly string activationBlockedReason;

    private PocoTypeDescriptor(PocoMember[] members, Func<T> activator, string activationBlockedReason)
        : base(typeof(T), members)
    {
        this.activator = activator;
        this.activationBlockedReason = activationBlockedReason;
    }

    /// <summary>
    /// Whether <typeparamref name="T"/> can be instantiated, so a query can materialize into it. False for an
    /// insert-only POCO, which needs no constructor of ours.
    /// </summary>
    public bool CanActivate => activator is not null;

    /// <summary>
    /// A compiled <c>new T()</c>, so a query pays one delegate call per row instead of
    /// <see cref="System.Activator"/>'s reflection.
    /// </summary>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be instantiated.</exception>
    public Func<T> Activator => activator ?? throw new InvalidOperationException(
        $"Type '{typeof(T).Name}' cannot be materialized from a query result: {activationBlockedReason}.");

    /// <summary>Discovers <typeparamref name="T"/>'s members and compiles its constructor.</summary>
    /// <returns>The descriptor.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> has nothing to map, or maps two
    /// properties to one column.</exception>
    public static PocoTypeDescriptor<T> Build()
    {
        PocoMember[] members = DiscoverMembers(typeof(T));
        (Func<T> activator, string blockedReason) = CompileActivator();
        return new PocoTypeDescriptor<T>(members, activator, blockedReason);
    }

    // Deliberately not fatal when T cannot be instantiated: the descriptor is shared with the insert path, which
    // never constructs a T, so an immutable POCO has to stay usable there. The reason is kept for the query path
    // to report if it does need one.
    private static (Func<T> Activator, string BlockedReason) CompileActivator()
    {
        Type type = typeof(T);

        // IsAbstract covers interfaces too. An abstract class can report a public parameterless constructor, and
        // Expression.New over it even builds — Compile is what refuses it, and only with "Can't compile a
        // NewExpression with a constructor declared on an abstract class". Checking here turns that into a message
        // that names the type, and keeps the type usable for insert instead of failing its whole descriptor.
        if (type.IsAbstract)
        {
            return (null, type.IsInterface ? "it is an interface" : "it is abstract");
        }

        ConstructorInfo constructor = type.GetConstructor(Type.EmptyTypes);
        return constructor is null
            ? (null, "it has no public parameterless constructor")
            : (Expression.Lambda<Func<T>>(Expression.New(constructor)).Compile(), null);
    }
}
