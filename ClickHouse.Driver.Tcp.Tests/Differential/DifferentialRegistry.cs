using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Adds implementations under test, and deliberate changes, to the differential tests. The tests find every
/// non-abstract class of this assembly that implements this interface, create it with its parameterless
/// constructor, and call <see cref="Register"/> once, in the order of the class's full name.
/// </summary>
internal interface IDifferentialRegistration
{
    /// <summary>Adds arms and deliberate changes to <paramref name="registry"/>.</summary>
    /// <param name="registry">The registry of the test run.</param>
    void Register(DifferentialRegistry registry);
}

/// <summary>
/// The implementations that the differential tests run: the reference arm of each tier (the old path), the
/// candidate arms, and the deliberate changes that the candidates make.
/// </summary>
internal sealed class DifferentialRegistry
{
    private static readonly Lazy<DifferentialRegistry> CurrentRegistry = new(Discover);

    private readonly Dictionary<Tier, Arm> references = new();
    private readonly List<Registration<Arm>> candidates = new();
    private readonly List<DeliberateChange> changes = new();

    /// <summary>
    /// The registry of the test run: <see cref="ReferenceArms"/> as the reference, and every
    /// <see cref="IDifferentialRegistration"/> of this assembly.
    /// </summary>
    public static DifferentialRegistry Current => CurrentRegistry.Value;

    /// <summary>The candidate arms, in the order they were added.</summary>
    public IReadOnlyList<Arm> Candidates => candidates.Select(c => c.Item).ToList();

    /// <summary>The deliberate changes, in the order they were declared.</summary>
    public IReadOnlyList<DeliberateChange> Changes => changes;

    /// <summary>A registry with <see cref="ReferenceArms"/> as the reference of every tier, and no candidates.</summary>
    /// <returns>The registry.</returns>
    public static DifferentialRegistry WithReference()
    {
        var registry = new DifferentialRegistry();
        registry.SetReference(ReferenceArms.ReadAs);
        registry.SetReference(ReferenceArms.Poco);
        registry.SetReference(ReferenceArms.CanRead);
        registry.SetReference(ReferenceArms.Write);
        registry.SetReference(ReferenceArms.CanWrite);
        return registry;
    }

    /// <summary>The reference arm of a tier, or null when the tier has none.</summary>
    /// <param name="tier">The tier.</param>
    /// <returns>The arm.</returns>
    public Arm Reference(Tier tier) => references.TryGetValue(tier, out Arm arm) ? arm : null;

    /// <summary>Makes <paramref name="arm"/> the reference of its tier, in place of the one before.</summary>
    /// <param name="arm">The arm.</param>
    public void SetReference(Arm arm) => references[arm.Tier] = arm;

    /// <summary>Removes the reference of a tier. The tier's candidates are then compared with its first candidate.</summary>
    /// <param name="tier">The tier.</param>
    public void RemoveReference(Tier tier) => references.Remove(tier);

    /// <summary>Adds a candidate arm.</summary>
    /// <param name="arm">The arm. Its name must be unique in the registry.</param>
    /// <param name="expectedFacets">The number of facets of the case list that the arm covers. The tests check it.</param>
    public void Add(Arm arm, int expectedFacets) => AddCandidate(arm, expectedFacets);

    /// <summary>Adds a candidate arm that covers every facet of its tier.</summary>
    /// <param name="arm">The arm. Its name must be unique in the registry.</param>
    public void AddForEveryFacet(Arm arm) => AddCandidate(arm, expectedFacets: null);

    /// <summary>Declares that the candidates give a different outcome from the reference for one read facet.</summary>
    /// <param name="caseId">The <see cref="DifferentialCase.Id"/> of the case.</param>
    /// <param name="tier"><see cref="Tier.ReadAs"/>, <see cref="Tier.Poco"/> or <see cref="Tier.CanRead"/>.</param>
    /// <param name="target">The read target of the facet.</param>
    /// <param name="expected">The outcome that every candidate that covers the facet must give.</param>
    /// <param name="reason">Why the outcome changes, for example the decision or the issue.</param>
    public void DeclareChange(string caseId, Tier tier, Type target, Expectation expected, string reason)
    {
        if (tier is not (Tier.ReadAs or Tier.Poco or Tier.CanRead))
        {
            throw new ArgumentOutOfRangeException(nameof(tier), tier, "A facet with a read target is a ReadAs, Poco or CanRead facet.");
        }

        DeclareChanges(
            $"{caseId} {tier}<{TypeNames.Of(target)}>",
            facet => facet.Case.Id == caseId && facet.Tier == tier && facet.Target == target,
            _ => expected,
            reason,
            expectedFacets: 1);
    }

    /// <summary>Declares that the candidates give a different outcome from the reference for one write facet.</summary>
    /// <param name="caseId">The <see cref="DifferentialCase.Id"/> of the case.</param>
    /// <param name="tier"><see cref="Tier.Write"/> or <see cref="Tier.CanWrite"/>.</param>
    /// <param name="inputLabel">The <see cref="WriteInput.Label"/> of the facet.</param>
    /// <param name="expected">The outcome that every candidate that covers the facet must give.</param>
    /// <param name="reason">Why the outcome changes, for example the decision or the issue.</param>
    public void DeclareChange(string caseId, Tier tier, string inputLabel, Expectation expected, string reason)
    {
        if (tier is not (Tier.Write or Tier.CanWrite))
        {
            throw new ArgumentOutOfRangeException(nameof(tier), tier, "A facet with a write input is a Write or CanWrite facet.");
        }

        DeclareChanges(
            $"{caseId} {tier}[{inputLabel}]",
            facet => facet.Case.Id == caseId && facet.Tier == tier && facet.Input.Label == inputLabel,
            _ => expected,
            reason,
            expectedFacets: 1);
    }

    /// <summary>Declares that the candidates give a different outcome from the reference for a family of facets.</summary>
    /// <param name="name">A unique name for the declaration, for failure messages.</param>
    /// <param name="facets">Selects the facets of the family.</param>
    /// <param name="expected">The outcome that every candidate that covers a facet of the family must give.</param>
    /// <param name="reason">Why the outcomes change, for example the decision or the issue.</param>
    /// <param name="expectedFacets">The number of facets of the case list in the family. The tests check it.</param>
    public void DeclareChanges(string name, Func<Facet, bool> facets, Func<Facet, Expectation> expected, string reason, int expectedFacets)
    {
        if (changes.Any(change => change.Name == name))
        {
            throw new ArgumentException($"A deliberate change called '{name}' is already declared.", nameof(name));
        }

        changes.Add(new DeliberateChange(name, facets, expected, reason, expectedFacets));
    }

    /// <summary>The candidates of the facet's tier that cover the facet.</summary>
    /// <param name="facet">The facet.</param>
    /// <returns>The arms, in the order they were added.</returns>
    public IEnumerable<Arm> CandidatesFor(Facet facet)
        => candidates.Select(c => c.Item).Where(arm => arm.Tier == facet.Tier && arm.Covers(facet));

    /// <summary>The deliberate change of a facet, or null when the facet has none.</summary>
    /// <param name="facet">The facet.</param>
    /// <returns>The change.</returns>
    /// <exception cref="InvalidOperationException">More than one change selects the facet.</exception>
    public DeliberateChange ChangeFor(Facet facet)
    {
        DeliberateChange found = null;
        foreach (DeliberateChange change in changes)
        {
            if (!change.Selects(facet))
            {
                continue;
            }

            if (found is not null)
            {
                throw new InvalidOperationException($"{facet}: the deliberate changes '{found.Name}' and '{change.Name}' both select this facet.");
            }

            found = change;
        }

        return found;
    }

    /// <summary>
    /// Checks the registry against a case list: unique arm names, the facet count of each arm and of each
    /// deliberate change, and a candidate for each facet with a deliberate change.
    /// </summary>
    /// <param name="cases">The case list.</param>
    /// <returns>One message for each problem. Empty when there is none.</returns>
    public List<string> Validate(IEnumerable<DifferentialCase> cases)
    {
        var problems = new List<string>();
        List<Facet> facets = cases.SelectMany(c => c.Facets()).ToList();

        foreach (IGrouping<string, Registration<Arm>> duplicate in candidates.GroupBy(c => c.Item.Name).Where(g => g.Count() > 1))
        {
            problems.Add($"{duplicate.Count()} candidate arms are called '{duplicate.Key}'.");
        }

        foreach (Registration<Arm> candidate in candidates)
        {
            Arm arm = candidate.Item;
            List<Facet> ofTier = facets.Where(f => f.Tier == arm.Tier).ToList();
            int covered = ofTier.Count(arm.Covers);
            int expected = candidate.ExpectedFacets ?? ofTier.Count;
            if (covered != expected)
            {
                problems.Add($"The arm '{arm.Name}' covers {covered} {arm.Tier} facets, not {expected}.");
            }
        }

        foreach (DeliberateChange change in changes)
        {
            List<Facet> selected = facets.Where(change.Selects).ToList();
            if (selected.Count != change.ExpectedFacets)
            {
                problems.Add($"The deliberate change '{change.Name}' selects {selected.Count} facets, not {change.ExpectedFacets}.");
            }

            foreach (Facet facet in selected.Where(f => !CandidatesFor(f).Any()))
            {
                problems.Add($"{facet}: the deliberate change '{change.Name}' selects this facet, but no candidate arm covers it.");
            }
        }

        foreach (Facet facet in facets)
        {
            try
            {
                ChangeFor(facet);
            }
            catch (InvalidOperationException e)
            {
                problems.Add(e.Message);
            }
        }

        return problems;
    }

    private static DifferentialRegistry Discover()
    {
        DifferentialRegistry registry = WithReference();
        IEnumerable<Type> registrations = typeof(DifferentialRegistry).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && !t.IsInterface && typeof(IDifferentialRegistration).IsAssignableFrom(t))
            .OrderBy(t => t.FullName, StringComparer.Ordinal);

        foreach (Type type in registrations)
        {
            var registration = (IDifferentialRegistration)Activator.CreateInstance(type, nonPublic: true);
            registration.Register(registry);
        }

        return registry;
    }

    private void AddCandidate(Arm arm, int? expectedFacets)
    {
        ArgumentNullException.ThrowIfNull(arm);
        if (expectedFacets is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedFacets), expectedFacets, "A facet count is zero or more.");
        }

        candidates.Add(new Registration<Arm>(arm, expectedFacets));
    }

    private sealed record Registration<T>(T Item, int? ExpectedFacets);
}

/// <summary>A declaration that the candidates give a different outcome from the reference for some facets.</summary>
internal sealed class DeliberateChange
{
    private readonly Func<Facet, bool> selects;
    private readonly Func<Facet, Expectation> expected;

    public DeliberateChange(string name, Func<Facet, bool> selects, Func<Facet, Expectation> expected, string reason, int expectedFacets)
    {
        Name = name;
        this.selects = selects;
        this.expected = expected;
        Reason = reason;
        ExpectedFacets = expectedFacets;
    }

    public string Name { get; }

    public string Reason { get; }

    public int ExpectedFacets { get; }

    public bool Selects(Facet facet) => selects(facet);

    public Expectation ExpectationFor(Facet facet) => expected(facet);
}
