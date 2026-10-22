using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Adds implementations under test, and declared outcomes, to the differential tests. The tests find every
/// non-abstract class of this assembly that implements this interface, create it with its parameterless
/// constructor, and call <see cref="Register"/> once, in the order of the class's full name.
/// </summary>
internal interface IDifferentialRegistration
{
    /// <summary>Adds arms and declared outcomes to <paramref name="registry"/>.</summary>
    /// <param name="registry">The registry of the test run.</param>
    void Register(DifferentialRegistry registry);
}

/// <summary>
/// The implementations that the differential tests run, and the declared outcomes. The first candidate of a tier that
/// covers a facet is its baseline: every other candidate must give the baseline's outcome, unless an outcome is declared
/// for the facet.
/// </summary>
internal sealed class DifferentialRegistry
{
    private static readonly Lazy<DifferentialRegistry> CurrentRegistry = new(Discover);

    private readonly List<Registration<Arm>> candidates = new();
    private readonly List<DeclaredOutcome> declared = new();

    /// <summary>
    /// The registry of the test run: the client's entry points (<see cref="WithClientArms"/>), then the arms of every
    /// <see cref="IDifferentialRegistration"/> of this assembly.
    /// </summary>
    public static DifferentialRegistry Current => CurrentRegistry.Value;

    /// <summary>The candidate arms, in the order they were added.</summary>
    public IReadOnlyList<Arm> Candidates => candidates.Select(c => c.Item).ToList();

    /// <summary>The declared outcomes, in the order they were declared.</summary>
    public IReadOnlyList<DeclaredOutcome> Declared => declared;

    /// <summary>
    /// A registry whose candidates are the client's entry point of each tier (<see cref="ClientArms"/>), for every facet
    /// of its tier. They are the first candidates, so they are the baseline of every facet.
    /// </summary>
    /// <returns>The registry.</returns>
    public static DifferentialRegistry WithClientArms()
    {
        var registry = new DifferentialRegistry();
        registry.AddForEveryFacet(ClientArms.ReadAs);
        registry.AddForEveryFacet(ClientArms.Poco);
        registry.AddForEveryFacet(ClientArms.CanRead);
        registry.AddForEveryFacet(ClientArms.Write);
        registry.AddForEveryFacet(ClientArms.CanWrite);
        registry.AddForEveryFacet(ClientArms.PocoWrite);
        registry.AddForEveryFacet(ClientArms.PocoCanWrite);
        registry.AddForEveryFacet(ClientArms.UntypedWrite);
        registry.AddForEveryFacet(ClientArms.UntypedCanWrite);
        return registry;
    }

    /// <summary>Adds a candidate arm.</summary>
    /// <param name="arm">The arm. Its name must be unique in the registry.</param>
    /// <param name="expectedFacets">The number of facets of the case list that the arm covers. The tests check it.</param>
    public void Add(Arm arm, int expectedFacets) => AddCandidate(arm, expectedFacets);

    /// <summary>Adds a candidate arm that covers every facet of its tier.</summary>
    /// <param name="arm">The arm. Its name must be unique in the registry.</param>
    public void AddForEveryFacet(Arm arm) => AddCandidate(arm, expectedFacets: null);

    /// <summary>
    /// Declares the outcome that every candidate gives for a family of facets whose candidates give different outcomes
    /// that are all right, so that no candidate is a baseline for the others. An example is the tail of a read that
    /// fails at the first NULL: a view of the whole column fails at the first NULL of the column, and a reader of the
    /// tail alone fails at the first NULL of the tail.
    /// </summary>
    /// <param name="name">A unique name for the declaration, for failure messages.</param>
    /// <param name="facets">Selects the facets of the family.</param>
    /// <param name="expected">The outcome that every candidate that covers a facet of the family must give.</param>
    /// <param name="reason">Why the candidates differ.</param>
    /// <param name="expectedFacets">The number of facets of the case list in the family. The tests check it.</param>
    public void DeclareOutcomes(string name, Func<Facet, bool> facets, Func<Facet, Expectation> expected, string reason, int expectedFacets)
    {
        if (declared.Any(outcome => outcome.Name == name))
        {
            throw new ArgumentException($"An outcome called '{name}' is already declared.", nameof(name));
        }

        declared.Add(new DeclaredOutcome(name, facets, expected, reason, expectedFacets));
    }

    /// <summary>The candidates of the facet's tier that cover the facet.</summary>
    /// <param name="facet">The facet.</param>
    /// <returns>The arms, in the order they were added.</returns>
    public IEnumerable<Arm> CandidatesFor(Facet facet)
        => candidates.Select(c => c.Item).Where(arm => arm.Tier == facet.Tier && arm.Covers(facet));

    /// <summary>The declared outcome of a facet, or null when the facet has none.</summary>
    /// <param name="facet">The facet.</param>
    /// <returns>The declared outcome.</returns>
    /// <exception cref="InvalidOperationException">More than one declared outcome selects the facet.</exception>
    public DeclaredOutcome DeclaredFor(Facet facet)
    {
        DeclaredOutcome found = null;
        foreach (DeclaredOutcome outcome in declared)
        {
            if (!outcome.Selects(facet))
            {
                continue;
            }

            if (found is not null)
            {
                throw new InvalidOperationException($"{facet}: the declared outcomes '{found.Name}' and '{outcome.Name}' both select this facet.");
            }

            found = outcome;
        }

        return found;
    }

    /// <summary>
    /// Checks the registry against a case list: unique arm names, the facet count of each arm and of each declared
    /// outcome, and a candidate for each facet with a declared outcome.
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

        foreach (DeclaredOutcome outcome in declared)
        {
            List<Facet> selected = facets.Where(outcome.Selects).ToList();
            if (selected.Count != outcome.ExpectedFacets)
            {
                problems.Add($"The declared outcome '{outcome.Name}' selects {selected.Count} facets, not {outcome.ExpectedFacets}.");
            }

            foreach (Facet facet in selected.Where(f => !CandidatesFor(f).Any()))
            {
                problems.Add($"{facet}: the declared outcome '{outcome.Name}' selects this facet, but no candidate arm covers it.");
            }
        }

        foreach (Facet facet in facets)
        {
            try
            {
                DeclaredFor(facet);
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
        DifferentialRegistry registry = WithClientArms();
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

/// <summary>An outcome that every candidate gives for some facets, in place of a comparison with the baseline.</summary>
internal sealed class DeclaredOutcome
{
    private readonly Func<Facet, bool> selects;
    private readonly Func<Facet, Expectation> expected;

    public DeclaredOutcome(string name, Func<Facet, bool> selects, Func<Facet, Expectation> expected, string reason, int expectedFacets)
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
