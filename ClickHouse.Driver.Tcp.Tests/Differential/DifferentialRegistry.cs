using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Adds implementations under test to the differential tests. The tests find every
/// non-abstract class of this assembly that implements this interface, create it with its parameterless
/// constructor, and call <see cref="Register"/> once, in the order of the class's full name.
/// </summary>
internal interface IDifferentialRegistration
{
    /// <summary>Adds arms to <paramref name="registry"/>.</summary>
    /// <param name="registry">The registry of the test run.</param>
    void Register(DifferentialRegistry registry);
}

/// <summary>
/// The implementations that the differential tests run. The first candidate of a tier that covers a facet is its
/// baseline: every other candidate must give the baseline's outcome.
/// </summary>
internal sealed class DifferentialRegistry
{
    private static readonly Lazy<DifferentialRegistry> CurrentRegistry = new(Discover);

    private readonly List<Registration<Arm>> candidates = new();

    /// <summary>
    /// The registry of the test run: the client's entry points (<see cref="WithClientArms"/>), then the arms of every
    /// <see cref="IDifferentialRegistration"/> of this assembly.
    /// </summary>
    public static DifferentialRegistry Current => CurrentRegistry.Value;

    /// <summary>The candidate arms, in the order they were added.</summary>
    public IReadOnlyList<Arm> Candidates => candidates.Select(c => c.Item).ToList();

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

    /// <summary>The candidates of the facet's tier that cover the facet.</summary>
    /// <param name="facet">The facet.</param>
    /// <returns>The arms, in the order they were added.</returns>
    public IEnumerable<Arm> CandidatesFor(Facet facet)
        => candidates.Select(c => c.Item).Where(arm => arm.Tier == facet.Tier && arm.Covers(facet));

    /// <summary>Checks the registry against a case list: unique arm names, and the facet count of each arm.</summary>
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
