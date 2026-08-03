#!/usr/bin/env dotnet
// Assembles CHANGELOG.md and RELEASENOTES.md from per-change fragment files in changelog.d/.
//
// Why fragments: two pull requests that both append to a shared "Unreleased" section always
// conflict, and GitHub does not honor .gitattributes merge drivers server-side, so `merge=union`
// cannot fix it (github/community discussion #9288). Union merge is not even the right answer
// locally -- it resolves by interleaving both sides' lines, which lands entries under the wrong
// heading. Instead every change drops its own file in changelog.d/, so two pull requests add two
// distinct paths and git has nothing to reconcile. This script folds them in at release time.
//
// CHANGELOG.md is the source of truth (full history). RELEASENOTES.md is *generated* from it by
// dropping every section below ReleaseNotesFloor -- it ships inside the NuGet package via the
// PackageReleaseNotes target in ClickHouse.Driver/ClickHouse.Driver.csproj.
//
//   dotnet run scripts/changelog.cs -- --new fixes 512-variant-null
//   dotnet run scripts/changelog.cs -- --check          # CI gate
//   dotnet run scripts/changelog.cs -- --render         # preview pending entries
//   dotnet run scripts/changelog.cs -- --release v1.4.0
//   dotnet run scripts/changelog.cs -- --verify-release 1.4.0   # release-workflow gate
//   dotnet run scripts/changelog.cs -- --sync-notes     # regenerate RELEASENOTES.md only
//
// On Unix the shebang also allows ./scripts/changelog.cs --check.

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;

// Fragment categories, in the order their sections are emitted. Both the headings and their
// order match existing CHANGELOG.md sections: v1.2.0 places Internal Improvements directly after
// Improvements, and the current Unreleased section places Deprecations between Improvements and
// Bug Fixes.
var categories = new[]
{
    ("breaking", "Breaking Changes"),
    ("features", "New Features"),
    ("improvements", "Improvements"),
    ("internal", "Internal Improvements"),
    ("deprecations", "Deprecations"),
    ("fixes", "Bug Fixes"),
    ("docs", "Documentation and Usage Examples"),
};

// RELEASENOTES.md keeps only sections at or above this version. Bump it when a new major line
// starts, so the notes shipped in the package describe the current major rather than all history.
const string ReleaseNotesFloor = "v1.0.0";

// A section header is a line whose successor is exactly "---" *and* which itself looks like a
// version. The version test is load-bearing: the v1.0.0 section body uses bare "---" lines as
// horizontal rules, and without it those would parse as phantom sections with empty titles.
var headerPattern = new Regex(@"^(Unreleased|v\d+\.\d+(\.\d+)?(-[0-9A-Za-z.]+)?)$", RegexOptions.Compiled);

var root = FindRoot();
var fragmentDir = Path.Combine(root, "changelog.d");
var changelogPath = Path.Combine(root, "CHANGELOG.md");
var releaseNotesPath = Path.Combine(root, "RELEASENOTES.md");

return args.Length == 0 ? Usage() : args[0] switch
{
    "--check" => Check(),
    "--render" => Render(),
    "--release" => args.Length >= 2 ? Release(args[1]) : Fail("--release needs a version, e.g. --release v1.4.0"),
    "--verify-release" => args.Length >= 2 ? VerifyRelease(args[1]) : Fail("--verify-release needs a version, e.g. --verify-release 1.4.0"),
    "--sync-notes" => SyncNotes(),
    "--new" => args.Length >= 3 ? NewFragment(args[1], args[2]) : Fail("--new needs a category and a name, e.g. --new fixes 512-variant-null"),
    "--help" or "-h" => Usage(0),
    _ => Fail($"unknown option '{args[0]}'"),
};

int Check()
{
    var problems = new List<string>();
    var fragments = LoadFragments(problems);

    // The whole scheme rests on nobody hand-editing the shared Unreleased section, so that is
    // what we actually enforce. Released sections below it are left alone -- fixing a typo in
    // shipped notes is still fine.
    var changelog = ReadLines(changelogPath);
    var sections = FindSections(changelog);
    if (sections.Count == 0 || sections[0].Title != "Unreleased")
    {
        problems.Add("CHANGELOG.md must start with an 'Unreleased' section.");
    }
    else if (!IsBodyEmpty(changelog, sections, 0))
    {
        problems.Add(
            "CHANGELOG.md has content under 'Unreleased'. Move each entry into its own "
            + "changelog.d/ fragment (dotnet run scripts/changelog.cs -- --new <category> <name>) "
            + "so concurrent pull requests cannot conflict.");
    }

    var expectedNotes = TrimToFloor(string.Join("\n", changelog));
    if (Normalize(File.ReadAllText(releaseNotesPath)) != expectedNotes)
    {
        problems.Add(
            "RELEASENOTES.md is out of sync with CHANGELOG.md. It is generated, not hand-edited -- "
            + "run: dotnet run scripts/changelog.cs -- --sync-notes");
    }

    if (problems.Count > 0)
    {
        Console.Error.WriteLine($"changelog: {problems.Count} problem(s) found:\n");
        foreach (var p in problems)
            Console.Error.WriteLine($"  - {p}\n");
        return 1;
    }

    Console.WriteLine($"changelog: OK ({fragments.Count} pending fragment(s)).");
    return 0;
}

int Render()
{
    var problems = new List<string>();
    var fragments = LoadFragments(problems);
    if (problems.Count > 0)
        return ReportProblems(problems);

    Console.Write(fragments.Count == 0 ? "_No pending changelog entries._\n" : RenderFragments(fragments));
    return 0;
}

int Release(string rawVersion)
{
    var version = NormalizeVersion(rawVersion);
    if (version is null)
        return Fail($"'{rawVersion}' is not a version like v1.4.0");

    var problems = new List<string>();
    var fragments = LoadFragments(problems);
    if (problems.Count > 0)
        return ReportProblems(problems);
    if (fragments.Count == 0)
        return Fail("no fragments in changelog.d/ -- nothing to release.");

    var changelog = ReadLines(changelogPath);
    var sections = FindSections(changelog);
    if (sections.Count == 0 || sections[0].Title != "Unreleased")
        return Fail("CHANGELOG.md must start with an 'Unreleased' section.");
    if (!IsBodyEmpty(changelog, sections, 0))
        return Fail("CHANGELOG.md has content under 'Unreleased'; move it into changelog.d/ fragments first.");
    if (sections.Any(s => s.Title == version))
        return Fail($"CHANGELOG.md already has a {version} section.");

    // Everything from the second section onward is untouched history.
    var rest = sections.Count > 1
        ? string.Join("\n", changelog.Skip(sections[1].Index))
        : string.Empty;

    var updated = new StringBuilder()
        .Append("Unreleased\n---\n\n")
        .Append(version).Append("\n---\n\n")
        .Append(RenderFragments(fragments))
        .Append('\n')
        .Append(rest)
        .ToString();

    updated = Normalize(updated);
    File.WriteAllText(changelogPath, updated);
    File.WriteAllText(releaseNotesPath, TrimToFloor(updated));
    foreach (var f in fragments)
        File.Delete(f.Path);

    Console.WriteLine($"changelog: released {version} from {fragments.Count} fragment(s).");
    Console.WriteLine($"  updated {Rel(changelogPath)}, regenerated {Rel(releaseNotesPath)}, removed {fragments.Count} fragment(s).");
    Console.WriteLine("  review the diff, then commit on a release-prep branch.");
    return 0;
}

// Gate for the release workflow: refuse to publish a version whose changelog was never assembled.
// Publishing to NuGet cannot be undone (packages can only be delisted), and RELEASENOTES.md is
// baked into the package via PackageReleaseNotes, so shipping before --release runs means shipping
// a package whose notes describe the *previous* version -- silently, with nothing to roll back.
int VerifyRelease(string rawVersion)
{
    var version = NormalizeVersion(rawVersion);
    if (version is null)
        return Fail($"'{rawVersion}' is not a version like 1.4.0");

    var problems = new List<string>();

    // Fragment *validity* is the pull request gate's job; here only their presence matters.
    var pending = LoadFragments([]);
    if (pending.Count > 0)
    {
        problems.Add(
            $"{pending.Count} fragment(s) still pending in changelog.d/, so their changes are in "
            + $"this build but not in its notes. Run: dotnet run scripts/changelog.cs -- --release {version}");
    }

    var changelog = ReadLines(changelogPath);
    var sections = FindSections(changelog);
    var newest = sections.FirstOrDefault(s => s.Title != "Unreleased");
    var newestVersion = newest is null ? null : ParseVersion(newest.Title);

    // Compare base versions, not titles: prereleases are cut against the section for the version
    // they lead up to (1.3.0-rc1 shipped against the v1.3.0 section), so requiring an exact title
    // match would block every release candidate.
    if (newestVersion is null)
        problems.Add("CHANGELOG.md has no released section to publish.");
    else if (newestVersion != ParseVersion(version))
        problems.Add(
            $"CHANGELOG.md's newest released section is {newest!.Title}, but this release is {version}. "
            + $"Either the changelog was not assembled for {version}, or the wrong version was entered.");

    if (Normalize(File.ReadAllText(releaseNotesPath)) != TrimToFloor(string.Join("\n", changelog)))
    {
        problems.Add(
            "RELEASENOTES.md is out of sync with CHANGELOG.md, so the package would ship stale notes. "
            + "Run: dotnet run scripts/changelog.cs -- --sync-notes");
    }

    if (problems.Count > 0)
    {
        Console.Error.WriteLine($"changelog: refusing to release {version} -- {problems.Count} problem(s):\n");
        foreach (var p in problems)
            Console.Error.WriteLine($"  - {p}\n");
        return 1;
    }

    Console.WriteLine($"changelog: {version} is ready to release (CHANGELOG.md section {newest!.Title}, no pending fragments).");
    return 0;
}

int SyncNotes()
{
    var generated = TrimToFloor(string.Join("\n", ReadLines(changelogPath)));
    var changed = Normalize(File.ReadAllText(releaseNotesPath)) != generated;
    File.WriteAllText(releaseNotesPath, generated);
    Console.WriteLine(changed
        ? $"changelog: regenerated {Rel(releaseNotesPath)} from {Rel(changelogPath)}."
        : $"changelog: {Rel(releaseNotesPath)} already up to date.");
    return 0;
}

int NewFragment(string category, string name)
{
    if (!categories.Any(c => c.Item1 == category))
        return Fail($"unknown category '{category}'. Valid: {string.Join(", ", categories.Select(c => c.Item1))}");

    var slug = Regex.Replace(name.ToLowerInvariant(), @"[^a-z0-9]+", "-").Trim('-');
    if (slug.Length == 0)
        return Fail($"'{name}' does not yield a usable file name.");

    Directory.CreateDirectory(fragmentDir);
    var path = Path.Combine(fragmentDir, $"{slug}.{category}.md");
    if (File.Exists(path))
        return Fail($"{Rel(path)} already exists.");

    File.WriteAllText(path, "* Describe the change here, in the past tense, as a single top-level bullet.\n");
    Console.WriteLine($"changelog: created {Rel(path)}");
    return 0;
}

// --- assembly -------------------------------------------------------------------------------

string RenderFragments(List<Fragment> fragments)
{
    var sb = new StringBuilder();
    foreach (var (key, heading) in categories)
    {
        var inCategory = fragments
            .Where(f => f.Category == key)
            .OrderBy(f => f.SortKey, StringComparer.Ordinal)
            .ToList();
        if (inCategory.Count == 0)
            continue;

        if (sb.Length > 0)
            sb.Append('\n');
        sb.Append($"**{heading}:**\n");
        foreach (var f in inCategory)
            sb.Append(f.Body).Append('\n');
    }

    return sb.ToString();
}

List<Fragment> LoadFragments(List<string> problems)
{
    var fragments = new List<Fragment>();
    if (!Directory.Exists(fragmentDir))
        return fragments;

    foreach (var path in Directory.GetFiles(fragmentDir, "*.md").OrderBy(p => p, StringComparer.Ordinal))
    {
        var fileName = Path.GetFileName(path);
        if (fileName.Equals("README.md", StringComparison.OrdinalIgnoreCase))
            continue;

        var stem = fileName[..^".md".Length];
        var dot = stem.LastIndexOf('.');
        if (dot <= 0)
        {
            problems.Add($"{Rel(path)}: file name needs a category, e.g. '{stem}.fixes.md'. Valid categories: {string.Join(", ", categories.Select(c => c.Item1))}");
            continue;
        }

        var category = stem[(dot + 1)..];
        if (!categories.Any(c => c.Item1 == category))
        {
            problems.Add($"{Rel(path)}: unknown category '{category}'. Valid: {string.Join(", ", categories.Select(c => c.Item1))}");
            continue;
        }

        var body = Normalize(File.ReadAllText(path)).TrimEnd('\n');
        if (body.Trim().Length == 0)
        {
            problems.Add($"{Rel(path)}: is empty.");
            continue;
        }

        if (!body.StartsWith("* ", StringComparison.Ordinal) && !body.StartsWith("- ", StringComparison.Ordinal))
        {
            problems.Add($"{Rel(path)}: must start with a top-level markdown bullet ('* '), since entries are concatenated into a bulleted section.");
            continue;
        }

        fragments.Add(new Fragment(path, category, SortKeyFor(stem[..dot]), body));
    }

    return fragments;
}

// Sorts a numeric prefix numerically, so 9-foo comes before 512-foo rather than after it.
string SortKeyFor(string stem)
{
    var digits = stem.TakeWhile(char.IsAsciiDigit).Count();
    return digits == 0 ? stem : stem[..digits].PadLeft(10, '0') + stem[digits..];
}

// --- changelog structure --------------------------------------------------------------------

List<Section> FindSections(List<string> lines)
{
    var sections = new List<Section>();
    for (var i = 0; i + 1 < lines.Count; i++)
    {
        if (lines[i + 1].TrimEnd() == "---" && headerPattern.IsMatch(lines[i].Trim()))
            sections.Add(new Section(i, lines[i].Trim()));
    }

    return sections;
}

bool IsBodyEmpty(List<string> lines, List<Section> sections, int index)
{
    var start = sections[index].Index + 2;
    var end = index + 1 < sections.Count ? sections[index + 1].Index : lines.Count;
    for (var i = start; i < end; i++)
    {
        if (lines[i].Trim().Length > 0)
            return false;
    }

    return true;
}

// RELEASENOTES.md = CHANGELOG.md with every section below the floor removed, and without the
// Unreleased stub.
string TrimToFloor(string changelog)
{
    var lines = Normalize(changelog).Split('\n').ToList();
    var floor = ParseVersion(ReleaseNotesFloor)!;

    foreach (var section in FindSections(lines))
    {
        var version = ParseVersion(section.Title);
        if (version is null || version >= floor)
            continue;

        lines = lines.Take(section.Index).ToList();
        break;
    }

    // These notes ship inside the NuGet package, so they should open on the newest released
    // version. The Unreleased section is always empty here (--check enforces that entries live in
    // changelog.d/ until release), and an empty stub section is just noise for consumers.
    var sections = FindSections(lines);
    if (sections.Count > 0 && sections[0].Title == "Unreleased" && IsBodyEmpty(lines, sections, 0))
        lines = lines.Skip(sections.Count > 1 ? sections[1].Index : lines.Count).ToList();

    while (lines.Count > 0 && lines[^1].Trim().Length == 0)
        lines.RemoveAt(lines.Count - 1);

    return lines.Count == 0 ? string.Empty : string.Join("\n", lines) + "\n";
}

Version? ParseVersion(string title)
{
    if (!title.StartsWith('v'))
        return null;
    var core = title[1..].Split('-')[0];
    return Version.TryParse(core.Count(c => c == '.') == 1 ? core + ".0" : core, out var v) ? v : null;
}

string? NormalizeVersion(string raw)
{
    var candidate = raw.StartsWith('v') ? raw : "v" + raw;
    return headerPattern.IsMatch(candidate) && candidate != "Unreleased" ? candidate : null;
}

// --- plumbing -------------------------------------------------------------------------------

// LF regardless of host: .gitattributes normalizes these files with `* text=auto`, so writing LF
// keeps the committed content stable whether the script runs on Linux or Windows.
string Normalize(string text)
{
    var normalized = text.Replace("\r\n", "\n").Replace('\r', '\n').TrimEnd('\n');
    return normalized.Length == 0 ? string.Empty : normalized + "\n";
}

List<string> ReadLines(string path) => Normalize(File.ReadAllText(path)).Split('\n').ToList();

string Rel(string path) => Path.GetRelativePath(root, path).Replace('\\', '/');

int ReportProblems(List<string> problems)
{
    foreach (var p in problems)
        Console.Error.WriteLine($"changelog: {p}");
    return 1;
}

int Fail(string message)
{
    Console.Error.WriteLine($"changelog: {message}");
    return 1;
}

int Usage(int exitCode = 1)
{
    var writer = exitCode == 0 ? Console.Out : Console.Error;
    writer.WriteLine("""
        Assembles CHANGELOG.md and RELEASENOTES.md from fragments in changelog.d/.

        Usage: dotnet run scripts/changelog.cs -- <command>

          --new <category> <name>  Create a fragment, e.g. --new fixes 512-variant-null
          --check                  Validate fragments, empty Unreleased, notes in sync (CI gate)
          --render                 Print the pending Unreleased section
          --release <version>      Fold fragments into the changelog as <version> and delete them
          --verify-release <ver>   Assert the changelog was assembled for <ver> (release gate)
          --sync-notes             Regenerate RELEASENOTES.md from CHANGELOG.md

        Categories: breaking, features, improvements, internal, deprecations, fixes, docs
        """);
    return exitCode;
}

// Walk up from this file to the repository root. CallerFilePath is the real source path for a
// file-based app, so this works no matter which directory the script is invoked from.
string FindRoot([CallerFilePath] string scriptPath = "")
{
    var dir = Path.GetDirectoryName(Path.GetFullPath(scriptPath))!;
    for (var d = new DirectoryInfo(dir); d is not null; d = d.Parent)
    {
        if (File.Exists(Path.Combine(d.FullName, "CHANGELOG.md")))
            return d.FullName;
    }

    throw new InvalidOperationException($"could not locate CHANGELOG.md above {dir}");
}

record Fragment(string Path, string Category, string SortKey, string Body);

record Section(int Index, string Title);
