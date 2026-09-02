#!/usr/bin/env python3
"""Context builder and comment poster for the Codex PR review workflow.

Two subcommands, one per job in `.github/workflows/codex-review.yml`:

  build-context   Collects everything the review needs into a directory, so the agent can run
                  with no network and no GitHub token. Writes `pr.json`, `pr.diff`,
                  `prior_review.md`, and `prior_threads.json`.

  post            Turns the agent's JSON into inline review comments and one sticky summary
                  comment. The agent has no write access, so this is the only step that changes
                  anything on the PR.

Both live in one file because they share the comment markers, which are the whole state model:

  * every inline comment the bot posts starts with `<!-- codex-review-key: <key> -->`. The key is
    the agent's identifier for one issue. `build-context` collects the keys of open threads and
    gives them back to the agent, which reuses a key when it reports the same issue again. `post`
    then skips keys that already have a thread, so a comment is posted once and not on every push.

  * the summary comment starts with `<!-- codex-review-summary -->`, which is how `post` finds
    and updates it instead of adding a new one.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

SUMMARY_MARKER = "<!-- codex-review-summary -->"
KEY_MARKER = "<!-- codex-review-key: {key} -->"
KEY_MARKER_RE = re.compile(r"<!--\s*codex-review-key:\s*([A-Za-z0-9._/-]{1,120})\s*-->")
# Opening fence of a GitHub suggestion block, which is only safe on the exact line the fix
# was written for.
SUGGESTION_FENCE_RE = re.compile(r"^```suggestion[^\n]*$", re.MULTILINE)

SEVERITY_LABEL = {
    "blocker": "❌ **Blocker**",
    "major": "⚠️ **Major**",
    "nit": "💡 **Nit**",
}
SEVERITY_ORDER = {"blocker": 0, "major": 1, "nit": 2}

# Cap on inline comments per run. A run that wants more than this has almost certainly lost the
# plot, and a wall of comments is unreadable anyway. Whatever is dropped is named in the summary.
MAX_INLINE_COMMENTS = 20

# How far `post` will move a finding to reach a commentable line. The agent is told to anchor on a
# line inside a diff hunk; this absorbs an off-by-a-few without silently relocating a comment.
MAX_ANCHOR_DRIFT = 15

# Linked issues pulled in for context. The PR body usually references one.
MAX_LINKED_ISSUES = 5

# Pages of 100 review threads to walk. A backstop against a paging loop, not a real limit.
MAX_THREAD_PAGES = 20

# Logins that count as this bot. A thread is only re-opened when the bot is the one that resolved
# it; a thread the PR author resolved stays resolved, because resolving it is how an author says
# "not this one" and re-opening it would be arguing back. Comments made with GITHUB_TOKEN are
# authored by `github-actions[bot]`.
BOT_LOGINS = {"github-actions"}


def gh(*args, stdin=None):
    """Run `gh` and return stdout. Raises with gh's stderr attached on failure."""
    proc = subprocess.run(
        ["gh", *args],
        input=stdin,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def graphql(query, **variables):
    args = ["api", "graphql", "-f", f"query={query}"]
    for name, value in variables.items():
        args += ["-F", f"{name}={value}"]
    return json.loads(gh(*args))


def env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


# ---------------------------------------------------------------------------- context


def review_threads(repo, pr_number):
    """The bot's own inline review threads, newest comment last.

    A thread counts as the bot's when its first comment carries a key marker. That is stricter
    than matching on author: it also excludes anything the bot's account posted by hand.
    """
    owner, name = repo.split("/", 1)
    query = """
    query($owner: String!, $name: String!, $pr: Int!, $after: String) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $pr) {
          reviewThreads(first: 100, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              isResolved
              resolvedBy { login }
              path
              line
              comments(first: 50) {
                nodes { databaseId author { login } body createdAt }
              }
            }
          }
        }
      }
    }
    """
    # Every page matters: a thread this misses is a comment posted twice, or one left open on an
    # issue that is gone. A long-lived PR can hold more threads than one page.
    nodes = []
    after = None
    for _ in range(MAX_THREAD_PAGES):
        cursor = {"after": after} if after else {}
        page = graphql(query, owner=owner, name=name, pr=pr_number, **cursor)
        page = page["data"]["repository"]["pullRequest"]["reviewThreads"]
        nodes += page["nodes"]
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
    else:
        print(f"::warning::stopped after {MAX_THREAD_PAGES} pages of review threads")

    threads = []
    for node in nodes:
        comments = node["comments"]["nodes"]
        if not comments:
            continue
        match = KEY_MARKER_RE.search(comments[0]["body"] or "")
        if not match:
            continue
        threads.append(
            {
                "key": match.group(1),
                "thread_id": node["id"],
                "path": node["path"],
                "line": node["line"],
                "isResolved": node["isResolved"],
                "resolvedBy": (node["resolvedBy"] or {}).get("login"),
                "comments": [
                    {
                        "author": (c["author"] or {}).get("login", "ghost"),
                        "createdAt": c["createdAt"],
                        "body": KEY_MARKER_RE.sub("", c["body"] or "").strip(),
                    }
                    for c in comments
                ],
            }
        )
    return threads


def summary_comment(repo, pr_number):
    """The existing sticky summary comment as (comment_id, body), or (None, "")."""
    comments = json.loads(
        gh("api", f"/repos/{repo}/issues/{pr_number}/comments", "--paginate")
    )
    for comment in comments:
        body = comment.get("body") or ""
        if body.startswith(SUMMARY_MARKER):
            return comment["id"], body[len(SUMMARY_MARKER):].lstrip()
    return None, ""


def linked_issues(repo, body):
    """Bodies of the issues the PR body references, so the agent knows the intended behavior."""
    numbers = []
    for match in re.finditer(r"(?:#|/issues/)(\d+)", body or ""):
        number = int(match.group(1))
        if number not in numbers:
            numbers.append(number)

    issues = []
    for number in numbers[:MAX_LINKED_ISSUES]:
        try:
            issues.append(
                json.loads(
                    gh(
                        "issue",
                        "view",
                        str(number),
                        "--repo",
                        repo,
                        "--json",
                        "number,title,body,state",
                    )
                )
            )
        except RuntimeError:
            # A reference can point at a PR, a deleted issue, or another repo's numbering.
            continue
    return issues


def build_context():
    repo = env("GH_REPO")
    pr_number = int(env("PR_NUMBER"))
    out_dir = Path(env("OUT_DIR"))
    out_dir.mkdir(parents=True, exist_ok=True)

    pr = json.loads(
        gh(
            "pr",
            "view",
            str(pr_number),
            "--repo",
            repo,
            "--json",
            "number,title,body,author,isDraft,baseRefName,headRefName,headRefOid,"
            "labels,files,additions,deletions,url",
        )
    )
    pr["linkedIssues"] = linked_issues(repo, pr.get("body") or "")
    (out_dir / "pr.json").write_text(json.dumps(pr, indent=2), encoding="utf-8")

    diff = gh("pr", "diff", str(pr_number), "--repo", repo)
    (out_dir / "pr.diff").write_text(diff, encoding="utf-8")

    _, prior_review = summary_comment(repo, pr_number)
    (out_dir / "prior_review.md").write_text(prior_review, encoding="utf-8")

    threads = review_threads(repo, pr_number)
    # The thread id is a write handle. The agent cannot post, but it also has no reason to see it.
    for thread in threads:
        thread.pop("thread_id", None)
    (out_dir / "prior_threads.json").write_text(
        json.dumps(threads, indent=2), encoding="utf-8"
    )

    print(
        f"context: {len(pr['files'])} changed files, {len(diff.splitlines())} diff lines, "
        f"{len(pr['linkedIssues'])} linked issues, {len(threads)} prior threads, "
        f"prior summary: {'yes' if prior_review else 'no'}"
    )


# ------------------------------------------------------------------------------- post


def commentable_lines(diff):
    """Map path -> set of new-file line numbers that GitHub will accept a RIGHT comment on.

    That is every added and every context line inside a hunk. A line outside all hunks is not
    part of the diff, and the API rejects it.
    """
    lines_by_path = {}
    path = None
    new_line = None

    for raw in diff.splitlines():
        if raw.startswith("+++ "):
            target = raw[4:].strip()
            path = None if target == "/dev/null" else re.sub(r"^b/", "", target)
            new_line = None
            continue
        if raw.startswith("@@"):
            match = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)", raw)
            new_line = int(match.group(1)) if match else None
            continue
        if path is None or new_line is None:
            continue
        if raw.startswith("\\"):  # "\ No newline at end of file"
            continue
        if raw.startswith((" ", "+")):
            lines_by_path.setdefault(path, set()).add(new_line)
            new_line += 1
        elif raw.startswith("-"):
            continue
        else:
            # Anything else ends the hunk body (next `diff --git`, `index`, mode lines).
            new_line = None

    return lines_by_path


def anchor(finding, lines_by_path):
    """Line to comment on, or None when the finding cannot be attached to the diff."""
    path = finding["path"]
    line = finding["line"]
    valid = lines_by_path.get(path)
    if not valid:
        return None
    if line in valid:
        return line
    nearest = min(valid, key=lambda candidate: (abs(candidate - line), candidate))
    if abs(nearest - line) > MAX_ANCHOR_DRIFT:
        return None
    return nearest


def inline_body(finding, line):
    """The comment body, for a comment that will sit on `line`."""
    label = SEVERITY_LABEL.get(finding["severity"], finding["severity"])
    body = finding["body"].strip()
    notes = []
    if line != finding["line"]:
        # GitHub applies a suggestion block to the line the comment sits on. That is not the line
        # this fix was written against, so the block must stop being one-click applicable.
        body = SUGGESTION_FENCE_RE.sub("```", body)
        notes.append(f"Anchored here; the finding names line {finding['line']}.")
    notes.append(
        "A reply here is read on the next run: an explanation that holds up drops the finding."
    )
    return (
        f"{KEY_MARKER.format(key=finding['key'])}\n"
        f"{label}: {finding['title']}\n\n"
        f"{body}\n\n"
        f"<sub>{' '.join(notes)}</sub>"
    )


def post_inline(repo, pr_number, head_sha, finding, line):
    payload = {
        "body": inline_body(finding, line),
        "commit_id": head_sha,
        "path": finding["path"],
        "line": line,
        "side": "RIGHT",
    }
    gh(
        "api",
        "--method",
        "POST",
        f"/repos/{repo}/pulls/{pr_number}/comments",
        "--input",
        "-",
        stdin=json.dumps(payload),
    )


def set_resolved(thread_id, resolved):
    mutation = (
        "mutation($id: ID!) { %s(input: {threadId: $id}) { thread { id } } }"
        % ("resolveReviewThread" if resolved else "unresolveReviewThread")
    )
    graphql(mutation, id=thread_id)


def upsert_summary(repo, pr_number, body):
    comment_id, _ = summary_comment(repo, pr_number)
    payload = json.dumps({"body": f"{SUMMARY_MARKER}\n{body}"})
    if comment_id is None:
        gh(
            "api",
            "--method",
            "POST",
            f"/repos/{repo}/issues/{pr_number}/comments",
            "--input",
            "-",
            stdin=payload,
        )
        print("summary: posted")
    else:
        gh(
            "api",
            "--method",
            "PATCH",
            f"/repos/{repo}/issues/comments/{comment_id}",
            "--input",
            "-",
            stdin=payload,
        )
        print(f"summary: updated comment {comment_id}")


def load_review():
    raw = env("REVIEW_JSON")
    try:
        review = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"agent output is not JSON: {e}") from e

    if not isinstance(review, dict):
        raise RuntimeError("agent output is not a JSON object")
    summary = review.get("summary_markdown")
    if not isinstance(summary, str) or not summary.strip():
        raise RuntimeError("agent output has no summary_markdown")

    findings = []
    seen_keys = set()
    for index, finding in enumerate(review.get("findings") or []):
        try:
            dismissed = finding.get("dismissed_but_real", False)
            if not isinstance(dismissed, bool):
                # Truthiness would read the string "false" as True and silently swallow the
                # inline comment. An unusable value falls back to commenting.
                print(
                    f"::warning::finding {index}: dismissed_but_real is {dismissed!r}, "
                    "not a boolean; treating it as false"
                )
                dismissed = False
            cleaned = {
                "key": str(finding["key"]).strip(),
                "severity": str(finding["severity"]).strip().lower(),
                "path": str(finding["path"]).strip().lstrip("/"),
                "line": int(finding["line"]),
                "title": str(finding["title"]).strip(),
                "body": str(finding["body"]),
                "dismissed_but_real": dismissed,
            }
        except (KeyError, TypeError, ValueError) as e:
            print(f"::warning::dropping malformed finding {index}: {e}")
            continue
        if not KEY_MARKER_RE.fullmatch(KEY_MARKER.format(key=cleaned["key"])):
            print(f"::warning::dropping finding {index}: unusable key {cleaned['key']!r}")
            continue
        if cleaned["key"] in seen_keys:
            print(f"::warning::dropping finding {index}: duplicate key {cleaned['key']}")
            continue
        if cleaned["severity"] not in SEVERITY_LABEL:
            print(f"::warning::dropping finding {index}: unknown severity {cleaned['severity']}")
            continue
        seen_keys.add(cleaned["key"])
        findings.append(cleaned)

    findings.sort(key=lambda f: SEVERITY_ORDER[f["severity"]])
    return summary.strip(), findings


def post():
    repo = env("GH_REPO")
    pr_number = int(env("PR_NUMBER"))
    head_sha = env("HEAD_SHA")

    summary, findings = load_review()
    lines_by_path = commentable_lines(gh("pr", "diff", str(pr_number), "--repo", repo))
    threads = review_threads(repo, pr_number)
    threads_by_key = {thread["key"]: thread for thread in threads}
    current_keys = {finding["key"] for finding in findings}

    posted = []
    unanchored = []
    dropped_by_cap = []

    for finding in findings:
        key = finding["key"]
        existing = threads_by_key.get(key)
        if existing is not None:
            # The issue already has a thread, so say nothing new on it. A thread the bot resolved
            # too early has to come back, but one the author resolved stays resolved: that is the
            # author declining the finding, and re-opening it would be arguing back.
            resolver = (existing["resolvedBy"] or "").removesuffix("[bot]")
            if (
                existing["isResolved"]
                and not finding["dismissed_but_real"]
                and resolver in BOT_LOGINS
            ):
                set_resolved(existing["thread_id"], False)
                print(f"unresolved thread for {key}: still reported")
            continue
        if finding["dismissed_but_real"]:
            # Kept over the author's objection. It stays in the summary; re-opening the argument
            # inline is what makes a review bot unbearable.
            continue
        if len(posted) >= MAX_INLINE_COMMENTS:
            dropped_by_cap.append(finding)
            continue

        line = anchor(finding, lines_by_path)
        if line is None:
            unanchored.append(finding)
            continue
        try:
            post_inline(repo, pr_number, head_sha, finding, line)
        except RuntimeError as e:
            print(f"::warning::could not comment on {finding['path']}:{line}: {e}")
            unanchored.append(finding)
            continue
        posted.append(finding)
        drift = "" if line == finding["line"] else f" (moved from {finding['line']})"
        print(f"commented {finding['severity']} on {finding['path']}:{line}{drift} [{key}]")

    for thread in threads:
        if thread["key"] not in current_keys and not thread["isResolved"]:
            set_resolved(thread["thread_id"], True)
            print(f"resolved thread for {thread['key']}: no longer reported")

    body = summary
    if unanchored:
        body += "\n\n##### Not anchored to the diff\n"
        body += "\n".join(
            f"- `{f['path']}:{f['line']}` {f['title']}" for f in unanchored
        )
        body += "\n\nNo inline comment could be attached to these lines."
    if dropped_by_cap:
        body += (
            f"\n\n##### Not posted inline\n{len(dropped_by_cap)} further findings were over the "
            f"{MAX_INLINE_COMMENTS}-comment limit for one run and are listed above only.\n"
        )
    upsert_summary(repo, pr_number, body)

    print(
        f"done: {len(posted)} new inline comments, {len(findings)} findings, "
        f"{len(threads)} prior threads"
    )


def main():
    commands = {"build-context": build_context, "post": post}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        print(f"usage: {sys.argv[0]} {{{'|'.join(commands)}}}", file=sys.stderr)
        return 2
    commands[sys.argv[1]]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
