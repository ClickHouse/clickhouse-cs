#!/usr/bin/env python3
"""Offline checks for codex_review.py. Run with `python3 .github/scripts/test_codex_review.py`.

No network and no `gh`: the diffs are fixtures and every call that would talk to GitHub is
replaced. The review workflow runs this before it calls the model, so a broken script costs a
second instead of a review.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import codex_review as cr  # noqa: E402

failures = []


def check(name, condition, detail=""):
    print(f"  {'ok  ' if condition else 'FAIL'} {name}" + (f" {detail}" if not condition else ""))
    if not condition:
        failures.append(name)


# A diff shaped like a real one: one file with three hunks, one added file, one deleted file,
# one rename, and a binary file. Each line is named after its line number on the new side, so
# the commentable set is readable: Main.cs is 90-96, 112-127 and 170-171.
DIFF = """diff --git a/src/Main.cs b/src/Main.cs
index 1111111..2222222 100644
--- a/src/Main.cs
+++ b/src/Main.cs
@@ -90,7 +90,7 @@ namespace N
 ctx90
 ctx91
 ctx92
-old93
+new93
 ctx94
 ctx95
 ctx96
@@ -112 +112,16 @@ namespace N
 ctx112
+added113
+added114
+added115
+added116
+added117
+added118
+added119
+added120
+added121
+added122
+added123
+added124
+added125
+added126
+added127
@@ -170 +170,2 @@ namespace N
 ctx170
+added171
diff --git a/src/Added.cs b/src/Added.cs
new file mode 100644
--- /dev/null
+++ b/src/Added.cs
@@ -0,0 +1,2 @@
+one
+two
diff --git a/src/Removed.cs b/src/Removed.cs
deleted file mode 100644
--- a/src/Removed.cs
+++ /dev/null
@@ -1,2 +0,0 @@
-one
-two
diff --git a/src/Old.cs b/src/New.cs
similarity index 90%
rename from src/Old.cs
rename to src/New.cs
--- a/src/Old.cs
+++ b/src/New.cs
@@ -5,3 +5,3 @@ class C
 ctx5
-gone
+added6
diff --git a/logo.png b/logo.png
index 3333333..4444444 100644
Binary files a/logo.png and b/logo.png differ
"""

print("commentable_lines")
lines = cr.commentable_lines(DIFF)
check("modified file, three hunks",
      lines.get("src/Main.cs") == set(range(90, 97)) | set(range(112, 128)) | {170, 171},
      sorted(lines.get("src/Main.cs", ())))
check("added file", lines.get("src/Added.cs") == {1, 2})
check("deleted file absent", "src/Removed.cs" not in lines)
check("rename keyed by the new path",
      "src/New.cs" in lines and "src/Old.cs" not in lines)
check("rename hunk lines", lines.get("src/New.cs") == {5, 6})
check("binary file absent", "logo.png" not in lines)
check("no extra paths", set(lines) == {"src/Main.cs", "src/Added.cs", "src/New.cs"}, set(lines))
check("single-line hunk header without a count",
      cr.commentable_lines("--- a/f.cs\n+++ b/f.cs\n@@ -7 +7 @@\n+only\n") == {"f.cs": {7}})
check("no-newline marker does not shift lines",
      cr.commentable_lines(
          "--- a/f.cs\n+++ b/f.cs\n@@ -1,2 +1,2 @@\n ctx\n-old\n"
          "\\ No newline at end of file\n+new\n\\ No newline at end of file\n")
      == {"f.cs": {1, 2}})
check("empty diff", cr.commentable_lines("") == {})

print("\nanchor")
check("exact line kept", cr.anchor({"path": "src/Main.cs", "line": 120}, lines) == 120)
check("line just past a hunk snapped", cr.anchor({"path": "src/Main.cs", "line": 98}, lines) == 96)
check("line far from any hunk rejected",
      cr.anchor({"path": "src/Main.cs", "line": 9999}, lines) is None)
check("unchanged file rejected", cr.anchor({"path": "src/Other.cs", "line": 1}, lines) is None)
check("at the drift limit accepted",
      cr.anchor({"path": "src/Main.cs", "line": 171 + cr.MAX_ANCHOR_DRIFT}, lines) == 171)
check("one past the drift limit rejected",
      cr.anchor({"path": "src/Main.cs", "line": 171 + cr.MAX_ANCHOR_DRIFT + 1}, lines) is None)

print("\nload_review")
GOOD = {"key": "reader-missing-token", "severity": "blocker", "path": "src/Main.cs", "line": 120,
        "title": "t", "body": "b", "dismissed_but_real": False}


def load(payload):
    os.environ["REVIEW_JSON"] = payload if isinstance(payload, str) else json.dumps(payload)
    return cr.load_review()


summary, findings = load({"summary_markdown": " s ", "verdict": "block", "findings": [GOOD]})
check("valid payload accepted", (summary, len(findings)) == ("s", 1))

_, findings = load({"summary_markdown": "s", "verdict": "approve", "findings": [
    dict(GOOD, key="k-nit", severity="nit"),
    dict(GOOD, key="k-blocker", severity="blocker"),
    dict(GOOD, key="k-major", severity="major"),
]})
check("sorted most severe first",
      [f["key"] for f in findings] == ["k-blocker", "k-major", "k-nit"],
      [f["key"] for f in findings])

_, findings = load({"summary_markdown": "s", "verdict": "approve", "findings": [
    GOOD,
    dict(GOOD, key="dup"),
    dict(GOOD, key="dup"),
    dict(GOOD, key="has spaces"),
    dict(GOOD, key="marker-injection -->"),
    dict(GOOD, key=""),
    dict(GOOD, key="bad-severity", severity="showstopper"),
    dict(GOOD, key="bad-line", line="soon"),
    {"key": "missing-fields"},
]})
check("bad findings dropped, good ones kept",
      [f["key"] for f in findings] == ["reader-missing-token", "dup"],
      [f["key"] for f in findings])

_, findings = load({"summary_markdown": "s", "verdict": "approve",
                    "findings": [dict(GOOD, path="/src/Main.cs")]})
check("leading slash stripped from path", findings[0]["path"] == "src/Main.cs")

for payload, why in [
    ("not json", "non-JSON output"),
    ("[1,2,3]", "JSON array"),
    ('"a string"', "JSON string"),
    (json.dumps({"verdict": "approve", "findings": []}), "missing summary"),
    (json.dumps({"summary_markdown": "  ", "verdict": "approve", "findings": []}),
     "blank summary"),
    ("", "empty output"),
]:
    try:
        load(payload)
        check(f"{why} rejected", False, "no exception raised")
    except RuntimeError:
        check(f"{why} rejected", True)

print("\ncomment markers round-trip")
body = cr.inline_body(GOOD)
check("key parses back out", cr.KEY_MARKER_RE.search(body).group(1) == "reader-missing-token")
check("severity label present", "Blocker" in body)
check("marker strips cleanly", "codex-review-key" not in cr.KEY_MARKER_RE.sub("", body))


def thread(key, thread_id, resolved, resolved_by=None):
    return {"key": key, "thread_id": thread_id, "path": "src/Main.cs", "line": 1,
            "isResolved": resolved, "resolvedBy": resolved_by, "comments": []}


def run_post(findings, threads, summary="**Summary**\nthe review"):
    """post() with the diff faked and every mutation recorded instead of sent."""
    calls = {"inline": [], "resolved": [], "unresolved": [], "summary": []}
    saved = (cr.gh, cr.review_threads, cr.post_inline, cr.set_resolved, cr.upsert_summary)
    cr.gh = lambda *a, **kw: DIFF
    cr.review_threads = lambda repo, pr: [dict(t) for t in threads]
    cr.post_inline = lambda repo, pr, sha, f, ln: calls["inline"].append((f["key"], ln))
    cr.set_resolved = lambda tid, res: calls["resolved" if res else "unresolved"].append(tid)
    cr.upsert_summary = lambda repo, pr, body: calls["summary"].append(body)
    os.environ.update({"GH_REPO": "o/r", "PR_NUMBER": "1", "HEAD_SHA": "abc123"})
    os.environ["REVIEW_JSON"] = json.dumps(
        {"summary_markdown": summary, "verdict": "block", "findings": findings})
    try:
        cr.post()
    finally:
        cr.gh, cr.review_threads, cr.post_inline, cr.set_resolved, cr.upsert_summary = saved
    return calls


def finding(key, line, **kw):
    return dict(GOOD, key=key, severity="major", line=line, title=f"title {key}", **kw)


print("\npost: which findings get an inline comment")
calls = run_post(
    findings=[
        finding("new", 120),
        finding("has-thread", 121),
        finding("snapped", 98),
        finding("unanchorable", 9999),
        finding("unchanged-file", 120, path="src/Other.cs"),
        finding("dismissed", 122, dismissed_but_real=True),
    ],
    threads=[thread("has-thread", "T1", resolved=False)],
)
keys = [c[0] for c in calls["inline"]]
check("new finding posted", "new" in keys)
check("finding that already has a thread not repeated", "has-thread" not in keys)
check("off-by-two line snapped onto the hunk", ("snapped", 96) in calls["inline"],
      calls["inline"])
check("unanchorable finding not posted", "unanchorable" not in keys)
check("finding on an unchanged file not posted", "unchanged-file" not in keys)
check("dismissed-but-real gets no inline comment", "dismissed" not in keys)
check("unanchored findings named in the summary",
      "Not anchored to the diff" in calls["summary"][0]
      and "unanchorable" in calls["summary"][0]
      and "src/Other.cs" in calls["summary"][0], calls["summary"][0][-300:])
check("agent's summary kept", "the review" in calls["summary"][0])
check("exactly one summary write", len(calls["summary"]) == 1)

print("\npost: resolving and re-opening threads")
calls = run_post(
    findings=[
        finding("bot-resolved", 120),
        finding("author-resolved", 121),
        finding("bot-resolved-dismissed", 122, dismissed_but_real=True),
    ],
    threads=[
        thread("bot-resolved", "T_bot", resolved=True, resolved_by="github-actions[bot]"),
        thread("author-resolved", "T_author", resolved=True, resolved_by="a-maintainer"),
        thread("bot-resolved-dismissed", "T_dis", resolved=True,
               resolved_by="github-actions[bot]"),
        thread("no-longer-reported", "T_gone", resolved=False),
        thread("gone-and-resolved", "T_gone2", resolved=True, resolved_by="a-maintainer"),
    ],
)
check("thread the bot resolved is re-opened while the issue stands",
      calls["unresolved"] == ["T_bot"], calls["unresolved"])
check("thread the author resolved is left alone", "T_author" not in calls["unresolved"])
check("dismissed-but-real does not re-open its thread", "T_dis" not in calls["unresolved"])
check("thread for a dropped finding is resolved", calls["resolved"] == ["T_gone"],
      calls["resolved"])
check("already-resolved thread not resolved again", "T_gone2" not in calls["resolved"])

print("\npost: the inline comment cap")
over = cr.MAX_INLINE_COMMENTS + 3
calls = run_post([finding(f"k{i}", 120) for i in range(over)], threads=[])
check("capped", len(calls["inline"]) == cr.MAX_INLINE_COMMENTS, len(calls["inline"]))
check("the overflow is disclosed, not dropped silently",
      "Not posted inline" in calls["summary"][0] and "3 further" in calls["summary"][0],
      calls["summary"][0][-200:])

print("\npost: a clean review")
calls = run_post([], threads=[], summary="Nothing to flag.")
check("nothing posted inline", calls["inline"] == [])
check("summary still written", calls["summary"] == ["Nothing to flag."])

print("\npost: a rejected comment does not lose the review")
saved = cr.post_inline


def boom(*a, **kw):
    raise RuntimeError("HTTP 422: line must be part of the diff")


calls = {"inline": [], "resolved": [], "unresolved": [], "summary": []}
saved_all = (cr.gh, cr.review_threads, cr.post_inline, cr.set_resolved, cr.upsert_summary)
cr.gh = lambda *a, **kw: DIFF
cr.review_threads = lambda repo, pr: []
cr.post_inline = boom
cr.set_resolved = lambda tid, res: None
cr.upsert_summary = lambda repo, pr, body: calls["summary"].append(body)
os.environ["REVIEW_JSON"] = json.dumps(
    {"summary_markdown": "s", "verdict": "block", "findings": [finding("rejected", 120)]})
cr.post()
cr.gh, cr.review_threads, cr.post_inline, cr.set_resolved, cr.upsert_summary = saved_all
check("run completes", len(calls["summary"]) == 1)
check("the rejected finding lands in the summary", "rejected" in calls["summary"][0],
      calls["summary"][0])

print()
if failures:
    print(f"{len(failures)} check(s) FAILED: {failures}")
    sys.exit(1)
print("all checks passed")
