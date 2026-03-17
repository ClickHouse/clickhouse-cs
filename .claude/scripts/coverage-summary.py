#!/usr/bin/env python3
"""Per-file coverage summary from cobertura XML, sorted worst-first."""

import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

# Parse args: XML paths come first, then optional --changed [ref]
xml_paths = []
changed_ref = None
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--changed":
        changed_ref = sys.argv[i + 1] if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("-") else "HEAD"
        if changed_ref != "HEAD":
            i += 1
        i += 1
    else:
        xml_paths.append(sys.argv[i])
        i += 1

if not xml_paths:
    print(f"Usage: {sys.argv[0]} <coverage.xml> ... [--changed [ref]]", file=sys.stderr)
    sys.exit(1)

# When shell glob expands to multiple files, pick the most recent one
xml_path = max(xml_paths, key=os.path.getmtime)

# If --changed, get list of changed files from git
changed_files = None
if changed_ref is not None:
    result = subprocess.run(
        ["git", "diff", "--name-only", changed_ref],
        capture_output=True, text=True,
    )
    changed_files = set(os.path.basename(f) for f in result.stdout.strip().splitlines())

tree = ET.parse(xml_path)
by_file = defaultdict(lambda: [0, 0])

for cls in tree.getroot().findall(".//class"):
    lines = cls.findall(".//line")
    if not lines:
        continue
    f = cls.get("filename", "")
    if changed_files is not None and os.path.basename(f) not in changed_files:
        continue
    by_file[f][0] += sum(1 for l in lines if int(l.get("hits", 0)) > 0)
    by_file[f][1] += len(lines)

if changed_files is not None and not by_file:
    print("No changed files found in coverage report.", file=sys.stderr)
    sys.exit(0)

for pct, path, cov, tot in sorted(
    [(c / t * 100 if t else 0, f, c, t) for f, (c, t) in by_file.items()]
):
    print(f"{pct:5.1f}%  ({cov:3d}/{tot:3d})  {path}")
