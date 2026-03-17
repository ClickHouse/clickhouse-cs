#!/usr/bin/env python3
"""Find uncovered lines for a specific file from cobertura XML."""

import os
import sys
import xml.etree.ElementTree as ET

if len(sys.argv) < 3:
    print(
        f"Usage: {sys.argv[0]} <path/to/coverage.cobertura.xml> ... <filename>",
        file=sys.stderr,
    )
    sys.exit(1)

# Last arg is the target filename; everything else is candidate XML paths.
# When shell glob expands to multiple files, pick the most recent one.
target = sys.argv[-1]
xml_path = max(sys.argv[1:-1], key=os.path.getmtime)

tree = ET.parse(xml_path)
found = False

for cls in tree.getroot().findall(".//class"):
    if target in cls.get("filename", ""):
        found = True
        uncovered = [
            l.get("number")
            for l in cls.findall(".//line")
            if int(l.get("hits", 0)) == 0
        ]
        if uncovered:
            print(f"{cls.get('filename')}")
            print(f"  Uncovered lines: {', '.join(uncovered)}")
        else:
            print(f"{cls.get('filename')}: fully covered")

if not found:
    print(f"No classes matching '{target}' found in coverage report.", file=sys.stderr)
    sys.exit(1)
