#!/usr/bin/env python3
#
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
"""Maintains the pinned integration test run order for the Spanner templates.

The Spanner template modules run their ITs with Surefire `runOrder=balanced` and
`runOrderStatisticsFileChecksum=spanner-it`, so Surefire orders IT classes using
`<module>/.surefire-spanner-it`. This script maintains that file so that the class
order is deterministic and starts the longest tests first.

Statistics file format (Surefire 3.5.5 RunEntryStatisticsMap): one line per class,
`<successfulBuilds>,<runtimeMs>,<fully.qualified.ClassName>`. Comments and blank lines
are not allowed. Surefire ranks a class by the SUM of the runtimes of its lines, so we
write exactly ONE line per class holding the class's LONGEST test. Values are kept
strictly unique, because Surefire breaks ties in HashMap order (non-deterministic).

Commands:
  install   Copy the canonical file `<module>/src/test/resources/it-run-order.stats` to
            `<module>/.surefire-spanner-it`, adding any IT class that is missing from it
            with the median runtime. Run by the Spanner PR workflow before the ITs.
            Without this, a missing class gets priority 0 and runtime 0 in Surefire's
            scheduler, which can push the longest class to the end of the queue.
  generate  Regenerate the canonical file from Surefire `TEST-*.xml` reports, e.g. the
            `surefire-integration-test-results` artifact of one or more Spanner PR runs.
            Uses the longest test case of each class (median across runs). Classes that
            have no report keep their previous value, or get the median if new.

Examples:
  python3 it_run_order.py install --module-dir v2/datastream-to-spanner
  python3 it_run_order.py generate --module-dir v2/datastream-to-spanner \
      --reports ~/Downloads/run1 ~/Downloads/run2
"""
import argparse
import os
import re
import statistics
import sys
import xml.etree.ElementTree as ET

CANONICAL_STATS = os.path.join("src", "test", "resources", "it-run-order.stats")
DEFAULT_CHECKSUM = "spanner-it"
_ABSTRACT_CLASS = re.compile(r"\babstract\s+class\b")


def discover_it_classes(module_dir):
  """Returns the fully qualified names of the concrete *IT classes of a module."""
  root = os.path.join(module_dir, "src", "test", "java")
  classes = set()
  for dirpath, _, filenames in os.walk(root):
    for name in filenames:
      if not name.endswith("IT.java"):
        continue
      path = os.path.join(dirpath, name)
      with open(path, encoding="utf-8") as f:
        if _ABSTRACT_CLASS.search(f.read()):
          continue
      rel = os.path.relpath(path, root)[: -len(".java")]
      classes.add(rel.replace(os.sep, "."))
  return classes


def read_stats(path):
  """Reads a statistics file into {class: runtimeMs} (one line per class expected)."""
  stats = {}
  if not os.path.exists(path):
    return stats
  with open(path, encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      parts = line.split(",")
      if len(parts) != 3:
        raise ValueError(f"{path}: expected '<builds>,<ms>,<class>', got: {line}")
      stats[parts[2]] = max(stats.get(parts[2], 0), int(parts[1]))
  return stats


def write_stats(path, stats):
  """Writes {class: runtimeMs} longest first, with strictly unique runtimes."""
  rows = sorted(stats.items(), key=lambda kv: (-kv[1], kv[0]))
  lines, previous = [], None
  for clazz, ms in rows:
    if previous is not None and ms >= previous:
      ms = previous - 1
    ms = max(ms, 1)
    lines.append(f"1,{ms},{clazz}")
    previous = ms
  with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
  return len(lines)


def fill_missing(stats, classes):
  """Adds classes missing from stats with the median runtime. Returns the added classes."""
  missing = sorted(classes - set(stats))
  if missing:
    default = int(statistics.median(stats.values())) if stats else 600000
    for clazz in missing:
      stats[clazz] = default
  return missing


def longest_test_per_class(report_paths):
  """Returns {class: [longest test ms per report file]} from Surefire TEST-*.xml files."""
  files = []
  for p in report_paths:
    if os.path.isfile(p):
      files.append(p)
      continue
    for dirpath, _, filenames in os.walk(p):
      files += [os.path.join(dirpath, n) for n in filenames
                if n.startswith("TEST-") and n.endswith(".xml")]
  result = {}
  for path in files:
    suite = ET.parse(path).getroot()
    clazz = suite.get("name")
    times = [float(tc.get("time", "0").replace(",", ""))
             for tc in suite.iter("testcase") if tc.find("skipped") is None]
    if not times:
      continue
    result.setdefault(clazz, []).append(int(max(times) * 1000))
  return result


def install(args):
  canonical = args.stats or os.path.join(args.module_dir, CANONICAL_STATS)
  stats = read_stats(canonical)
  missing = fill_missing(stats, discover_it_classes(args.module_dir))
  target = os.path.join(args.module_dir, f".surefire-{args.checksum}")
  count = write_stats(target, stats)
  print(f"{target}: {count} classes ({len(missing)} not in {canonical}, using median)")
  for clazz in missing:
    print(f"  missing: {clazz}")


def generate(args):
  canonical = args.stats or os.path.join(args.module_dir, CANONICAL_STATS)
  classes = discover_it_classes(args.module_dir)
  previous = read_stats(canonical)
  measured = longest_test_per_class(args.reports)
  stats = {c: int(statistics.median(v)) for c, v in measured.items() if c in classes}
  kept = {c: ms for c, ms in previous.items() if c in classes and c not in stats}
  stats.update(kept)
  missing = fill_missing(stats, classes)
  count = write_stats(canonical, stats)
  print(f"{canonical}: {count} classes ({len(stats) - len(kept) - len(missing)} measured, "
        f"{len(kept)} kept from previous file, {len(missing)} defaulted to median)")


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)
  sub = parser.add_subparsers(dest="command", required=True)
  for name, func in (("install", install), ("generate", generate)):
    p = sub.add_parser(name)
    p.add_argument("--module-dir", required=True, help="Maven module, e.g. v2/datastream-to-spanner")
    p.add_argument("--stats", help=f"Canonical stats file (default: <module>/{CANONICAL_STATS})")
    p.set_defaults(func=func)
    if name == "install":
      p.add_argument("--checksum", default=DEFAULT_CHECKSUM,
                     help="Must match runOrderStatisticsFileChecksum in the module pom")
    else:
      p.add_argument("--reports", nargs="+", required=True,
                     help="TEST-*.xml files or directories containing them")
  args = parser.parse_args(argv)
  args.func(args)


if __name__ == "__main__":
  main(sys.argv[1:])
