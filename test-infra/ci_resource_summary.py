#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Compares runs recorded by ci_resource_monitor.sh side by side.

Usage: ci_resource_summary.py <stats_dir> <mode> [<mode> ...]
Each <stats_dir>/<mode>/ holds host.txt, host_samples.csv, container_samples.csv and
optionally surefire/ (copied surefire reports). Prints a markdown table.
"""
import csv
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime


def _rows(path):
  if not os.path.exists(path):
    return []
  with open(path, newline="") as f:
    return list(csv.DictReader(f))


def _num(rows, key):
  out = []
  for r in rows:
    try:
      out.append(float(r[key]))
    except (KeyError, TypeError, ValueError):
      pass
  return out


def _peak_avg(values):
  if not values:
    return "n/a"
  return f"{max(values):.0f} / {sum(values) / len(values):.0f}"


def _duration_min(rows):
  if len(rows) < 2:
    return "n/a"
  fmt = "%Y-%m-%dT%H:%M:%SZ"
  start = datetime.strptime(rows[0]["ts"], fmt)
  end = datetime.strptime(rows[-1]["ts"], fmt)
  return f"{(end - start).total_seconds() / 60:.1f}"


def _tests(mode_dir):
  total = failed = 0
  times = []
  for path in glob.glob(os.path.join(mode_dir, "surefire", "TEST-*.xml")):
    root = ET.parse(path).getroot()
    total += int(root.get("tests", 0))
    failed += int(root.get("failures", 0)) + int(root.get("errors", 0))
    times.append(float(root.get("time", 0)))
  if not total:
    return "n/a", "n/a"
  return f"{total - failed}/{total} passed", f"{max(times):.0f}"


def _mysql_usage(mode_dir):
  last = None
  for path in glob.glob(os.path.join(mode_dir, "surefire", "*-output.txt")):
    with open(path, errors="replace") as f:
      for line in f:
        if "Shared mysql container usage:" in line:
          last = line.split("usage:", 1)[1].strip()
  return last or "n/a (no shared container)"


def summarize(stats_dir, modes):
  metrics = [
      ("Tests", lambda d, h, c: _tests(d)[0]),
      ("Longest test (s)", lambda d, h, c: _tests(d)[1]),
      ("Monitored time (min)", lambda d, h, c: _duration_min(h)),
      ("Host CPU busy % (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "cpu_busy_pct"))),
      ("Load average 1m (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "load1"))),
      ("Host RAM used MB (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "mem_used_mb"))),
      ("Host RAM total MB", lambda d, h, c: _peak_avg(_num(h, "mem_total_mb")).split(" ")[0]),
      ("Swap used MB (peak)", lambda d, h, c: _peak_avg(_num(h, "swap_used_mb")).split(" ")[0]),
      ("Disk used GB (peak)", lambda d, h, c: _peak_avg(_num(h, "disk_used_gb")).split(" ")[0]),
      ("Containers running (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "containers_running"))),
      ("Distinct containers seen", lambda d, h, c: str(len({r["container"] for r in c}))),
      ("Containers CPU % (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "containers_cpu_pct"))),
      ("Containers RAM MB (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "containers_mem_mb"))),
      ("Java RSS MB (peak / avg)", lambda d, h, c: _peak_avg(_num(h, "java_rss_mb"))),
  ]
  data = {}
  for mode in modes:
    d = os.path.join(stats_dir, mode)
    data[mode] = (d, _rows(os.path.join(d, "host_samples.csv")),
                  _rows(os.path.join(d, "container_samples.csv")))

  lines = ["| Metric | " + " | ".join(modes) + " |",
           "|---|" + "---|" * len(modes)]
  for name, fn in metrics:
    lines.append(f"| {name} | " + " | ".join(fn(*data[m]) for m in modes) + " |")
  lines.append("")
  for mode in modes:
    host_txt = os.path.join(stats_dir, mode, "host.txt")
    host = open(host_txt).read().strip().replace("\n", "; ") if os.path.exists(host_txt) else "n/a"
    lines.append(f"- **{mode}** host: {host}")
    lines.append(f"- **{mode}** shared MySQL: {_mysql_usage(os.path.join(stats_dir, mode))}")
  return "\n".join(lines)


if __name__ == "__main__":
  report = "## S6 resource usage\n\n" + summarize(sys.argv[1], sys.argv[2:])
  print(report)
  summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
  if summary_file:
    with open(summary_file, "a") as f:
      f.write(report + "\n")
