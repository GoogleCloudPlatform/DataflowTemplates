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
Each <stats_dir>/<mode>/ holds host.txt, host_samples.csv, container_samples.csv, console.log and
surefire/<module>/TEST-*.xml. Prints markdown tables:
  1. Machine: CPU, memory, disk, containers, Java memory.
  2. Containers per database engine: how many started, peak at once, CPU-seconds, memory.
  3. Tests per template and source: passed / failed / skipped, average / max / total time.
  4. Shared database servers per template and engine: databases, connections.
"""
import collections
import csv
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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


def _peak(values):
  return f"{max(values):.0f}" if values else "n/a"


def _duration_min(rows):
  if len(rows) < 2:
    return "n/a"
  start = datetime.strptime(rows[0]["ts"], TS_FORMAT)
  end = datetime.strptime(rows[-1]["ts"], TS_FORMAT)
  return f"{(end - start).total_seconds() / 60:.1f}"


def _cpu_seconds(rows, key):
  """Integrates a CPU % column over time (100% for 1 s = 1 CPU-second)."""
  total = 0.0
  prev = None
  for r in rows:
    ts = datetime.strptime(r["ts"], TS_FORMAT)
    if prev is not None:
      try:
        total += float(r[key]) / 100 * (ts - prev).total_seconds()
      except (KeyError, ValueError):
        pass
    prev = ts
  return total


def _engine_of_image(image):
  image = image.lower()
  for key, engine in [("ryuk", "ryuk (testcontainers)"), ("mysql", "mysql"),
                      ("postgres", "postgres"), ("cassandra", "cassandra"),
                      ("oracle", "oracle"), ("mssql", "sqlserver")]:
    if key in image:
      return engine
  return "other (" + image + ")"


def _source_of_test(name):
  for pattern, source in [(r"MySql|MySQL", "mysql"), (r"Oracle", "oracle"),
                          (r"SQLServer|SqlServer|MSSQL", "sqlserver"),
                          (r"Cassandra", "cassandra"), (r"Astra", "astra"),
                          (r"PostgreSQL|Postgres", "postgres")]:
    if re.search(pattern, name):
      return source
  return "other / none"


def _machine_table(stats_dir, modes, data):
  metrics = [
      ("Monitored time (min)", lambda h, c: _duration_min(h)),
      ("Host CPU busy % (peak / avg)", lambda h, c: _peak_avg(_num(h, "cpu_busy_pct"))),
      ("Host CPU-seconds (all cores)", None),
      ("Load average 1m (peak / avg)", lambda h, c: _peak_avg(_num(h, "load1"))),
      ("Host RAM used MB (peak / avg)", lambda h, c: _peak_avg(_num(h, "mem_used_mb"))),
      ("Host RAM total MB", lambda h, c: _peak(_num(h, "mem_total_mb"))),
      ("Swap used MB (peak)", lambda h, c: _peak(_num(h, "swap_used_mb"))),
      ("Disk used GB (peak)", lambda h, c: _peak(_num(h, "disk_used_gb"))),
      ("Containers running (peak / avg)", lambda h, c: _peak_avg(_num(h, "containers_running"))),
      ("Distinct containers seen", lambda h, c: str(len({r["container"] for r in c}))),
      ("Containers CPU % (peak / avg)", lambda h, c: _peak_avg(_num(h, "containers_cpu_pct"))),
      ("Containers CPU-seconds", lambda h, c: f"{_cpu_seconds(h, 'containers_cpu_pct'):.0f}"),
      ("Containers RAM MB (peak / avg)", lambda h, c: _peak_avg(_num(h, "containers_mem_mb"))),
      ("Java RSS MB (peak / avg)", lambda h, c: _peak_avg(_num(h, "java_rss_mb"))),
  ]
  lines = ["### Machine", "", "| Metric | " + " | ".join(modes) + " |",
           "|---|" + "---|" * len(modes)]
  for name, fn in metrics:
    if fn is None:  # Host CPU-seconds needs each machine's core count.
      cells = [f"{_cpu_seconds(data[m][0], 'cpu_busy_pct') * _cores(stats_dir, m):.0f}"
               if data[m][0] else "n/a" for m in modes]
    else:
      cells = [fn(*data[m]) for m in modes]
    lines.append(f"| {name} | " + " | ".join(cells) + " |")
  return lines


def _cores(stats_dir, mode):
  host_txt = os.path.join(stats_dir, mode, "host.txt")
  if os.path.exists(host_txt):
    m = re.search(r"nproc=(\d+)", open(host_txt).read())
    if m:
      return int(m.group(1))
  return 1


def _sample_seconds(host_rows):
  """Maps each sample timestamp to the seconds since the previous sample (capped at 30 s)."""
  out = {}
  prev = None
  for r in host_rows:
    ts = datetime.strptime(r["ts"], TS_FORMAT)
    out[r["ts"]] = min((ts - prev).total_seconds(), 30.0) if prev is not None else 10.0
    prev = ts
  return out


def _container_engine_table(modes, data):
  lines = ["", "### Containers per database engine", "",
           "| Mode | Engine | Containers started | Peak running at once | CPU-seconds | "
           "RAM MB (peak / avg, all of that engine) |",
           "|---|---|---|---|---|---|"]
  for mode in modes:
    h, c = data[mode]
    dt = _sample_seconds(h)
    if c and "image" not in c[0]:
      lines.append(f"| {mode} | (no image column in this run) | | | | |")
      continue
    by_engine = collections.defaultdict(list)
    for r in c:
      by_engine[_engine_of_image(r["image"])].append(r)
    for engine, rows in sorted(by_engine.items()):
      names = {r["container"] for r in rows}
      per_ts = collections.defaultdict(lambda: [0, 0.0, 0.0])
      for r in rows:
        agg = per_ts[r["ts"]]
        agg[0] += 1
        agg[1] += float(r["cpu_pct"])
        agg[2] += float(r["mem_mb"])
      ordered = sorted(per_ts.items())
      cpu_seconds = sum(v[1] / 100 * dt.get(ts, 10.0) for ts, v in ordered)
      lines.append(
          f"| {mode} | {engine} | {len(names)} | {max(v[0] for _, v in ordered)} | "
          f"{cpu_seconds:.0f} | "
          f"{_peak_avg([v[2] for _, v in ordered])} |")
  return lines


def _tests_table(stats_dir, modes):
  lines = ["", "### Tests per template and source", "",
           "| Mode | Template | Source | Tests | Passed | Failed | Skipped | "
           "Avg s | Max s | Total min |",
           "|---|---|---|---|---|---|---|---|---|---|"]
  for mode in modes:
    groups = collections.defaultdict(lambda: [0, 0, 0, []])
    for path in glob.glob(os.path.join(stats_dir, mode, "surefire", "**", "TEST-*.xml"),
                          recursive=True):
      rel = os.path.relpath(path, os.path.join(stats_dir, mode, "surefire"))
      module = rel.split(os.sep)[0] if os.sep in rel else "(unknown)"
      for tc in ET.parse(path).getroot().iter("testcase"):
        cls = (tc.get("classname") or "").split(".")[-1]
        g = groups[(module, _source_of_test(cls))]
        g[0] += 1
        if tc.find("skipped") is not None:
          g[2] += 1
        elif tc.find("failure") is not None or tc.find("error") is not None:
          g[1] += 1
          g[3].append(float(tc.get("time", 0)))
        else:
          g[3].append(float(tc.get("time", 0)))
    for (module, source), (n, failed, skipped, times) in sorted(groups.items()):
      ran = [t for t in times if t > 0]
      lines.append(
          f"| {mode} | {module} | {source} | {n} | {n - failed - skipped} | {failed} | "
          f"{skipped} | {sum(ran) / len(ran) if ran else 0:.0f} | {max(ran) if ran else 0:.0f} | "
          f"{sum(ran) / 60:.1f} |")
  return lines


def _shared_usage(mode_dir):
  """Returns {(module, engine): last usage dict} from 'Shared <engine> container usage' lines."""
  usage = {}
  pattern = re.compile(r"Shared (\w+) container usage(?: \[([^\]]*)\])?: \{(.*)\}")
  paths = [os.path.join(mode_dir, "console.log")]
  paths += glob.glob(os.path.join(mode_dir, "surefire", "**", "*-output.txt"), recursive=True)
  for path in [p for p in paths if os.path.exists(p)]:
    with open(path, errors="replace") as f:
      for line in f:
        m = pattern.search(line)
        if m:
          fields = dict(kv.split("=", 1) for kv in m.group(3).split(", ") if "=" in kv)
          # Lines are in time order per server, so the last one has the final state.
          usage[(m.group(2) or "(unknown)", m.group(1))] = fields
  return usage


def _connections_table(stats_dir, modes):
  lines = ["", "### Shared database servers", "",
           "| Mode | Template | Engine | Databases created / dropped / left | Max connections | "
           "Peak used (server) | Peak sampled | Refused | Total connections |",
           "|---|---|---|---|---|---|---|---|---|"]
  found = False
  for mode in modes:
    for (module, engine), u in sorted(_shared_usage(os.path.join(stats_dir, mode)).items()):
      found = True
      if engine == "cassandra":
        dbs = f"{u.get('keyspacesSeen', 'n/a')} seen / - / {u.get('keyspacesLeft', 'n/a')} (keyspaces)"
      else:
        dbs = (f"{u.get('databasesCreated', 'n/a')} / {u.get('databasesDropped', 'n/a')} / "
               f"{u.get('databasesLeft', 'n/a')}")
      lines.append(
          f"| {mode} | {module} | {engine} | {dbs} | {u.get('maxConnections', 'n/a')} | "
          f"{u.get('maxUsedConnections', 'n/a')} | {u.get('peakSampledConnections', 'n/a')} | "
          f"{u.get('refusedConnections', 'n/a')} | {u.get('totalConnections', 'n/a')} |")
  if not found:
    lines.append("| - | - | - | no shared server usage logged (expected for baseline) | | | | | |")
  return lines


def _per_minute(host_rows):
  """Returns {minute: (ram_gb, cpu_cores, max_containers)} of all running Testcontainers."""
  if not host_rows:
    return {}
  start = datetime.strptime(host_rows[0]["ts"], TS_FORMAT)
  buckets = collections.defaultdict(list)
  for r in host_rows:
    minute = int((datetime.strptime(r["ts"], TS_FORMAT) - start).total_seconds() // 60) + 1
    buckets[minute].append(
        (float(r["containers_mem_mb"]), float(r["containers_cpu_pct"]),
         int(r["containers_running"])))
  return {m: (sum(v[0] for v in vs) / len(vs) / 1024, sum(v[1] for v in vs) / len(vs) / 100,
              max(v[2] for v in vs)) for m, vs in buckets.items()}


def _per_minute_tables(modes, data):
  per = {mode: _per_minute(data[mode][0]) for mode in modes}
  lines = ["### Testcontainers total per minute: summary", "",
           "| | " + " | ".join(modes) + " |", "|---|" + "---|" * len(modes)]
  for label, i, unit in [("RAM", 0, "GB"), ("CPU", 1, "cores")]:
    for agg, fn in [("peak", max), ("avg", lambda x: sum(x) / len(x))]:
      cells = [f"{fn([v[i] for v in per[m].values()]):.2f}" if per[m] else "n/a" for m in modes]
      lines.append(f"| {label} {agg} ({unit}) | " + " | ".join(cells) + " |")
  lines.append("| Readings (minutes) | " + " | ".join(str(len(per[m])) for m in modes) + " |")
  lines += ["", "### Testcontainers total per minute", "",
            "| Min | " + " | ".join(f"{m} RAM (GB) | {m} CPU (cores) | {m} containers"
                                   for m in modes) + " |",
            "|---|" + "---|---|---|" * len(modes)]
  for minute in range(1, max((max(p) for p in per.values() if p), default=0) + 1):
    cells = []
    for m in modes:
      v = per[m].get(minute)
      cells += [f"{v[0]:.2f}", f"{v[1]:.2f}", str(v[2])] if v else ["-", "-", "-"]
    lines.append(f"| {minute} | " + " | ".join(cells) + " |")
  lines.append("")
  return lines


def summarize(stats_dir, modes):
  data = {}
  for mode in modes:
    d = os.path.join(stats_dir, mode)
    data[mode] = (_rows(os.path.join(d, "host_samples.csv")),
                  _rows(os.path.join(d, "container_samples.csv")))
  lines = _per_minute_tables(modes, data)
  lines += _machine_table(stats_dir, modes, data)
  lines += _container_engine_table(modes, data)
  lines += _tests_table(stats_dir, modes)
  lines += _connections_table(stats_dir, modes)
  lines.append("")
  for mode in modes:
    host_txt = os.path.join(stats_dir, mode, "host.txt")
    host = open(host_txt).read().strip().replace("\n", "; ") if os.path.exists(host_txt) else "n/a"
    lines.append(f"- **{mode}** host: {host}")
  return "\n".join(lines)


if __name__ == "__main__":
  report = "## S6 resource usage\n\n" + summarize(sys.argv[1], sys.argv[2:])
  print(report)
  summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
  if summary_file:
    with open(summary_file, "a") as f:
      f.write(report + "\n")
