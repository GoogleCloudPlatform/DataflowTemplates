#!/usr/bin/env python3
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
"""Reports Surefire test retries in the GitHub Actions UI.

Reads Surefire TEST-*.xml reports and:
  - writes a Markdown summary to $GITHUB_STEP_SUMMARY (stdout if unset), shown on the workflow
    run page;
  - prints ::warning / ::error workflow commands, shown as annotations on the run and PR checks.

With -Dsurefire.rerunFailingTestsCount=N, Surefire records per <testcase>:
  - failed, then passed on a retry: <flakyFailure> (assertion) / <flakyError> (other exception),
    one per failed run;
  - failed on every run: <failure>/<error> (first run) + <rerunFailure>/<rerunError> (each retry).

Uses only the Python standard library. Always exits 0, so it never fails the job.

Usage: surefire_retry_report.py [--root DIR] [--no-annotate]
"""
import argparse
import os
import sys
import xml.etree.ElementTree as ET

FLAKY_TAGS = ("flakyFailure", "flakyError")
RERUN_TAGS = ("rerunFailure", "rerunError")
FINAL_TAGS = ("failure", "error")

# Limits: a job summary step is capped at 1 MiB, and GitHub only shows a limited number of
# annotations, so keep both bounded.
MAX_MESSAGE_CHARS = 200
MAX_TABLE_ROWS = 200
MAX_ANNOTATIONS_PER_KIND = 20


def find_reports(root):
  """Yields paths of target/surefire-reports/TEST-*.xml files under root."""
  for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules")]
    if os.path.basename(dirpath) != "surefire-reports":
      continue
    for name in filenames:
      if name.startswith("TEST-") and name.endswith(".xml"):
        yield os.path.join(dirpath, name)


def module_of(path, root):
  """Returns the Maven module path for a report, e.g. 'v2/sourcedb-to-spanner'."""
  rel = os.path.relpath(path, root)
  marker = os.sep + "target" + os.sep
  return rel.split(marker)[0] if marker in rel else os.path.dirname(rel)


def first_line(element):
  text = element.get("message") or element.text or ""
  lines = text.strip().splitlines()
  line = lines[0].strip() if lines else ""
  if len(line) > MAX_MESSAGE_CHARS:
    line = line[: MAX_MESSAGE_CHARS - 1] + "…"
  return line


def md_cell(text):
  return text.replace("|", "\\|").replace("\n", " ")


def annotation_text(text):
  # Workflow command values must escape %, CR and LF.
  return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def collect(root):
  stats = {"files": 0, "unparsable": [], "total": 0, "passed": 0, "failed": 0, "skipped": 0}
  flaky, failed_after_retries = [], []
  for path in sorted(find_reports(root)):
    try:
      suite = ET.parse(path).getroot()
    except ET.ParseError as e:
      stats["unparsable"].append(f"{os.path.relpath(path, root)} ({e})")
      continue
    stats["files"] += 1
    module = module_of(path, root)
    for tc in suite.iter("testcase"):
      stats["total"] += 1
      name = f"{tc.get('classname', '').split('.')[-1]}.{tc.get('name', '')}"
      children = list(tc)
      flaky_runs = [c for c in children if c.tag in FLAKY_TAGS]
      reruns = [c for c in children if c.tag in RERUN_TAGS]
      finals = [c for c in children if c.tag in FINAL_TAGS]
      if any(c.tag == "skipped" for c in children):
        stats["skipped"] += 1
      elif finals:
        stats["failed"] += 1
        if reruns:
          first = finals[0]
          failed_after_retries.append(
              dict(module=module, name=name, runs=1 + len(reruns), tag=first.tag,
                   message=first_line(first)))
      else:
        stats["passed"] += 1
        if flaky_runs:
          first = flaky_runs[0]
          flaky.append(
              dict(module=module, name=name, runs=1 + len(flaky_runs), tag=first.tag,
                   message=first_line(first)))
  return stats, flaky, failed_after_retries


def table(rows, header):
  out = [header, "|---|---|---|---|---|"]
  for r in rows[:MAX_TABLE_ROWS]:
    out.append(f"| `{r['name']}` | `{r['module']}` | {r['runs']} | `{r['tag']}` | "
               f"{md_cell(r['message'])} |")
  if len(rows) > MAX_TABLE_ROWS:
    out.append(f"\n_…and {len(rows) - MAX_TABLE_ROWS} more; see the uploaded test report "
               "artifact._")
  out.append("")
  return out


def render_markdown(stats, flaky, failed_after_retries):
  out = ["## 🔁 Integration test retries", ""]
  out.append("| Tests | ✅ Passed | ⚠️ Flaky (passed on retry) | ❌ Failed | "
             "↻ Failed after retries | ⏭ Skipped |")
  out.append("|---|---|---|---|---|---|")
  out.append(f"| {stats['total']} | {stats['passed']} | {len(flaky)} | {stats['failed']} | "
             f"{len(failed_after_retries)} | {stats['skipped']} |")
  out.append("")
  if flaky:
    out.append("### ⚠️ Flaky: failed, then passed on retry")
    out += table(flaky, "| Test | Module | Runs | First failure type | First failure |")
  if failed_after_retries:
    out.append("### ↻ Failed even after retries")
    out += table(failed_after_retries, "| Test | Module | Runs | Type | First failure |")
  if not flaky and not failed_after_retries:
    out.append("_No test was retried._")
    out.append("")
  if stats["unparsable"]:
    out.append("<details><summary>Unparsable report files</summary>")
    out.append("")
    out += [f"- `{u}`" for u in stats["unparsable"]]
    out.append("</details>")
    out.append("")
  out.append(f"<sub>Parsed {stats['files']} Surefire report file(s).</sub>")
  return "\n".join(out) + "\n"


def annotations(flaky, failed_after_retries):
  out = []
  for r in flaky[:MAX_ANNOTATIONS_PER_KIND]:
    retries = r["runs"] - 1
    out.append(f"::warning title=Flaky IT (passed after {retries} "
               f"{'retry' if retries == 1 else 'retries'})::"
               f"{annotation_text(r['name'] + ' [' + r['module'] + ']: ' + r['message'])}")
  for r in failed_after_retries[:MAX_ANNOTATIONS_PER_KIND]:
    out.append(f"::error title=IT failed on all {r['runs']} runs::"
               f"{annotation_text(r['name'] + ' [' + r['module'] + ']: ' + r['message'])}")
  return out


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
  parser.add_argument("--root", default=".", help="Directory to search for Surefire reports")
  parser.add_argument("--no-annotate", action="store_true", help="Do not print annotations")
  args = parser.parse_args(argv)
  try:
    stats, flaky, failed_after_retries = collect(args.root)
    markdown = render_markdown(stats, flaky, failed_after_retries)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
      with open(summary_path, "a", encoding="utf-8") as f:
        f.write(markdown)
    else:
      sys.stdout.write(markdown)
    if not args.no_annotate:
      for line in annotations(flaky, failed_after_retries):
        print(line)
  except Exception as e:  # never fail the job because of the report
    print(f"::warning title=Retry report failed::{annotation_text(repr(e))}")
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
