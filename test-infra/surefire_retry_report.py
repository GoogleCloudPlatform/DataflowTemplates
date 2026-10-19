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


def annotation_property(text):
  # Workflow command property values (e.g. title) must also escape ':' and ','.
  return annotation_text(text).replace(":", "%3A").replace(",", "%2C")


# Prefix SkipRerunOfGenuineFailuresRule puts on the rerun failure of a genuine failure that was
# not re-run (see SkipRerunOfGenuineFailuresRule.NOT_RERUN_MESSAGE_PREFIX).
NOT_RERUN_PREFIX = "Not re-run, genuine failure in an earlier attempt: "


def plural(n, word):
  return f"{n} {word}" if n == 1 else f"{n} {word.replace('retry', 'retries')}"


def collect(root):
  """Returns (stats, rows): one row per test that failed at least once."""
  stats = {"files": 0, "unparsable": [], "total": 0, "passed_first": 0, "flaky": 0,
           "failed": 0, "skipped": 0}
  rows = []
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
        not_rerun = any((c.get("message") or "").startswith(NOT_RERUN_PREFIX) for c in reruns)
        if not_rerun:
          status, title = "❌ Failed: genuine failure, not retried", \
              "IT failed: genuine failure, not retried"
        elif reruns:
          status = f"❌ Failed after {plural(len(reruns), 'retry')}"
          title = f"IT failed after {plural(len(reruns), 'retry')}"
        else:
          status, title = "❌ Failed (no retry)", "IT failed (no retry)"
        rows.append(dict(module=module, name=name, status=status, title=title, final="failed",
                         reason=first_line(finals[0])))
      else:
        if flaky_runs:
          stats["flaky"] += 1
          retries = plural(len(flaky_runs), 'retry')
          rows.append(dict(module=module, name=name, final="flaky",
                           status=f"⚠️ Passed after {retries}",
                           title=f"Flaky IT: passed after {retries}",
                           reason=first_line(flaky_runs[0])))
        else:
          stats["passed_first"] += 1
  # Failures first, then flaky; stable within each group.
  rows.sort(key=lambda r: 0 if r["final"] == "failed" else 1)
  return stats, rows


def render_markdown(stats, rows):
  out = ["## 🔁 Integration test retries", ""]
  out.append("| Tests | ✅ Passed first time | ⚠️ Passed on retry | ❌ Failed | ⏭ Skipped |")
  out.append("|---|---|---|---|---|")
  out.append(f"| {stats['total']} | {stats['passed_first']} | {stats['flaky']} | "
             f"{stats['failed']} | {stats['skipped']} |")
  out.append("")
  if rows:
    out.append("### Tests that failed at least once")
    out.append("| Test | Module | Final status | Failure reason |")
    out.append("|---|---|---|---|")
    for r in rows[:MAX_TABLE_ROWS]:
      out.append(f"| `{r['name']}` | `{r['module']}` | {r['status']} | {md_cell(r['reason'])} |")
    if len(rows) > MAX_TABLE_ROWS:
      out.append(f"\n_…and {len(rows) - MAX_TABLE_ROWS} more; see the uploaded test report "
                 "artifact._")
    out.append("")
  else:
    out.append("_No test failed._")
    out.append("")
  if stats["unparsable"]:
    out.append("<details><summary>Unparsable report files</summary>")
    out.append("")
    out += [f"- `{u}`" for u in stats["unparsable"]]
    out.append("</details>")
    out.append("")
  out.append(f"<sub>Parsed {stats['files']} Surefire report file(s). Failure reason is the error "
             "from the first failed attempt.</sub>")
  return "\n".join(out) + "\n"


def annotations(rows):
  out, counts = [], {"warning": 0, "error": 0}
  for r in rows:
    level = "warning" if r["final"] == "flaky" else "error"
    if counts[level] >= MAX_ANNOTATIONS_PER_KIND:
      continue
    counts[level] += 1
    out.append(f"::{level} title={annotation_property(r['title'])}::"
               f"{annotation_text(r['name'] + ' [' + r['module'] + ']: ' + r['reason'])}")
  return out


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
  parser.add_argument("--root", default=".", help="Directory to search for Surefire reports")
  parser.add_argument("--no-annotate", action="store_true", help="Do not print annotations")
  args = parser.parse_args(argv)
  try:
    stats, rows = collect(args.root)
    markdown = render_markdown(stats, rows)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
      with open(summary_path, "a", encoding="utf-8") as f:
        f.write(markdown)
    else:
      sys.stdout.write(markdown)
    if not args.no_annotate:
      for line in annotations(rows):
        print(line)
  except Exception as e:  # never fail the job because of the report
    print(f"::warning title=Retry report failed::{annotation_text(repr(e))}")
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
