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

"""Compares YAML blueprint transform config parameters against Beam YAML docs."""

import argparse
from pathlib import Path
import sys
from typing import Any
import urllib.parse
import urllib.request

from bs4 import BeautifulSoup
import jinja2
import jinja2.meta
from tabulate import tabulate
import yaml

DEFAULT_YAMLDOC_URL = "https://beam.apache.org/releases/yamldoc/current/"


def fetch_yamldoc_html(url: str = DEFAULT_YAMLDOC_URL) -> str:
    """Fetches the Beam YAML Transform Index HTML from the given HTTPS URL."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"Only valid HTTPS URLs are permitted, got: {url!r}")
    req = urllib.request.Request(
        url, headers={"User-Agent": "DataflowTemplates-YamlConfigChecker/1.0"}
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8")


def parse_beam_yamldoc_html(
    raw_html: str,
) -> tuple[str, dict[str, dict[str, dict[str, Any]]]]:
    """Parses Beam YAML documentation HTML into transform config parameter specs."""
    soup = BeautifulSoup(raw_html, "html.parser")
    ver_div = soup.select_one("div.version")
    beam_version = ver_div.get_text(strip=True) if ver_div else "unknown"

    doc_transforms: dict[str, dict[str, dict[str, Any]]] = {}
    for h2 in soup.find_all("h2", id=True):
        params: dict[str, dict[str, Any]] = {}
        for sib in h2.find_next_siblings():
            if sib.name in ("hr", "h2"):
                break
            if sib.name == "h3" and sib.get("id", "").startswith("configuration"):
                nxt = sib.find_next_sibling()
                if nxt and nxt.name == "ul":
                    for li in nxt.find_all("li", recursive=False):
                        strong, code = li.find("strong"), li.find("code")
                        if strong and code:
                            first_line = li.get_text().split(":", 1)[0]
                            params[strong.get_text(strip=True)] = {
                                "type": code.get_text(strip=True),
                                "optional": "(Optional)" in first_line,
                            }
            elif sib.name == "h3" and sib.get("id", "").startswith("usage"):
                nxt = sib.find_next_sibling("div", class_="codehilite")
                if nxt:
                    try:
                        usage = yaml.safe_load(nxt.get_text())
                        if isinstance(usage, dict) and isinstance(usage.get("config"), dict):
                            for k in usage["config"]:
                                params.setdefault(k, {"type": "?", "optional": True})
                    except yaml.YAMLError:
                        pass
        doc_transforms[h2.get_text(strip=True)] = params

    return beam_version, doc_transforms


def extract_blueprint_transforms(yaml_path: Path) -> tuple[list[dict[str, Any]], bool]:
    """Renders a Jinja YAML blueprint and extracts its transform types and config keys."""
    source = yaml_path.read_text(encoding="utf-8")
    env = jinja2.Environment()
    undeclared = jinja2.meta.find_undeclared_variables(env.parse(source))
    rendered = env.from_string(source).render({v: "placeholder" for v in undeclared})

    spec = yaml.safe_load(rendered) or {}
    raw_transforms = spec.get("pipeline", {}).get("transforms", [])
    extracted = [
        {
            "type": t.get("type", ""),
            "name": t.get("name") or t.get("type", ""),
            "config_keys": [
                k for k in (t.get("config") or {}) if not str(k).startswith("__")
            ],
        }
        for t in raw_transforms
        if isinstance(t, dict)
    ]
    return extracted, bool(spec.get("providers"))


def compare_blueprints(
    input_path: Path,
    doc_transforms: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Compares YAML blueprints in input_path against parsed Beam YAML docs."""
    yaml_files = (
        [input_path] if input_path.is_file() else sorted(input_path.glob("*.yaml"))
    )
    results: list[dict[str, Any]] = []
    for yaml_path in yaml_files:
        transforms, has_custom_providers = extract_blueprint_transforms(yaml_path)
        transform_reports = []
        for t in transforms:
            t_type, cfg_keys = t["type"], set(t["config_keys"])
            doc_params = doc_transforms.get(t_type)
            if doc_params is None:
                transform_reports.append({
                    "type": t_type,
                    "name": t["name"],
                    "in_beam_docs": False,
                    "has_custom_providers": has_custom_providers,
                    "invalid_params": [],
                    "missing_required_params": [],
                    "missing_optional_params": [],
                })
                continue

            doc_keys = set(doc_params.keys())
            missing = sorted(doc_keys - cfg_keys)
            transform_reports.append({
                "type": t_type,
                "name": t["name"],
                "in_beam_docs": True,
                "has_custom_providers": has_custom_providers,
                "invalid_params": sorted(cfg_keys - doc_keys),
                "missing_required_params": [k for k in missing if not doc_params[k]["optional"]],
                "missing_optional_params": [k for k in missing if doc_params[k]["optional"]],
            })
        results.append({"blueprint": yaml_path.name, "transforms": transform_reports})
    return results


def _fmt_params(params: list[str]) -> str:
    return ", ".join(f"`{p}`" for p in params)


def generate_markdown_report(
    results: list[dict[str, Any]],
    beam_version: str,
    yamldoc_url: str = DEFAULT_YAMLDOC_URL,
) -> tuple[str, bool, bool]:
    """Generates a Markdown report from blueprint comparison results."""
    invalid_rows, missing_req_rows, missing_opt_rows, custom_rows = [], [], [], []

    for bp in results:
        b = f"`{bp['blueprint']}`"
        for t in bp["transforms"]:
            row_prefix = [b, f"`{t['type']}`", f"`{t['name']}`"]
            if not t["in_beam_docs"]:
                custom_rows.append([*row_prefix, f"`{t['has_custom_providers']}`"])
                continue
            if t["invalid_params"]:
                invalid_rows.append([*row_prefix, _fmt_params(t["invalid_params"])])
            if t["missing_required_params"]:
                missing_req_rows.append([*row_prefix, _fmt_params(t["missing_required_params"])])
            if t["missing_optional_params"]:
                missing_opt_rows.append([*row_prefix, _fmt_params(t["missing_optional_params"])])

    has_invalid_or_req = bool(invalid_rows or missing_req_rows)
    has_any_mismatch = bool(invalid_rows or missing_req_rows or missing_opt_rows)

    summary_table = tabulate(
        [
            ["**Beam YAML Doc Version**", f"`{beam_version}`"],
            ["**Blueprints Checked**", len(results)],
            ["**Transforms with Invalid / Undocumented Config Keys**", len(invalid_rows)],
            ["**Transforms Missing Required Documented Keys**", len(missing_req_rows)],
            ["**Transforms Missing Optional Documented Keys**", len(missing_opt_rows)],
            ["**Custom Provider Transforms**", len(custom_rows)],
        ],
        headers=["Metric", "Count"],
        tablefmt="github",
    )

    sections = [
        "# YAML Blueprint Config Parameter Report\n\n"
        f"Comparison of YAML blueprints in `yaml/src/main/yaml/` against [Beam YAML Transform Index ({beam_version})]({yamldoc_url}).",
        f"## Summary\n\n{summary_table}",
        "## 🚨 Invalid / Unknown Config Parameters in Blueprints\n\n"
        + (
            "These parameters are used in blueprint `config:` blocks but do **not** exist in the published Beam YAML documentation:\n\n"
            + tabulate(
                invalid_rows,
                headers=["Blueprint", "Transform Type", "Step Name", "Invalid / Unknown Parameters"],
                tablefmt="github",
            )
            if invalid_rows
            else "No invalid or unknown config parameters found in blueprints."
        ),
    ]

    if missing_req_rows:
        sections.append(
            "## ❗ Documented Required Parameters Missing from Blueprints\n\n"
            + tabulate(
                missing_req_rows,
                headers=["Blueprint", "Transform Type", "Step Name", "Missing Required Parameters"],
                tablefmt="github",
            )
        )

    sections.append(
        "## ⚠️ Documented Optional Parameters Missing from Blueprints\n\n"
        + (
            "These optional parameters exist in the Beam YAML documentation for the transform but are not exposed in the blueprint's `config:`:\n\n"
            + tabulate(
                missing_opt_rows,
                headers=["Blueprint", "Transform Type", "Step Name", "Missing Optional Parameters"],
                tablefmt="github",
            )
            if missing_opt_rows
            else "No documented optional parameters missing from blueprints."
        )
    )

    if custom_rows:
        sections.append(
            "## ℹ️ Custom Provider Transforms\n\n"
            "Transforms not present in standard Beam YAML docs (defined via custom `providers:` in the blueprint):\n\n"
            + tabulate(
                custom_rows,
                headers=["Blueprint", "Transform Type", "Step Name", "Custom Provider Declared"],
                tablefmt="github",
            )
        )

    return "\n\n".join(sections) + "\n", has_any_mismatch, has_invalid_or_req


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare YAML blueprint transform config parameters against Beam YAML documentation."
    )
    parser.add_argument("input_dir", nargs="?", default="yaml/src/main/yaml")
    parser.add_argument("--url", default=DEFAULT_YAMLDOC_URL)
    parser.add_argument("--markdown-output")
    parser.add_argument("--github-output")
    parser.add_argument("--fail-on-invalid", action="store_true")
    parser.add_argument("--fail-on-mismatch", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input_dir)
    if not input_path.exists():
        print(f"Error: Input path {input_path} does not exist.", file=sys.stderr)
        return 1

    beam_version, doc_transforms = parse_beam_yamldoc_html(fetch_yamldoc_html(args.url))
    results = compare_blueprints(input_path, doc_transforms)
    report, has_any_mismatch, has_invalid_or_req = generate_markdown_report(
        results, beam_version, args.url
    )
    print(report)

    if args.markdown_output:
        Path(args.markdown_output).write_text(report, encoding="utf-8")
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as f:
            f.write(
                f"has_mismatches={str(has_any_mismatch).lower()}\n"
                f"has_invalid_params={str(has_invalid_or_req).lower()}\n"
                f"beam_version={beam_version}\n"
            )

    if (args.fail_on_mismatch and has_any_mismatch) or (
        args.fail_on_invalid and has_invalid_or_req
    ):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
