# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dataflow Templates Container VEX Automation

This directory maintains the canonical Vulnerability Exploitability eXchange (VEX) configuration and automation for Dataflow Flex Template Base (`gcr.io/dataflow-templates-base/*`) and Template (`gcr.io/dataflow-templates/*`) container images.

## Components

- `dataflow_templates_base_vex.yaml`: Single source-of-truth YAML mapping Dataflow template base container families (`python_template_launcher_base`, `java_template_launcher_base`, `go_template_launcher_base`) and shared rule sets to CISA VEX justifications.
- `vex_manager.py`: CLI tool that:
  1. **`generate`**: Produces **OpenVEX v0.2.0** (`openvex/*.openvex.json`), **CSAF 2.0 VEX** (`csaf/*.vex.json`, with multi-registry `product_tree.branches` and `notes[].title` entries compatible with `gcloud artifacts vulnerabilities load-vex`), and AutoVM/Lichene triage rule snippets (`lichene/dataflow_templates_vex_rules.pi`).
  2. **`scan`**: Scans published container images across **Google Artifact Registry** (`gcloud artifacts vulnerabilities list`), **Trivy** (`trivy image --format json`), and **`govulncheck`** (`govulncheck -mode binary`), diffs discovered CVEs against `dataflow_templates_base_vex.yaml`, performs automated + optional LLM reachability triage, and updates `dataflow_templates_base_vex.yaml` when new non-exploitable CVEs are verified.
  3. **`attach`**: Attaches OpenVEX OCI attestations via `cosign attest --yes --type https://openvex.dev/ns` and loads CSAF 2.0 VEX notes into Artifact Registry via `gcloud artifacts vulnerabilities load-vex`.
