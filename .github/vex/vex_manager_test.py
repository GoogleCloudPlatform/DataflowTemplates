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

"""Unit tests for .github/vex/vex_manager.py."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import vex_manager


class DataflowTemplatesVexManagerTest(unittest.TestCase):

  def test_generate_openvex_csaf_and_lichene(self) -> None:
    config_path = Path(__file__).with_name("dataflow_templates_base_vex.yaml")
    with tempfile.TemporaryDirectory() as tmpdir:
      rc = vex_manager.main([
          "generate",
          f"--config={config_path}",
          f"--output-dir={tmpdir}",
          "--timestamp=2026-11-10T00:00:00Z",
      ])
      self.assertEqual(rc, 0)

      openvex_file = (
          Path(tmpdir) / "openvex" / "python_template_launcher_base.openvex.json"
      )
      csaf_file = Path(tmpdir) / "csaf" / "python_template_launcher_base.vex.json"
      lichene_file = Path(tmpdir) / "lichene" / "dataflow_templates_vex_rules.pi"

      self.assertTrue(openvex_file.exists())
      self.assertTrue(csaf_file.exists())
      self.assertTrue(lichene_file.exists())

      openvex_doc = json.loads(openvex_file.read_text(encoding="utf-8"))
      cves = {s["vulnerability"]["name"] for s in openvex_doc["statements"]}
      self.assertIn("CVE-2026-75803", cves)
      self.assertIn("CVE-2026-95619", cves)

      csaf_doc = json.loads(csaf_file.read_text(encoding="utf-8"))
      branch_names = {b["name"] for b in csaf_doc["product_tree"]["branches"]}
      self.assertIn(
          "us-docker.pkg.dev/dataflow-templates-base/gcr.io/python311-template-launcher-base",
          branch_names,
      )
      port_variants = vex_manager.expand_uri_variants(
          "localhost:5000/project/image:latest"
      )
      self.assertIn("localhost:5000/project/image", port_variants)
      self.assertIn("localhost:5000/project/image:latest", port_variants)


if __name__ == "__main__":
  unittest.main()
