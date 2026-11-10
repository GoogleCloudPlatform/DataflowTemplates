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

import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../main/python"))
)

import check_yaml_config_params

SAMPLE_YAMLDOC_HTML = """
<html>
  <body>
    <div class="version">2.77.0</div>
    <hr><h2 id="readfrompubsub">ReadFromPubSub</h2>
    <h3 id="configuration_1">Configuration</h3>
    <ul>
      <li><p><strong>topic</strong> <code>string</code> (Optional) : Pub/Sub topic.</p></li>
      <li><p><strong>format</strong> <code>string</code> : Message format.</p></li>
      <li><p><strong>attributes_map</strong> <code>string</code> (Optional) : Attribute map field.</p></li>
      <li>
        <p><strong>error_handling</strong> <code>Row</code> (Optional) : Error handling config.</p>
        <p>Row fields:</p>
        <ul>
          <li><strong>output</strong> <code>string</code> : Output error collection</li>
        </ul>
      </li>
    </ul>
    <h3 id="usage_1">Usage</h3>
    <div class="codehilite"><pre><span></span><code>type: ReadFromPubSub
config:
  topic: "topic"
  format: "format"
  attributes_map: "attributes_map"
  error_handling:
    output: "output"
</code></pre></div>
  </body>
</html>
"""


class CheckYamlConfigParamsTest(unittest.TestCase):

    def test_parse_beam_yamldoc_html(self):
        version, transforms = check_yaml_config_params.parse_beam_yamldoc_html(
            SAMPLE_YAMLDOC_HTML
        )
        self.assertEqual(version, "2.77.0")
        self.assertIn("ReadFromPubSub", transforms)
        params = transforms["ReadFromPubSub"]
        self.assertEqual(
            set(params.keys()),
            {"topic", "format", "attributes_map", "error_handling"},
        )
        self.assertTrue(params["topic"]["optional"])
        self.assertFalse(params["format"]["optional"])
        # Nested Row field 'output' must not be parsed as top-level parameter
        self.assertNotIn("output", params)

    def test_extract_and_compare_blueprint(self):
        blueprint_yaml = """
template:
  name: "Test_PubSub_Yaml"
  requirements: {
    "Req 1"
  }
pipeline:
  transforms:
    - type: ReadFromPubSub
      name: ReadMessages
      config:
        topic: {{ topic }}
        format: {{ format | default('JSON') }}
        attribute_map: {{ attributesMap }}
    - type: CustomSink
      name: WriteCustom
      config:
        custom_param: "{{ val }}"
providers:
  - include: gs://example/provider.yaml
"""
        _, doc_transforms = check_yaml_config_params.parse_beam_yamldoc_html(
            SAMPLE_YAMLDOC_HTML
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as tmp:
            tmp.write(blueprint_yaml)
            tmp_path = Path(tmp.name)

        try:
            results = check_yaml_config_params.compare_blueprints(
                tmp_path, doc_transforms
            )
            self.assertEqual(len(results), 1)
            t_reports = results[0]["transforms"]
            self.assertEqual(len(t_reports), 2)

            pubsub_report = t_reports[0]
            self.assertEqual(pubsub_report["type"], "ReadFromPubSub")
            self.assertTrue(pubsub_report["in_beam_docs"])
            self.assertEqual(pubsub_report["invalid_params"], ["attribute_map"])
            self.assertEqual(pubsub_report["missing_required_params"], [])
            self.assertEqual(
                pubsub_report["missing_optional_params"],
                ["attributes_map", "error_handling"],
            )

            custom_report = t_reports[1]
            self.assertEqual(custom_report["type"], "CustomSink")
            self.assertFalse(custom_report["in_beam_docs"])
            self.assertTrue(custom_report["has_custom_providers"])

            md, has_mismatch, has_invalid = (
                check_yaml_config_params.generate_markdown_report(
                    results, "2.77.0"
                )
            )
            self.assertTrue(has_mismatch)
            self.assertTrue(has_invalid)
            self.assertIn("`attribute_map`", md)
            self.assertIn("`attributes_map`", md)
        finally:
            os.remove(tmp_path)

    def test_fetch_yamldoc_html_rejects_non_https(self):
        with self.assertRaises(ValueError):
            check_yaml_config_params.fetch_yamldoc_html(
                "http://beam.apache.org/releases/yamldoc/current/"
            )


if __name__ == "__main__":
    unittest.main()
