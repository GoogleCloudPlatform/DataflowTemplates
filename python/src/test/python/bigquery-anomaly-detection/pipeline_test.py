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

"""Unit tests for bqmonitor.pipeline helpers."""

import json
import logging
import unittest

logging.basicConfig(level=logging.INFO)

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult

from bqmonitor.pipeline import _FormatAnomalyAsJson
from bqmonitor.pipeline import _parse_detector_spec
from bqmonitor.pipeline import _parse_table_ref
from bqmonitor.pipeline import _ThresholdAlert
from bqmonitor.pipeline import _unpack_result


class ParseTableRefTest(unittest.TestCase):
  """Tests for _parse_table_ref()."""

  def test_colon_format(self):
    p, d, t = _parse_table_ref('my-project:my_dataset.my_table')
    self.assertEqual(p, 'my-project')
    self.assertEqual(d, 'my_dataset')
    self.assertEqual(t, 'my_table')

  def test_dot_format(self):
    p, d, t = _parse_table_ref('my-project.my_dataset.my_table')
    self.assertEqual(p, 'my-project')
    self.assertEqual(d, 'my_dataset')
    self.assertEqual(t, 'my_table')

  def test_invalid_format_raises(self):
    with self.assertRaises(ValueError):
      _parse_table_ref('not_valid')

  def test_empty_raises(self):
    with self.assertRaises(ValueError):
      _parse_table_ref('')


class UnpackResultTest(unittest.TestCase):
  """Tests for _unpack_result()."""

  def test_keyed(self):
    result = object()
    key, r = _unpack_result(('mykey', result))
    self.assertEqual(key, 'mykey')
    self.assertIs(r, result)

  def test_unkeyed(self):
    result = object()
    key, r = _unpack_result(result)
    self.assertIsNone(key)
    self.assertIs(r, result)


class ParseDetectorSpecTest(unittest.TestCase):
  """Tests for _parse_detector_spec()."""

  def test_zscore(self):
    detector = _parse_detector_spec('{"type":"ZScore"}')
    self.assertEqual(type(detector).__name__, 'ZScore')

  def test_iqr(self):
    detector = _parse_detector_spec('{"type":"IQR"}')
    self.assertEqual(type(detector).__name__, 'IQR')

  def test_robust_zscore(self):
    detector = _parse_detector_spec('{"type":"RobustZScore"}')
    self.assertEqual(type(detector).__name__, 'RobustZScore')

  def test_threshold(self):
    detector = _parse_detector_spec(
        '{"type":"Threshold","expression":"value >= 100"}')
    self.assertIsInstance(detector, _ThresholdAlert)

  def test_threshold_missing_expression_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec('{"type":"Threshold"}')
    self.assertIn('expression', str(ctx.exception))

  def test_threshold_invalid_expression_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec(
          '{"type":"Threshold","expression":"import os"}')
    self.assertIn('Invalid threshold expression', str(ctx.exception))

  def test_unknown_type_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec('{"type":"Unknown"}')
    self.assertIn('Unknown', str(ctx.exception))

  def test_invalid_json_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec('{bad json}')
    self.assertIn('Invalid JSON', str(ctx.exception))

  def test_missing_type_raises(self):
    with self.assertRaises(ValueError):
      _parse_detector_spec('{"config":{}}')

  def test_zscore_with_threshold(self):
    spec = json.dumps({
        'type': 'ZScore',
        'config': {
            'threshold_criterion': {
                'type': 'FixedThreshold',
                'config': {'cutoff': 10}}}})
    detector = _parse_detector_spec(spec)
    self.assertEqual(type(detector).__name__, 'ZScore')


class ThresholdAlertTest(unittest.TestCase):
  """Tests for _ThresholdAlert DoFn."""

  def _make_row(self, value):
    return beam.Row(value=value, window_start=0.0, window_end=1.0)

  def _run_dofn(self, expression, element):
    dofn = _ThresholdAlert(expression)
    dofn.setup()
    return list(dofn.process(element))

  def test_above_threshold(self):
    results = self._run_dofn('value >= 100', self._make_row(500.0))
    self.assertEqual(len(results), 1)
    result = results[0]
    self.assertIsInstance(result, AnomalyResult)
    self.assertEqual(result.predictions[0].label, 1)
    self.assertEqual(result.predictions[0].score, 500.0)
    self.assertEqual(
        result.predictions[0].model_id, 'Threshold(value >= 100)')

  def test_below_threshold(self):
    results = self._run_dofn('value >= 100', self._make_row(50.0))
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].predictions[0].label, 0)

  def test_keyed_element(self):
    row = self._make_row(200.0)
    results = self._run_dofn('value >= 100', ('mykey', row))
    self.assertEqual(len(results), 1)
    key, result = results[0]
    self.assertEqual(key, 'mykey')
    self.assertEqual(result.predictions[0].label, 1)

  def test_range_expression(self):
    dofn = _ThresholdAlert('value > 100 or value < -100')
    dofn.setup()

    # Above range.
    results = list(dofn.process(self._make_row(200.0)))
    self.assertEqual(results[0].predictions[0].label, 1)

    # Below range.
    results = list(dofn.process(self._make_row(-200.0)))
    self.assertEqual(results[0].predictions[0].label, 1)

    # Within range.
    results = list(dofn.process(self._make_row(50.0)))
    self.assertEqual(results[0].predictions[0].label, 0)

  def test_less_than_threshold(self):
    results = self._run_dofn('value <= 0.01', self._make_row(0.005))
    self.assertEqual(results[0].predictions[0].label, 1)

    results = self._run_dofn('value <= 0.01', self._make_row(0.5))
    self.assertEqual(results[0].predictions[0].label, 0)


class FormatAnomalyAsJsonTest(unittest.TestCase):
  """Tests for _FormatAnomalyAsJson DoFn."""

  def _make_result(self, label, value=42.0, score=5.0, model_id='TestModel'):
    row = beam.Row(value=value, window_start=1000.0, window_end=1001.0)
    prediction = AnomalyPrediction(
        model_id=model_id, score=score, label=label)
    return AnomalyResult(example=row, predictions=[prediction])

  def test_outlier_emits_json(self):
    dofn = _FormatAnomalyAsJson()
    results = list(dofn.process(self._make_result(label=1)))
    self.assertEqual(len(results), 1)
    payload = json.loads(results[0])
    self.assertIn('Anomaly detected', payload['event_description'])
    self.assertEqual(payload['agent_id'], 'TestModel')

  def test_normal_emits_nothing(self):
    dofn = _FormatAnomalyAsJson()
    results = list(dofn.process(self._make_result(label=0)))
    self.assertEqual(len(results), 0)

  def test_warmup_emits_nothing(self):
    dofn = _FormatAnomalyAsJson()
    results = list(dofn.process(self._make_result(label=-2)))
    self.assertEqual(len(results), 0)

  def test_keyed_outlier_includes_key(self):
    dofn = _FormatAnomalyAsJson()
    result = self._make_result(label=1)
    outputs = list(dofn.process(('campaign_search', result)))
    self.assertEqual(len(outputs), 1)
    payload = json.loads(outputs[0])
    self.assertEqual(payload['key'], 'campaign_search')

  def test_threshold_model_id(self):
    dofn = _FormatAnomalyAsJson()
    result = self._make_result(
        label=1, model_id='Threshold(value >= 100)')
    outputs = list(dofn.process(result))
    payload = json.loads(outputs[0])
    self.assertEqual(payload['agent_id'], 'Threshold(value >= 100)')


if __name__ == '__main__':
  unittest.main()
