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
import time
import unittest

logging.basicConfig(level=logging.INFO)

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue

from parameterized import parameterized, param
from bqmonitor.metric import ComputeMetric, FanoutStrategy, MetricSpec
from bqmonitor.pipeline import _anomaly_id
from bqmonitor.pipeline import _compute_anomaly_message
from bqmonitor.pipeline import _FormatAnomalyAsJson
from bqmonitor.pipeline import _FormatResultForBQ
from bqmonitor.pipeline import _is_outlier
from bqmonitor.pipeline import _key_anomaly_for_async
from bqmonitor.pipeline import _parse_detector_spec
from bqmonitor.pipeline import _parse_table_ref
from bqmonitor.pipeline import _parse_webhook_spec
from bqmonitor.pipeline import _PostAnomalyToWebhook
from bqmonitor.pipeline import _RateLimitAlerts
from bqmonitor.pipeline import _substitute_template_tree
from bqmonitor.pipeline import _ThresholdAlert
from bqmonitor.pipeline import _UNKEYED_SENTINEL
from bqmonitor.pipeline import _unpack_result
from bqmonitor.pipeline import _validate_template_tree
from apache_beam.utils.timestamp import Timestamp
from bqmonitor.pipeline import _validate_message_format


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
    return beam.Row(value=value, window_start=Timestamp(0), window_end=Timestamp(1))

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
    self.assertIsNone(result.predictions[0].score)
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
    row = beam.Row(value=value, window_start=Timestamp(1000), window_end=Timestamp(1001))
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


class FormatResultForBQTest(unittest.TestCase):
  """Tests for _FormatResultForBQ DoFn."""

  def _make_result(self, label, value=42.0, score=5.0):
    row = beam.Row(value=value, window_start=Timestamp(1000), window_end=Timestamp(1001))
    prediction = AnomalyPrediction(
        model_id='TestModel', score=score, label=label)
    return AnomalyResult(example=row, predictions=[prediction])

  def test_outlier_row(self):
    dofn = _FormatResultForBQ()
    results = list(dofn.process(self._make_result(label=1, value=99.0,
                                                  score=4.5)))
    self.assertEqual(len(results), 1)
    row = results[0]
    self.assertAlmostEqual(row['value'], 99.0)
    self.assertAlmostEqual(row['score'], 4.5)
    self.assertEqual(row['label'], 1)
    self.assertIn('window_start', row)
    self.assertIn('window_end', row)
    self.assertNotIn('key', row)

  def test_normal_row(self):
    dofn = _FormatResultForBQ()
    results = list(dofn.process(self._make_result(label=0)))
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0]['label'], 0)

  def test_warmup_row(self):
    dofn = _FormatResultForBQ()
    results = list(dofn.process(self._make_result(label=-2)))
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0]['label'], -2)

  def test_keyed_row_includes_key(self):
    dofn = _FormatResultForBQ()
    result = self._make_result(label=1)
    outputs = list(dofn.process(('campaign_search', result)))
    self.assertEqual(len(outputs), 1)
    self.assertEqual(outputs[0]['key'], 'campaign_search')

  def test_none_score(self):
    row = beam.Row(value=10.0, window_start=Timestamp(0), window_end=Timestamp(1))
    prediction = AnomalyPrediction(
        model_id='Test', score=None, label=0)
    result = AnomalyResult(example=row, predictions=[prediction])
    dofn = _FormatResultForBQ()
    outputs = list(dofn.process(result))
    self.assertIsNone(outputs[0]['score'])


# ---------------------------------------------------------------------------
# Aggregation pipeline integration tests
# ---------------------------------------------------------------------------


class AggregationPipelineTest(unittest.TestCase):
  """Tests that ComputeMetric + ZScore pipeline produces correct aggregations.

  For each (agg_type, window_type, keyed) combination, we feed deterministic
  data through the pipeline and verify the sink output values match hand-
  computed expected aggregations.
  """

  # 10 rows across 3 seconds, 2 keys
  RAW_DATA = [
      # second 0: key=a values=[10, 20], key=b values=[30]
      {'ts': 0.1, 'key': 'a', 'value': 10.0},
      {'ts': 0.5, 'key': 'a', 'value': 20.0},
      {'ts': 0.8, 'key': 'b', 'value': 30.0},
      # second 1: key=a values=[40], key=b values=[50, 60]
      {'ts': 1.2, 'key': 'a', 'value': 40.0},
      {'ts': 1.4, 'key': 'b', 'value': 50.0},
      {'ts': 1.9, 'key': 'b', 'value': 60.0},
      # second 2: key=a values=[70, 80], key=b values=[90]
      {'ts': 2.1, 'key': 'a', 'value': 70.0},
      {'ts': 2.5, 'key': 'a', 'value': 80.0},
      {'ts': 2.7, 'key': 'b', 'value': 90.0},
  ]

  # Expected aggregations per 1-second fixed window (unkeyed)
  # window_start is a Timestamp
  EXPECTED_FIXED_UNKEYED = {
      # window [0,1): values 10,20,30
      Timestamp(0): {'SUM': 60.0, 'COUNT': 3, 'MIN': 10.0, 'MAX': 30.0, 'MEAN': 20.0},
      # window [1,2): values 40,50,60
      Timestamp(1): {'SUM': 150.0, 'COUNT': 3, 'MIN': 40.0, 'MAX': 60.0, 'MEAN': 50.0},
      # window [2,3): values 70,80,90
      Timestamp(2): {'SUM': 240.0, 'COUNT': 3, 'MIN': 70.0, 'MAX': 90.0, 'MEAN': 80.0},
  }

  # Expected aggregations per 1-second fixed window (keyed by 'key')
  EXPECTED_FIXED_KEYED = {
      (Timestamp(0), 'a'): {'SUM': 30.0, 'COUNT': 2, 'MIN': 10.0, 'MAX': 20.0, 'MEAN': 15.0},
      (Timestamp(0), 'b'): {'SUM': 30.0, 'COUNT': 1, 'MIN': 30.0, 'MAX': 30.0, 'MEAN': 30.0},
      (Timestamp(1), 'a'): {'SUM': 40.0, 'COUNT': 1, 'MIN': 40.0, 'MAX': 40.0, 'MEAN': 40.0},
      (Timestamp(1), 'b'): {'SUM': 110.0, 'COUNT': 2, 'MIN': 50.0, 'MAX': 60.0, 'MEAN': 55.0},
      (Timestamp(2), 'a'): {'SUM': 150.0, 'COUNT': 2, 'MIN': 70.0, 'MAX': 80.0, 'MEAN': 75.0},
      (Timestamp(2), 'b'): {'SUM': 90.0, 'COUNT': 1, 'MIN': 90.0, 'MAX': 90.0, 'MEAN': 90.0},
  }

  # Expected sliding windows (size=1, period=0.5) — unkeyed
  # [0.0, 1.0): ts 0.1,0.5,0.8 → values 10,20,30
  # [0.5, 1.5): ts 0.5,0.8,1.2,1.4 → values 20,30,40,50
  # [1.0, 2.0): ts 1.2,1.4,1.9 → values 40,50,60
  # [1.5, 2.5): ts 1.9,2.1 → values 60,70
  # [2.0, 3.0): ts 2.1,2.5,2.7 → values 70,80,90
  EXPECTED_SLIDING_UNKEYED = {
      Timestamp.of(-0.5): {'SUM': 10.0, 'COUNT': 1, 'MIN': 10.0, 'MAX': 10.0},
      Timestamp(0): {'SUM': 60.0, 'COUNT': 3, 'MIN': 10.0, 'MAX': 30.0},
      Timestamp.of(0.5): {'SUM': 140.0, 'COUNT': 4, 'MIN': 20.0, 'MAX': 50.0},
      Timestamp(1): {'SUM': 150.0, 'COUNT': 3, 'MIN': 40.0, 'MAX': 60.0},
      Timestamp.of(1.5): {'SUM': 130.0, 'COUNT': 2, 'MIN': 60.0, 'MAX': 70.0},
      Timestamp(2): {'SUM': 240.0, 'COUNT': 3, 'MIN': 70.0, 'MAX': 90.0},
      Timestamp.of(2.5): {'SUM': 170.0, 'COUNT': 2, 'MIN': 80.0, 'MAX': 90.0},
  }

  # Expected sliding windows (size=1, period=0.5) — keyed by 'key'
  EXPECTED_SLIDING_KEYED = {
      (Timestamp.of(-0.5), 'a'): {'SUM': 10.0, 'COUNT': 1, 'MIN': 10.0, 'MAX': 10.0, 'MEAN': 10.0},
      (Timestamp(0), 'a'): {'SUM': 30.0, 'COUNT': 2, 'MIN': 10.0, 'MAX': 20.0, 'MEAN': 15.0},
      (Timestamp(0), 'b'): {'SUM': 30.0, 'COUNT': 1, 'MIN': 30.0, 'MAX': 30.0, 'MEAN': 30.0},
      (Timestamp.of(0.5), 'a'): {'SUM': 60.0, 'COUNT': 2, 'MIN': 20.0, 'MAX': 40.0, 'MEAN': 30.0},
      (Timestamp.of(0.5), 'b'): {'SUM': 80.0, 'COUNT': 2, 'MIN': 30.0, 'MAX': 50.0, 'MEAN': 40.0},
      (Timestamp(1), 'a'): {'SUM': 40.0, 'COUNT': 1, 'MIN': 40.0, 'MAX': 40.0, 'MEAN': 40.0},
      (Timestamp(1), 'b'): {'SUM': 110.0, 'COUNT': 2, 'MIN': 50.0, 'MAX': 60.0, 'MEAN': 55.0},
      (Timestamp.of(1.5), 'a'): {'SUM': 70.0, 'COUNT': 1, 'MIN': 70.0, 'MAX': 70.0, 'MEAN': 70.0},
      (Timestamp.of(1.5), 'b'): {'SUM': 60.0, 'COUNT': 1, 'MIN': 60.0, 'MAX': 60.0, 'MEAN': 60.0},
      (Timestamp(2), 'a'): {'SUM': 150.0, 'COUNT': 2, 'MIN': 70.0, 'MAX': 80.0, 'MEAN': 75.0},
      (Timestamp(2), 'b'): {'SUM': 90.0, 'COUNT': 1, 'MIN': 90.0, 'MAX': 90.0, 'MEAN': 90.0},
      (Timestamp.of(2.5), 'a'): {'SUM': 80.0, 'COUNT': 1, 'MIN': 80.0, 'MAX': 80.0, 'MEAN': 80.0},
      (Timestamp.of(2.5), 'b'): {'SUM': 90.0, 'COUNT': 1, 'MIN': 90.0, 'MAX': 90.0, 'MEAN': 90.0},
  }

  def _make_metric_spec(self, agg, window_type='fixed', group_by=None):
    """Create a MetricSpec for the given aggregation and window type."""
    window = {'type': window_type, 'size_seconds': 1}
    if window_type == 'sliding':
      window['period_seconds'] = 0.5
    spec = {
        'aggregation': {
            'window': window,
            'measures': [{'field': 'value', 'agg': agg, 'alias': 'total'}],
        }
    }
    if group_by:
      spec['aggregation']['group_by'] = group_by
    return MetricSpec.from_dict(spec)

  _FANOUT_STRATEGIES_NO_HOTKEY = [
      param(fanout='none'),
      param(fanout='sharded'),
      param(fanout='precombine'),
  ]

  def _assert_aggregation(self, agg, expected, window_type='fixed',
                          group_by=None, fanout_strategy='none'):
    """Run ComputeMetric and assert output matches expected values."""
    metric_spec = self._make_metric_spec(agg, window_type, group_by)
    elements = [
        TimestampedValue(row, row['ts'])
        for row in self.RAW_DATA
    ]

    with beam.Pipeline() as p:
      metrics = (
          p
          | beam.Create(elements)
          | 'ComputeMetric' >> ComputeMetric(
              metric_spec,
              fanout_strategy=FanoutStrategy(fanout_strategy),
              fanout=4)
      )

      def extract(element):
        if isinstance(element, tuple) and len(element) == 2:
          key_tuple, row = element
          key_str = (key_tuple[0] if len(key_tuple) == 1
                     else str(key_tuple))
          return ((row.window_start, key_str), row.value)
        else:
          return (element.window_start, element.value)

      extracted = metrics | 'Extract' >> beam.Map(extract)
      assert_that(extracted, equal_to(expected))

  def _expected_list(self, expected_dict, agg):
    """Convert expected dict to list of (key, value) for equal_to."""
    return [(k, v[agg]) for k, v in expected_dict.items()]

  _FANOUT_STRATEGIES = [
      param(fanout='none'),
      param(fanout='sharded'),
      param(fanout='hotkey_fanout'),
      param(fanout='precombine'),
  ]

  # --- Fixed window, unkeyed ---

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_sum_fixed_unkeyed(self, fanout):
    self._assert_aggregation('SUM', self._expected_list(
        self.EXPECTED_FIXED_UNKEYED, 'SUM'), fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_count_fixed_unkeyed(self, fanout):
    self._assert_aggregation('COUNT', self._expected_list(
        self.EXPECTED_FIXED_UNKEYED, 'COUNT'), fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_min_fixed_unkeyed(self, fanout):
    self._assert_aggregation('MIN', self._expected_list(
        self.EXPECTED_FIXED_UNKEYED, 'MIN'), fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_max_fixed_unkeyed(self, fanout):
    self._assert_aggregation('MAX', self._expected_list(
        self.EXPECTED_FIXED_UNKEYED, 'MAX'), fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_mean_fixed_unkeyed(self, fanout):
    self._assert_aggregation('MEAN', self._expected_list(
        self.EXPECTED_FIXED_UNKEYED, 'MEAN'), fanout_strategy=fanout)

  # --- Fixed window, keyed ---

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_sum_fixed_keyed(self, fanout):
    self._assert_aggregation('SUM', self._expected_list(
        self.EXPECTED_FIXED_KEYED, 'SUM'), group_by=['key'],
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_count_fixed_keyed(self, fanout):
    self._assert_aggregation('COUNT', self._expected_list(
        self.EXPECTED_FIXED_KEYED, 'COUNT'), group_by=['key'],
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_min_fixed_keyed(self, fanout):
    self._assert_aggregation('MIN', self._expected_list(
        self.EXPECTED_FIXED_KEYED, 'MIN'), group_by=['key'],
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_max_fixed_keyed(self, fanout):
    self._assert_aggregation('MAX', self._expected_list(
        self.EXPECTED_FIXED_KEYED, 'MAX'), group_by=['key'],
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES)
  def test_mean_fixed_keyed(self, fanout):
    self._assert_aggregation('MEAN', self._expected_list(
        self.EXPECTED_FIXED_KEYED, 'MEAN'), group_by=['key'],
        fanout_strategy=fanout)

  # --- Sliding window, unkeyed ---
  # hotkey_fanout excluded: https://github.com/apache/beam/issues/20528

  @parameterized.expand(_FANOUT_STRATEGIES_NO_HOTKEY)
  def test_sum_sliding_unkeyed(self, fanout):
    self._assert_aggregation('SUM', self._expected_list(
        self.EXPECTED_SLIDING_UNKEYED, 'SUM'), window_type='sliding',
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES_NO_HOTKEY)
  def test_count_sliding_unkeyed(self, fanout):
    self._assert_aggregation('COUNT', self._expected_list(
        self.EXPECTED_SLIDING_UNKEYED, 'COUNT'), window_type='sliding',
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES_NO_HOTKEY)
  def test_min_sliding_unkeyed(self, fanout):
    self._assert_aggregation('MIN', self._expected_list(
        self.EXPECTED_SLIDING_UNKEYED, 'MIN'), window_type='sliding',
        fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES_NO_HOTKEY)
  def test_max_sliding_unkeyed(self, fanout):
    self._assert_aggregation('MAX', self._expected_list(
        self.EXPECTED_SLIDING_UNKEYED, 'MAX'), window_type='sliding',
        fanout_strategy=fanout)

  # --- Sliding window, keyed ---

  @parameterized.expand(_FANOUT_STRATEGIES_NO_HOTKEY)
  def test_sum_sliding_keyed(self, fanout):
    self._assert_aggregation(
        'SUM', self._expected_list(self.EXPECTED_SLIDING_KEYED, 'SUM'),
        window_type='sliding', group_by=['key'], fanout_strategy=fanout)

  @parameterized.expand(_FANOUT_STRATEGIES_NO_HOTKEY)
  def test_mean_sliding_keyed(self, fanout):
    self._assert_aggregation(
        'MEAN', self._expected_list(self.EXPECTED_SLIDING_KEYED, 'MEAN'),
        window_type='sliding', group_by=['key'], fanout_strategy=fanout)


class ValidateMessageFormatTest(unittest.TestCase):
  """Tests for _validate_message_format."""

  def test_valid_anomaly_fields(self):
    _validate_message_format(
        '{value} {score} {key} {window_start}', None)

  def test_valid_with_metadata(self):
    _validate_message_format(
        '{value} {job_id}', {'job_id': 'abc'})

  def test_unknown_field_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _validate_message_format('{value} {bogus}', None)
    self.assertIn('bogus', str(ctx.exception))

  def test_unknown_field_with_metadata(self):
    with self.assertRaises(ValueError):
      _validate_message_format('{job_id} {nope}', {'job_id': 'x'})

  def test_all_fields_valid(self):
    fmt = ('{value} {score} {label} {threshold} {model_id} '
           '{info} {key} {window_start} {window_end}')
    _validate_message_format(fmt, None)

  def test_no_placeholders(self):
    _validate_message_format('static message', None)


class FormatAnomalyCustomFormatTest(unittest.TestCase):
  """Tests for _FormatAnomalyAsJson with custom message_format."""

  def _make_result(self, label, value=42.0, score=5.0, model_id='TestModel'):
    row = beam.Row(value=value, window_start=Timestamp(1000), window_end=Timestamp(1001))
    prediction = AnomalyPrediction(
        model_id=model_id, score=score, label=label)
    return AnomalyResult(example=row, predictions=[prediction])

  def test_custom_format(self):
    dofn = _FormatAnomalyAsJson(
        message_format='alert: value={value} score={score}')
    results = list(dofn.process(self._make_result(label=1, value=99.0,
                                                  score=4.5)))
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0], b'alert: value=99.0 score=4.5')

  def test_custom_format_with_metadata(self):
    dofn = _FormatAnomalyAsJson(
        message_format='{{"alert": "{key}", "job": "{job_id}"}}',
        message_metadata={'job_id': 'pipeline-123'})
    results = list(dofn.process(('sensor_1', self._make_result(label=1))))
    payload = json.loads(results[0])
    self.assertEqual(payload['alert'], 'sensor_1')
    self.assertEqual(payload['job'], 'pipeline-123')

  def test_metadata_does_not_override_anomaly_fields(self):
    dofn = _FormatAnomalyAsJson(
        message_format='v={value}',
        message_metadata={'value': 'SHOULD_NOT_APPEAR'})
    results = list(dofn.process(self._make_result(label=1, value=77.0)))
    self.assertEqual(results[0], b'v=77.0')

  def test_none_score_renders_as_null(self):
    row = beam.Row(value=10.0, window_start=Timestamp(0), window_end=Timestamp(1))
    prediction = AnomalyPrediction(
        model_id='Test', score=None, label=1)
    result = AnomalyResult(example=row, predictions=[prediction])
    dofn = _FormatAnomalyAsJson(message_format='s={score}')
    results = list(dofn.process(result))
    self.assertEqual(results[0], b's=null')

  def test_unkeyed_key_renders_empty(self):
    dofn = _FormatAnomalyAsJson(message_format='k=[{key}]')
    results = list(dofn.process(self._make_result(label=1)))
    self.assertEqual(results[0], b'k=[]')

  def test_metadata_only_fields(self):
    dofn = _FormatAnomalyAsJson(
        message_format='{env}-{team}',
        message_metadata={'env': 'prod', 'team': 'oncall'})
    results = list(dofn.process(self._make_result(label=1)))
    self.assertEqual(results[0], b'prod-oncall')


class ParseWebhookSpecTest(unittest.TestCase):
  """Tests for _parse_webhook_spec."""

  def _minimal_spec(self, **overrides):
    spec = {
        'endpoint': 'https://example.com/api',
        'body': {'text': 'static'},
    }
    spec.update(overrides)
    return json.dumps(spec)

  def test_minimal_valid(self):
    out = _parse_webhook_spec(self._minimal_spec(), None)
    self.assertEqual(out['endpoint'], 'https://example.com/api')
    self.assertEqual(out['body'], {'text': 'static'})
    self.assertEqual(out['method'], 'POST')
    self.assertEqual(out['headers'], {})
    self.assertEqual(
        out['scopes'], ['https://www.googleapis.com/auth/cloud-platform'])
    self.assertEqual(out['timeout_seconds'], 600.0)
    self.assertEqual(out['parallelism'], 5)
    self.assertEqual(out['callback_frequency_seconds'], 30.0)

  def test_full_spec(self):
    out = _parse_webhook_spec(
        json.dumps({
            'endpoint': 'https://api.example.com/v1/chat',
            'body': {'q': '{anomaly_message}'},
            'method': 'put',
            'headers': {'X-Trace': '{key}'},
            'scopes': ['https://www.googleapis.com/auth/cloud-platform'],
            'timeout_seconds': 30,
            'parallelism': 8,
            'callback_frequency_seconds': 15,
        }),
        None)
    self.assertEqual(out['method'], 'PUT')
    self.assertEqual(out['headers'], {'X-Trace': '{key}'})
    self.assertEqual(out['timeout_seconds'], 30.0)
    self.assertEqual(out['parallelism'], 8)
    self.assertEqual(out['callback_frequency_seconds'], 15.0)

  def test_invalid_json_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec('{bad json}', None)
    self.assertIn('Invalid JSON', str(ctx.exception))

  def test_not_object_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec('["a","b"]', None)
    self.assertIn('JSON object', str(ctx.exception))

  def test_missing_endpoint_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(json.dumps({'body': {}}), None)
    self.assertIn("'endpoint'", str(ctx.exception))

  def test_missing_body_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(
          json.dumps({'endpoint': 'https://example.com'}), None)
    self.assertIn("'body'", str(ctx.exception))

  def test_non_http_endpoint_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(self._minimal_spec(endpoint='ftp://x'), None)
    self.assertIn('http(s)', str(ctx.exception))

  def test_unknown_key_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(
          self._minimal_spec(unexpected='field'), None)
    self.assertIn('unexpected', str(ctx.exception))

  def test_bad_method_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(self._minimal_spec(method='DELETE'), None)
    self.assertIn('method', str(ctx.exception))

  def test_bad_body_type_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(self._minimal_spec(body='just a string'), None)
    self.assertIn('body', str(ctx.exception))

  def test_body_can_be_list(self):
    out = _parse_webhook_spec(
        self._minimal_spec(body=[{'text': 'hi'}]), None)
    self.assertEqual(out['body'], [{'text': 'hi'}])

  def test_bad_headers_shape_raises(self):
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(headers=['x']), None)

  def test_bad_header_value_type_raises(self):
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(headers={'X': 1}), None)

  def test_bad_scopes_raises(self):
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(scopes=[]), None)
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(scopes='not-a-list'), None)
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(scopes=[123]), None)

  def test_bad_timeout_raises(self):
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(timeout_seconds=0), None)
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(timeout_seconds=-5), None)
    with self.assertRaises(ValueError):
      _parse_webhook_spec(self._minimal_spec(timeout_seconds='10s'), None)

  def test_parallelism_override(self):
    out = _parse_webhook_spec(self._minimal_spec(parallelism=5), None)
    self.assertEqual(out['parallelism'], 5)

  def test_bad_parallelism_raises(self):
    for bad in (0, -1, 1.5, 'many', True):
      with self.assertRaises(ValueError, msg=f'parallelism={bad!r}'):
        _parse_webhook_spec(self._minimal_spec(parallelism=bad), None)

  def test_callback_frequency_override(self):
    out = _parse_webhook_spec(
        self._minimal_spec(callback_frequency_seconds=15), None)
    self.assertEqual(out['callback_frequency_seconds'], 15.0)

  def test_bad_callback_frequency_raises(self):
    for bad in (0, -2.5, 'fast', True):
      with self.assertRaises(
          ValueError, msg=f'callback_frequency_seconds={bad!r}'):
        _parse_webhook_spec(
            self._minimal_spec(callback_frequency_seconds=bad), None)

  def test_unknown_placeholder_in_body_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(
          self._minimal_spec(body={'q': 'hello {bogus}'}), None)
    self.assertIn('bogus', str(ctx.exception))

  def test_unknown_placeholder_in_header_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_webhook_spec(
          self._minimal_spec(headers={'X-Trace': '{bogus}'}), None)
    self.assertIn('bogus', str(ctx.exception))

  def test_metadata_keys_allowed_as_placeholders(self):
    out = _parse_webhook_spec(
        self._minimal_spec(body={'q': '{env}-{value}'}),
        {'env': 'prod'})
    self.assertEqual(out['body'], {'q': '{env}-{value}'})

  def test_anomaly_message_placeholder_allowed(self):
    out = _parse_webhook_spec(
        self._minimal_spec(body={'q': '{anomaly_message}'}), None)
    self.assertEqual(out['body'], {'q': '{anomaly_message}'})

  def test_non_string_leaves_pass_through(self):
    out = _parse_webhook_spec(
        self._minimal_spec(
            body={'count': 1, 'enabled': True, 'pct': 0.5, 'tag': None}),
        None)
    self.assertEqual(
        out['body'],
        {'count': 1, 'enabled': True, 'pct': 0.5, 'tag': None})


class SubstituteTemplateTreeTest(unittest.TestCase):
  """Tests for _substitute_template_tree."""

  def test_dict_substitution(self):
    out = _substitute_template_tree(
        {'text': '{a} and {b}'}, {'a': 1, 'b': 2})
    self.assertEqual(out, {'text': '1 and 2'})

  def test_nested_substitution(self):
    tree = {
        'messages': [
            {'userMessage': {'text': 'Anomaly: {value}'}},
            {'userMessage': {'text': 'Key: {key}'}},
        ],
        'meta': {'agent': 'projects/{project}/agents/x'},
    }
    out = _substitute_template_tree(
        tree, {'value': 99.0, 'key': 'campaign_a', 'project': 'p1'})
    self.assertEqual(
        out['messages'][0]['userMessage']['text'], 'Anomaly: 99.0')
    self.assertEqual(out['messages'][1]['userMessage']['text'],
                     'Key: campaign_a')
    self.assertEqual(out['meta']['agent'], 'projects/p1/agents/x')

  def test_non_string_leaves_unchanged(self):
    tree = {'n': 1, 'b': True, 'f': 2.5, 'z': None, 's': '{a}'}
    out = _substitute_template_tree(tree, {'a': 'X'})
    self.assertEqual(out, {'n': 1, 'b': True, 'f': 2.5, 'z': None, 's': 'X'})

  def test_does_not_mutate_input(self):
    tree = {'a': [{'b': '{x}'}]}
    out = _substitute_template_tree(tree, {'x': 'sub'})
    self.assertEqual(tree['a'][0]['b'], '{x}')
    self.assertEqual(out['a'][0]['b'], 'sub')

  def test_static_string_passes_through(self):
    out = _substitute_template_tree({'x': 'literal'}, {})
    self.assertEqual(out, {'x': 'literal'})


class ValidateTemplateTreeTest(unittest.TestCase):
  """Tests for _validate_template_tree."""

  def test_valid_tree(self):
    _validate_template_tree(
        {'a': '{x}', 'b': [{'c': '{y}'}]}, {'x', 'y'})

  def test_unknown_at_top_level_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _validate_template_tree({'a': '{bogus}'}, {'x'})
    self.assertIn('bogus', str(ctx.exception))

  def test_unknown_nested_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _validate_template_tree(
          {'a': [{'b': 'ok'}, {'c': '{nope}'}]}, {'ok'})
    self.assertIn('nope', str(ctx.exception))

  def test_non_string_leaves_skipped(self):
    _validate_template_tree({'a': 1, 'b': True, 'c': None}, set())


class ComputeAnomalyMessageTest(unittest.TestCase):
  """Tests for _compute_anomaly_message."""

  def _fields(self, **overrides):
    base = {
        'value': 42.0, 'score': 5.0, 'label': 1, 'threshold': 'null',
        'model_id': 'ZScore', 'info': '', 'key': '',
        'window_start': '2026-05-10T00:00:00Z',
        'window_end': '2026-05-10T00:01:00Z',
    }
    base.update(overrides)
    return base

  def test_default_message(self):
    msg = _compute_anomaly_message(self._fields(), None, None)
    self.assertIn('Anomaly detected', msg)
    self.assertIn('value=42.0', msg)
    self.assertIn('score=5.0', msg)

  def test_custom_format(self):
    msg = _compute_anomaly_message(
        self._fields(value=77.0), 'alert v={value}', None)
    self.assertEqual(msg, 'alert v=77.0')

  def test_metadata_visible_in_custom_format(self):
    msg = _compute_anomaly_message(
        self._fields(), '{env}-{model_id}', {'env': 'prod'})
    self.assertEqual(msg, 'prod-ZScore')

  def test_anomaly_fields_override_metadata(self):
    msg = _compute_anomaly_message(
        self._fields(value=10.0),
        'v={value}',
        {'value': 'SHOULD_NOT_APPEAR'})
    self.assertEqual(msg, 'v=10.0')


class _StubResponse:
  """Minimal stand-in for a requests Response used by DoFn tests."""

  def __init__(self, status_code=200, text=''):
    self.status_code = status_code
    self.text = text

  def raise_for_status(self):
    if self.status_code >= 400:
      raise RuntimeError(f'HTTP {self.status_code}')


class _StubSession:
  """Captures the most recent request() call for assertions."""

  def __init__(self, status_code=200):
    self.calls = []
    self._status_code = status_code

  def request(self, method, url, json=None, headers=None, timeout=None):
    self.calls.append({
        'method': method, 'url': url, 'json': json,
        'headers': headers, 'timeout': timeout,
    })
    return _StubResponse(status_code=self._status_code)


class PostAnomalyToWebhookTest(unittest.TestCase):
  """Tests for _PostAnomalyToWebhook DoFn (session stubbed; no network)."""

  def _make_result(self, label, value=42.0, score=5.0, model_id='ZScore'):
    row = beam.Row(
        value=value,
        window_start=Timestamp(1000), window_end=Timestamp(1001))
    prediction = AnomalyPrediction(
        model_id=model_id, score=score, label=label)
    return AnomalyResult(example=row, predictions=[prediction])

  def _make_dofn(self, body, *, message_format=None, message_metadata=None,
                 headers=None, status_code=200, method='POST',
                 endpoint='https://example.com/x'):
    spec = {
        'endpoint': endpoint,
        'body': body,
        'method': method,
        'headers': headers or {},
        'scopes': ['https://www.googleapis.com/auth/cloud-platform'],
        'timeout_seconds': 10.0,
    }
    dofn = _PostAnomalyToWebhook(spec, message_format, message_metadata)
    dofn._session = _StubSession(status_code=status_code)
    return dofn

  def test_outlier_triggers_post(self):
    dofn = self._make_dofn({'q': 'value={value}'})
    dofn.process(self._make_result(label=1, value=99.0))
    self.assertEqual(len(dofn._session.calls), 1)
    call = dofn._session.calls[0]
    self.assertEqual(call['method'], 'POST')
    self.assertEqual(call['url'], 'https://example.com/x')
    self.assertEqual(call['json'], {'q': 'value=99.0'})

  def test_success_log_includes_window_and_elapsed(self):
    """The success path emits an INFO log line that names the specific
    anomaly window and how long the POST took, so operators can trace
    individual anomalies through the webhook stage.

    Importantly, the grouping key is NOT logged: in keyed pipelines the
    key can hold user-derived values (account id, customer segment,
    etc.) that shouldn't leak into pipeline logs. This test feeds a
    keyed element with a recognizable sentinel value and asserts the
    sentinel does NOT appear in the captured log output.
    """
    dofn = self._make_dofn({'q': '{value}'})
    sensitive_key = 'SENSITIVE_CUSTOMER_ID_12345'
    with self.assertLogs('bqmonitor.pipeline', level='INFO') as ctx:
      dofn.process((sensitive_key, self._make_result(label=1, value=99.0)))
    success_lines = [r for r in ctx.output if 'posted anomaly' in r]
    self.assertEqual(
        len(success_lines), 1, f'expected one success log, got: {ctx.output}')
    line = success_lines[0]
    # Window timestamps come from Timestamp(1000)/Timestamp(1001) as RFC3339.
    self.assertIn('window=', line)
    self.assertIn('1970-01-01T00:16:40', line)  # Timestamp(1000)
    self.assertIn('model_id=ZScore', line)
    self.assertIn('status=200', line)
    # Elapsed time is formatted as "%.2fs"; just confirm the suffix.
    self.assertRegex(line, r'in \d+\.\d{2}s')
    # Privacy: the grouping key must not appear anywhere in the log
    # output — neither the value (could be PII) nor a 'key=' label.
    for log_line in ctx.output:
      self.assertNotIn(sensitive_key, log_line)
      self.assertNotIn('key=', log_line)

  def test_anomaly_message_default(self):
    dofn = self._make_dofn({'q': '{anomaly_message}'})
    dofn.process(self._make_result(label=1, value=12.0))
    body_q = dofn._session.calls[0]['json']['q']
    self.assertIn('Anomaly detected', body_q)
    self.assertIn('value=12.0', body_q)

  def test_anomaly_message_from_message_format(self):
    dofn = self._make_dofn(
        {'q': '{anomaly_message}'},
        message_format='custom alert v={value}')
    dofn.process(self._make_result(label=1, value=99.0))
    self.assertEqual(
        dofn._session.calls[0]['json']['q'], 'custom alert v=99.0')

  def test_metadata_visible_in_body(self):
    dofn = self._make_dofn(
        {'q': '{env} v={value}'},
        message_metadata={'env': 'prod'})
    dofn.process(self._make_result(label=1, value=7.0))
    self.assertEqual(
        dofn._session.calls[0]['json']['q'], 'prod v=7.0')

  def test_keyed_element_substitutes_key(self):
    dofn = self._make_dofn({'q': 'k={key}'})
    dofn.process(('campaign_search', self._make_result(label=1)))
    self.assertEqual(
        dofn._session.calls[0]['json']['q'], 'k=campaign_search')

  def test_headers_substituted(self):
    dofn = self._make_dofn(
        {'q': 'x'},
        headers={'X-Anomaly-Key': '{key}'})
    dofn.process(('campaign_a', self._make_result(label=1)))
    self.assertEqual(
        dofn._session.calls[0]['headers'],
        {'X-Anomaly-Key': 'campaign_a'})

  def test_nested_body_substituted(self):
    dofn = self._make_dofn({
        'messages': [{
            'userMessage': {
                'text': '{anomaly_message}',
                'runtime_params': {'static': 'value'},
            },
        }],
        'dataAgentContext': {'dataAgent': 'projects/p/dataAgents/a'},
    })
    dofn.process(self._make_result(label=1, value=88.0))
    posted = dofn._session.calls[0]['json']
    self.assertIn(
        'value=88.0', posted['messages'][0]['userMessage']['text'])
    self.assertEqual(
        posted['messages'][0]['userMessage']['runtime_params']['static'],
        'value')
    self.assertEqual(
        posted['dataAgentContext']['dataAgent'],
        'projects/p/dataAgents/a')

  def test_5xx_raises_for_retry(self):
    """5xx → transient; bundle retry covers server-side flapping."""
    dofn = self._make_dofn({'q': '{value}'}, status_code=500)
    with self.assertRaises(RuntimeError):
      dofn.process(self._make_result(label=1))
    # The POST was attempted even though it failed.
    self.assertEqual(len(dofn._session.calls), 1)

  def test_503_raises_for_retry(self):
    dofn = self._make_dofn({'q': '{value}'}, status_code=503)
    with self.assertRaises(RuntimeError):
      dofn.process(self._make_result(label=1))

  def test_429_raises_for_retry(self):
    """429 Too Many Requests → transient; back off and retry."""
    dofn = self._make_dofn({'q': '{value}'}, status_code=429)
    with self.assertRaises(RuntimeError):
      dofn.process(self._make_result(label=1))

  def test_408_raises_for_retry(self):
    dofn = self._make_dofn({'q': '{value}'}, status_code=408)
    with self.assertRaises(RuntimeError):
      dofn.process(self._make_result(label=1))

  def test_permanent_4xx_dropped(self):
    """Permanent 4xx (e.g. 400 bad request) is logged and dropped, not
    raised. Raising would cause streaming Dataflow to retry the bundle
    indefinitely, blocking pipeline progress on a misconfigured run."""
    for status in (400, 401, 403, 404, 422):
      dofn = self._make_dofn({'q': '{value}'}, status_code=status)
      # Should not raise.
      dofn.process(self._make_result(label=1))
      # Should still have attempted the POST once.
      self.assertEqual(
          len(dofn._session.calls), 1,
          f'expected one call for status {status}')

  def test_method_propagated(self):
    dofn = self._make_dofn({'q': 'x'}, method='PUT')
    dofn.process(self._make_result(label=1))
    self.assertEqual(dofn._session.calls[0]['method'], 'PUT')

  def test_timeout_propagated(self):
    dofn = self._make_dofn({'q': 'x'})
    dofn.process(self._make_result(label=1))
    self.assertEqual(dofn._session.calls[0]['timeout'], 10.0)


class AnomalyIdTest(unittest.TestCase):
  """Tests for _anomaly_id used by AsyncWrapper for per-element dedup."""

  def _result(self, ws=1000, we=1001, model_id='ZScore', value=42.0):
    row = beam.Row(
        value=value, window_start=Timestamp(ws), window_end=Timestamp(we))
    pred = AnomalyPrediction(model_id=model_id, score=1.0, label=1)
    return AnomalyResult(example=row, predictions=[pred])

  def test_stable_for_same_anomaly(self):
    a = self._result()
    b = self._result()
    self.assertEqual(_anomaly_id(a), _anomaly_id(b))

  def test_differs_by_window(self):
    self.assertNotEqual(
        _anomaly_id(self._result(ws=1000)),
        _anomaly_id(self._result(ws=2000)))

  def test_differs_by_model_id(self):
    self.assertNotEqual(
        _anomaly_id(self._result(model_id='ZScore')),
        _anomaly_id(self._result(model_id='IQR')))

  def test_hashable(self):
    # AsyncWrapper stores ids as dict keys, so they must be hashable.
    hash(_anomaly_id(self._result()))


class KeyAnomalyForAsyncTest(unittest.TestCase):
  """Tests for _key_anomaly_for_async."""

  def test_already_keyed_string_passes_through_unchanged(self):
    self.assertEqual(_key_anomaly_for_async(('mykey', 'val')), ('mykey', 'val'))

  def test_tuple_key_stringified(self):
    """Regression: upstream keyed pipelines emit tuple keys (e.g.
    ``('campaign_a',)`` for a single-column ``group_by``). AsyncWrapper
    feeds the key to ``random.seed`` which rejects tuples, so the key
    must be stringified before reaching the stateful DoFns."""
    out = _key_anomaly_for_async((('campaign_a',), 'val'))
    self.assertEqual(out, ("('campaign_a',)", 'val'))
    import random as _random
    _random.seed(out[0])  # would TypeError on tuple before the fix

  def test_multi_column_tuple_key_stringified(self):
    out = _key_anomaly_for_async((('search', 'mobile'), 'val'))
    self.assertEqual(out, ("('search', 'mobile')", 'val'))

  def test_unkeyed_gets_sentinel(self):
    self.assertEqual(
        _key_anomaly_for_async('val'), (_UNKEYED_SENTINEL, 'val'))

  def test_unkeyed_anomaly_result(self):
    result = AnomalyResult(
        example=beam.Row(
            value=1.0,
            window_start=Timestamp(0), window_end=Timestamp(1)),
        predictions=[
            AnomalyPrediction(model_id='X', score=1.0, label=1)])
    out = _key_anomaly_for_async(result)
    self.assertEqual(out[0], _UNKEYED_SENTINEL)
    self.assertIs(out[1], result)


class _FakeBagState:
  """In-memory stand-in for a Beam BagStateSpec.

  Matches the contract AsyncWrapper expects (``add``, ``clear``,
  ``read``). Modeled directly on the helper used in Beam's own
  apache_beam/transforms/async_dofn_test.py so the test patterns
  translate one-to-one.
  """

  def __init__(self, items):
    self.items = list(items)
    self.lock = __import__('threading').Lock()

  def add(self, item):
    with self.lock:
      self.items.append(item)

  def clear(self):
    with self.lock:
      self.items = []

  def read(self):
    with self.lock:
      return self.items.copy()


class _FakeTimer:
  def __init__(self, t):
    self.time = t

  def set(self, t):
    self.time = t


class AsyncWebhookWiringTest(unittest.TestCase):
  """End-to-end tests of _PostAnomalyToWebhook wrapped in AsyncWrapper.

  We bypass the real GCP auth path by overriding the inner DoFn's setup
  (so AsyncWrapper.setup doesn't try to fetch ADC) and inject a
  _StubSession before triggering any processing. State is simulated
  with _FakeBagState / _FakeTimer per the Beam-internal test pattern.
  """

  def setUp(self):
    super().setUp()
    # Reset the wrapper's module-level pool registry so tests don't
    # leak state into each other.
    from apache_beam.transforms.async_dofn import AsyncWrapper as _AW
    _AW.reset_state()

  def _build(self, body, *, status_code=200, parallelism=2,
             callback_frequency=0.1):
    from apache_beam.transforms.async_dofn import AsyncWrapper as _AW
    spec = {
        'endpoint': 'https://example.com/x',
        'body': body,
        'method': 'POST',
        'headers': {},
        'scopes': ['https://www.googleapis.com/auth/cloud-platform'],
        'timeout_seconds': 10.0,
    }
    sync_dofn = _PostAnomalyToWebhook(spec, None, None)
    stub = _StubSession(status_code=status_code)
    # Bypass real google.auth.default() by overriding setup; AsyncWrapper
    # calls sync_dofn.setup() inside its own setup().
    sync_dofn.setup = lambda: setattr(sync_dofn, '_session', stub)
    async_dofn = _AW(
        sync_dofn,
        parallelism=parallelism,
        callback_frequency=callback_frequency,
        id_fn=_anomaly_id)
    async_dofn.setup()
    return async_dofn, sync_dofn, stub

  def _make_result(self, label=1, value=42.0, ws=1000, we=1001):
    row = beam.Row(
        value=value,
        window_start=Timestamp(ws), window_end=Timestamp(we))
    pred = AnomalyPrediction(model_id='ZScore', score=5.0, label=label)
    return AnomalyResult(example=row, predictions=[pred])

  def _wait_empty(self, async_dofn, timeout=10):
    elapsed = 0
    while not async_dofn.is_empty():
      time.sleep(0.5)
      elapsed += 0.5
      if elapsed > timeout:
        raise TimeoutError('async dofn did not drain')
    # Give callbacks a moment to settle once the future completes.
    time.sleep(0.5)

  def test_outlier_posts_via_async_wrapper(self):
    async_dofn, _, stub = self._build({'q': 'value={value}'})
    msg = ('k1', self._make_result(label=1, value=99.0))
    state = _FakeBagState([])
    timer = _FakeTimer(0)

    result = async_dofn.process(msg, to_process=state, timer=timer)
    # Async: nothing emitted yet; element is queued in bag state.
    self.assertEqual(result, [])
    self.assertEqual(state.items, [msg])
    self.assertNotEqual(timer.time, 0)  # timer was set

    self._wait_empty(async_dofn)
    # Now drive the timer callback to harvest the finished future.
    async_dofn.commit_finished_items(state, timer)
    self.assertEqual(state.items, [])

    # The inner DoFn's stub session recorded exactly one POST with the
    # substituted body.
    self.assertEqual(len(stub.calls), 1)
    self.assertEqual(stub.calls[0]['json'], {'q': 'value=99.0'})

class _FakeReadModifyWriteState:
  """In-memory stand-in for a Beam ReadModifyWriteStateSpec, used to
  unit-test _RateLimitAlerts without a real pipeline.

  Matches the contract the DoFn relies on: ``read()`` returns the last
  value written (or None if never written), ``write(v)`` overwrites,
  ``clear()`` resets to the never-written state.
  """

  def __init__(self):
    self._value = None

  def read(self):
    return self._value

  def write(self, v):
    self._value = v

  def clear(self):
    self._value = None


class IsOutlierTest(unittest.TestCase):
  """Tests for _is_outlier, the filter predicate that gates entry to the
  rate-limit stage. Without this filter, the keyed reshuffle in front of
  _RateLimitAlerts would shuffle every detection result (outlier, normal,
  warmup) instead of just the small minority that need alerting.
  """

  def _result(self, label):
    row = beam.Row(
        value=1.0, window_start=Timestamp(0), window_end=Timestamp(1))
    return AnomalyResult(
        example=row,
        predictions=[
            AnomalyPrediction(model_id='ZScore', score=1.0, label=label)])

  def test_outlier_label_one_kept(self):
    self.assertTrue(_is_outlier(self._result(label=1)))

  def test_normal_label_zero_filtered(self):
    self.assertFalse(_is_outlier(self._result(label=0)))

  def test_warmup_label_minus_two_filtered(self):
    self.assertFalse(_is_outlier(self._result(label=-2)))

  def test_keyed_element_unwrapped_correctly(self):
    """Keyed-pipeline tuples must be unwrapped to test the value's label."""
    self.assertTrue(_is_outlier(('campaign_a', self._result(label=1))))
    self.assertFalse(_is_outlier(('campaign_a', self._result(label=0))))


class RateLimitAlertsTest(unittest.TestCase):
  """Tests for _RateLimitAlerts.

  The DoFn is stateful (per-key ReadModifyWriteStateSpec). We bypass
  Beam's state plumbing by passing a _FakeReadModifyWriteState directly
  as the keyword argument the DoFn declares for the StateParam.
  """

  def _make_element(self, window_end_sec, key='k1', label=1):
    """Build a (key, AnomalyResult) element. Only window_end matters
    for the rate limiter's decision; window_start is arbitrary.

    ``label`` defaults to 1 (outlier) for the typical test path.
    Pass 0 for "normal" or -2 for "warmup" to exercise the
    label-gating behavior."""
    row = beam.Row(
        value=1.0,
        window_start=Timestamp(max(0, window_end_sec - 1)),
        window_end=Timestamp(window_end_sec))
    pred = AnomalyPrediction(model_id='ZScore', score=5.0, label=label)
    return (key, AnomalyResult(example=row, predictions=[pred]))

  def _run(self, dofn, element, state):
    return list(dofn.process(element, last_event=state))

  def test_first_anomaly_fires(self):
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    out = self._run(dofn, self._make_element(window_end_sec=100), state)
    self.assertEqual(len(out), 1, 'first event for a key must fire')
    # last_event_micros recorded.
    self.assertEqual(state.read(), 100 * 1_000_000)

  def test_within_cooldown_suppressed(self):
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    # T0 fires.
    self._run(dofn, self._make_element(window_end_sec=0), state)
    # T+5 min: well within 10 min cooldown.
    out = self._run(dofn, self._make_element(window_end_sec=300), state)
    self.assertEqual(out, [], 'within-cooldown event must be suppressed')
    # State STILL advanced — session-window semantics.
    self.assertEqual(state.read(), 300 * 1_000_000)

  def test_after_cooldown_fires(self):
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    self._run(dofn, self._make_element(window_end_sec=0), state)
    out = self._run(
        dofn, self._make_element(window_end_sec=600), state)
    self.assertEqual(len(out), 1, 'event >=cooldown after prior must fire')
    self.assertEqual(state.read(), 600 * 1_000_000)

  def test_session_extends_with_continuous_events(self):
    """User's canonical example: events at T0, T1, T2, T8, T19 with a
    10-minute (600s) cooldown. Only T0 and T19 should fire — even
    though T19 is more than 10 min after the first fire, every event
    in between has been pushing last_event forward."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    fired = []
    for minute in (0, 1, 2, 8, 19):
      out = self._run(
          dofn, self._make_element(window_end_sec=minute * 60), state)
      if out:
        fired.append(minute)
    self.assertEqual(
        fired, [0, 19],
        f'expected fires at minutes [0, 19] only; got {fired}')

  def test_cooldown_zero_is_passthrough_and_never_touches_state(self):
    """cooldown=0 is the documented "rate limiting disabled" mode.
    The DoFn must:
      - yield every outlier (no suppression), AND
      - never read or write state.

    The second property matters for Dataflow --update: the stage is
    always topologically present so operators can toggle the cooldown
    without redeploying, and that toggle must not leave stale state
    behind from the disabled-cooldown phase. We assert state stays
    untouched by checking the FakeReadModifyWriteState's internal
    value stays at its initial None across many invocations.
    """
    dofn = _RateLimitAlerts(cooldown_seconds=0)
    state = _FakeReadModifyWriteState()
    for sec in (0, 1, 2, 3, 600, 6000):
      out = self._run(dofn, self._make_element(window_end_sec=sec), state)
      self.assertEqual(len(out), 1, f'event at {sec}s should fire')
    self.assertIsNone(
        state.read(),
        'cooldown=0 must not write state, but state is %r' % state.read())

  def test_negative_cooldown_is_also_passthrough(self):
    """Defensive: negative cooldowns are treated the same as 0 (the
    `<= 0` boundary). This guards against accidentally passing -1 as
    a "disabled" sentinel through some other code path."""
    dofn = _RateLimitAlerts(cooldown_seconds=-1)
    state = _FakeReadModifyWriteState()
    for sec in (0, 100, 200):
      out = self._run(dofn, self._make_element(window_end_sec=sec), state)
      self.assertEqual(len(out), 1)
    self.assertIsNone(state.read())

  def test_exactly_at_cooldown_fires(self):
    """The rule is `delta >= cooldown`, not strictly `>`, so an event
    at exactly cooldown_seconds after the prior must fire."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    self._run(dofn, self._make_element(window_end_sec=0), state)
    out = self._run(
        dofn, self._make_element(window_end_sec=600), state)
    self.assertEqual(len(out), 1)

  def test_state_is_per_key_in_practice(self):
    """The DoFn body doesn't manage per-key sharding itself — Beam does
    that by giving the DoFn a fresh state instance per key. This test
    documents the contract by passing independent state instances for
    two different keys and verifying neither suppresses the other's
    first event."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state_a = _FakeReadModifyWriteState()
    state_b = _FakeReadModifyWriteState()
    # Both keys fire at T0 because each has its own state.
    self.assertEqual(
        len(self._run(
            dofn, self._make_element(window_end_sec=0, key='a'),
            state_a)), 1)
    self.assertEqual(
        len(self._run(
            dofn, self._make_element(window_end_sec=0, key='b'),
            state_b)), 1)
    # Key a's next event is within cooldown — suppressed under state_a.
    self.assertEqual(
        len(self._run(
            dofn, self._make_element(window_end_sec=60, key='a'),
            state_a)), 0)
    # But key b's next event is also within cooldown — suppressed under
    # state_b, INDEPENDENT of what state_a is doing.
    self.assertEqual(
        len(self._run(
            dofn, self._make_element(window_end_sec=60, key='b'),
            state_b)), 0)

  def test_normal_label_passes_through_without_touching_state(self):
    """Regression: only outliers (label=1) should affect state.
    Normal results (label=0) must pass through unchanged and must NOT
    write last_event, otherwise a quiet baseline of normal traffic
    would silently extend the session before any real outlier."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    # Feed a stream of normal results at increasing timestamps.
    for sec in (0, 60, 120, 180):
      out = self._run(
          dofn, self._make_element(window_end_sec=sec, label=0), state)
      self.assertEqual(len(out), 1, 'normal results must pass through')
    # State must still be unset — normals do not touch it.
    self.assertIsNone(
        state.read(),
        f'normals must not write last_event; got {state.read()!r}')

  def test_warmup_label_passes_through_without_touching_state(self):
    """Same as the normal case but for warmup results (label=-2)."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    for sec in (0, 60, 120):
      out = self._run(
          dofn, self._make_element(window_end_sec=sec, label=-2), state)
      self.assertEqual(len(out), 1, 'warmup results must pass through')
    self.assertIsNone(state.read())

  def test_warmup_then_outlier_fires_first_outlier(self):
    """Warmup results before the first outlier must NOT eat the
    initial fire. The outlier's session starts fresh."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    # 5 minutes of warmup at +60s intervals — none should touch state.
    for sec in (0, 60, 120, 180, 240, 300):
      self._run(
          dofn, self._make_element(window_end_sec=sec, label=-2), state)
    # First real outlier at T=350s: must fire (no prior outlier).
    out = self._run(
        dofn, self._make_element(window_end_sec=350, label=1), state)
    self.assertEqual(
        len(out), 1,
        'first outlier must fire even after a long warmup phase')
    self.assertEqual(state.read(), 350 * 1_000_000)

  def test_outliers_interleaved_with_normals(self):
    """A real-world scenario: outliers at minutes 0 and 11, with a
    stream of normal results in between. The normals must not extend
    the session, so the outlier at 11min still fires (11 >= 10)."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    fired = []
    # T0: outlier → fires.
    if self._run(dofn, self._make_element(0, label=1), state):
      fired.append(0)
    # T1..T10 minutes: normals (label=0).
    for m in range(1, 11):
      self._run(dofn, self._make_element(m * 60, label=0), state)
    # T11 min: outlier — gap from prior outlier (T0) is 11 min ≥ 10.
    if self._run(dofn, self._make_element(11 * 60, label=1), state):
      fired.append(11)
    self.assertEqual(
        fired, [0, 11],
        'normals between outliers must NOT extend the session')

  def test_suppress_logs_window_not_key(self):
    """Privacy: the suppression log must include the anomaly window
    but not the grouping key (matches the webhook DoFn's policy)."""
    dofn = _RateLimitAlerts(cooldown_seconds=600)
    state = _FakeReadModifyWriteState()
    # Fire once to seed state.
    self._run(
        dofn, self._make_element(window_end_sec=0, key='SECRET_KEY'),
        state)
    # Now suppress with a recognizable key.
    with self.assertLogs('bqmonitor.pipeline', level='INFO') as ctx:
      self._run(
          dofn,
          self._make_element(window_end_sec=60, key='SECRET_KEY'),
          state)
    suppressed = [r for r in ctx.output if 'suppressed' in r]
    self.assertEqual(
        len(suppressed), 1, f'expected 1 suppression log, got: {ctx.output}')
    line = suppressed[0]
    self.assertIn('window=', line)
    for log_line in ctx.output:
      self.assertNotIn('SECRET_KEY', log_line)
      self.assertNotIn('key=', log_line)


if __name__ == '__main__':
  unittest.main()
