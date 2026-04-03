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
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue

from parameterized import parameterized, param
from bqmonitor.metric import ComputeMetric, FanoutStrategy, MetricSpec
from bqmonitor.pipeline import _FormatAnomalyAsJson
from bqmonitor.pipeline import _FormatResultForBQ
from bqmonitor.pipeline import _parse_detector_spec
from bqmonitor.pipeline import _parse_table_ref
from bqmonitor.pipeline import _ThresholdAlert
from bqmonitor.pipeline import _unpack_result
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

  def test_custom_format_non_outlier_suppressed(self):
    dofn = _FormatAnomalyAsJson(message_format='alert: {value}')
    results = list(dofn.process(self._make_result(label=0)))
    self.assertEqual(len(results), 0)

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


if __name__ == '__main__':
  unittest.main()
