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

"""Unit tests for bqmonitor.metric."""

import json
import logging
import unittest

logging.basicConfig(level=logging.INFO)

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window as beam_window
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.utils.timestamp import Timestamp

from bqmonitor.metric import AggOp
from bqmonitor.metric import AggregationSpec
from bqmonitor.metric import ComputeMetric
from bqmonitor.metric import DerivedField
from bqmonitor.metric import FanoutStrategy
from bqmonitor.metric import MeasureSpec
from bqmonitor.metric import MetricSpec
from bqmonitor.metric import WindowSpec
from bqmonitor.metric import WindowType
from bqmonitor.metric import _MapperSidePrecombine
from bqmonitor.metric import _PostCombineFn
from bqmonitor.metric import _SumCombineFn
from bqmonitor.safe_eval import Expr


class MetricSpecValidationTest(unittest.TestCase):
  """Tests for MetricSpec validation."""

  def _simple_spec(self, **kwargs):
    defaults = dict(
        aggregation=AggregationSpec(
            window=WindowSpec(type=WindowType.FIXED, size_seconds=60),
            measures=[MeasureSpec(field='amount', agg=AggOp.SUM,
                                  alias='total')],
        ))
    defaults.update(kwargs)
    return MetricSpec(**defaults)

  def test_simple_spec_valid(self):
    spec = self._simple_spec()
    self.assertEqual(len(spec.aggregation.measures), 1)

  def test_no_measures_raises(self):
    # @specifiable uses lazy init; access an attribute to trigger validation.
    with self.assertRaises(ValueError) as ctx:
      spec = MetricSpec(aggregation=AggregationSpec(measures=[]))
      _ = spec.aggregation
    self.assertIn('at least one measure', str(ctx.exception))

  def test_multiple_measures_without_combiner_raises(self):
    with self.assertRaises(ValueError) as ctx:
      spec = MetricSpec(aggregation=AggregationSpec(
          measures=[
              MeasureSpec(field='a', agg=AggOp.SUM, alias='x'),
              MeasureSpec(field='b', agg=AggOp.COUNT, alias='y'),
          ]))
      _ = spec.aggregation
    self.assertIn('measure_combiner is required', str(ctx.exception))

  def test_multiple_measures_with_combiner(self):
    spec = MetricSpec(
        aggregation=AggregationSpec(
            measures=[
                MeasureSpec(field='a', agg=AggOp.SUM, alias='clicks'),
                MeasureSpec(field='a', agg=AggOp.COUNT, alias='impressions'),
            ]),
        measure_combiner=Expr('clicks / impressions'))
    self.assertIsNotNone(spec.measure_combiner)

  def test_combiner_unknown_field_raises(self):
    with self.assertRaises(ValueError) as ctx:
      spec = MetricSpec(
          aggregation=AggregationSpec(
              measures=[
                  MeasureSpec(field='a', agg=AggOp.SUM, alias='clicks'),
              ]),
          measure_combiner=Expr('clicks / impressions'))
      _ = spec.aggregation
    self.assertIn('unknown fields', str(ctx.exception))

  def test_sliding_without_period_raises(self):
    with self.assertRaises(ValueError) as ctx:
      spec = MetricSpec(aggregation=AggregationSpec(
          window=WindowSpec(type=WindowType.SLIDING, size_seconds=60),
          measures=[MeasureSpec(field='a', agg=AggOp.SUM, alias='x')]))
      _ = spec.aggregation
    self.assertIn('period_seconds', str(ctx.exception))

  def test_sliding_with_period(self):
    spec = MetricSpec(aggregation=AggregationSpec(
        window=WindowSpec(
            type=WindowType.SLIDING, size_seconds=60, period_seconds=10),
        measures=[MeasureSpec(field='a', agg=AggOp.SUM, alias='x')]))
    self.assertEqual(spec.aggregation.window.period_seconds, 10)


class MetricSpecRequiredColumnsTest(unittest.TestCase):
  """Tests for required_source_columns()."""

  def test_simple_sum(self):
    spec = MetricSpec(aggregation=AggregationSpec(
        measures=[MeasureSpec(field='amount', agg=AggOp.SUM, alias='total')]))
    self.assertEqual(spec.required_source_columns(), {'amount'})

  def test_count_excludes_field(self):
    spec = MetricSpec(aggregation=AggregationSpec(
        measures=[MeasureSpec(field='x', agg=AggOp.COUNT, alias='cnt')]))
    self.assertEqual(spec.required_source_columns(), set())

  def test_group_by_included(self):
    spec = MetricSpec(aggregation=AggregationSpec(
        group_by=['region', 'product'],
        measures=[MeasureSpec(field='amount', agg=AggOp.SUM, alias='total')]))
    self.assertEqual(
        spec.required_source_columns(), {'region', 'product', 'amount'})

  def test_derived_field_references(self):
    spec = MetricSpec(
        aggregation=AggregationSpec(
            measures=[MeasureSpec(
                field='is_success', agg=AggOp.SUM, alias='successes')]),
        derived_fields=[
            DerivedField(
                name='is_success',
                expression=Expr("1 if status == 'ok' else 0"))
        ])
    # 'is_success' is derived, so excluded; 'status' is a source ref.
    self.assertEqual(spec.required_source_columns(), {'status'})

  def test_ctr_metric(self):
    spec = MetricSpec(
        aggregation=AggregationSpec(
            group_by=['campaign_type'],
            measures=[
                MeasureSpec(field='is_click', agg=AggOp.SUM, alias='clicks'),
                MeasureSpec(
                    field='is_click', agg=AggOp.COUNT, alias='impressions'),
            ]),
        measure_combiner=Expr('clicks / impressions'))
    # is_click (from SUM), campaign_type (from group_by).
    # COUNT's field is excluded.
    self.assertEqual(
        spec.required_source_columns(), {'is_click', 'campaign_type'})


class MetricSpecFromDictTest(unittest.TestCase):
  """Tests for MetricSpec.from_dict() deserialization."""

  def test_simple(self):
    d = {
        'aggregation': {
            'window': {'type': 'fixed', 'size_seconds': 60},
            'measures': [
                {'field': 'amount', 'agg': 'SUM', 'alias': 'total'}],
        }}
    spec = MetricSpec.from_dict(d)
    self.assertEqual(spec.aggregation.window.type, WindowType.FIXED)
    self.assertEqual(spec.aggregation.window.size_seconds, 60)
    self.assertEqual(len(spec.aggregation.measures), 1)
    self.assertEqual(spec.aggregation.measures[0].agg, AggOp.SUM)

  def test_with_combiner(self):
    d = {
        'aggregation': {
            'measures': [
                {'field': 'x', 'agg': 'SUM', 'alias': 'clicks'},
                {'field': 'x', 'agg': 'COUNT', 'alias': 'impressions'}],
        },
        'measure_combiner': {'expression': 'clicks / impressions'},
    }
    spec = MetricSpec.from_dict(d)
    self.assertIsNotNone(spec.measure_combiner)
    self.assertEqual(spec.measure_combiner.field_refs(), {'clicks', 'impressions'})

  def test_with_derived_fields(self):
    d = {
        'aggregation': {
            'measures': [
                {'field': 'is_ok', 'agg': 'SUM', 'alias': 'ok_count'}],
        },
        'derived_fields': [
            {'name': 'is_ok', 'expression': "1 if status == 'ok' else 0"}],
    }
    spec = MetricSpec.from_dict(d)
    self.assertEqual(len(spec.derived_fields), 1)
    self.assertEqual(spec.derived_fields[0].name, 'is_ok')

  def test_roundtrip_json(self):
    """from_dict(to_dict(spec)) should produce an equivalent spec."""
    original = MetricSpec(
        aggregation=AggregationSpec(
            window=WindowSpec(
                type=WindowType.SLIDING, size_seconds=300, period_seconds=60),
            group_by=['region'],
            measures=[
                MeasureSpec(field='a', agg=AggOp.SUM, alias='clicks'),
                MeasureSpec(field='a', agg=AggOp.COUNT, alias='impressions'),
            ]),
        measure_combiner=Expr('clicks / impressions'),
        name='ctr')
    d = original.to_dict()
    # Verify JSON-serializable.
    json_str = json.dumps(d)
    restored = MetricSpec.from_dict(json.loads(json_str))
    self.assertEqual(restored.name, 'ctr')
    self.assertEqual(restored.aggregation.window.type, WindowType.SLIDING)
    self.assertEqual(restored.aggregation.window.period_seconds, 60)
    self.assertEqual(len(restored.aggregation.measures), 2)
    self.assertEqual(
        restored.required_source_columns(), {'a', 'region'})


class MetricSpecToDictTest(unittest.TestCase):
  """Tests for MetricSpec.to_dict() serialization."""

  def test_simple(self):
    spec = MetricSpec(aggregation=AggregationSpec(
        window=WindowSpec(type=WindowType.FIXED, size_seconds=60),
        measures=[MeasureSpec(field='amount', agg=AggOp.SUM, alias='total')]))
    d = spec.to_dict()
    self.assertEqual(d['aggregation']['window']['type'], 'fixed')
    self.assertEqual(d['aggregation']['measures'][0]['agg'], 'SUM')

  def test_excludes_optional_none(self):
    spec = MetricSpec(aggregation=AggregationSpec(
        measures=[MeasureSpec(field='x', agg=AggOp.SUM, alias='y')]))
    d = spec.to_dict()
    self.assertNotIn('derived_fields', d)
    self.assertNotIn('measure_combiner', d)
    self.assertNotIn('name', d)


class MapperSidePrecombineDoFnTest(unittest.TestCase):
  """Unit tests for _MapperSidePrecombine DoFn."""

  def _run_precombine(self, combine_fn, windowed_values, max_keys=100_000):
    """Run _MapperSidePrecombine on a list of WindowedValues, return results."""
    dofn = _MapperSidePrecombine(combine_fn, max_keys=max_keys)
    dofn.setup()
    dofn.start_bundle()
    mid_results = []
    for wv in windowed_values:
      result = dofn.process(
          wv.value, window=wv.windows[0], timestamp=wv.timestamp)
      if result:
        mid_results.extend(result)
    finish_results = list(dofn.finish_bundle())
    dofn.teardown()
    return mid_results + finish_results

  def _make_wv(self, key, value, timestamp=0, window=None):
    if window is None:
      window = beam_window.GlobalWindow()
    return WindowedValue((key, value), Timestamp(timestamp), (window,))

  def test_single_key_sums(self):
    """Multiple values for one key should be folded into one accumulator."""
    elements = [
        self._make_wv('a', 10),
        self._make_wv('a', 20),
        self._make_wv('a', 30),
    ]
    results = self._run_precombine(_SumCombineFn(), elements)
    self.assertEqual(len(results), 1)
    key, acc = results[0].value
    self.assertEqual(key, 'a')
    self.assertEqual(acc, 60)

  def test_multiple_keys(self):
    """Each key gets its own accumulator."""
    elements = [
        self._make_wv('a', 1),
        self._make_wv('b', 2),
        self._make_wv('a', 3),
        self._make_wv('b', 4),
    ]
    results = self._run_precombine(_SumCombineFn(), elements)
    result_dict = {wv.value[0]: wv.value[1] for wv in results}
    self.assertEqual(result_dict, {'a': 4, 'b': 6})

  def test_fixed_windows_separate(self):
    """Same key in different windows should produce separate accumulators."""
    w1 = beam_window.IntervalWindow(0, 60)
    w2 = beam_window.IntervalWindow(60, 120)
    elements = [
        self._make_wv('a', 10, timestamp=5, window=w1),
        self._make_wv('a', 20, timestamp=70, window=w2),
        self._make_wv('a', 30, timestamp=15, window=w1),
    ]
    results = self._run_precombine(_SumCombineFn(), elements)
    self.assertEqual(len(results), 2)
    by_window = {wv.windows[0]: wv.value for wv in results}
    self.assertEqual(by_window[w1], ('a', 40))
    self.assertEqual(by_window[w2], ('a', 20))

  def test_sliding_windows_separate(self):
    """An element assigned to overlapping sliding windows produces separate
    accumulators per window (upstream WindowInto does the expansion)."""
    w1 = beam_window.IntervalWindow(0, 60)
    w2 = beam_window.IntervalWindow(10, 70)
    # Simulate WindowInto expansion: one element in two windows.
    elements = [
        self._make_wv('a', 5, timestamp=15, window=w1),
        self._make_wv('a', 5, timestamp=15, window=w2),
        self._make_wv('a', 3, timestamp=25, window=w1),
        self._make_wv('a', 3, timestamp=25, window=w2),
    ]
    results = self._run_precombine(_SumCombineFn(), elements)
    self.assertEqual(len(results), 2)
    by_window = {wv.windows[0]: wv.value[1] for wv in results}
    self.assertEqual(by_window[w1], 8)
    self.assertEqual(by_window[w2], 8)

  def test_timestamp_preserves_earliest(self):
    """Output timestamp should be the earliest of all folded elements."""
    w = beam_window.IntervalWindow(0, 60)
    elements = [
        self._make_wv('a', 1, timestamp=30, window=w),
        self._make_wv('a', 2, timestamp=10, window=w),
        self._make_wv('a', 3, timestamp=50, window=w),
    ]
    results = self._run_precombine(_SumCombineFn(), elements)
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].timestamp, Timestamp(10))

  def test_window_metadata_preserved(self):
    """Output WindowedValue should carry the correct window."""
    w = beam_window.IntervalWindow(100, 200)
    elements = [self._make_wv('k', 42, timestamp=150, window=w)]
    results = self._run_precombine(_SumCombineFn(), elements)
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].windows, (w,))

  def test_eviction_under_pressure(self):
    """When max_keys is hit, entries are evicted (yielded from process)."""
    # max_keys=3, insert 4 distinct keys → should evict some during process.
    elements = [
        self._make_wv('a', 1),
        self._make_wv('b', 2),
        self._make_wv('c', 3),
        self._make_wv('d', 4),
    ]
    results = self._run_precombine(_SumCombineFn(), elements, max_keys=3)
    result_dict = {wv.value[0]: wv.value[1] for wv in results}
    self.assertEqual(result_dict, {'a': 1, 'b': 2, 'c': 3, 'd': 4})

  def test_evicted_values_not_lost(self):
    """Values added to evicted keys before eviction are in the output."""
    elements = [
        self._make_wv('a', 10),
        self._make_wv('a', 20),  # a=30 now
        self._make_wv('b', 5),
        self._make_wv('c', 7),
        # max_keys=2, so inserting 'c' triggers eviction of 'a' or 'b'.
        # After eviction, 'a' or 'b' is yielded mid-process.
    ]
    results = self._run_precombine(_SumCombineFn(), elements, max_keys=2)
    result_dict = {wv.value[0]: wv.value[1] for wv in results}
    self.assertEqual(result_dict['a'], 30)
    self.assertEqual(result_dict['b'], 5)
    self.assertEqual(result_dict['c'], 7)

  def test_empty_bundle(self):
    """No elements → no output."""
    results = self._run_precombine(_SumCombineFn(), [])
    self.assertEqual(results, [])


class MapperSidePrecombinePipelineTest(unittest.TestCase):
  """Integration tests: _MapperSidePrecombine + _PostCombineFn in a pipeline."""

  _STREAMING_OPTIONS = beam.options.pipeline_options.PipelineOptions(
      ['--streaming'])

  def test_keyed_sum_with_precombine(self):
    """Precombine + CombinePerKey(_PostCombineFn) produces correct sums."""
    spec = MetricSpec(
        aggregation=AggregationSpec(
            window=WindowSpec(type=WindowType.FIXED, size_seconds=60),
            group_by=['region'],
            measures=[MeasureSpec(field='amount', agg=AggOp.SUM,
                                  alias='total')]))
    rows = [
        {'region': 'us', 'amount': 10},
        {'region': 'us', 'amount': 20},
        {'region': 'eu', 'amount': 5},
        {'region': 'eu', 'amount': 15},
        {'region': 'us', 'amount': 30},
    ]
    with TestPipeline(options=self._STREAMING_OPTIONS) as p:
      timestamped = (
          p
          | beam.Create(rows)
          | beam.Map(lambda r: beam_window.TimestampedValue(r, 10)))
      result = timestamped | ComputeMetric(spec,
                                           fanout_strategy=FanoutStrategy.PRECOMBINE)
      totals = result | beam.MapTuple(
          lambda k, row: (k, row.value))

      assert_that(totals, equal_to([
          (('us',), 60.0),
          (('eu',), 20.0),
      ]))

  def test_global_sum_with_precombine(self):
    """Precombine + CombineGlobally(_PostCombineFn) produces correct sum."""
    spec = MetricSpec(
        aggregation=AggregationSpec(
            window=WindowSpec(type=WindowType.FIXED, size_seconds=60),
            measures=[MeasureSpec(field='amount', agg=AggOp.SUM,
                                  alias='total')]))
    rows = [{'amount': v} for v in [10, 20, 30, 40]]
    with TestPipeline(options=self._STREAMING_OPTIONS) as p:
      timestamped = (
          p
          | beam.Create(rows)
          | beam.Map(lambda r: beam_window.TimestampedValue(r, 10)))
      result = timestamped | ComputeMetric(
          spec, fanout_strategy=FanoutStrategy.PRECOMBINE)
      totals = result | beam.Map(lambda row: row.value)
      assert_that(totals, equal_to([100.0]))

  def test_multi_measure_with_precombine(self):
    """Precombine works with TupleCombineFn (multiple measures)."""
    spec = MetricSpec(
        aggregation=AggregationSpec(
            window=WindowSpec(type=WindowType.FIXED, size_seconds=60),
            group_by=['region'],
            measures=[
                MeasureSpec(field='amount', agg=AggOp.SUM, alias='total'),
                MeasureSpec(field='amount', agg=AggOp.COUNT, alias='cnt'),
            ]),
        measure_combiner=Expr('total / cnt'))
    rows = [
        {'region': 'us', 'amount': 10},
        {'region': 'us', 'amount': 30},
    ]
    with TestPipeline(options=self._STREAMING_OPTIONS) as p:
      timestamped = (
          p
          | beam.Create(rows)
          | beam.Map(lambda r: beam_window.TimestampedValue(r, 10)))
      result = timestamped | ComputeMetric(spec,
                                           fanout_strategy=FanoutStrategy.PRECOMBINE)
      values = result | beam.MapTuple(lambda k, row: row.value)
      assert_that(values, equal_to([20.0]))

  def test_sliding_window_with_precombine(self):
    """Precombine correctly separates accumulators across overlapping windows."""
    spec = MetricSpec(
        aggregation=AggregationSpec(
            window=WindowSpec(type=WindowType.SLIDING,
                              size_seconds=30, period_seconds=10),
            group_by=['key'],
            measures=[MeasureSpec(field='v', agg=AggOp.SUM, alias='total')]))
    rows = [
        {'key': 'a', 'v': 10},
        {'key': 'a', 'v': 20},
    ]
    # t=105 with size=30, period=10 → windows [80,110), [90,120), [100,130)
    with TestPipeline(options=beam.options.pipeline_options.PipelineOptions(
        ['--streaming'])) as p:
      timestamped = (
          p
          | beam.Create(rows)
          | beam.Map(lambda r: beam_window.TimestampedValue(r, 105)))
      result = timestamped | ComputeMetric(spec,
                                           fanout_strategy=FanoutStrategy.PRECOMBINE)
      totals = result | beam.MapTuple(lambda _, row: row.value)
      # Each of the 3 sliding windows sees both elements → sum=30.
      assert_that(totals, equal_to([30.0, 30.0, 30.0]))

  def test_precombine_matches_no_precombine(self):
    """PRECOMBINE strategy produces same results as NONE."""
    spec = MetricSpec(
        aggregation=AggregationSpec(
            window=WindowSpec(type=WindowType.FIXED, size_seconds=60),
            group_by=['key'],
            measures=[MeasureSpec(field='v', agg=AggOp.SUM, alias='total')]))
    rows = [{'key': k, 'v': v}
            for k in ['a', 'b', 'c']
            for v in range(1, 11)]

    def run_pipeline(strategy):
      with TestPipeline(options=self._STREAMING_OPTIONS) as p:
        timestamped = (
            p
            | beam.Create(rows)
            | beam.Map(lambda r: beam_window.TimestampedValue(r, 10)))
        result = timestamped | ComputeMetric(
            spec, fanout_strategy=strategy)
        totals = result | beam.MapTuple(lambda _, row: row.value)
        assert_that(totals, equal_to([55.0, 55.0, 55.0]))

    run_pipeline(FanoutStrategy.NONE)
    run_pipeline(FanoutStrategy.PRECOMBINE)


if __name__ == '__main__':
  unittest.main()
