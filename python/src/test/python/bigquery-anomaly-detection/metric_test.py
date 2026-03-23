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

from bqmonitor.metric import AggOp
from bqmonitor.metric import AggregationSpec
from bqmonitor.metric import DerivedField
from bqmonitor.metric import MeasureSpec
from bqmonitor.metric import MetricSpec
from bqmonitor.metric import WindowSpec
from bqmonitor.metric import WindowType
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


if __name__ == '__main__':
  unittest.main()
