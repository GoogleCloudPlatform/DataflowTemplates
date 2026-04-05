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

"""Configurable metric computation for anomaly detection pipelines.

This module provides a ``MetricSpec`` configuration system and a
``ComputeMetric`` PTransform that computes windowed, grouped metrics from
raw row dicts (e.g., from ``ReadBigQueryChangeHistory``). The output is
suitable for feeding directly into ``AnomalyDetection``.

Example usage::

  from bqmonitor.metric import (
      MetricSpec, AggregationSpec, WindowSpec, MeasureSpec,
      DerivedField, WindowType, AggOp, ComputeMetric)
  from bqmonitor.safe_eval import Expr
  from apache_beam.ml.anomaly.transforms import AnomalyDetection
  from apache_beam.ml.anomaly.detectors.zscore import ZScore

  # CUJ 1: Total revenue per hour
  spec = MetricSpec(
      name='revenue',
      aggregation=AggregationSpec(
          window=WindowSpec(type=WindowType.FIXED, size_seconds=3600),
          measures=[MeasureSpec(
              field='transaction_amount', agg=AggOp.SUM, alias='revenue')],
      ),
  )
  result = cdc_rows | ComputeMetric(spec) | AnomalyDetection(ZScore())

  # CUJ 2: CTR grouped by dimensions
  spec = MetricSpec(
      name='ctr',
      aggregation=AggregationSpec(
          window=WindowSpec(type=WindowType.FIXED, size_seconds=86400),
          group_by=['campaign_type', 'user_segment'],
          measures=[
              MeasureSpec(field='is_click', agg=AggOp.SUM, alias='clicks'),
              MeasureSpec(field='is_click', agg=AggOp.COUNT,
                          alias='impressions'),
          ],
      ),
      measure_combiner=Expr.from_string("clicks / impressions"),
  )

  # CUJ 3: Success rate with derived field
  spec = MetricSpec(
      name='success_rate',
      derived_fields=[
          DerivedField(
              name='is_success',
              expression=Expr.from_string(
                  "1 if status == 'success' else 0")),
      ],
      aggregation=AggregationSpec(
          window=WindowSpec(type=WindowType.FIXED, size_seconds=86400),
          group_by=['brand_name', 'category'],
          measures=[
              MeasureSpec(field='is_success', agg=AggOp.SUM,
                          alias='successes'),
              MeasureSpec(field='is_success', agg=AggOp.COUNT, alias='total'),
          ],
      ),
      measure_combiner=Expr.from_string("successes / total"),
  )
"""

import dataclasses
import random
from enum import Enum
from typing import Any
from typing import Optional
from typing import Tuple

import apache_beam as beam
from apache_beam.transforms import combiners
from apache_beam.transforms import window as beam_window

from bqmonitor.safe_eval import Expr
from apache_beam.ml.anomaly.specifiable import specifiable


class WindowType(Enum):
  """Window type for metric aggregation."""
  FIXED = 'fixed'
  SLIDING = 'sliding'


class AggOp(Enum):
  """Aggregation operator."""
  SUM = 'SUM'
  COUNT = 'COUNT'
  MIN = 'MIN'
  MAX = 'MAX'
  MEAN = 'MEAN'


class FanoutStrategy(Enum):
  """Strategy for aggregation parallelism. Applies to both keyed and global.

  NONE: Plain Combine, no fanout. Relies on combiner lifting for mapper-side
      pre-combining.
  HOTKEY_FANOUT: Uses Combine.with_fanout(). Per-bundle nonce sharding.
      Good when only a few keys are hot.
  SHARDED: Per-element random sharding with _PreCombineFn/_PostCombineFn.
      Uniform distribution regardless of bundle count. One extra GBK, but
      distributes streaming state I/O across shard keys.
  PRECOMBINE: Mapper-side pre-aggregation within each bundle before shuffle.
      Reduces shuffle volume by folding values per key locally (like
      PGBKCVOperation) while preserving Dataflow incremental state-side
      merging on the downstream Combine.
  """
  NONE = 'none'
  HOTKEY_FANOUT = 'hotkey_fanout'
  SHARDED = 'sharded'
  PRECOMBINE = 'precombine'


@dataclasses.dataclass(frozen=True)
class WindowSpec:
  """Window configuration for metric aggregation.

  Args:
    type: FIXED or SLIDING window.
    size_seconds: Window size in seconds.
    period_seconds: Slide period in seconds (required for SLIDING, ignored for
      FIXED).
  """
  type: WindowType = WindowType.FIXED
  size_seconds: float = 3600
  period_seconds: Optional[float] = None


@dataclasses.dataclass(frozen=True)
class DerivedField:
  """Pre-aggregation column derivation via expression.

  Args:
    name: Name of the new field to create.
    expression: A compiled ``Expr`` callable, e.g.
      ``Expr.from_string("1 if status == 'success' else 0")``.
  """
  name: str
  expression: Expr


@dataclasses.dataclass(frozen=True)
class MeasureSpec:
  """A single aggregation measure.

  Args:
    field: Input field name to aggregate.
    agg: The aggregation operator.
    alias: Output name for this measure's result.
  """
  field: str
  agg: AggOp
  alias: str


@dataclasses.dataclass(frozen=True)
class AggregationSpec:
  """Windowed grouped aggregation configuration.

  Args:
    window: Window configuration.
    group_by: Field names for grouping. Empty list means global aggregation.
    measures: List of aggregation measures.
  """
  window: WindowSpec = dataclasses.field(default_factory=WindowSpec)
  group_by: list = dataclasses.field(default_factory=list)
  measures: list = dataclasses.field(default_factory=list)


@specifiable
class MetricSpec:
  """Complete metric computation specification.

  Defines how to transform raw row dicts into a single numeric metric value
  suitable for anomaly detection.

  Args:
    aggregation: Windowed grouped aggregation spec.
    derived_fields: Optional pre-aggregation derived fields.
    measure_combiner: Optional post-aggregation ``Expr`` operating on measure
      aliases. Required when there are multiple measures.
    name: Optional human-readable metric name.
  """
  def __init__(
      self,
      aggregation,
      derived_fields=None,
      measure_combiner=None,
      name=None,
  ):
    self.name = name
    self.aggregation = aggregation
    self.derived_fields = derived_fields or []
    self.measure_combiner = measure_combiner
    self._validate()

  def _validate(self):
    agg = self.aggregation
    if not agg.measures:
      raise ValueError("MetricSpec requires at least one measure")
    if self.measure_combiner is None and len(agg.measures) > 1:
      raise ValueError(
          "measure_combiner is required when there are multiple measures. "
          f"Got {len(agg.measures)} measures: "
          f"{[m.alias for m in agg.measures]}")
    if (agg.window.type == WindowType.SLIDING and
        agg.window.period_seconds is None):
      raise ValueError("period_seconds is required for SLIDING windows")
    for df in self.derived_fields:
      if not isinstance(df.expression, Expr):
        raise TypeError(
            f"DerivedField.expression must be an Expr, "
            f"got {type(df.expression).__name__}")
    if (self.measure_combiner is not None and
        not isinstance(self.measure_combiner, Expr)):
      raise TypeError(
          f"measure_combiner must be an Expr, "
          f"got {type(self.measure_combiner).__name__}")
    # Validate that measure_combiner only references known measure aliases.
    if self.measure_combiner is not None:
      aliases = {m.alias for m in agg.measures}
      unknown = self.measure_combiner.field_refs() - aliases
      if unknown:
        raise ValueError(
            f"measure_combiner references unknown fields: {unknown}. "
            f"Available measure aliases: {aliases}")

  def required_source_columns(self):
    """Return the set of source table columns needed by this metric spec.

    This includes group_by fields, measure fields (excluding derived field
    names), and field references from derived field expressions.
    """
    derived_names = {df.name for df in self.derived_fields}
    cols = set()
    cols.update(self.aggregation.group_by)
    for m in self.aggregation.measures:
      if m.agg != AggOp.COUNT and m.field not in derived_names:
        cols.add(m.field)
    for df in self.derived_fields:
      cols.update(df.expression.field_refs())
    return cols

  def to_dict(self):
    """Serialize to a plain dict suitable for JSON."""
    result = {
        'aggregation': {
            'window': {
                'type': self.aggregation.window.type.value,
                'size_seconds': self.aggregation.window.size_seconds,
                'period_seconds': self.aggregation.window.period_seconds,
            },
            'group_by': list(self.aggregation.group_by),
            'measures': [{
                'field': m.field, 'agg': m.agg.value, 'alias': m.alias
            } for m in self.aggregation.measures],
        },
    }
    if self.derived_fields:
      result['derived_fields'] = [{
          'name': df.name, 'expression': str(df.expression)
      } for df in self.derived_fields]
    if self.measure_combiner is not None:
      result['measure_combiner'] = {'expression': str(self.measure_combiner)}
    if self.name is not None:
      result['name'] = self.name
    return result

  @classmethod
  def from_dict(cls, d):
    """Construct a MetricSpec from a plain dict (e.g., loaded from JSON).

    Expressions (``measure_combiner`` and ``derived_fields[].expression``)
    are Python expression strings, e.g.::

      "measure_combiner": {"expression": "clicks / impressions"}
      "expression": "1 if status == 'success' else 0"

    Args:
      d: Dictionary with keys matching the MetricSpec constructor.

    Returns:
      MetricSpec instance.

    Raises:
      TypeError: If an expression is not a string.
      SyntaxError: If an expression string is not valid Python syntax.
      ValueError: If an expression uses unsupported constructs, or if
        measure_combiner references fields not in the measure aliases.
    """
    agg_dict = d['aggregation']
    window_dict = agg_dict.get('window', {})
    window = WindowSpec(
        type=WindowType(window_dict.get('type', 'fixed')),
        size_seconds=window_dict.get('size_seconds', 3600),
        period_seconds=window_dict.get('period_seconds'),
    )
    measures = [
        MeasureSpec(field=m['field'], agg=AggOp(m['agg']), alias=m['alias'])
        for m in agg_dict.get('measures', [])
    ]
    derived_fields = None
    if 'derived_fields' in d and d['derived_fields']:
      derived_fields = []
      for df in d['derived_fields']:
        expr_val = df['expression']
        if not isinstance(expr_val, str):
          raise TypeError(
              f"derived_fields[].expression must be a string, "
              f"got {type(expr_val).__name__}. "
              f"Example: \"1 if status == 'success' else 0\"")
        derived_fields.append(
            DerivedField(
                name=df['name'], expression=Expr.from_string(expr_val)))
    measure_combiner = None
    if 'measure_combiner' in d and d['measure_combiner'] is not None:
      mc = d['measure_combiner']
      expr_val = mc['expression'] if isinstance(mc, dict) else mc
      if not isinstance(expr_val, str):
        raise TypeError(
            f"measure_combiner.expression must be a string, "
            f"got {type(expr_val).__name__}. "
            f"Example: \"clicks / impressions\"")
      measure_combiner = Expr.from_string(expr_val)
    return cls(
        aggregation=AggregationSpec(
            window=window,
            group_by=agg_dict.get('group_by', []),
            measures=measures,
        ),
        derived_fields=derived_fields,
        measure_combiner=measure_combiner,
        name=d.get('name'),
        _run_init=True,
    )


# ---------------------------------------------------------------------------
# Internal CombineFn and DoFns
# ---------------------------------------------------------------------------


class _SumCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return 0

  def add_input(self, accumulator, element):
    return accumulator + element

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, accumulator):
    return accumulator


class _MinCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return float('inf')

  def add_input(self, accumulator, element):
    return element if element < accumulator else accumulator

  def merge_accumulators(self, accumulators):
    return min(accumulators)

  def extract_output(self, accumulator):
    return accumulator


class _MaxCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return float('-inf')

  def add_input(self, accumulator, element):
    return element if element > accumulator else accumulator

  def merge_accumulators(self, accumulators):
    return max(accumulators)

  def extract_output(self, accumulator):
    return accumulator


def _get_combiner_for_agg(agg_op):
  """Map AggOp enum to a Beam CombineFn instance."""
  if agg_op == AggOp.SUM:
    return _SumCombineFn()
  elif agg_op == AggOp.COUNT:
    return combiners.CountCombineFn()
  elif agg_op == AggOp.MIN:
    return _MinCombineFn()
  elif agg_op == AggOp.MAX:
    return _MaxCombineFn()
  elif agg_op == AggOp.MEAN:
    return combiners.MeanCombineFn()
  else:
    raise ValueError(f"Unknown aggregation operator: {agg_op}")


class _PreCombineFn(beam.CombineFn):
  """Stage 1 wrapper: extract_output returns the raw accumulator."""
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn

  def create_accumulator(self):
    return self._combine_fn.create_accumulator()

  def add_input(self, accumulator, element):
    return self._combine_fn.add_input(accumulator, element)

  def merge_accumulators(self, accumulators):
    return self._combine_fn.merge_accumulators(accumulators)

  def extract_output(self, accumulator):
    return accumulator  # pass raw accumulator, NOT final output


class _PostCombineFn(beam.CombineFn):
  """Stage 2 wrapper: add_input merges an accumulator from Stage 1."""
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn

  def create_accumulator(self):
    return self._combine_fn.create_accumulator()

  def add_input(self, accumulator, element):
    return self._combine_fn.merge_accumulators([accumulator, element])

  def merge_accumulators(self, accumulators):
    return self._combine_fn.merge_accumulators(accumulators)

  def extract_output(self, accumulator):
    return self._combine_fn.extract_output(accumulator)


class _MapperSidePrecombine(beam.DoFn):
  """Mapper-side pre-aggregation within each bundle.

  Mirrors PGBKCVOperation (operations.py) but as a user-level DoFn so
  Dataflow still sees the downstream CombinePerKey and applies incremental
  state-side merging.

  Buffers KV<key, value> elements in an in-memory table keyed by
  (window, key), folding values via the CombineFn. On finish_bundle,
  emits KV<key, accumulator> with correct window and timestamp metadata.

  The downstream CombinePerKey must use _PostCombineFn to treat incoming
  elements as accumulators.

  Uses OUTPUT_AT_EOW timestamp semantics (the Beam default): output
  timestamp is window.max_timestamp(), matching PGBKCVOperation behavior.
  """
  def __init__(self, combine_fn, max_keys=100_000):
    self._combine_fn = combine_fn
    self._max_keys = max_keys

  def setup(self):
    self._combine_fn.setup()
    # Cache bound methods to avoid attribute lookup per element.
    self._add_input = self._combine_fn.add_input
    self._create_accumulator = self._combine_fn.create_accumulator
    compact_method = getattr(self._combine_fn, 'compact', None)
    if (compact_method is not None and
        compact_method.__func__ is not beam.CombineFn.compact):
      self._compact = compact_method
    else:
      self._compact = None

  def start_bundle(self):
    self._table = {}  # (window, key) -> accumulator
    self._key_count = 0

  def process(self, element, window=beam.DoFn.WindowParam):
    key, value = element
    wkey = (window, key)
    table = self._table
    entry = table.get(wkey)
    if entry is not None:
      table[wkey] = self._add_input(entry, value)
    else:
      if self._key_count >= self._max_keys:
        # Evict 10% of entries, same strategy as PGBKCVOperation.
        target = self._key_count * 9 // 10
        old_wkeys = []
        for old_wkey, old_acc in table.items():
          old_wkeys.append(old_wkey)
          yield self._make_windowed_value(old_wkey, old_acc)
          self._key_count -= 1
          if self._key_count <= target:
            break
        for old_wkey in reversed(old_wkeys):
          del table[old_wkey]
      self._key_count += 1
      table[wkey] = self._add_input(self._create_accumulator(), value)

  def finish_bundle(self):
    for wkey, acc in self._table.items():
      yield self._make_windowed_value(wkey, acc)
    self._table = None
    self._key_count = 0

  def _make_windowed_value(self, wkey, accumulator):
    window, key = wkey
    if self._compact is not None:
      accumulator = self._compact(accumulator)
    return beam.utils.windowed_value.WindowedValue(
        (key, accumulator), window.max_timestamp(), (window,))

  def teardown(self):
    self._combine_fn.teardown()


class _DerivedFieldsFn:
  """Callable that evaluates derived field expressions on each row dict.

  Each derived field's ``expression`` is a compiled ``Expr`` callable.
  This class is passed to ``beam.Map`` and is pickle-safe because ``Expr``
  implements ``__reduce__``.
  """
  def __init__(self, derived_fields):
    self._fields = [(df.name, df.expression) for df in derived_fields]

  def __call__(self, row):
    row = dict(row)
    for name, expr in self._fields:
      row[name] = expr(row)
    return row


class _ApplyMetricExpr(beam.DoFn):
  """DoFn that evaluates a post-aggregation expression on combined results."""
  def __init__(self, measure_combiner, is_keyed):
    self._measure_combiner = measure_combiner
    self._is_keyed = is_keyed

  def process(self, element, window=beam.DoFn.WindowParam):
    if self._is_keyed:
      key, agg_dict = element
    else:
      agg_dict = element

    if self._measure_combiner is not None:
      value = float(self._measure_combiner(agg_dict))
    else:
      value = float(next(iter(agg_dict.values())))

    row = beam.Row(
        value=value,
        window_start=window.start,
        window_end=window.end)

    if self._is_keyed:
      yield (key, row)
    else:
      yield row


class ComputeMetric(beam.PTransform):
  """Transforms raw row dicts into metric beam.Rows for anomaly detection.

  Takes a ``PCollection[dict]`` with event-time timestamps and produces
  either ``PCollection[beam.Row]`` (for global aggregation) or
  ``PCollection[tuple[key, beam.Row]]`` (for grouped aggregation).

  The output is directly compatible with ``AnomalyDetection``.

  Args:
    metric_spec: A ``MetricSpec`` defining the metric computation.
    fanout_strategy: Strategy for aggregation parallelism. Applies to both
        keyed and global aggregation. Default: SHARDED.
    fanout: Number of shards for SHARDED or HOTKEY_FANOUT strategies.
        Ignored for NONE and PRECOMBINE. Default: 400.
  """
  def __init__(self, metric_spec, fanout_strategy=FanoutStrategy.SHARDED,
               fanout=400):
    super().__init__()
    self._spec = metric_spec
    self._fanout_strategy = fanout_strategy
    self._fanout = fanout

  def expand(self, pcoll):
    spec = self._spec
    agg = spec.aggregation

    # Step 1: Apply derived fields
    if spec.derived_fields:
      pcoll = pcoll | 'DerivedFields' >> beam.Map(
          _DerivedFieldsFn(spec.derived_fields))

    # Step 2: Apply windowing
    if agg.window.type == WindowType.FIXED:
      window_fn = beam_window.FixedWindows(agg.window.size_seconds)
    elif agg.window.type == WindowType.SLIDING:
      window_fn = beam_window.SlidingWindows(
          agg.window.size_seconds, agg.window.period_seconds)
    else:
      raise ValueError(f"Unknown window type: {agg.window.type}")

    windowed = pcoll | 'Window' >> beam.WindowInto(window_fn)

    # Step 3: Aggregate
    measures = agg.measures
    aliases = [m.alias for m in measures]
    is_keyed = bool(agg.group_by)

    # Single-measure optimization: skip TupleCombineFn overhead (avoids
    # tuple creation/unpacking per element on the hot path).
    if len(measures) == 1:
      combine_fn = _get_combiner_for_agg(measures[0].agg)
      _m0 = measures[0]
      _a0 = aliases[0]

      def extract_fields(row_dict):
        return row_dict.get(_m0.field) if _m0.agg != AggOp.COUNT else 1

      def to_alias_dict(value):
        return {_a0: value}
    else:
      combine_fn = combiners.TupleCombineFn(
          *[_get_combiner_for_agg(m.agg) for m in measures])

      def extract_fields(row_dict):
        return tuple(
            row_dict.get(m.field) if m.agg != AggOp.COUNT else 1
            for m in measures)

      def to_alias_dict(values):
        return dict(zip(aliases, values))

    strategy = self._fanout_strategy
    post_fn = _PostCombineFn(combine_fn)

    if is_keyed:
      group_by_fields = agg.group_by

      def extract_key_and_fields(row_dict):
        key = tuple(row_dict.get(f) for f in group_by_fields)
        return (key, extract_fields(row_dict))

      keyed = windowed | 'ExtractKey' >> beam.Map(extract_key_and_fields)

      if strategy == FanoutStrategy.NONE:
        aggregated = (
            keyed
            | 'Combine' >> beam.CombinePerKey(combine_fn)
            | 'ToDict' >> beam.MapTuple(lambda k, v: (k, to_alias_dict(v))))
      elif strategy == FanoutStrategy.HOTKEY_FANOUT:
        aggregated = (
            keyed
            | 'Combine' >> beam.CombinePerKey(
                combine_fn).with_hot_key_fanout(self._fanout)
            | 'ToDict' >> beam.MapTuple(lambda k, v: (k, to_alias_dict(v))))
      elif strategy == FanoutStrategy.SHARDED:
        _num_shards = self._fanout
        pre_fn = _PreCombineFn(combine_fn)

        def _shard_key_fields(kv):
          key, value = kv
          return ((key, random.randint(0, _num_shards - 1)), value)

        aggregated = (
            keyed
            | 'ShardKey' >> beam.Map(_shard_key_fields)
            | 'PartialCombine' >> beam.CombinePerKey(pre_fn)
            | 'DropShard' >> beam.MapTuple(
                lambda k, v: (k[0], v))
            | 'FinalCombine' >> beam.CombinePerKey(post_fn)
            | 'ToDict' >> beam.MapTuple(lambda k, v: (k, to_alias_dict(v))))
      elif strategy == FanoutStrategy.PRECOMBINE:
        aggregated = (
            keyed
            | 'Precombine' >> beam.ParDo(
                _MapperSidePrecombine(combine_fn))
            | 'Combine' >> beam.CombinePerKey(post_fn)
            | 'ToDict' >> beam.MapTuple(lambda k, v: (k, to_alias_dict(v))))
      else:
        raise ValueError(f"Unknown fanout strategy: {strategy}")
    else:
      if strategy == FanoutStrategy.NONE:
        aggregated = (
            windowed
            | 'ExtractFields' >> beam.Map(extract_fields)
            | 'Combine' >> beam.CombineGlobally(
                combine_fn).without_defaults()
            | 'ToDict' >> beam.Map(to_alias_dict))
      elif strategy == FanoutStrategy.HOTKEY_FANOUT:
        aggregated = (
            windowed
            | 'ExtractFields' >> beam.Map(extract_fields)
            | 'Combine' >> beam.CombineGlobally(
                combine_fn).with_fanout(self._fanout).without_defaults()
            | 'ToDict' >> beam.Map(to_alias_dict))
      elif strategy == FanoutStrategy.SHARDED:
        _num_shards = self._fanout
        pre_fn = _PreCombineFn(combine_fn)

        def _shard_fields(row_dict):
          return (random.randint(0, _num_shards - 1),
                  extract_fields(row_dict))

        aggregated = (
            windowed
            | 'ShardAndExtract' >> beam.Map(_shard_fields)
            | 'PartialCombine' >> beam.CombinePerKey(pre_fn)
            | 'DropShard' >> beam.Values()
            | 'FinalCombine' >> beam.CombineGlobally(
                post_fn).without_defaults()
            | 'ToDict' >> beam.Map(to_alias_dict))
      elif strategy == FanoutStrategy.PRECOMBINE:
        aggregated = (
            windowed
            | 'ExtractFields' >> beam.Map(
                lambda row_dict: (None, extract_fields(row_dict)))
            | 'Precombine' >> beam.ParDo(
                _MapperSidePrecombine(combine_fn))
            | 'DropKey' >> beam.Values()
            | 'Combine' >> beam.CombineGlobally(
                post_fn).without_defaults()
            | 'ToDict' >> beam.Map(to_alias_dict))
      else:
        raise ValueError(f"Unknown fanout strategy: {strategy}")

    # Step 4: Apply metric expression and set output type hints
    metric_dofn = _ApplyMetricExpr(spec.measure_combiner, is_keyed)

    if is_keyed:
      # AnomalyDetection checks isinstance(element_type, TupleConstraint)
      # to detect keyed input. We must annotate the output type.
      result = aggregated | 'MetricExpr' >> beam.ParDo(
          metric_dofn).with_output_types(Tuple[Any, beam.Row])
    else:
      result = aggregated | 'MetricExpr' >> beam.ParDo(
          metric_dofn).with_output_types(beam.Row)

    return result
