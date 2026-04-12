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

"""Unit tests for bqmonitor.relative_change_detector."""

import logging
import math
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.utils.timestamp import Timestamp

from bqmonitor.relative_change_detector import (
    _RelativeChangeConfig,
    _compute_pct_change,
    _check_alert,
    IncSlidingMeanTracker,
    RelativeChangeDoFn,
)
from bqmonitor.pipeline import _parse_detector_spec

logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------

class ConfigTest(unittest.TestCase):

  def test_valid_config_pct_only(self):
    cfg = _RelativeChangeConfig(
        direction='decrease', threshold_pct=20.0, lookback_windows=1)
    self.assertEqual(cfg.direction, 'decrease')
    self.assertEqual(cfg.threshold_pct, 20.0)
    self.assertIsNone(cfg.absolute_threshold)
    self.assertEqual(cfg.lookback_windows, 1)

  def test_valid_config_absolute_only(self):
    cfg = _RelativeChangeConfig(
        direction='increase', absolute_threshold=1.0, lookback_windows=1)
    self.assertIsNone(cfg.threshold_pct)
    self.assertEqual(cfg.absolute_threshold, 1.0)

  def test_valid_config_both_thresholds(self):
    cfg = _RelativeChangeConfig(
        direction='both', threshold_pct=20.0,
        absolute_threshold=5.0, lookback_windows=1)
    self.assertEqual(cfg.threshold_pct, 20.0)
    self.assertEqual(cfg.absolute_threshold, 5.0)

  def test_invalid_direction(self):
    with self.assertRaises(ValueError):
      _RelativeChangeConfig(
          direction='sideways', threshold_pct=20.0, lookback_windows=1)

  def test_no_threshold_raises(self):
    with self.assertRaises(ValueError) as ctx:
      _RelativeChangeConfig(direction='decrease', lookback_windows=1)
    self.assertIn('threshold_pct', str(ctx.exception))

  def test_negative_pct_threshold(self):
    with self.assertRaises(ValueError):
      _RelativeChangeConfig(
          direction='decrease', threshold_pct=-5, lookback_windows=1)

  def test_negative_absolute_threshold(self):
    with self.assertRaises(ValueError):
      _RelativeChangeConfig(
          direction='decrease', absolute_threshold=-1.0, lookback_windows=1)

  def test_zero_lookback(self):
    with self.assertRaises(ValueError):
      _RelativeChangeConfig(
          direction='decrease', threshold_pct=20.0, lookback_windows=0)

  def test_valid_directions(self):
    for d in ('decrease', 'increase', 'both'):
      cfg = _RelativeChangeConfig(
          direction=d, threshold_pct=10.0, lookback_windows=1)
      self.assertEqual(cfg.direction, d)


# ---------------------------------------------------------------------------
# IncSlidingMeanTracker tests
# ---------------------------------------------------------------------------

class IncSlidingMeanTrackerTest(unittest.TestCase):

  def test_empty(self):
    t = IncSlidingMeanTracker(3)
    self.assertTrue(math.isnan(t.get()))
    self.assertEqual(t.count, 0)

  def test_single_value(self):
    t = IncSlidingMeanTracker(3)
    t.push(10.0)
    self.assertAlmostEqual(t.get(), 10.0)
    self.assertEqual(t.count, 1)

  def test_mean_of_three(self):
    t = IncSlidingMeanTracker(3)
    for v in [10.0, 20.0, 30.0]:
      t.push(v)
    self.assertAlmostEqual(t.get(), 20.0)
    self.assertEqual(t.count, 3)

  def test_eviction(self):
    t = IncSlidingMeanTracker(3)
    for v in [10.0, 20.0, 30.0]:
      t.push(v)
    # Push a 4th, evicts 10.0
    t.push(40.0)
    self.assertAlmostEqual(t.get(), 30.0)  # mean(20, 30, 40)
    self.assertEqual(t.count, 3)

  def test_window_size_1(self):
    t = IncSlidingMeanTracker(1)
    t.push(100.0)
    self.assertAlmostEqual(t.get(), 100.0)
    t.push(200.0)
    self.assertAlmostEqual(t.get(), 200.0)

  def test_incremental_accuracy(self):
    """Verify incremental matches simple average over many pushes."""
    t = IncSlidingMeanTracker(5)
    values = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
    for v in values:
      t.push(float(v))
    # Last 5: 36, 49, 64, 81, 100
    expected = (36 + 49 + 64 + 81 + 100) / 5
    self.assertAlmostEqual(t.get(), expected, places=10)


# ---------------------------------------------------------------------------
# Pure function tests
# ---------------------------------------------------------------------------

class ComputePctChangeTest(unittest.TestCase):

  def test_increase(self):
    pct, valid = _compute_pct_change(120, 100)
    self.assertTrue(valid)
    self.assertAlmostEqual(pct, 20.0)

  def test_decrease(self):
    pct, valid = _compute_pct_change(80, 100)
    self.assertTrue(valid)
    self.assertAlmostEqual(pct, -20.0)

  def test_no_change(self):
    pct, valid = _compute_pct_change(100, 100)
    self.assertTrue(valid)
    self.assertAlmostEqual(pct, 0.0)

  def test_zero_baseline_nonzero_current(self):
    # baseline=0 → pct is mathematically undefined, marked invalid.
    _, valid = _compute_pct_change(50, 0)
    self.assertFalse(valid)

  def test_zero_both(self):
    pct, valid = _compute_pct_change(0, 0)
    self.assertFalse(valid)
    self.assertAlmostEqual(pct, 0.0)

  def test_negative_baseline(self):
    # -100 to -80 = 20% increase (towards zero)
    pct, valid = _compute_pct_change(-80, -100)
    self.assertTrue(valid)
    self.assertAlmostEqual(pct, 20.0)


class CheckAlertTest(unittest.TestCase):
  """Tests for _check_alert(current, baseline, pct_change, pct_valid,
     direction, threshold_pct, absolute_threshold)."""

  def _call(self, current, baseline, direction,
            threshold_pct=None, absolute_threshold=None):
    pct_change, pct_valid = _compute_pct_change(current, baseline)
    return _check_alert(current, baseline, pct_change, pct_valid,
                        direction, threshold_pct, absolute_threshold)

  # -- Percentage-only alerts --

  def test_decrease_pct_alert(self):
    # 75 from 100 = -25% < -20%
    self.assertTrue(self._call(75, 100, 'decrease', threshold_pct=20.0))

  def test_decrease_pct_no_alert(self):
    # 90 from 100 = -10%, below threshold
    self.assertFalse(self._call(90, 100, 'decrease', threshold_pct=20.0))

  def test_decrease_ignores_increase(self):
    # 150 from 100 = +50%, not a decrease
    self.assertFalse(self._call(150, 100, 'decrease', threshold_pct=20.0))

  def test_increase_pct_alert(self):
    self.assertTrue(self._call(125, 100, 'increase', threshold_pct=20.0))

  def test_increase_ignores_decrease(self):
    self.assertFalse(self._call(50, 100, 'increase', threshold_pct=20.0))

  def test_both_pct_alert_decrease(self):
    self.assertTrue(self._call(75, 100, 'both', threshold_pct=20.0))

  def test_both_pct_alert_increase(self):
    self.assertTrue(self._call(125, 100, 'both', threshold_pct=20.0))

  def test_exact_pct_threshold(self):
    # 80 from 100 = -20%, meets -20% threshold
    self.assertTrue(self._call(80, 100, 'decrease', threshold_pct=20.0))
    self.assertTrue(self._call(120, 100, 'increase', threshold_pct=20.0))

  # -- Absolute-only alerts --

  def test_decrease_abs_alert(self):
    # 0.5 from 2.0 = delta -1.5, meets abs threshold 1.0
    self.assertTrue(self._call(0.5, 2.0, 'decrease', absolute_threshold=1.0))

  def test_decrease_abs_no_alert(self):
    # 1.5 from 2.0 = delta -0.5, below 1.0
    self.assertFalse(self._call(1.5, 2.0, 'decrease', absolute_threshold=1.0))

  def test_increase_abs_alert(self):
    self.assertTrue(self._call(3.0, 2.0, 'increase', absolute_threshold=1.0))

  def test_both_abs_alert(self):
    self.assertTrue(self._call(3.5, 2.0, 'both', absolute_threshold=1.0))
    self.assertTrue(self._call(0.5, 2.0, 'both', absolute_threshold=1.0))

  # -- Combined: either threshold triggers --

  def test_either_triggers_pct_only(self):
    # pct hits (-25%) but abs below threshold (delta=-25 from 100)
    self.assertTrue(self._call(75, 100, 'decrease',
                               threshold_pct=20.0, absolute_threshold=50.0))

  def test_either_triggers_abs_only(self):
    # pct below threshold (-5%) but abs hits (delta=-5 from 100)
    self.assertTrue(self._call(95, 100, 'decrease',
                               threshold_pct=20.0, absolute_threshold=5.0))

  def test_either_no_trigger(self):
    # Both below
    self.assertFalse(self._call(99, 100, 'decrease',
                                threshold_pct=20.0, absolute_threshold=5.0))

  # -- Zero baseline: pct is invalid, only absolute fires --

  def test_zero_baseline_abs_triggers(self):
    # baseline=0, current=2.0 → delta=2.0, abs threshold=1.0 triggers
    self.assertTrue(self._call(2.0, 0, 'increase', absolute_threshold=1.0))

  def test_zero_baseline_pct_does_not_trigger(self):
    # baseline=0 → pct is mathematically undefined, no pct alert.
    # Use absolute_threshold if you want to alert on changes from zero.
    self.assertFalse(self._call(2.0, 0, 'increase', threshold_pct=20.0))

  def test_zero_baseline_zero_current_no_alert(self):
    # baseline=0, current=0 → pct invalid, delta=0 → no alert
    self.assertFalse(self._call(0, 0, 'increase',
                                threshold_pct=20.0, absolute_threshold=1.0))


# ---------------------------------------------------------------------------
# TestStream helpers
# ---------------------------------------------------------------------------

def _make_row(value, window_start):
  return beam.Row(
      value=float(value),
      window_start=Timestamp.of(window_start),
      window_end=Timestamp.of(window_start + 1))


def _create_test_stream(rows, batch_size=2):
  """Create a TestStream from (timestamp, beam.Row) pairs.

  Inserts elements in batches of ``batch_size`` before advancing
  processing time, mimicking a real pipeline where multiple elements
  arrive between timer firings.
  """
  test_stream = TestStream()
  wm = None
  for index, (ts, row) in enumerate(rows):
    test_stream.add_elements([row], event_timestamp=ts)
    if wm is None or wm < ts:
      wm = ts
      test_stream.advance_watermark_to(wm)
    if (index + 1) % batch_size == 0:
      test_stream.advance_processing_time(10)
  # Final advance for any remaining elements in a partial batch.
  test_stream.advance_processing_time(10)
  test_stream.advance_watermark_to_infinity()
  return test_stream


# ---------------------------------------------------------------------------
# End-to-end DoFn tests with TestStream
# ---------------------------------------------------------------------------


def _check_labels(expected_labels):
  """Return an assert_that checker that validates labels."""
  def _check(actual):
    actual_labels = [r.predictions[0].label for _, r in actual]
    assert actual_labels == expected_labels, (
        f'labels {actual_labels} != {expected_labels}')
  return _check


def _check_labels_and_scores(expected):
  """Return checker for (label, score) pairs."""
  def _check(actual):
    actual_pairs = []
    for _, r in actual:
      prediction = r.predictions[0]
      score = (round(prediction.score, 2)
               if prediction.score is not None else None)
      actual_pairs.append((prediction.label, score))
    assert actual_pairs == expected, (
        f'{actual_pairs} != {expected}')
  return _check


class RelativeChangeDoFnTest(unittest.TestCase):
  """End-to-end tests for RelativeChangeDoFn using TestStream + Prism."""

  def setUp(self):
    self.options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
    ])

  def _run(self, rows, checker, **dofn_kwargs):
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(rows)
          | beam.WithKeys("k")
          | beam.ParDo(RelativeChangeDoFn(**dofn_kwargs)))
      assert_that(result, checker)

  def test_warmup(self):
    """First element is warmup (label=-2)."""
    rows = [(1, _make_row(100.0, 1))]
    self._run(rows, _check_labels([-2]),
              direction='decrease', threshold_pct=20.0, lookback_windows=1)

  def test_warmup_with_lookback_3(self):
    """First 3 elements are warmup with lookback_windows=3."""
    rows = [(i, _make_row(float(i), i)) for i in range(1, 4)]
    self._run(rows, _check_labels([-2, -2, -2]),
              direction='decrease', threshold_pct=20.0, lookback_windows=3)

  def test_decrease_detected(self):
    """25% decrease triggers alert."""
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(75.0, 2)),
    ]
    self._run(rows, _check_labels_and_scores([(-2, None), (1, -25.0)]),
              direction='decrease', threshold_pct=20.0, lookback_windows=1)

  def test_decrease_not_triggered(self):
    """10% decrease does not trigger 20% threshold."""
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(90.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 0]),
              direction='decrease', threshold_pct=20.0, lookback_windows=1)

  def test_increase_detected(self):
    """50% increase triggers alert."""
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(150.0, 2)),
    ]
    self._run(rows, _check_labels_and_scores([(-2, None), (1, 50.0)]),
              direction='increase', threshold_pct=20.0, lookback_windows=1)

  def test_both_direction(self):
    """Both decrease and increase trigger with direction='both'."""
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(75.0, 2)),
        (3, _make_row(120.0, 3)),
    ]
    self._run(rows, _check_labels([-2, 1, 1]),
              direction='both', threshold_pct=20.0, lookback_windows=1)

  def test_lookback_3_uses_mean(self):
    """lookback_windows=3 uses mean of last 3 values as baseline."""
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(100.0, 2)),
        (3, _make_row(100.0, 3)),
        (4, _make_row(70.0, 4)),
    ]
    self._run(rows,
              _check_labels_and_scores(
                  [(-2, None), (-2, None), (-2, None), (1, -30.0)]),
              direction='decrease', threshold_pct=20.0, lookback_windows=3)

  def test_zero_baseline_pct_only_no_alert(self):
    """Zero baseline with pct-only threshold does not alert.

    pct change is mathematically undefined when baseline is 0.
    Users must use absolute_threshold to alert on changes from zero.
    """
    rows = [
        (1, _make_row(0.0, 1)),
        (2, _make_row(50.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 0]),
              direction='increase', threshold_pct=20.0, lookback_windows=1)

  def test_zero_baseline_absolute_triggers(self):
    """Zero baseline with absolute_threshold triggers alert."""
    rows = [
        (1, _make_row(0.0, 1)),
        (2, _make_row(50.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 1]),
              direction='increase', absolute_threshold=1.0,
              lookback_windows=1)

  def test_zero_to_zero(self):
    """Zero to zero is not an alert."""
    rows = [
        (1, _make_row(0.0, 1)),
        (2, _make_row(0.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 0]),
              direction='decrease', threshold_pct=20.0, lookback_windows=1)

  def test_many_elements_sequential(self):
    """Multiple elements processed in order with correct scores."""
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(100.0, 2)),
        (3, _make_row(80.0, 3)),
        (4, _make_row(80.0, 4)),
        (5, _make_row(60.0, 5)),
    ]
    self._run(rows, _check_labels([-2, 0, 1, 0, 1]),
              direction='decrease', threshold_pct=20.0, lookback_windows=1)

  def test_absolute_threshold_only(self):
    """Alert on absolute delta with no pct threshold."""
    # baseline=0, current=2.0 → delta=+2.0, abs threshold=1.0 → alert
    rows = [
        (1, _make_row(0.0, 1)),
        (2, _make_row(2.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 1]),
              direction='increase', absolute_threshold=1.0,
              lookback_windows=1)

  def test_absolute_below_threshold(self):
    """No alert when absolute delta is below threshold."""
    rows = [
        (1, _make_row(2.0, 1)),
        (2, _make_row(2.5, 2)),  # delta=+0.5, below 1.0
    ]
    self._run(rows, _check_labels([-2, 0]),
              direction='increase', absolute_threshold=1.0,
              lookback_windows=1)

  def test_either_threshold_triggers_abs(self):
    """Alert fires from absolute threshold even when pct doesn't hit."""
    # baseline=100, current=95 → pct=-5% (below 20%) but delta=-5 (meets 5)
    rows = [
        (1, _make_row(100.0, 1)),
        (2, _make_row(95.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 1]),
              direction='decrease', threshold_pct=20.0,
              absolute_threshold=5.0, lookback_windows=1)

  def test_either_threshold_triggers_pct(self):
    """Alert fires from pct threshold even when absolute doesn't hit."""
    # baseline=2, current=1 → pct=-50% (meets 20%) but delta=-1 (below 5)
    rows = [
        (1, _make_row(2.0, 1)),
        (2, _make_row(1.0, 2)),
    ]
    self._run(rows, _check_labels([-2, 1]),
              direction='decrease', threshold_pct=20.0,
              absolute_threshold=5.0, lookback_windows=1)


# ---------------------------------------------------------------------------
# Parse spec integration tests
# ---------------------------------------------------------------------------

class ParseSpecTest(unittest.TestCase):

  def test_minimal_raises_without_direction(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec('{"type":"RelativeChange"}')
    self.assertIn('direction', str(ctx.exception))

  def test_minimal_raises_without_lookback(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec(
          '{"type":"RelativeChange","direction":"decrease"}')
    self.assertIn('lookback_windows', str(ctx.exception))

  def test_raises_without_threshold(self):
    with self.assertRaises(ValueError) as ctx:
      _parse_detector_spec(
          '{"type":"RelativeChange","direction":"decrease",'
          '"lookback_windows":1}')
    self.assertIn('threshold_pct', str(ctx.exception))

  def test_full_config_pct(self):
    cfg = _parse_detector_spec(
        '{"type":"RelativeChange","direction":"increase",'
        '"threshold_pct":50,"lookback_windows":3}')
    self.assertEqual(cfg.direction, 'increase')
    self.assertEqual(cfg.threshold_pct, 50.0)
    self.assertIsNone(cfg.absolute_threshold)
    self.assertEqual(cfg.lookback_windows, 3)

  def test_full_config_absolute(self):
    cfg = _parse_detector_spec(
        '{"type":"RelativeChange","direction":"increase",'
        '"absolute_threshold":1.0,"lookback_windows":3}')
    self.assertIsNone(cfg.threshold_pct)
    self.assertEqual(cfg.absolute_threshold, 1.0)

  def test_full_config_both_thresholds(self):
    cfg = _parse_detector_spec(
        '{"type":"RelativeChange","direction":"increase",'
        '"threshold_pct":500,"absolute_threshold":1.0,'
        '"lookback_windows":3}')
    self.assertEqual(cfg.threshold_pct, 500.0)
    self.assertEqual(cfg.absolute_threshold, 1.0)

  def test_config_nested(self):
    cfg = _parse_detector_spec(
        '{"type":"RelativeChange","config":{'
        '"direction":"both","threshold_pct":10,"lookback_windows":2}}')
    self.assertEqual(cfg.direction, 'both')
    self.assertEqual(cfg.threshold_pct, 10.0)
    self.assertEqual(cfg.lookback_windows, 2)

  def test_top_level_overrides_config(self):
    cfg = _parse_detector_spec(
        '{"type":"RelativeChange","direction":"increase",'
        '"threshold_pct":50,"lookback_windows":1,'
        '"config":{"direction":"decrease"}}')
    self.assertEqual(cfg.direction, 'increase')

  def test_in_supported_detectors(self):
    from bqmonitor.pipeline import _SUPPORTED_DETECTORS
    self.assertIn('RelativeChange', _SUPPORTED_DETECTORS)


if __name__ == '__main__':
  unittest.main()
