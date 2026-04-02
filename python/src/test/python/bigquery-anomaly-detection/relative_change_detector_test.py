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
from apache_beam.ml.anomaly.base import AnomalyResult
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

  def test_valid_config(self):
    cfg = _RelativeChangeConfig(
        direction='decrease', threshold_pct=20.0, lookback_windows=1)
    self.assertEqual(cfg.direction, 'decrease')
    self.assertEqual(cfg.threshold_pct, 20.0)
    self.assertEqual(cfg.lookback_windows, 1)

  def test_invalid_direction(self):
    with self.assertRaises(ValueError):
      _RelativeChangeConfig(
          direction='sideways', threshold_pct=20.0, lookback_windows=1)

  def test_negative_threshold(self):
    with self.assertRaises(ValueError):
      _RelativeChangeConfig(
          direction='decrease', threshold_pct=-5, lookback_windows=1)

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
    # Push a 4th — evicts 10.0
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
    pct, valid = _compute_pct_change(50, 0)
    self.assertTrue(valid)
    self.assertEqual(pct, float('inf'))

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

  def test_decrease_alert(self):
    self.assertTrue(_check_alert(-25.0, 'decrease', 20.0))

  def test_decrease_no_alert(self):
    self.assertFalse(_check_alert(-10.0, 'decrease', 20.0))

  def test_decrease_ignores_increase(self):
    self.assertFalse(_check_alert(50.0, 'decrease', 20.0))

  def test_increase_alert(self):
    self.assertTrue(_check_alert(25.0, 'increase', 20.0))

  def test_increase_no_alert(self):
    self.assertFalse(_check_alert(10.0, 'increase', 20.0))

  def test_increase_ignores_decrease(self):
    self.assertFalse(_check_alert(-50.0, 'increase', 20.0))

  def test_both_alert_decrease(self):
    self.assertTrue(_check_alert(-25.0, 'both', 20.0))

  def test_both_alert_increase(self):
    self.assertTrue(_check_alert(25.0, 'both', 20.0))

  def test_both_no_alert(self):
    self.assertFalse(_check_alert(10.0, 'both', 20.0))

  def test_exact_threshold(self):
    self.assertTrue(_check_alert(-20.0, 'decrease', 20.0))
    self.assertTrue(_check_alert(20.0, 'increase', 20.0))
    self.assertTrue(_check_alert(20.0, 'both', 20.0))


# ---------------------------------------------------------------------------
# DoFn simulation tests
# ---------------------------------------------------------------------------

class RelativeChangeDoFnTest(unittest.TestCase):
  """Tests for RelativeChangeDoFn using simulated stateful processing.

  Simulates the timer-based processing by walking through elements in
  timestamp order, maintaining a mean tracker the same way the DoFn does.
  """

  def _make_row(self, value, window_start):
    return beam.Row(
        value=value,
        window_start=Timestamp.of(window_start),
        window_end=Timestamp.of(window_start + 1))

  def _simulate(self, elements, direction='decrease', threshold_pct=20.0,
                lookback_windows=1):
    """Simulate the DoFn's on_timer logic with sorted elements.

    Elements are (key, row) tuples processed in timestamp order.
    Returns list of (key, AnomalyResult) tuples.
    """
    tracker = IncSlidingMeanTracker(lookback_windows)
    total_pushed = 0
    results = []

    # Sort by window_start
    sorted_elements = sorted(elements, key=lambda x: x[1].window_start)

    for key, row in sorted_elements:
      current_value = row.value

      if total_pushed < lookback_windows:
        from apache_beam.ml.anomaly.base import AnomalyPrediction
        prediction = AnomalyPrediction(
            model_id='RelativeChange', score=None, label=-2,
            info=f'warmup: {total_pushed}/{lookback_windows}')
        result = AnomalyResult(example=row, predictions=[prediction])
        results.append((key, result))
      else:
        baseline = tracker.get()
        pct_change, is_valid = _compute_pct_change(current_value, baseline)
        if is_valid:
          is_alert = _check_alert(pct_change, direction, threshold_pct)
        else:
          is_alert = False

        from apache_beam.ml.anomaly.base import AnomalyPrediction
        prediction = AnomalyPrediction(
            model_id='RelativeChange',
            score=pct_change if not math.isinf(pct_change) else None,
            label=1 if is_alert else 0,
            info=f'baseline={baseline:.4f} '
                 f'current={current_value:.4f} '
                 f'pct_change={pct_change:.2f}%')
        result = AnomalyResult(example=row, predictions=[prediction])
        results.append((key, result))

      tracker.push(current_value)
      total_pushed += 1

    return results

  def test_warmup(self):
    elements = [('k', self._make_row(100.0, 0))]
    results = self._simulate(elements, lookback_windows=1)
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0][1].predictions[0].label, -2)

  def test_warmup_with_lookback_3(self):
    elements = [('k', self._make_row(float(i), i)) for i in range(3)]
    results = self._simulate(elements, lookback_windows=3)
    for r in results:
      self.assertEqual(r[1].predictions[0].label, -2)

  def test_decrease_detected(self):
    # lookback_windows=1: baseline = mean of last 1 value = previous value
    elements = [
        ('k', self._make_row(100.0, 0)),
        ('k', self._make_row(75.0, 1)),  # baseline=100, -25%
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=20.0)
    self.assertEqual(results[0][1].predictions[0].label, -2)  # warmup
    self.assertEqual(results[1][1].predictions[0].label, 1)   # alert
    self.assertAlmostEqual(results[1][1].predictions[0].score, -25.0)

  def test_decrease_not_triggered(self):
    elements = [
        ('k', self._make_row(100.0, 0)),
        ('k', self._make_row(90.0, 1)),  # baseline=100, -10%
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=20.0)
    self.assertEqual(results[1][1].predictions[0].label, 0)

  def test_increase_detected(self):
    elements = [
        ('k', self._make_row(100.0, 0)),
        ('k', self._make_row(150.0, 1)),  # baseline=100, +50%
    ]
    results = self._simulate(elements, direction='increase',
                             threshold_pct=20.0)
    self.assertEqual(results[1][1].predictions[0].label, 1)
    self.assertAlmostEqual(results[1][1].predictions[0].score, 50.0)

  def test_both_direction(self):
    elements = [
        ('k', self._make_row(100.0, 0)),
        ('k', self._make_row(75.0, 1)),   # baseline=100, -25%
        ('k', self._make_row(120.0, 2)),  # baseline=75 (window=1), +60%
    ]
    results = self._simulate(elements, direction='both',
                             threshold_pct=20.0)
    self.assertEqual(results[1][1].predictions[0].label, 1)  # decrease
    self.assertEqual(results[2][1].predictions[0].label, 1)  # increase

  def test_lookback_3_uses_mean(self):
    # lookback_windows=3: baseline = mean of last 3 values
    elements = [
        ('k', self._make_row(100.0, 0)),  # warmup
        ('k', self._make_row(100.0, 1)),  # warmup
        ('k', self._make_row(100.0, 2)),  # warmup
        ('k', self._make_row(70.0, 3)),   # baseline=mean(100,100,100)=100, -30%
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=20.0, lookback_windows=3)
    self.assertEqual(results[0][1].predictions[0].label, -2)
    self.assertEqual(results[1][1].predictions[0].label, -2)
    self.assertEqual(results[2][1].predictions[0].label, -2)
    self.assertEqual(results[3][1].predictions[0].label, 1)
    self.assertAlmostEqual(results[3][1].predictions[0].score, -30.0)

  def test_lookback_3_mean_smooths_outlier(self):
    # One outlier in the lookback window gets smoothed out
    elements = [
        ('k', self._make_row(100.0, 0)),  # warmup
        ('k', self._make_row(50.0, 1)),   # warmup
        ('k', self._make_row(100.0, 2)),  # warmup
        ('k', self._make_row(90.0, 3)),   # baseline=mean(100,50,100)=83.33
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=20.0, lookback_windows=3)
    # (90 - 83.33) / 83.33 = +8%, not a 20% decrease
    self.assertEqual(results[3][1].predictions[0].label, 0)

  def test_zero_baseline(self):
    elements = [
        ('k', self._make_row(0.0, 0)),
        ('k', self._make_row(50.0, 1)),
    ]
    results = self._simulate(elements, direction='increase',
                             threshold_pct=20.0)
    # Division by zero → infinite change → alert
    self.assertEqual(results[1][1].predictions[0].label, 1)
    self.assertIsNone(results[1][1].predictions[0].score)

  def test_zero_to_zero(self):
    elements = [
        ('k', self._make_row(0.0, 0)),
        ('k', self._make_row(0.0, 1)),
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=20.0)
    self.assertEqual(results[1][1].predictions[0].label, 0)

  def test_info_field_contents(self):
    elements = [
        ('k', self._make_row(100.0, 0)),
        ('k', self._make_row(80.0, 1)),
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=10.0)
    info = results[1][1].predictions[0].info
    self.assertIn('baseline=100.0000', info)
    self.assertIn('current=80.0000', info)
    self.assertIn('pct_change=-20.00%', info)

  def test_mean_evolves_with_sliding_window(self):
    # With lookback_windows=2, the mean slides forward
    elements = [
        ('k', self._make_row(100.0, 0)),  # warmup
        ('k', self._make_row(200.0, 1)),  # warmup
        ('k', self._make_row(150.0, 2)),  # baseline=mean(100,200)=150, 0%
        ('k', self._make_row(100.0, 3)),  # baseline=mean(200,150)=175, -42.9%
    ]
    results = self._simulate(elements, direction='decrease',
                             threshold_pct=20.0, lookback_windows=2)
    self.assertEqual(results[2][1].predictions[0].label, 0)  # 0% change
    self.assertEqual(results[3][1].predictions[0].label, 1)  # -42.9%
    self.assertAlmostEqual(
        results[3][1].predictions[0].score,
        (100.0 - 175.0) / 175.0 * 100.0, places=2)


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

  def test_full_config(self):
    cfg = _parse_detector_spec(
        '{"type":"RelativeChange","direction":"increase",'
        '"threshold_pct":50,"lookback_windows":3}')
    self.assertEqual(cfg.direction, 'increase')
    self.assertEqual(cfg.threshold_pct, 50.0)
    self.assertEqual(cfg.lookback_windows, 3)

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
        '"lookback_windows":1,"config":{"direction":"decrease"}}')
    self.assertEqual(cfg.direction, 'increase')

  def test_in_supported_detectors(self):
    from bqmonitor.pipeline import _SUPPORTED_DETECTORS
    self.assertIn('RelativeChange', _SUPPORTED_DETECTORS)


if __name__ == '__main__':
  unittest.main()
