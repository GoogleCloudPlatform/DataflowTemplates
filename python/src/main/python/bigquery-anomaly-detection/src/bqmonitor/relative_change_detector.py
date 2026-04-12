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

"""Relative change anomaly detector.

Compares the current metric window value against the mean of the last N
windows and alerts if the change exceeds a percentage threshold, an
absolute threshold, or both. Either threshold triggers the alert.

Example detector specs::

    {"type": "RelativeChange", "direction": "decrease", "threshold_pct": 20}
    {"type": "RelativeChange", "direction": "increase", "threshold_pct": 500,
     "absolute_threshold": 1.0}
    {"type": "RelativeChange", "direction": "both", "absolute_threshold": 5.0,
     "lookback_windows": 3}

Pipeline integration::

    (key, beam.Row) -> RelativeChangeDoFn -> (key, AnomalyResult)
                      (stateful, per-key)

The DoFn buffers elements using ``TimestampBufferDoFnBag`` and event-time
timers to handle out-of-order arrival (backlog, backfill, polling gaps).
Elements are only processed once the watermark guarantees completeness.

The baseline mean is tracked incrementally using the same algorithm as
``apache_beam.ml.anomaly.univariate.mean.IncSlidingMeanTracker``:
on each push, the mean is updated via ``delta / n`` with O(1)
complexity. The tracker state (mean, n, deque of last N values) is
persisted in the base class's ``EXTRA_STATE`` across timer firings.
"""

import dataclasses
import logging
import math
from collections import deque
from typing import Optional

from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult

from bqmonitor.timestamp_buffer import TimestampBufferDoFnBag

_LOGGER = logging.getLogger(__name__)

_VALID_DIRECTIONS = ('decrease', 'increase', 'both')


@dataclasses.dataclass(frozen=True)
class _RelativeChangeConfig:
  """Configuration for the RelativeChange detector.

  At least one of ``threshold_pct`` or ``absolute_threshold`` must be
  provided. If both are provided, either one triggers the alert.
  """
  direction: str
  lookback_windows: int
  threshold_pct: Optional[float] = None
  absolute_threshold: Optional[float] = None

  def __post_init__(self):
    if self.direction not in _VALID_DIRECTIONS:
      raise ValueError(
          f"direction must be one of {_VALID_DIRECTIONS}, "
          f"got '{self.direction}'")
    if self.threshold_pct is None and self.absolute_threshold is None:
      raise ValueError(
          "At least one of 'threshold_pct' or 'absolute_threshold' "
          "must be provided.")
    if self.threshold_pct is not None and self.threshold_pct < 0:
      raise ValueError(
          f'threshold_pct must be >= 0, got {self.threshold_pct}')
    if (self.absolute_threshold is not None
        and self.absolute_threshold < 0):
      raise ValueError(
          f'absolute_threshold must be >= 0, '
          f'got {self.absolute_threshold}')
    if self.lookback_windows < 1:
      raise ValueError(
          f'lookback_windows must be >= 1, got {self.lookback_windows}')


# Backward-compatible alias used by pipeline.py.
RelativeChangeConfig = _RelativeChangeConfig


class IncSlidingMeanTracker:
  """Incremental sliding window mean tracker.

  Uses the same algorithm as
  ``apache_beam.ml.anomaly.univariate.mean.IncSlidingMeanTracker``:
  maintains a running mean updated via ``delta / n`` on each push,
  with O(1) amortized cost. When the window is full, the oldest value
  is evicted and the mean is adjusted.

  This class is serializable so it can be stored in Beam
  ReadModifyWriteState.
  """

  def __init__(self, window_size):
    self._window_size = window_size
    self._queue = deque(maxlen=window_size)
    self._mean = 0.0
    self._n = 0

  def push(self, x):
    """Push a new value, evicting the oldest if the window is full."""
    delta = x - self._mean

    if len(self._queue) >= self._window_size:
      old_x = self._queue.popleft()
      self._n -= 1
      delta += (self._mean - old_x)

    self._queue.append(x)
    self._n += 1

    if self._n > 0:
      self._mean += delta / self._n
    else:
      self._mean = 0.0

  def get(self):
    """Return the current mean, or NaN if empty."""
    if self._n < 1:
      return float('nan')
    return self._mean

  @property
  def count(self):
    return self._n


def _compute_pct_change(current, baseline):
  """Compute percentage change from baseline to current.

  Returns (pct_change, is_valid) where is_valid is False when
  baseline is zero (pct change is mathematically undefined).
  When baseline is zero, use ``absolute_threshold`` to alert.
  """
  if baseline == 0:
    return (0.0, False)
  return ((current - baseline) / abs(baseline) * 100.0, True)


def _check_alert(current, baseline, pct_change, pct_valid,
                 direction, threshold_pct, absolute_threshold):
  """Check if the change triggers an alert.

  Triggers if either the pct threshold or absolute threshold is met.
  pct threshold is skipped when pct_valid is False (baseline is zero).
  """
  delta = current - baseline
  if direction == 'decrease':
    pct_hit = (pct_valid and threshold_pct is not None
               and pct_change <= -threshold_pct)
    abs_hit = (absolute_threshold is not None
               and -delta >= absolute_threshold)
  elif direction == 'increase':
    pct_hit = (pct_valid and threshold_pct is not None
               and pct_change >= threshold_pct)
    abs_hit = (absolute_threshold is not None
               and delta >= absolute_threshold)
  else:  # both
    pct_hit = (pct_valid and threshold_pct is not None
               and abs(pct_change) >= threshold_pct)
    abs_hit = (absolute_threshold is not None
               and abs(delta) >= absolute_threshold)
  return pct_hit or abs_hit


class RelativeChangeDoFn(TimestampBufferDoFnBag):
  """Stateful DoFn that detects relative changes between windows.

  Subclasses ``TimestampBufferDoFnBag`` for the buffer/timer/trim
  machinery. Uses the base class's ``EXTRA_STATE`` to persist the
  mean tracker and total-pushed counter across timer firings.

  Outputs:
      (key, AnomalyResult)
  """

  def __init__(self, direction='decrease', threshold_pct=None,
               absolute_threshold=None, lookback_windows=1,
               batch_interval_sec=5):
    if threshold_pct is None and absolute_threshold is None:
      raise ValueError(
          "At least one of 'threshold_pct' or 'absolute_threshold' "
          "must be provided.")
    super().__init__(
        context_size=0,
        batch_interval_sec=batch_interval_sec)
    self._direction = direction
    self._threshold_pct = threshold_pct
    self._absolute_threshold = absolute_threshold
    self._lookback_windows = lookback_windows

  def process_element(self, key, element_ts, value, context, **extra):
    es = extra['extra_state']
    state = es.read() or {}
    tracker = state.get('tracker') or IncSlidingMeanTracker(
        self._lookback_windows)
    total_pushed = state.get('total_pushed', 0)

    row = value
    current_value = row.value

    if total_pushed < self._lookback_windows:
      prediction = AnomalyPrediction(
          model_id='RelativeChange',
          score=None,
          label=-2,
          info=(f'warmup: {total_pushed}'
                f'/{self._lookback_windows}'))
      result = AnomalyResult(example=row, predictions=[prediction])
      yield (key, result)
    else:
      baseline = tracker.get()

      pct_change, is_valid = _compute_pct_change(
          current_value, baseline)

      is_alert = _check_alert(
          current_value, baseline, pct_change, is_valid,
          self._direction, self._threshold_pct,
          self._absolute_threshold)

      info = (f'baseline={baseline:.4f} '
              f'current={current_value:.4f} '
              f'pct_change={pct_change:.2f}% '
              f'abs_delta={current_value - baseline:.4f}')

      prediction = AnomalyPrediction(
          model_id='RelativeChange',
          score=pct_change if not math.isinf(pct_change) else None,
          label=1 if is_alert else 0,
          info=info)
      result = AnomalyResult(example=row, predictions=[prediction])
      yield (key, result)

    # Push current value into the tracker AFTER scoring, so it
    # becomes part of the baseline for future elements.
    tracker.push(current_value)
    total_pushed += 1

    state['tracker'] = tracker
    state['total_pushed'] = total_pushed
    es.write(state)
