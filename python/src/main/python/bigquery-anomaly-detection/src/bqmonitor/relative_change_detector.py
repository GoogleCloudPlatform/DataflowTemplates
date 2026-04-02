"""Relative change anomaly detector.

Compares the current metric window value against the mean of the last N
windows and alerts if the percentage change exceeds a threshold.

Example detector specs::

    {"type": "RelativeChange", "direction": "decrease", "threshold_pct": 20}
    {"type": "RelativeChange", "direction": "increase", "threshold_pct": 50,
     "lookback_windows": 3}

Pipeline integration::

    (key, beam.Row) ──→ RelativeChangeDoFn ──→ (key, AnomalyResult)
                         (stateful, per-key)

The DoFn buffers elements using BagState and event-time timers to
handle out-of-order arrival (backlog, backfill, polling gaps). Elements
are only processed once the watermark guarantees completeness.

The baseline mean is tracked incrementally using the same algorithm as
``apache_beam.ml.anomaly.univariate.mean.IncSlidingMeanTracker``:
on each push, the mean is updated via ``delta / n`` with O(1)
complexity. The tracker state (mean, n, deque of last N values) is
persisted in ReadModifyWriteState across timer firings.
"""

import dataclasses
import logging
import math
from collections import deque

import apache_beam as beam
from apache_beam.coders import FastPrimitivesCoder
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)

# Sentinel: any timestamp before this is "uninitialized"
_MIN_TIMESTAMP = Timestamp(0)

_VALID_DIRECTIONS = ('decrease', 'increase', 'both')


@dataclasses.dataclass(frozen=True)
class _RelativeChangeConfig:
  """Configuration for the RelativeChange detector."""
  direction: str
  threshold_pct: float
  lookback_windows: int

  def __post_init__(self):
    if self.direction not in _VALID_DIRECTIONS:
      raise ValueError(
          f"direction must be one of {_VALID_DIRECTIONS}, "
          f"got '{self.direction}'")
    if self.threshold_pct < 0:
      raise ValueError(
          f'threshold_pct must be >= 0, got {self.threshold_pct}')
    if self.lookback_windows < 1:
      raise ValueError(
          f'lookback_windows must be >= 1, got {self.lookback_windows}')


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
  baseline is zero (division undefined).
  """
  if baseline == 0:
    return (float('inf') if current != 0 else 0.0, current != 0)
  return ((current - baseline) / abs(baseline) * 100.0, True)


def _check_alert(pct_change, direction, threshold_pct):
  """Check if the percentage change triggers an alert."""
  if direction == 'decrease':
    return pct_change <= -threshold_pct
  elif direction == 'increase':
    return pct_change >= threshold_pct
  else:  # both
    return abs(pct_change) >= threshold_pct


class RelativeChangeDoFn(beam.DoFn):
  """Stateful DoFn that detects relative changes between windows.

  Buffers per-key metric values using BagState with event-time timers.
  On ``process()``: buffers the element and sets a timer for the
  earliest unprocessed element timestamp. On ``on_timer()``: processes
  all elements with timestamps up to the fire time, comparing each
  against the incremental sliding mean of the last ``lookback_windows``
  values.

  The mean tracker state is persisted in ReadModifyWriteState across
  timer firings. The buffer only holds unprocessed elements — once
  processed, entries are removed and the numeric history lives entirely
  in the mean tracker.

  Outputs:
      (key, AnomalyResult)
  """

  BUFFER_BAG = BagStateSpec('buffer', FastPrimitivesCoder())
  MEAN_TRACKER = ReadModifyWriteStateSpec(
      'mean_tracker', FastPrimitivesCoder())
  TOTAL_PUSHED = ReadModifyWriteStateSpec(
      'total_pushed', FastPrimitivesCoder())
  LAST_PROCESSED_TS = ReadModifyWriteStateSpec(
      'last_processed', FastPrimitivesCoder())
  TIMER_TARGET = ReadModifyWriteStateSpec(
      'timer_target', FastPrimitivesCoder())
  EMIT_TIMER = beam.transforms.userstate.TimerSpec(
      'emit', beam.transforms.timeutil.TimeDomain.WATERMARK)

  def __init__(self, direction='decrease', threshold_pct=20.0,
               lookback_windows=1):
    self._direction = direction
    self._threshold_pct = threshold_pct
    self._lookback_windows = lookback_windows

  def process(self, element,
              buffer_bag=beam.DoFn.StateParam(BUFFER_BAG),
              last_processed=beam.DoFn.StateParam(LAST_PROCESSED_TS),
              timer_target=beam.DoFn.StateParam(TIMER_TARGET),
              emit_timer=beam.DoFn.TimerParam(EMIT_TIMER),
              element_timestamp=beam.DoFn.TimestampParam):
    """Buffer the element and set timer for earliest unprocessed."""
    _, row = element
    buffer_bag.add((element_timestamp, row))

    lp = last_processed.read()
    is_new = lp is None or element_timestamp > lp

    if is_new:
      current_target = timer_target.read()
      if current_target is None or element_timestamp < current_target:
        timer_target.write(element_timestamp)
        emit_timer.set(element_timestamp)

  @beam.transforms.userstate.on_timer(EMIT_TIMER)
  def on_emit(self,
              buffer_bag=beam.DoFn.StateParam(BUFFER_BAG),
              mean_tracker_state=beam.DoFn.StateParam(MEAN_TRACKER),
              total_pushed_state=beam.DoFn.StateParam(TOTAL_PUSHED),
              last_processed=beam.DoFn.StateParam(LAST_PROCESSED_TS),
              timer_target=beam.DoFn.StateParam(TIMER_TARGET),
              emit_timer=beam.DoFn.TimerParam(EMIT_TIMER),
              key=beam.DoFn.KeyParam,
              fire_timestamp=beam.DoFn.TimestampParam):
    """Process all unprocessed elements up to fire_timestamp."""
    lp = last_processed.read() or _MIN_TIMESTAMP

    all_entries = sorted(buffer_bag.read(), key=lambda x: x[0])

    unprocessed = [(ts, row) for ts, row in all_entries
                   if ts > lp and ts <= fire_timestamp]

    if not unprocessed:
      return

    # Restore mean tracker from state (or create new).
    tracker = mean_tracker_state.read()
    if tracker is None:
      tracker = IncSlidingMeanTracker(self._lookback_windows)
    total_pushed = total_pushed_state.read() or 0

    for _, row in unprocessed:
      current_value = row.value

      if total_pushed < self._lookback_windows:
        # Warmup: not enough history to compute a baseline yet.
        prediction = AnomalyPrediction(
            model_id='RelativeChange',
            score=None,
            label=-2,
            info=(f'warmup: {total_pushed}'
                  f'/{self._lookback_windows}'))
        result = AnomalyResult(example=row, predictions=[prediction])
        yield (key, result)
      else:
        # Compare current against the mean of the last N values.
        baseline = tracker.get()

        pct_change, is_valid = _compute_pct_change(
            current_value, baseline)

        if is_valid:
          is_alert = _check_alert(
              pct_change, self._direction, self._threshold_pct)
        else:
          is_alert = False

        info = (f'baseline={baseline:.4f} '
                f'current={current_value:.4f} '
                f'pct_change={pct_change:.2f}%')

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

    # Persist tracker and count.
    mean_tracker_state.write(tracker)
    total_pushed_state.write(total_pushed)

    # Trim buffer: only keep unprocessed (future) entries.
    # The numeric history lives in the mean tracker.
    future = [(ts, row) for ts, row in all_entries
              if ts > fire_timestamp]
    buffer_bag.clear()
    for entry in future:
      buffer_bag.add(entry)

    last_processed.write(fire_timestamp)

    # Reschedule timer for next unprocessed element if any.
    if future:
      next_ts = future[0][0]
      timer_target.write(next_ts)
      emit_timer.set(next_ts)
    else:
      timer_target.write(None)
