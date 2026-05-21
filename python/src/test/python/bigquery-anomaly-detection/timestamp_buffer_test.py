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

import logging
import shutil
import sys
import unittest

from parameterized import parameterized_class

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from bqmonitor.timestamp_buffer import TimestampBufferDoFnBag
from bqmonitor.timestamp_buffer import TimestampBufferDoFnOLS

logging.basicConfig(level=logging.WARNING)


# ---------------------------------------------------------------------------
# Test DoFns
# ---------------------------------------------------------------------------

class _CollectBag(TimestampBufferDoFnBag):
  """Emits each element as (key, (ts_seconds, value))."""

  def __init__(self):
    super().__init__(context_size=0)

  def process_element(self, key, element_ts, value, context,
                      **extra_state):
    yield (key, (int(element_ts.micros // 1e6), value))


class _CollectOLS(TimestampBufferDoFnOLS):
  """Same as _CollectBag but uses OrderedListState."""

  def __init__(self):
    super().__init__(context_size=0)

  def process_element(self, key, element_ts, value, context,
                      **extra_state):
    yield (key, (int(element_ts.micros // 1e6), value))


class _CounterBag(TimestampBufferDoFnBag):
  """Counts elements across timer firings. Emits (key, (count, value))."""

  def __init__(self):
    super().__init__(context_size=0)

  def process_element(self, key, element_ts, value, context, **extra):
    es = extra['extra_state']
    state = es.read() or {}
    count = state.get('count', 0) + 1
    state['count'] = count
    es.write(state)
    yield (key, (count, value))


class _CounterOLS(TimestampBufferDoFnOLS):
  """Same as _CounterBag but uses OrderedListState."""

  def __init__(self):
    super().__init__(context_size=0)

  def process_element(self, key, element_ts, value, context, **extra):
    es = extra['extra_state']
    state = es.read() or {}
    count = state.get('count', 0) + 1
    state['count'] = count
    es.write(state)
    yield (key, (count, value))


class _ContextBag(TimestampBufferDoFnBag):
  """Emits (key, (ts, value, context_len))."""

  def __init__(self):
    super().__init__(context_size=3)

  def process_element(self, key, element_ts, value, context,
                      **extra_state):
    yield (key, (int(element_ts.micros // 1e6), value, len(context)))


class _ContextOLS(TimestampBufferDoFnOLS):
  """Same as _ContextBag but uses OrderedListState."""

  def __init__(self):
    super().__init__(context_size=3)

  def process_element(self, key, element_ts, value, context,
                      **extra_state):
    yield (key, (int(element_ts.micros // 1e6), value, len(context)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _finalize_test_stream(test_stream):
  """Drain all pending timers by advancing watermark and processing time.

  Alternates watermark and processing-time advances so Prism can
  fire cross-domain timer chains (event-time MIN_WM arms
  processing-time BATCH, which arms next MIN_WM, etc.).
  """
  for i in range(10):
    test_stream.advance_watermark_to(1_000_000_000 + i)
    test_stream.advance_processing_time(10)
  test_stream.advance_watermark_to_infinity()


def _create_test_stream(elements, batch_size=2):
  """Create a TestStream from (timestamp, value) pairs.

  Emits raw values; use ``beam.WithKeys`` to add keys separately
  so PrismRunner resolves key coders correctly for stateful DoFns.

  Inserts elements in batches of ``batch_size`` before advancing
  processing time, mimicking a real pipeline where multiple elements
  arrive between timer firings.
  """
  test_stream = TestStream()
  wm = None
  for index, (ts, val) in enumerate(elements):
    test_stream.add_elements([val], event_timestamp=ts)
    if wm is None or wm < ts:
      wm = ts
      test_stream.advance_watermark_to(wm)
    if (index + 1) % batch_size == 0:
      test_stream.advance_processing_time(10)
  test_stream.advance_processing_time(10)
  test_stream.advance_watermark_to_infinity()
  return test_stream


def _create_keyed_test_stream(elements, batch_size=2):
  """Create a TestStream from (timestamp, key, value) triples."""
  test_stream = TestStream()
  wm = None
  for index, (ts, k, val) in enumerate(elements):
    test_stream.add_elements([(k, val)], event_timestamp=ts)
    if wm is None or wm < ts:
      wm = ts
      test_stream.advance_watermark_to(wm)
    if (index + 1) % batch_size == 0:
      test_stream.advance_processing_time(10)
  test_stream.advance_processing_time(10)
  test_stream.advance_watermark_to_infinity()
  return test_stream


_go_installed = shutil.which('go') is not None
_in_windows = sys.platform == "win32"


@unittest.skipUnless(_go_installed, 'Go is not installed.')
@unittest.skipIf(_in_windows, reason="Not supported on Windows")
@parameterized_class(
    ('collect_dofn', 'counter_dofn', 'context_dofn'),
    [
        (_CollectBag, _CounterBag, _ContextBag),
        (_CollectOLS, _CounterOLS, _ContextOLS),
    ])
class TimestampBufferTest(unittest.TestCase):

  def setUp(self):
    self.options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
    ])

  # -----------------------------------------------------------------
  # Basic functionality
  # -----------------------------------------------------------------

  def test_in_order_elements(self):
    """Elements arriving in order are emitted in order."""
    elements = [(i, i * 10) for i in range(1, 6)]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(elements)
          | beam.WithKeys("key")
          | beam.ParDo(self.collect_dofn()))
      assert_that(
          result,
          equal_to([
              ("key", (1, 10)),
              ("key", (2, 20)),
              ("key", (3, 30)),
              ("key", (4, 40)),
              ("key", (5, 50)),
          ]))

  def test_out_of_order_elements(self):
    """Out-of-order elements are emitted when watermark passes."""
    elements = [(1, 10), (2, 20), (3, 30), (5, 50), (4, 40)]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(elements)
          | beam.WithKeys("key")
          | beam.ParDo(self.collect_dofn()))
      assert_that(
          result,
          equal_to([
              ("key", (1, 10)),
              ("key", (2, 20)),
              ("key", (3, 30)),
              ("key", (4, 40)),
              ("key", (5, 50)),
          ]))

  def test_multiple_keys(self):
    """Each key is buffered independently."""
    elements = [
        (1, "a", 10),
        (1, "b", 100),
        (2, "a", 20),
        (2, "b", 200),
        (3, "a", 30),
    ]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_keyed_test_stream(elements)
          | beam.Map(lambda kv: (kv[0], kv[1]))
          | beam.ParDo(self.collect_dofn()))
      assert_that(
          result,
          equal_to([
              ("a", (1, 10)),
              ("b", (1, 100)),
              ("a", (2, 20)),
              ("b", (2, 200)),
              ("a", (3, 30)),
          ]))

  # -----------------------------------------------------------------
  # Extra state persistence
  # -----------------------------------------------------------------

  def test_extra_state_counter(self):
    """Extra state (counter) persists across separate timer firings.

    Uses explicit watermark advances between groups to force separate
    timer firings, proving cross-firing state persistence.
    """
    test_stream = (
        TestStream()
        .add_elements([10], event_timestamp=1)
        .add_elements([20], event_timestamp=2)
        .advance_watermark_to(5)
        .add_elements([100], event_timestamp=10)
        .add_elements([110], event_timestamp=11))
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("key")
          | beam.ParDo(self.counter_dofn()))
      assert_that(
          result,
          equal_to([
              ("key", (1, 10)),
              ("key", (2, 20)),
              ("key", (3, 100)),
              ("key", (4, 110)),
          ]))

  # -----------------------------------------------------------------
  # Context reader / trim
  # -----------------------------------------------------------------

  def test_context_reader(self):
    """BufferReader provides context lookback via read_before()."""
    elements = [(i, i * 10) for i in range(1, 8)]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(elements)
          | beam.WithKeys("key")
          | beam.ParDo(self.context_dofn()))
      # Each element gets context_len = # entries in [ts-3, ts).
      # ts=1: [] -> 0
      # ts=2: [1] -> 1
      # ts=3: [1,2] -> 2
      # ts=4: [1,2,3] -> 3
      # ts=5: [2,3,4] -> 3
      # ts=6: [3,4,5] -> 3
      # ts=7: [4,5,6] -> 3
      assert_that(
          result,
          equal_to([
              ("key", (1, 10, 0)),
              ("key", (2, 20, 1)),
              ("key", (3, 30, 2)),
              ("key", (4, 40, 3)),
              ("key", (5, 50, 3)),
              ("key", (6, 60, 3)),
              ("key", (7, 70, 3)),
          ]))

  # -----------------------------------------------------------------
  # Edge cases
  # -----------------------------------------------------------------

  def test_single_element(self):
    """A single element is processed correctly."""
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream([(42, "only")])
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn()))
      assert_that(result, equal_to([("k", (42, "only"))]))

  def test_duplicate_timestamps(self):
    """Two elements with the same timestamp are both emitted."""
    test_stream = (
        TestStream()
        .add_elements(["first"], event_timestamp=5)
        .add_elements(["second"], event_timestamp=5)
        .advance_watermark_to(5)
        )
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn()))
      assert_that(
          result,
          equal_to([
              ("k", (5, "first")),
              ("k", (5, "second")),
          ]))

  def test_consecutive_identical_values(self):
    """Identical values at different timestamps are not deduped."""
    elements = [(i, 999) for i in range(1, 6)]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(elements)
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn()))
      assert_that(
          result,
          equal_to([
              ("k", (1, 999)),
              ("k", (2, 999)),
              ("k", (3, 999)),
              ("k", (4, 999)),
              ("k", (5, 999)),
          ]))

  def test_gap_then_burst(self):
    """Elements with a large gap, then a burst, all get processed."""
    test_stream = (
        TestStream()
        .add_elements([10], event_timestamp=1)
        .add_elements([20], event_timestamp=2)
        .advance_watermark_to(5)
        # Large gap, no elements for a while
        .add_elements([1000], event_timestamp=100)
        .add_elements([1010], event_timestamp=101)
        .add_elements([1020], event_timestamp=102)
        .add_elements([1030], event_timestamp=103)
        .add_elements([1040], event_timestamp=104)
        )
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn()))
      assert_that(
          result,
          equal_to([
              ("k", (1, 10)),
              ("k", (2, 20)),
              ("k", (100, 1000)),
              ("k", (101, 1010)),
              ("k", (102, 1020)),
              ("k", (103, 1030)),
              ("k", (104, 1040)),
          ]))

  def test_watermark_holds_then_releases_batch(self):
    """Multiple elements buffered before watermark advances.

    All elements arrive before any watermark advance, then watermark
    jumps past all of them. They should all be emitted in one batch.
    """
    test_stream = (
        TestStream()
        .add_elements([10], event_timestamp=1)
        .add_elements([20], event_timestamp=2)
        .add_elements([30], event_timestamp=3)
        .add_elements([40], event_timestamp=4)
        .add_elements([50], event_timestamp=5)
        # No watermark advance yet, all 5 elements are buffered.
        # Now advance watermark past all of them at once.
        .advance_watermark_to(10)
        )
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.counter_dofn()))
      # All 5 should be counted sequentially regardless of batching.
      assert_that(
          result,
          equal_to([
              ("k", (1, 10)),
              ("k", (2, 20)),
              ("k", (3, 30)),
              ("k", (4, 40)),
              ("k", (5, 50)),
          ]))

  def test_context_survives_trim(self):
    """Context is built from prev_tail + batch elements.

    With batched processing, all elements in the same batch share
    context from the same prev_tail + earlier-in-batch elements.
    Context size is 3 (set in _ContextBag/__init__).
    """
    test_stream = (
        TestStream()
        .add_elements([10], event_timestamp=1)
        .add_elements([20], event_timestamp=2)
        .add_elements([30], event_timestamp=3)
        .add_elements([40], event_timestamp=4)
        .add_elements([50], event_timestamp=5)
        .add_elements([60], event_timestamp=6)
        .advance_watermark_to(8)
        .add_elements([90], event_timestamp=9)
        .add_elements([100], event_timestamp=10)
        )
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.context_dofn()))
      # All 8 elements in one batch. Context = last 3 from batch.
      # idx=0 (ts=1): context=[] → 0
      # idx=1 (ts=2): context=[(1,10)] → 1
      # idx=2 (ts=3): context=[(1,10),(2,20)] → 2
      # idx=3 (ts=4): context=[(1,10),(2,20),(3,30)] → 3
      # idx=4 (ts=5): context=[(2,20),(3,30),(4,40)] → 3
      # idx=5 (ts=6): context=[(3,30),(4,40),(5,50)] → 3
      # idx=6 (ts=9): context=[(4,40),(5,50),(6,60)] → 3
      # idx=7 (ts=10): context=[(5,50),(6,60),(9,90)] → 3
      expected = [
          ("k", (1, 10, 0)),
          ("k", (2, 20, 1)),
          ("k", (3, 30, 2)),
          ("k", (4, 40, 3)),
          ("k", (5, 50, 3)),
          ("k", (6, 60, 3)),
          ("k", (9, 90, 3)),
          ("k", (10, 100, 3)),
      ]
      assert_that(result, equal_to(expected))

  # -----------------------------------------------------------------
  # Stress tests
  # -----------------------------------------------------------------

  def test_many_elements_sequential(self):
    """200 elements with sequential timestamps all get processed."""
    n = 200
    elements = [(i, i) for i in range(1, n + 1)]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(elements)
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn())
          | beam.Map(lambda x: x[1]))  # strip key for easier assert
      assert_that(
          result,
          equal_to([(i, i) for i in range(1, n + 1)]))

  def test_many_elements_counter_continuity(self):
    """Counter stays correct across 100 elements with forced timer gaps."""
    test_stream = TestStream()
    n = 100
    for i in range(1, n + 1):
      test_stream.add_elements([i * 10], event_timestamp=i)
      # Force a watermark advance every 10 elements to create
      # multiple timer firings.
      if i % 10 == 0:
        test_stream.advance_watermark_to(i + 1)
    _finalize_test_stream(test_stream)

    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.counter_dofn())
          | beam.Map(lambda x: x[1]))
      assert_that(
          result,
          equal_to([(i, i * 10) for i in range(1, n + 1)]))

  def test_many_keys_independent(self):
    """10 keys with 20 elements each, all processed independently."""
    num_keys = 10
    elements_per_key = 20
    test_stream = TestStream()
    for ts in range(1, elements_per_key + 1):
      for k_idx in range(num_keys):
        key = f"k{k_idx}"
        val = k_idx * 1000 + ts
        test_stream.add_elements([(key, val)], event_timestamp=ts)
      test_stream.advance_watermark_to(ts)
    _finalize_test_stream(test_stream)

    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.Map(lambda kv: (kv[0], kv[1]))
          | beam.ParDo(self.collect_dofn()))

      expected = []
      for ts in range(1, elements_per_key + 1):
        for k_idx in range(num_keys):
          key = f"k{k_idx}"
          val = k_idx * 1000 + ts
          expected.append((key, (ts, val)))
      assert_that(result, equal_to(expected))

  def test_burst_after_long_silence(self):
    """50 elements arrive after a long silence period."""
    test_stream = (
        TestStream()
        .add_elements([1], event_timestamp=1)
        .advance_watermark_to(5))
    # Long silence, then burst of 50 elements
    for i in range(50):
      test_stream.add_elements([1000 + i], event_timestamp=1000 + i)
    _finalize_test_stream(test_stream)

    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn())
          | beam.Map(lambda x: x[1]))
      expected = [(1, 1)] + [(1000 + i, 1000 + i) for i in range(50)]
      assert_that(result, equal_to(expected))

  def test_context_with_many_elements(self):
    """Context reader works correctly over 50 elements with trim."""
    n = 50
    elements = [(i, i * 10) for i in range(1, n + 1)]
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | _create_test_stream(elements)
          | beam.WithKeys("k")
          | beam.ParDo(self.context_dofn())
          | beam.Map(lambda x: x[1]))

      # Context is read_before(ts, 3s) = entries in [ts-3, ts).
      # Trim keeps entries within 5s of fire_ts.
      # For ts >= 4, there are always 3 entries in [ts-3, ts).
      expected = []
      for i in range(1, n + 1):
        ctx_len = min(i - 1, 3)
        expected.append((i, i * 10, ctx_len))
      assert_that(result, equal_to(expected))


  def test_late_element_dropped(self):
    """Elements arriving after safe_watermark are dropped."""
    # Elements at t=1..5 are processed, then a duplicate late
    # element at t=3 arrives. It should be silently dropped since
    # safe_watermark has already advanced past t=3.
    elements = [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
    test_stream = _create_test_stream(elements)
    # Inject a late element: t=3 again with different value.
    # By this point safe_watermark >= 5, so t=3 is late.
    test_stream = TestStream()
    for ts, val in elements:
      test_stream.add_elements([val], event_timestamp=ts)
    test_stream.advance_watermark_to(5)
    test_stream.advance_processing_time(10)
    # Late element at t=3.
    test_stream.add_elements([999], event_timestamp=3)
    test_stream.advance_watermark_to(6)
    test_stream.advance_processing_time(10)
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.collect_dofn()))
      # val=999 should NOT appear — it's late. Original t=3
      # with val=30 should be present.
      assert_that(
          result,
          equal_to([
              ("k", (1, 10)),
              ("k", (2, 20)),
              ("k", (3, 30)),
              ("k", (4, 40)),
              ("k", (5, 50)),
          ]))



  def test_elements_arrive_during_batch_wait(self):
    """New elements arriving while BATCH_TIMER is pending get
    processed in the next cycle.

    Cycle 1: t=1,t=2 arrive. Watermark advances to 2, MIN_WM fires,
    arms MID/MAX probes (max=2), arms BATCH. safe_watermark=2.
    While BATCH is pending (5s wall-clock wait), t=5,t=6 arrive.
    BATCH fires: processes t=1,t=2. Finds t=5 remaining,
    calls start_next_cycle(t=5) with preserved max_event_time=6.

    Cycle 2: MIN_WM fires at t=5. Arms MAX_WM at t=6 (from
    preserved max_event_time). safe_watermark advances to 6.
    BATCH fires: processes t=5,t=6.
    """
    test_stream = (
        TestStream()
        # Cycle 1 elements.
        .add_elements([10], event_timestamp=1)
        .add_elements([20], event_timestamp=2)
        .advance_watermark_to(2)
        # Advance processing time to fire BATCH for cycle 1,
        # but inject new elements first (simulating arrival
        # during the BATCH wait period).
        .add_elements([50], event_timestamp=5)
        .add_elements([60], event_timestamp=6)
        .advance_watermark_to(6)
        .advance_processing_time(10))
    _finalize_test_stream(test_stream)
    with TestPipeline(options=self.options) as p:
      result = (
          p
          | test_stream
          | beam.WithKeys("k")
          | beam.ParDo(self.counter_dofn()))
      # Counter proves two separate cycles: 1,2 in cycle 1,
      # then 3,4 in cycle 2.
      assert_that(
          result,
          equal_to([
              ("k", (1, 10)),
              ("k", (2, 20)),
              ("k", (3, 50)),
              ("k", (4, 60)),
          ]))


if __name__ == '__main__':
  unittest.main()
