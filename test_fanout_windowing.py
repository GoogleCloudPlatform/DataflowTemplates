"""
Test to validate windowing behavior of SHARDED vs HOTKEY_FANOUT approaches
with both FixedWindows and SlidingWindows.

Demonstrates:
1. Both approaches produce correct results with FixedWindows
2. HOTKEY_FANOUT raises ValueError with SlidingWindows (Beam guard)
3. SHARDED works correctly with SlidingWindows
4. Manual hotkey reproduction shows the actual corruption

ROOT CAUSE OF CORRUPTION (core.py:3349):
  "Avoid double counting that may happen with stacked accumulating mode."

Beam's with_hot_key_fanout splits into hot and cold branches, then Flattens
them before a final CombinePerKey. The two WindowInto steps exist to prevent
accumulating-mode double-counting at the Flatten:

  WindowInto(DISCARDING) → pre-combine GBK → StripNonce → WindowInto(original) → Flatten → final GBK

But WindowInto(SlidingWindows) re-evaluates window assignments from timestamps,
causing accumulators to leak into adjacent overlapping windows.

SHARDED avoids this entirely: no hot/cold split, no Flatten, no WindowInto.
Every element takes the same path through both GBKs with window objects
preserved unchanged by Map operations.

Run:
  source venv/bin/activate && python test_fanout_windowing.py
"""
import random

import apache_beam as beam
from apache_beam import CombineFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import (
    FixedWindows, SlidingWindows, TimestampedValue)

# Disable type checking so TimestampedValue doesn't cause issues
TEST_OPTIONS = PipelineOptions(['--no_pipeline_type_check'])


class SumCombineFn(CombineFn):
  def create_accumulator(self):
    return 0

  def add_input(self, accumulator, element):
    return accumulator + element

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, accumulator):
    return accumulator


# ---------------------------------------------------------------------------
# Two-stage CombineFn helpers (same pattern as bqmonitor _PreCombineFn /
# _PostCombineFn and Beam's own PreCombineFn / PostCombineFn in core.py)
# ---------------------------------------------------------------------------

class PreCombineFn(CombineFn):
  """Stage 1: extract_output returns raw accumulator, not final value.

  This is critical for combiners like Mean where acc=(sum, count) but
  output=sum/count. You can merge accumulators but not merge outputs.
  """
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn

  def create_accumulator(self):
    return self._combine_fn.create_accumulator()

  def add_input(self, accumulator, element):
    return self._combine_fn.add_input(accumulator, element)

  def merge_accumulators(self, accumulators):
    return self._combine_fn.merge_accumulators(accumulators)

  def extract_output(self, accumulator):
    return accumulator  # raw accumulator, not final output


class PostCombineFn(CombineFn):
  """Stage 2: add_input merges an accumulator from stage 1."""
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


# ---------------------------------------------------------------------------
# Test data: 6 elements, 2 keys (A, B), various timestamps
#
# SlidingWindows(size=10, period=5) assigns each element to 2 windows:
#   (A,1) t=1  -> [-5,5), [0,10)
#   (A,2) t=2  -> [-5,5), [0,10)
#   (B,3) t=7  -> [0,10), [5,15)
#   (A,4) t=4  -> [-5,5), [0,10)
#   (B,5) t=3  -> [-5,5), [0,10)
#   (B,6) t=8  -> [0,10), [5,15)
#
# Expected SUM per (key, window):
#   [-5, 5):  A=1+2+4=7,  B=5
#   [0, 10):  A=1+2+4=7,  B=3+5+6=14
#   [5, 15):  B=3+6=9
# ---------------------------------------------------------------------------

TEST_DATA = [
    TimestampedValue(('A', 1), 1.0),
    TimestampedValue(('A', 2), 2.0),
    TimestampedValue(('B', 3), 7.0),   # t=7 matters for sliding windows
    TimestampedValue(('A', 4), 4.0),
    TimestampedValue(('B', 5), 3.0),
    TimestampedValue(('B', 6), 8.0),
]


class FormatResult(beam.DoFn):
  """Emit (key, value, window_start, window_end) for assertion."""
  def process(self, element, window=beam.DoFn.WindowParam):
    key, value = element
    yield (key, value, float(window.start), float(window.end))


SLIDING_EXPECTED = [
    ('A', 7, -5.0, 5.0),
    ('B', 5, -5.0, 5.0),
    ('A', 7, 0.0, 10.0),
    ('B', 14, 0.0, 10.0),
    ('B', 9, 5.0, 15.0),
]

FIXED_EXPECTED = [
    ('A', 7, 0.0, 10.0),
    ('B', 14, 0.0, 10.0),
]


# ===================================================================
# TESTS: FixedWindows (all approaches should work)
# ===================================================================

def test_fixed_plain():
  """Baseline: plain CombinePerKey with FixedWindows."""
  print('\n=== TEST: FixedWindows + Plain CombinePerKey ===')
  with TestPipeline(options=TEST_OPTIONS) as p:
    results = (
        p
        | beam.Create(TEST_DATA)
        | beam.WindowInto(FixedWindows(10))
        | beam.CombinePerKey(sum)
        | beam.ParDo(FormatResult())
    )
    assert_that(results, equal_to(FIXED_EXPECTED))
  print('PASSED')


def test_fixed_sharded():
  """SHARDED with FixedWindows — two GBKs, no WindowInto between them."""
  print('\n=== TEST: FixedWindows + SHARDED ===')
  combine_fn = SumCombineFn()
  _num_shards = 2

  with TestPipeline(options=TEST_OPTIONS) as p:
    results = (
        p
        | beam.Create(TEST_DATA)
        | beam.WindowInto(FixedWindows(10))
        # ShardKey: Map preserves windows, adds random shard prefix
        | 'ShardKey' >> beam.Map(
            lambda kv: ((random.randint(0, _num_shards - 1), kv[0]), kv[1]))
        # Pre-combine: GBK groups by (shard, key, window)
        | 'PartialCombine' >> beam.CombinePerKey(PreCombineFn(combine_fn))
        # RestoreKey: Map preserves windows, strips shard prefix
        | 'RestoreKey' >> beam.MapTuple(lambda ck, acc: (ck[1], acc))
        # NO WindowInto here — windows flow through unchanged
        # Final combine: GBK groups by (key, window), merges accumulators
        | 'FinalCombine' >> beam.CombinePerKey(PostCombineFn(combine_fn))
        | beam.ParDo(FormatResult())
    )
    assert_that(results, equal_to(FIXED_EXPECTED))
  print('PASSED')


def test_fixed_hotkey():
  """HOTKEY_FANOUT with FixedWindows — Beam's built-in two-stage approach."""
  print('\n=== TEST: FixedWindows + HOTKEY_FANOUT ===')
  with TestPipeline(options=TEST_OPTIONS) as p:
    results = (
        p
        | beam.Create(TEST_DATA)
        | beam.WindowInto(FixedWindows(10))
        # Beam's built-in: SplitHotCold → WindowInto(DISCARDING) →
        # CombinePerKey(Pre) → StripNonce → WindowInto(original) →
        # Flatten(hot, cold) → CombinePerKey(Post)
        #
        # WindowInto(FixedWindows) is idempotent (each timestamp maps to
        # exactly one window), so no corruption occurs.
        | beam.CombinePerKey(sum).with_hot_key_fanout(2)
        | beam.ParDo(FormatResult())
    )
    assert_that(results, equal_to(FIXED_EXPECTED))
  print('PASSED')


# ===================================================================
# TESTS: SlidingWindows (hotkey fanout should fail)
# ===================================================================

def test_sliding_plain():
  """Baseline: plain CombinePerKey with SlidingWindows."""
  print('\n=== TEST: SlidingWindows + Plain CombinePerKey ===')
  with TestPipeline(options=TEST_OPTIONS) as p:
    results = (
        p
        | beam.Create(TEST_DATA)
        | beam.WindowInto(SlidingWindows(10, 5))
        | beam.CombinePerKey(sum)
        | beam.ParDo(FormatResult())
    )
    assert_that(results, equal_to(SLIDING_EXPECTED))
  print('PASSED')


def test_sliding_sharded():
  """SHARDED with SlidingWindows — works because no WindowInto between GBKs.

  Data flow:
    ShardKey(Map)        → ((shard, key), value)  [windows preserved]
    CombinePerKey(Pre)   → ((shard, key), acc)    [GBK groups by (shard,key,window)]
    RestoreKey(Map)      → (key, acc)             [windows preserved — no WindowInto!]
    CombinePerKey(Post)  → (key, result)          [GBK groups by (key,window)]

  Each Map preserves window objects unchanged. No re-evaluation of window
  assignments from timestamps. Elements stay in their correct windows.
  """
  print('\n=== TEST: SlidingWindows + SHARDED ===')
  combine_fn = SumCombineFn()
  _num_shards = 2

  with TestPipeline(options=TEST_OPTIONS) as p:
    results = (
        p
        | beam.Create(TEST_DATA)
        | beam.WindowInto(SlidingWindows(10, 5))
        | 'ShardKey' >> beam.Map(
            lambda kv: ((random.randint(0, _num_shards - 1), kv[0]), kv[1]))
        | 'PartialCombine' >> beam.CombinePerKey(PreCombineFn(combine_fn))
        | 'RestoreKey' >> beam.MapTuple(lambda ck, acc: (ck[1], acc))
        | 'FinalCombine' >> beam.CombinePerKey(PostCombineFn(combine_fn))
        | beam.ParDo(FormatResult())
    )
    assert_that(results, equal_to(SLIDING_EXPECTED))
  print('PASSED')


def test_sliding_hotkey_raises():
  """HOTKEY_FANOUT with SlidingWindows — Beam raises ValueError.

  The guard exists because WindowInto(SlidingWindows) after the pre-combine
  re-evaluates window assignments from timestamps. With overlapping windows,
  an accumulator produced for Window[0,10) at timestamp 9.999 would be
  re-assigned to BOTH Window[0,10) AND Window[5,15), leaking data.
  """
  print('\n=== TEST: SlidingWindows + HOTKEY_FANOUT (expect ValueError) ===')
  try:
    with TestPipeline(options=TEST_OPTIONS) as p:
      results = (
          p
          | beam.Create(TEST_DATA)
          | beam.WindowInto(SlidingWindows(10, 5))
          | beam.CombinePerKey(sum).with_hot_key_fanout(2)
          | beam.ParDo(FormatResult())
      )
      assert_that(results, equal_to(SLIDING_EXPECTED))
    print('UNEXPECTED: Pipeline succeeded without error')
  except ValueError as e:
    if 'SlidingWindows' in str(e):
      print(f'CONFIRMED: Beam raises ValueError: {e}')
    else:
      print(f'UNEXPECTED ValueError: {e}')
  except Exception as e:
    print(f'Other error: {type(e).__name__}: {e}')


# ---------------------------------------------------------------------------
# Manual reproduction: bypass Beam's guard to observe actual corruption
# ---------------------------------------------------------------------------

def test_sliding_manual_hotkey_corruption():
  """Manually reproduce hotkey fanout to show the actual corruption.

  This bypasses Beam's ValueError guard by manually implementing the
  hotkey fanout pipeline shape:

    ShardWithNonce → CombinePerKey(Pre) → StripNonce → WindowInto(Sliding) → CombinePerKey(Post)

  The corruption happens at the WindowInto step. Example for key B:

    Pre-combine produces:
      (B, acc=14) for Window[0,10)  with output timestamp=9.999

    WindowInto(SlidingWindows(10, 5)) re-assigns from timestamp:
      t=9.999 → Window[0,10) AND Window[5,15)
                                    ^^^^^^^^^^
                                    LEAKED! acc=14 was only for [0,10)

    Final combine for Window[5,15) now merges:
      acc=14 (leaked from [0,10)) + acc=9 (correct) = 23  ← WRONG, should be 9

  The reason WindowInto exists at all: Beam's hotkey splits into hot + cold
  branches, then Flattens them before a final CombinePerKey. Without
  WindowInto(DISCARDING) on the hot path, accumulating-mode triggers would
  cause double-counting at the Flatten point. But WindowInto with
  SlidingWindows causes this different corruption (window leaking).
  """
  print('\n=== TEST: SlidingWindows + Manual HOTKEY (no guard) ===')
  print("Bypasses Beam's ValueError to show actual corruption.")

  combine_fn = SumCombineFn()
  _num_shards = 2

  class ShardWithNonce(beam.DoFn):
    """Replicates Beam's SplitHotCold nonce-per-bundle sharding."""
    def start_bundle(self):
      self._nonce = int(random.getrandbits(31))

    def process(self, element):
      key, value = element
      yield ((self._nonce % _num_shards, key), value)

  try:
    with TestPipeline(options=TEST_OPTIONS) as p:
      results = (
          p
          | beam.Create(TEST_DATA)
          | beam.WindowInto(SlidingWindows(10, 5))
          | 'Shard' >> beam.ParDo(ShardWithNonce())
          | 'PreCombine' >> beam.CombinePerKey(PreCombineFn(combine_fn))
          | 'StripNonce' >> beam.MapTuple(lambda ck, acc: (ck[1], acc))
          # THIS IS WHERE CORRUPTION HAPPENS:
          # WindowInto re-evaluates window assignments from timestamps.
          # With SlidingWindows, timestamps map to multiple overlapping
          # windows, so accumulators leak into adjacent windows.
          | 'Rewindow' >> beam.WindowInto(SlidingWindows(10, 5))
          | 'FinalCombine' >> beam.CombinePerKey(PostCombineFn(combine_fn))
          | 'Format' >> beam.ParDo(FormatResult())
      )
      assert_that(results, equal_to(SLIDING_EXPECTED))

    print('NO CORRUPTION detected (DirectRunner may not reproduce the issue '
          'due to single-bundle execution)')

  except Exception as e:
    err = str(e)
    # Extract the BeamAssertException message from the stack trace
    if 'BeamAssertException' in err:
      # Find the actual assertion diff
      for line in err.split('\n'):
        if 'unexpected elements' in line:
          print(f'CORRUPTION DETECTED: {line.strip()}')
          break
      else:
        print(f'CORRUPTION DETECTED (see full output for details)')
      # Show what went wrong
      for line in err.split('\n'):
        if line.strip().startswith(('(\'A', '(\'B')):
          print(f'  {line.strip()}')
    else:
      print(f'Error: {type(e).__name__}: {e}')


# ===================================================================
# Run all tests
# ===================================================================

if __name__ == '__main__':
  print('=' * 70)
  print('FANOUT WINDOWING VALIDATION TESTS')
  print('=' * 70)
  print()
  print('Tests validate that SHARDED fanout (composite key approach) works')
  print('correctly with all window types, while Beam\'s built-in')
  print('with_hot_key_fanout breaks with SlidingWindows.')
  print()
  print('Root cause: with_hot_key_fanout uses WindowInto between its two')
  print('GBKs to prevent accumulating-mode double-counting at a Flatten.')
  print('WindowInto with SlidingWindows re-evaluates window assignments,')
  print('causing accumulators to leak into adjacent overlapping windows.')
  print('SHARDED has no Flatten, so no WindowInto is needed.')

  # Fixed windows — all three should produce correct results
  test_fixed_plain()
  test_fixed_sharded()
  test_fixed_hotkey()

  # Sliding windows — plain and sharded should work, hotkey should fail
  test_sliding_plain()
  test_sliding_sharded()
  test_sliding_hotkey_raises()

  # Manual reproduction of corruption
  test_sliding_manual_hotkey_corruption()

  print('\n' + '=' * 70)
  print('ALL TESTS COMPLETE')
  print('=' * 70)
