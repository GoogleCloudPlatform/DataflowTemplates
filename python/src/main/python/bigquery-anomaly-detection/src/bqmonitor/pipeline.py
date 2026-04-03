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

"""Anomaly monitoring pipeline for BigQuery tables.

Reads streaming CDC data from BigQuery, computes a configurable windowed
metric, runs anomaly detection, and publishes anomalies to Pub/Sub.

Designed to be run as a Dataflow Flex Template or locally with DirectRunner.

Usage (Flex Template)::

    gcloud dataflow flex-template run "sales-monitor-$(date +%Y%m%d)" \\
        --template-file-gcs-location "gs://bucket/anomaly_monitor.json" \\
        --parameters table="project:dataset.table" \\
        --parameters metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":3600},"measures":[{"field":"transaction_amount","agg":"SUM","alias":"revenue"}]}}' \\
        --parameters detector_spec='{"type":"ZScore"}' \\
        --region us-central1

Usage (PrismRunner)::

    python main.py \\
        --table=project:dataset.table \\
        --metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":3600},"measures":[{"field":"transaction_amount","agg":"SUM","alias":"revenue"}]}}' \\
        --detector_spec='{"type":"ZScore"}' \\
        --runner=PrismRunner

Usage (DataflowRunner)::

    python main.py \\
        --table=project:dataset.table \\
        --metric_spec='<json>' \\
        --detector_spec='<json>' \\
        --runner=DataflowRunner \\
        --project=my-project \\
        --region=us-central1 \\
        --temp_location=gs://bucket/temp \\
        --staging_location=gs://bucket/staging \\
        --setup_file=./setup.py


metric_spec JSON Reference
==========================

Top-level ``metric_spec`` object::

    {
      "aggregation": { ... },           # required
      "derived_fields": [ ... ],         # optional, pre-aggregation
      "measure_combiner": { ... }        # optional (required if >1 measure)
    }

aggregation
-----------
::

    "aggregation": {
      "window": {
        "type": "fixed" | "sliding",
        "size_seconds": <number>,        # window size in seconds
        "period_seconds": <number>       # slide period (required for sliding)
      },
      "group_by": ["field1", "field2"],  # optional, omit for global agg
      "measures": [
        {"field": "<col>", "agg": "<AGG>", "alias": "<name>"},
        ...
      ]
    }

Aggregation operators (``agg``): ``SUM``, ``COUNT``, ``MIN``, ``MAX``, ``MEAN``.

For ``COUNT``, the ``field`` value is ignored — it counts all rows in the
group.

Expressions
-----------
Both ``measure_combiner.expression`` and ``derived_fields[].expression``
are Python expression strings. Bare names are field references, and the
following syntax is supported:

- Arithmetic: ``+``, ``-``, ``*``, ``/``, ``//``, ``%``, ``**``
- Comparisons: ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``
- Boolean logic: ``and``, ``or``, ``not``
- Negation: ``-field``
- Conditional: ``true_val if condition else false_val``
- Functions: ``abs()``, ``min()``, ``max()``, ``round()``
- Grouping: parentheses for precedence

``measure_combiner`` references measure aliases and is validated at
pipeline construction time.

derived_fields
--------------
Computed before aggregation. Each entry creates a new column available to
measures::

    "derived_fields": [
      {"name": "is_success", "expression": "1 if status == 'success' else 0"}
    ]

measure_combiner
----------------
Post-aggregation expression that combines measure aliases into a single
value. Required when there are multiple measures (e.g., ratio metrics)::

    "measure_combiner": {"expression": "clicks / impressions"}
    "measure_combiner": {"expression": "(successes + partial) / total"}


detector_spec JSON Reference
=============================

Top-level ``detector_spec`` object::

    {"type": "<DetectorName>", "config": { ... }}

The ``type`` must be a registered ``@specifiable`` detector class name.
``config`` keys map to that class's ``__init__`` parameters plus inherited
``AnomalyDetector`` parameters.

Common AnomalyDetector parameters (all detectors)::

    "config": {
      "threshold_criterion": { ... },       # optional, see below
      "model_id": "<string>"                # optional detector ID
    }

``features`` is automatically set to ``['value']`` to match
``ComputeMetric`` output; it does not need to be specified.

window_size
-----------
All detectors maintain an internal sliding window of recent values for their
statistical trackers (mean, stdev, quantiles, etc.).  The default is 1000
data points.  Use ``window_size`` as a shorthand to override this for all
internal trackers at once::

    {"type": "ZScore", "config": {"window_size": 500}}

Available detectors
-------------------

**ZScore** — ``|value - mean| / stdev`` (default threshold: 3)::

    {"type": "ZScore"}

**IQR** — Interquartile Range (default threshold: 1.5)::

    {"type": "IQR"}

**RobustZScore** — Modified Z-Score using median/MAD (default threshold: 3.5)::

    {"type": "RobustZScore"}

threshold_criterion
-------------------
Override the default threshold by nesting a specifiable threshold object.

**FixedThreshold** — static cutoff (scores >= cutoff are outliers)::

    "threshold_criterion": {
      "type": "FixedThreshold",
      "config": {"cutoff": 10}
    }

**QuantileThreshold** — dynamic cutoff at a quantile of observed scores::

    "threshold_criterion": {
      "type": "QuantileThreshold",
      "config": {"quantile": 0.95}
    }

Both accept optional ``normal_label`` (default 0), ``outlier_label``
(default 1), and ``missing_label`` (default -2).

**Threshold** — fixed threshold alert based on a boolean expression.
No warmup period, no history buffer. Alerts whenever the expression
evaluates to true::

    {"type": "Threshold", "expression": "value >= 0.5"}
    {"type": "Threshold", "expression": "value > 100 or value < -100"}
    {"type": "Threshold", "expression": "value <= 0.01"}

The expression receives the computed metric as ``value`` and supports
all safe expression operators (see Expressions section above).


Examples
--------

Simple SUM metric with ZScore::

    --metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":3600},"measures":[{"field":"transaction_amount","agg":"SUM","alias":"revenue"}]}}'
    --detector_spec='{"type":"ZScore"}'

Grouped ratio metric (CTR) with ZScore::

    --metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":10},"group_by":["campaign_type","browser_version"],"measures":[{"field":"is_click","agg":"SUM","alias":"clicks"},{"field":"is_click","agg":"COUNT","alias":"impressions"}]},"measure_combiner":{"expression":"clicks / impressions"}}'
    --detector_spec='{"type":"ZScore"}'

Derived field + ratio + custom threshold::

    --metric_spec='{"derived_fields":[{"name":"is_success","expression":"1 if status == \\'success\\' else 0"}],"aggregation":{"window":{"type":"fixed","size_seconds":10},"group_by":["brand_name","category"],"measures":[{"field":"is_success","agg":"SUM","alias":"successes"},{"field":"is_success","agg":"COUNT","alias":"total"}]},"measure_combiner":{"expression":"successes / total"}}'
    --detector_spec='{"type":"ZScore","config":{"threshold_criterion":{"type":"FixedThreshold","config":{"cutoff":10}}}}'
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.utils.timestamp import Duration

from bqmonitor.metric import ComputeMetric
from bqmonitor.metric import FanoutStrategy
from bqmonitor.metric import MetricSpec
from bqmonitor.safe_eval import Expr
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.transforms import AnomalyDetection

# Import detectors so they register with @specifiable before from_spec.
from apache_beam.ml.anomaly.detectors import zscore  # noqa: F401
from apache_beam.ml.anomaly.detectors import iqr  # noqa: F401
from apache_beam.ml.anomaly.detectors import robust_zscore  # noqa: F401

_LOGGER = logging.getLogger(__name__)

_SUPPORTED_DETECTORS = ('ZScore', 'IQR', 'RobustZScore')


@dataclass(frozen=True)
class OffsetKey:
  """Key that pairs an optional grouping key with a window offset.

  Used to route each sliding-window offset to an independent detector state.
  Fixed windows always get offset=0.
  """
  key: Optional[tuple]
  offset: int  # window_start micros mod window_size micros

# Matches project:dataset.table or project.dataset.table
_TABLE_RE = re.compile(
    r'^[a-zA-Z0-9][a-zA-Z0-9_-]*[:\.][a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_topic_path(topic):
  """Validate that a Pub/Sub topic is a full resource path.

  Args:
    topic: Full Pub/Sub topic path, e.g.
        'projects/my-project/topics/my-topic'.

  Returns:
    The validated topic path.

  Raises:
    ValueError: If the topic is not a full resource path.
  """
  if not (topic.startswith('projects/') and '/topics/' in topic):
    raise ValueError(
        f"--topic must be a full Pub/Sub resource path "
        f"(projects/<project>/topics/<topic>), got: '{topic}'")
  return topic


def _unpack_result(element):
  """Unpack a possibly-keyed AnomalyResult element.

  Returns:
    (key, result) where key is None for unkeyed elements.
  """
  if isinstance(element, tuple) and len(element) == 2:
    return element[0], element[1]
  return None, element


def _parse_table_ref(table):
  """Parse and validate a table reference string.

  Args:
    table: Table reference in 'project:dataset.table' or
        'project.dataset.table' format.

  Returns:
    (project, dataset, table_name) tuple.

  Raises:
    ValueError: If the table string doesn't match the expected format.
  """
  if not _TABLE_RE.match(table):
    raise ValueError(
        f"Invalid --table format: '{table}'. "
        f"Expected: project:dataset.table or project.dataset.table")
  if ':' in table:
    project, rest = table.split(':', 1)
    dataset, table_name = rest.split('.', 1)
  else:
    project, dataset, table_name = table.split('.', 2)
  return project, dataset, table_name


# ---------------------------------------------------------------------------
# DoFns
# ---------------------------------------------------------------------------


class _LogAnomalyResult(beam.DoFn):
  """Logs each AnomalyResult at WARNING level for visibility in Dataflow."""
  def process(self, element):
    key, result = _unpack_result(element)
    prediction = result.predictions[0]
    example = result.example

    if prediction.label == 1:
      tag = '!! OUTLIER !!'
    elif prediction.label == 0:
      tag = 'NORMAL'
    else:
      tag = 'WARMUP'

    ws = example.window_start.to_rfc3339()
    we = example.window_end.to_rfc3339()
    window_str = f'{ws}-{we}'

    if key is not None:
      _LOGGER.warning(
          '[%s] window=%s key=%s value=%.2f score=%s label=%s',
          tag, window_str, key, example.value, prediction.score,
          prediction.label)
    else:
      _LOGGER.warning(
          '[%s] window=%s value=%.2f score=%s label=%s',
          tag, window_str, example.value, prediction.score,
          prediction.label)
    yield element


class _ThresholdAlert(beam.DoFn):
  """Evaluates a threshold expression against metric values.

  Emits AnomalyResult elements consistent with the statistical detectors,
  allowing threshold alerts to flow through the same logging and Pub/Sub
  pipeline.

  The expression is evaluated with ``value`` bound to the metric value.
  If it evaluates to truthy, the element is labelled as an outlier (1);
  otherwise it is labelled normal (0).

  Example expressions: ``value >= 0.5``, ``value <= 0.01``,
  ``value > 100 or value < -100``.
  """

  def __init__(self, expression_text):
    self._expression_text = expression_text
    self._expr = None

  def setup(self):
    self._expr = Expr(self._expression_text)

  def process(self, element):
    if isinstance(element, tuple) and len(element) == 2:
      key, row = element
    else:
      key, row = None, element

    value = row.value
    is_alert = bool(self._expr({'value': value}))

    prediction = AnomalyPrediction(
        model_id=f'Threshold({self._expression_text})',
        score=None,
        label=1 if is_alert else 0,
        threshold=None)

    result = AnomalyResult(example=row, predictions=[prediction])

    if key is not None:
      yield (key, result)
    else:
      yield result


class _FormatAnomalyAsJson(beam.DoFn):
  """Converts anomaly results (label == 1) to JSON byte strings for Pub/Sub."""
  def process(self, element):
    key, result = _unpack_result(element)
    prediction = result.predictions[0]
    if prediction.label != 1:
      return

    example = result.example
    ws = example.window_start.to_rfc3339()
    we = example.window_end.to_rfc3339()

    payload = {
        'event_description': (
            f'Anomaly detected value={example.value}'
            f' score={prediction.score}'
            f' in window={ws}-{we}'),
        'agent_id': prediction.model_id,
    }
    if key is not None:
      payload['key'] = str(key)

    yield json.dumps(payload).encode('utf-8')


_SINK_SCHEMA = {
    'fields': [
        {'name': 'window_start', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'window_end', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'value', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
        {'name': 'score', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'label', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'key', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


class _FormatResultForBQ(beam.DoFn):
  """Converts all AnomalyResult elements to BQ row dicts."""
  def process(self, element):
    key, result = _unpack_result(element)
    prediction = result.predictions[0]
    example = result.example

    row = {
        'window_start': example.window_start.to_rfc3339(),
        'window_end': example.window_end.to_rfc3339(),
        'value': float(example.value),
        'score': float(prediction.score) if prediction.score is not None
        else None,
        'label': int(prediction.label),
    }
    if key is not None:
      row['key'] = str(key)

    yield row


# ---------------------------------------------------------------------------
# Pipeline options
# ---------------------------------------------------------------------------


class AnomalyMonitorOptions(PipelineOptions):
  """Pipeline options for the anomaly monitor."""
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--table',
        default=None,
        help='BigQuery table to monitor. '
        'Format: project:dataset.table')
    parser.add_argument(
        '--metric_spec',
        default=None,
        help='JSON string defining the metric computation. '
        'See MetricSpec.from_dict() for schema.')
    parser.add_argument(
        '--detector_spec',
        default=None,
        help='JSON string defining the anomaly detector. '
        'Format: {"type":"ZScore"} or '
        '{"type":"ZScore","config":{"threshold_criterion":{...}}}')
    parser.add_argument(
        '--poll_interval_sec',
        type=int,
        default=60,
        help='Seconds between BigQuery CDC polls. Default 60.')
    parser.add_argument(
        '--change_function',
        default='APPENDS',
        choices=['APPENDS', 'CHANGES'],
        help='BigQuery change function to use. Default APPENDS.')
    parser.add_argument(
        '--buffer_sec',
        type=float,
        default=15.0,
        help='Safety buffer behind now() in seconds. Default 15.')
    parser.add_argument(
        '--start_offset_sec',
        type=float,
        default=60.0,
        help='Start reading from this many seconds ago. Default 60.')
    parser.add_argument(
        '--duration_sec',
        type=float,
        default=0.0,
        help='How long to run in seconds. 0 means run forever. Default 0.')
    parser.add_argument(
        '--temp_dataset',
        default=None,
        help='BigQuery dataset for temp tables. If unset, auto-created.')
    parser.add_argument(
        '--topic',
        default=None,
        help='Pub/Sub topic for anomaly results. '
        'Full path: projects/<project>/topics/<topic>.')
    parser.add_argument(
        '--log_all_results',
        default='false',
        help='Log all anomaly detection results (normal, outlier, warmup) '
        'at WARNING level. Default: false.')
    parser.add_argument(
        '--sink_table',
        default=None,
        help='BigQuery table to write all anomaly detection results to. '
        'Format: project:dataset.table. If unset, results are not written '
        'to BigQuery.')
    parser.add_argument(
        '--decompress_shards',
        type=int,
        default=1200,
        help='Number of shards for CDC Arrow batch decompression fan-out. '
        'Spreads decompression CPU across workers. '
        '0 disables fan-out (decode inline). Default: 1200.')
    parser.add_argument(
        '--fanout_strategy',
        default='sharded',
        choices=['sharded', 'hotkey_fanout', 'none', 'precombine'],
        help='Parallelism strategy for metric aggregation: '
        'sharded, hotkey_fanout, precombine, or none. Default: sharded.')
    parser.add_argument(
        '--fanout',
        type=int,
        default=400,
        help='Number of shards for sharded or hotkey_fanout strategies. '
        'Ignored for none and precombine. Default: 400.')


# ---------------------------------------------------------------------------
# Spec parsing
# ---------------------------------------------------------------------------


def _parse_metric_spec(json_str):
  """Parse a MetricSpec from a JSON string.

  Raises:
    ValueError: If the JSON is malformed or the spec is invalid.
  """
  try:
    d = json.loads(json_str)
  except json.JSONDecodeError as e:
    raise ValueError(
        f"Invalid JSON in --metric_spec: {e}. "
        f"Value: {json_str[:200]}") from e
  try:
    return MetricSpec.from_dict(d)
  except (ValueError, TypeError, KeyError) as e:
    raise ValueError(f"Invalid --metric_spec: {e}") from e


def _dict_to_spec(d):
  """Recursively convert nested dicts with ``type`` keys into Spec objects.

  ``json.loads`` produces plain dicts, but ``Specifiable.from_spec`` expects
  ``Spec`` objects for nested specifiables (e.g. ``threshold_criterion``
  inside a detector config).  Without this conversion the nested dict passes
  through ``_specifiable_from_spec_helper`` unchanged and the detector
  receives a raw dict instead of the expected ``ThresholdFn`` instance.
  """
  if isinstance(d, dict) and 'type' in d:
    config = d.get('config', {})
    if config:
      config = {k: _dict_to_spec(v) for k, v in config.items()}
    return Spec(type=d['type'], config=config)
  if isinstance(d, list):
    return [_dict_to_spec(item) for item in d]
  return d


def _expand_window_size(d):
  """Expand ``window_size`` shorthand into detector-specific tracker configs.

  Instead of constructing deeply nested tracker specs, users can write::

      {"type": "ZScore", "config": {"window_size": 500}}

  This expands into the full nested tracker configuration that each detector
  type expects.  If the user already set explicit tracker configs, those take
  precedence (``setdefault`` semantics).

  Raises:
    ValueError: If window_size is not a positive integer.
  """
  config = d.get('config', {})
  ws = config.pop('window_size', None)
  if ws is None:
    return

  if not isinstance(ws, int) or ws <= 0:
    raise ValueError(
        f"window_size must be a positive integer, got {ws!r}")

  detector_type = d['type']

  if detector_type == 'ZScore':
    config.setdefault(
        'sub_stat_tracker',
        {'type': 'IncSlidingMeanTracker', 'config': {'window_size': ws}})
    config.setdefault(
        'stdev_tracker',
        {'type': 'IncSlidingStdevTracker', 'config': {'window_size': ws}})
  elif detector_type == 'IQR':
    config.setdefault(
        'q1_tracker',
        {
            'type': 'BufferedSlidingQuantileTracker',
            'config': {'window_size': ws, 'q': 0.25}
        })
    # q3_tracker auto-derives from q1_tracker in IQR.__init__
  elif detector_type == 'RobustZScore':
    _median_tracker_spec = {
        'type': 'MedianTracker',
        'config': {
            'quantile_tracker': {
                'type': 'BufferedSlidingQuantileTracker',
                'config': {'window_size': ws, 'q': 0.5}
            }
        }
    }
    config.setdefault(
        'mad_tracker',
        {
            'type': 'MadTracker',
            'config': {
                'median_tracker': _median_tracker_spec,
                'diff_median_tracker': {
                    'type': 'MedianTracker',
                    'config': {
                        'quantile_tracker': {
                            'type': 'BufferedSlidingQuantileTracker',
                            'config': {'window_size': ws, 'q': 0.5}
                        }
                    }
                }
            }
        })


def _parse_detector_spec(json_str):
  """Parse an anomaly detector from a JSON Spec string.

  The JSON should have the form::

      {"type": "ZScore"}

  Nested specifiable objects (e.g. ``threshold_criterion``) are supported::

      {"type": "ZScore", "config": {
          "threshold_criterion": {"type": "FixedThreshold", "config": {"cutoff": 10}}
      }}

  A ``window_size`` shorthand sets the history buffer for all internal
  trackers::

      {"type": "ZScore", "config": {"window_size": 500}}

  **Threshold** — a simple fixed-threshold alerter that evaluates a boolean
  expression against the metric value. No warmup period, no history::

      {"type": "Threshold", "expression": "value >= 0.5"}
      {"type": "Threshold", "expression": "value > 100 or value < -100"}

  The expression may use ``value`` (the computed metric) and all safe
  expression operators (see Expressions section above).

  For statistical detectors, the ``type`` field must match a registered
  @specifiable detector class (e.g. ZScore, IQR, RobustZScore).

  ``features`` is automatically set to ``['value']`` to match the output of
  ``ComputeMetric``. Any user-supplied ``features`` is overwritten.

  Returns:
    For statistical detectors: an instantiated AnomalyDetector.
    For Threshold: a ``_ThresholdAlert`` DoFn instance.

  Raises:
    ValueError: If the JSON is malformed, detector type is unknown, or
        the spec is otherwise invalid.
  """
  try:
    d = json.loads(json_str)
  except json.JSONDecodeError as e:
    raise ValueError(
        f"Invalid JSON in --detector_spec: {e}. "
        f"Value: {json_str[:200]}") from e

  if not isinstance(d, dict) or 'type' not in d:
    raise ValueError(
        "detector_spec must be a JSON object with a 'type' field. "
        f"Example: {{\"type\":\"ZScore\"}}. Got: {json_str[:200]}")

  detector_type = d['type']

  if detector_type == 'Threshold':
    expr_text = d.get('expression')
    if not expr_text:
      raise ValueError(
          "Threshold detector requires an 'expression' field. "
          "Example: {\"type\":\"Threshold\",\"expression\":\"value >= 0.5\"}")
    # Validate the expression at parse time.
    try:
      expr = Expr(expr_text)
    except (ValueError, SyntaxError) as e:
      raise ValueError(
          f"Invalid threshold expression: {e}") from e
    if 'value' not in expr.field_refs():
      _LOGGER.warning(
          "Threshold expression '%s' does not reference 'value'. "
          "It will receive the computed metric value as 'value'.", expr_text)
    return _ThresholdAlert(expr_text)

  if detector_type not in _SUPPORTED_DETECTORS:
    raise ValueError(
        f"Unknown detector type '{detector_type}'. "
        f"Supported detectors: {', '.join(_SUPPORTED_DETECTORS)}, Threshold")

  d.setdefault('config', {})
  d['config']['features'] = ['value']
  _expand_window_size(d)
  spec = _dict_to_spec(d)
  try:
    return Specifiable.from_spec(spec, _run_init=True)
  except (ValueError, TypeError) as e:
    raise ValueError(
        f"Failed to construct {detector_type} detector: {e}") from e


# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------


def _preflight_checks(options, metric_spec):
  """Validate GCP resources are accessible before building the pipeline.

  Checks:
    - BigQuery source table exists and is readable.
    - Required metric columns exist in the source table (dry-run query).
    - BigQuery temp dataset is writable (if specified) or datasets.create
      permission exists (dry-run only — does not actually create).
    - Pub/Sub topic exists.

  Logs warnings and continues if a check cannot be performed (e.g. missing
  client library). Raises ValueError on definite failures.
  """
  project, dataset, table_name = _parse_table_ref(options.table)
  topic_path = _validate_topic_path(options.topic)

  required_columns = sorted(metric_spec.required_source_columns())
  _check_bq_source_table(project, dataset, table_name, options,
                         required_columns)
  _check_bq_temp_dataset(project, options)
  _check_pubsub_topic(topic_path)


def _check_bq_source_table(project, dataset, table_name, options,
                           required_columns):
  """Verify the source BigQuery table exists and required columns are present.

  Runs a dry-run CDC query selecting the columns referenced by the metric
  spec. This validates table access, CDC function access, and column
  existence in a single round-trip.
  """
  try:
    from apache_beam.io.gcp import bigquery_tools
    from apache_beam.io.gcp.internal.clients import bigquery
  except ImportError:
    _LOGGER.warning(
        '[Preflight] Skipping BQ table check: '
        'BigQuery client libraries not available')
    return

  try:
    bq = bigquery_tools.BigQueryWrapper()
    bq.get_table(project, dataset, table_name)
    _LOGGER.info(
        '[Preflight] Source table %s:%s.%s is accessible',
        project, dataset, table_name)
  except Exception as e:
    raise ValueError(
        f"Cannot access BigQuery table '{project}:{dataset}.{table_name}'. "
        f"Verify it exists and the service account has "
        f"bigquery.tables.get and bigquery.tables.getData permissions. "
        f"Error: {e}") from e

  # Dry-run a CDC query selecting the metric's required columns.
  # This validates CDC function access and column existence in one step.
  select_clause = ', '.join(required_columns) if required_columns else '1'
  try:
    sql = (
        f"SELECT {select_clause} FROM {options.change_function}"
        f"(TABLE `{project}.{dataset}.{table_name}`, "
        f"NULL, NULL) LIMIT 0")
    _LOGGER.info('[Preflight] Dry-run query: %s', sql)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                query=bigquery.JobConfigurationQuery(
                    query=sql,
                    useLegacySql=False),
                dryRun=True)))
    bq.client.jobs.Insert(request)
    _LOGGER.info(
        '[Preflight] %s() access and columns %s verified for %s:%s.%s',
        options.change_function, required_columns,
        project, dataset, table_name)
  except Exception as e:
    raise ValueError(
        f"Cannot execute {options.change_function}() on "
        f"'{project}:{dataset}.{table_name}' "
        f"with columns {required_columns}. "
        f"Verify the table has change history enabled, the columns exist, "
        f"and the service account has bigquery.jobs.create permission. "
        f"Error: {e}") from e


def _check_bq_temp_dataset(project, options):
  """Verify access to the temp dataset (if specified), or check that
  datasets.create permission exists for auto-creation."""
  try:
    from apache_beam.io.gcp import bigquery_tools
    from apache_beam.io.gcp.internal.clients import bigquery
    from apitools.base.py.exceptions import HttpError
  except ImportError:
    _LOGGER.warning(
        '[Preflight] Skipping BQ temp dataset check: '
        'BigQuery client libraries not available')
    return

  if options.temp_dataset:
    try:
      bq = bigquery_tools.BigQueryWrapper()
      bq.client.datasets.Get(
          bigquery.BigqueryDatasetsGetRequest(
              projectId=project, datasetId=options.temp_dataset))
      _LOGGER.info(
          '[Preflight] Temp dataset %s:%s exists',
          project, options.temp_dataset)
    except HttpError as e:
      if e.status_code == 404:
        raise ValueError(
            f"Temp dataset '{project}:{options.temp_dataset}' not found. "
            f"Create it or omit --temp_dataset for auto-creation.") from e
      elif e.status_code == 403:
        raise ValueError(
            f"No access to temp dataset '{project}:{options.temp_dataset}'. "
            f"Verify the service account has "
            f"bigquery.datasets.get permission.") from e
      raise

    # Verify we can write to the temp dataset by doing a dry-run query
    # with a destination table in it.
    try:
      temp_table_ref = bigquery.TableReference(
          projectId=project,
          datasetId=options.temp_dataset,
          tableId='beam_ch_preflight_check')
      request = bigquery.BigqueryJobsInsertRequest(
          projectId=project,
          job=bigquery.Job(
              configuration=bigquery.JobConfiguration(
                  query=bigquery.JobConfigurationQuery(
                      query='SELECT 1',
                      useLegacySql=False,
                      destinationTable=temp_table_ref,
                      writeDisposition='WRITE_TRUNCATE'),
                  dryRun=True)))
      bq.client.jobs.Insert(request)
      _LOGGER.info(
          '[Preflight] Write access to temp dataset %s:%s verified',
          project, options.temp_dataset)
    except Exception as e:
      raise ValueError(
          f"Cannot write to temp dataset '{project}:{options.temp_dataset}'. "
          f"Verify the service account has bigquery.tables.create and "
          f"bigquery.tables.updateData permissions on this dataset. "
          f"Error: {e}") from e
  else:
    _LOGGER.info(
        '[Preflight] No --temp_dataset specified; '
        'will auto-create at runtime (requires bigquery.datasets.create)')


def _check_pubsub_topic(topic_path):
  """Verify the Pub/Sub topic exists."""
  try:
    from google.cloud import pubsub_v1
    from google.api_core.exceptions import NotFound, PermissionDenied
  except ImportError:
    _LOGGER.warning(
        '[Preflight] Skipping Pub/Sub check: '
        'google-cloud-pubsub not available')
    return

  try:
    publisher = pubsub_v1.PublisherClient()
    publisher.get_topic(topic=topic_path)
    _LOGGER.info('[Preflight] Pub/Sub topic %s is accessible', topic_path)
  except NotFound:
    raise ValueError(
        f"Pub/Sub topic '{topic_path}' not found. "
        f"Create it with: gcloud pubsub topics create {topic_path}")
  except PermissionDenied as e:
    raise ValueError(
        f"No permission to access Pub/Sub topic '{topic_path}'. "
        f"Verify the service account has pubsub.topics.get and "
        f"pubsub.topics.publish permissions. Error: {e}") from e
  except Exception as e:
    _LOGGER.warning(
        '[Preflight] Could not verify Pub/Sub topic %s: %s',
        topic_path, e)


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------


def build_pipeline(pipeline, options, metric_spec, detector):
  """Construct the anomaly monitoring pipeline.

  Args:
    pipeline: A beam.Pipeline instance.
    options: AnomalyMonitorOptions with table, poll_interval_sec, etc.
    metric_spec: Parsed MetricSpec instance.
    detector: Parsed anomaly detector instance.

  Returns:
    The final PCollection (for testing).
  """
  from bqmonitor.cdc import ReadBigQueryChangeHistory

  start_time = time.time() - options.start_offset_sec
  stop_time = (
      time.time() + options.duration_sec if options.duration_sec > 0 else None)

  _LOGGER.info('Anomaly Monitor Pipeline')
  _LOGGER.info('  Table: %s', options.table)
  _LOGGER.info('  Detector: %s', type(detector).__name__)
  _LOGGER.info('  Poll interval: %d sec', options.poll_interval_sec)
  _LOGGER.info('  Change function: %s', options.change_function)

  columns = sorted(metric_spec.required_source_columns())
  _LOGGER.info('  Columns: %s', columns)

  # Auto-rename pseudo-columns if they conflict with user column names.
  change_type_col = 'change_type'
  change_ts_col = 'change_timestamp'
  col_set = set(columns)
  if change_type_col in col_set:
    change_type_col = '_bqm_change_type'
    _LOGGER.info(
        '  Renamed pseudo-column change_type -> %s to avoid conflict',
        change_type_col)
  if change_ts_col in col_set:
    change_ts_col = '_bqm_change_timestamp'
    _LOGGER.info(
        '  Renamed pseudo-column change_timestamp -> %s to avoid conflict',
        change_ts_col)

  cdc_kwargs = dict(
      table=options.table,
      poll_interval_sec=options.poll_interval_sec,
      start_time=start_time,
      change_function=options.change_function,
      buffer_sec=options.buffer_sec,
      columns=columns,
      change_type_column=change_type_col,
      change_timestamp_column=change_ts_col,
      decompress_shards=(
          options.decompress_shards if options.decompress_shards > 0
          else None))
  if stop_time is not None:
    cdc_kwargs['stop_time'] = stop_time
  if options.temp_dataset:
    cdc_kwargs['temp_dataset'] = options.temp_dataset

  rows = pipeline | 'ReadCDC' >> ReadBigQueryChangeHistory(**cdc_kwargs)
  fanout_strategy = FanoutStrategy(options.fanout_strategy)
  metrics = rows | 'ComputeMetric' >> ComputeMetric(
      metric_spec, fanout_strategy=fanout_strategy, fanout=options.fanout)

  # Rewindow into GlobalWindows so the anomaly detector sees the full
  # stream of window results as a time series, not isolated per-window.
  from apache_beam.transforms.window import GlobalWindows
  global_metrics = metrics | 'Rewindow' >> beam.WindowInto(
      GlobalWindows())

  # Wrap every element's key in an OffsetKey so that stateful detectors
  # (ZScore, IQR, RobustZScore) route each sliding-window offset to
  # independent state. For example, size=60 period=30 produces offsets
  # 0 and 30 — key "K" becomes OffsetKey("K", 0) and OffsetKey("K", 30).
  # Fixed windows always get offset=0.
  _window_duration = Duration(metric_spec.aggregation.window.size_seconds)
  has_group_by = bool(metric_spec.aggregation.group_by)

  def _add_offset_key(element, _wd=_window_duration, _keyed=has_group_by):
    if _keyed:
      key, row = element
    else:
      key, row = None, element
    offset = (row.window_start % _wd).micros
    return (OffsetKey(key=key, offset=offset), row)

  if not isinstance(detector, _ThresholdAlert):
    global_metrics = (
        global_metrics | 'AddOffsetKey' >> beam.Map(_add_offset_key))

  if isinstance(detector, _ThresholdAlert):
    anomalies = global_metrics | 'DetectAnomalies' >> beam.ParDo(detector)
  else:
    global_metrics = (
        global_metrics
        | 'TypeHintMetrics' >> beam.Map(lambda x: x).with_output_types(
            beam.typehints.Tuple[Any, beam.Row]))
    anomalies = global_metrics | 'DetectAnomalies' >> AnomalyDetection(detector)

  # Strip OffsetKey back to the original key (or no key) for downstream.
  if not isinstance(detector, _ThresholdAlert):

    def _strip_offset_key(element, _keyed=has_group_by):
      offset_key, result = element
      if _keyed:
        return (offset_key.key, result)
      return result

    anomalies = (
        anomalies | 'StripOffsetKey' >> beam.Map(_strip_offset_key))

  if options.log_all_results.lower() == 'true':
    _ = anomalies | 'LogResults' >> beam.ParDo(_LogAnomalyResult())

  # Publish anomalies (label == 1) to Pub/Sub.
  topic_path = _validate_topic_path(options.topic)

  _ = (
      anomalies
      | 'FormatAnomalies' >> beam.ParDo(_FormatAnomalyAsJson())
      | 'WriteToPubSub' >> WriteToPubSub(topic=topic_path))

  # Write all results to a BigQuery sink table (if configured).
  if options.sink_table:
    sink_table = options.sink_table.replace(':', '.')
    _ = (
        anomalies
        | 'FormatForBQ' >> beam.ParDo(_FormatResultForBQ())
        | 'WriteSink' >> WriteToBigQuery(
            table=sink_table,
            method='STREAMING_INSERTS',
            schema=_SINK_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

  return anomalies


def run(argv=None):
  """Main entry point."""
  options = PipelineOptions(argv)
  monitor_options = options.view_as(AnomalyMonitorOptions)

  # Validate required options.
  for required_opt in ('table', 'metric_spec', 'detector_spec', 'topic'):
    if getattr(monitor_options, required_opt) is None:
      raise ValueError(f'--{required_opt} is required')

  # Validate table format.
  _parse_table_ref(monitor_options.table)

  # Parse specs early so errors surface before pipeline construction.
  metric_spec = _parse_metric_spec(monitor_options.metric_spec)
  detector = _parse_detector_spec(monitor_options.detector_spec)

  # Check GCP resources are accessible.
  _preflight_checks(monitor_options, metric_spec)

  options.view_as(SetupOptions).save_main_session = True

  from apache_beam.options.pipeline_options import StandardOptions
  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:
    build_pipeline(p, monitor_options, metric_spec, detector)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
