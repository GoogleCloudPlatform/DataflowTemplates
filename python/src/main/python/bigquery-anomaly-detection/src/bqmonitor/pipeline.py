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
metric, runs anomaly detection, and emits anomalies to Pub/Sub and/or a
REST webhook. Run as a Dataflow Flex Template or locally with DirectRunner.

Alerts are rate-limited by default with a 10-minute session-window gap;
tune via ``--alert_cooldown_seconds`` (0 disables). The optional BigQuery
sink table records every anomaly regardless.

See ``python main.py --help`` for the full set of options and their
JSON-schema details (``--metric_spec``, ``--detector_spec``,
``--webhook_spec``).
"""

import json
import logging
import re
import string
import time
from dataclasses import dataclass
from typing import Any
from typing import Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.async_dofn import AsyncWrapper
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.utils.timestamp import Duration
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery

from bqmonitor.metric import ComputeMetric
from bqmonitor.metric import FanoutStrategy
from bqmonitor.metric import MetricSpec
from bqmonitor.relative_change_detector import RelativeChangeConfig
from bqmonitor.relative_change_detector import RelativeChangeDoFn
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

_SUPPORTED_DETECTORS = ('ZScore', 'IQR', 'RobustZScore', 'RelativeChange')

_WEBHOOK_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)
_WEBHOOK_DEFAULT_METHOD = 'POST'
# Default timeout is 10 minutes
_WEBHOOK_DEFAULT_TIMEOUT_SEC = 600.0
# AsyncWrapper parallelism: how many in-flight requests one worker can have.
_WEBHOOK_DEFAULT_PARALLELISM = 5
# How often the AsyncWrapper timer fires to harvest finished futures.
_WEBHOOK_DEFAULT_CALLBACK_FREQUENCY_SEC = 30.0
_WEBHOOK_ALLOWED_METHODS = frozenset({'POST', 'PUT', 'PATCH'})
_WEBHOOK_KNOWN_KEYS = frozenset({
    'endpoint', 'body', 'method', 'headers', 'scopes', 'timeout_seconds',
    'parallelism', 'callback_frequency_seconds',
})

# 4xx codes that are still transient: server is telling us to back off
# or retry later. All 5xx codes are also treated as transient. Every
# other 4xx is treated as a permanent client-side problem (bad URL, bad
# auth, bad payload schema) and the anomaly is dropped rather than
# retried indefinitely.
_TRANSIENT_RETRY_STATUSES = frozenset({408, 425, 429})

# Synthetic key attached to unkeyed pipelines' elements so they can flow
# through stateful DoFns. _unpack_result translates it back to None.
_UNKEYED_SENTINEL = '__bqm_unkeyed__'

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
  if not (topic.startswith('projects/') and '/topics/' in topic):
    raise ValueError(
        f"--topic must be a full Pub/Sub resource path "
        f"(projects/<project>/topics/<topic>), got: '{topic}'")
  return topic


def _is_keyed_pair(element):
  return isinstance(element, tuple) and len(element) == 2


def _unpack_result(element):
  """Returns ``(key, result)``. Internal sentinel keys are mapped to None."""
  if _is_keyed_pair(element):
    key, value = element
    return (None if key == _UNKEYED_SENTINEL else key), value
  return None, element


def _parse_message_metadata(metadata_str):
  if not metadata_str:
    return None
  try:
    metadata = json.loads(metadata_str)
  except json.JSONDecodeError as e:
    raise ValueError(
        f'--message_metadata must be valid JSON: {e}') from e
  if not isinstance(metadata, dict):
    raise ValueError(
        '--message_metadata must be a JSON object (dict), '
        f'got {type(metadata).__name__}')
  return metadata


def _parse_table_ref(table):
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
  """Emits AnomalyResult(label=1) when ``expression`` is truthy, else label=0.

  ``value`` is bound to the metric value. The output matches the shape
  of the statistical detectors so downstream sinks are uniform.
  """

  def __init__(self, expression_text):
    self._expression_text = expression_text
    self._expr = None

  def setup(self):
    self._expr = Expr(self._expression_text)

  def process(self, element):
    if _is_keyed_pair(element):
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


# Fields available for --message_format templates.
_ANOMALY_FIELDS = frozenset({
    'value', 'score', 'label', 'threshold', 'model_id', 'info',
    'key', 'window_start', 'window_end',
})


def _validate_format_placeholders(format_str, known_fields, location=''):
  referenced = {
      fname for _, fname, _, _ in string.Formatter().parse(format_str)
      if fname is not None}
  unknown = referenced - known_fields
  if unknown:
    loc = f' at {location}' if location else ''
    raise ValueError(
        f'Template{loc} references unknown fields: {sorted(unknown)}. '
        f'Allowed fields: {sorted(known_fields)}.')


def _validate_message_format(format_str, metadata):
  _validate_format_placeholders(
      format_str, _ANOMALY_FIELDS | set(metadata or {}))


def _validate_template_tree(tree, known_fields, location=''):
  """Walks dicts/lists and validates every string leaf as a format string."""
  if isinstance(tree, dict):
    for k, v in tree.items():
      _validate_template_tree(v, known_fields, location=f'{location}.{k}')
  elif isinstance(tree, list):
    for i, v in enumerate(tree):
      _validate_template_tree(v, known_fields, location=f'{location}[{i}]')
  elif isinstance(tree, str):
    _validate_format_placeholders(tree, known_fields, location=location)


def _substitute_template_tree(tree, fields):
  """Returns a new tree with every string leaf format-substituted."""
  if isinstance(tree, dict):
    return {k: _substitute_template_tree(v, fields) for k, v in tree.items()}
  if isinstance(tree, list):
    return [_substitute_template_tree(v, fields) for v in tree]
  if isinstance(tree, str):
    return tree.format(**fields)
  return tree


def _build_anomaly_fields(key, result):
  """Returns the ``_ANOMALY_FIELDS``-keyed dict used for template substitution.

  None-valued prediction fields are coerced to ``''`` or ``'null'`` for
  safe interpolation into user-supplied format strings.
  """
  prediction = result.predictions[0]
  example = result.example

  return {
      'value': example.value,
      'score': prediction.score if prediction.score is not None else 'null',
      'label': prediction.label,
      'threshold': (prediction.threshold
                    if prediction.threshold is not None else 'null'),
      'model_id': prediction.model_id or '',
      'info': prediction.info or '',
      'key': str(key) if key is not None else '',
      'window_start': example.window_start.to_rfc3339(),
      'window_end': example.window_end.to_rfc3339(),
  }


def _compute_anomaly_message(fields, message_format, message_metadata):
  if message_format is None:
    return (
        f'Anomaly detected value={fields["value"]}'
        f' score={fields["score"]}'
        f' in window={fields["window_start"]}-{fields["window_end"]}')
  # Anomaly fields win on key collision with metadata.
  merged = {**(message_metadata or {}), **fields}
  return message_format.format(**merged)


class _FormatAnomalyAsJson(beam.DoFn):
  """Renders an anomaly for Pub/Sub as UTF-8 bytes.

  If ``message_format`` is set, emits ``message_format.format(...)``.
  Otherwise emits a default JSON envelope. Inputs are assumed already
  filtered to outliers by the upstream pipeline.
  """

  def __init__(self, message_format=None, message_metadata=None):
    self._message_format = message_format
    self._message_metadata = message_metadata or {}

  def process(self, element):
    key, result = _unpack_result(element)
    fields = _build_anomaly_fields(key, result)
    message = _compute_anomaly_message(
        fields, self._message_format, self._message_metadata)

    if self._message_format is not None:
      yield message.encode('utf-8')
      return

    payload = {
        'event_description': message,
        'agent_id': fields['model_id'],
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
        {'name': 'info', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'key', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


def _anomaly_id(value):
  """Stable hashable identity per anomaly. Used as AsyncWrapper's id_fn
  for dedup across runner replays — must survive a serialize/deserialize
  round-trip, hence (window_micros, model_id) rather than object identity."""
  example = value.example
  prediction = value.predictions[0]
  return (
      example.window_start.micros,
      example.window_end.micros,
      prediction.model_id or '',
  )


def _key_anomaly_for_async(element):
  """Ensure (str, V) shape. Keys are stringified because AsyncWrapper's
  ``next_time_to_fire`` calls ``random.seed(key)`` which rejects tuples
  (upstream keyed pipelines emit tuple keys like ``('campaign_a',)``).
  Unkeyed pipelines get the sentinel."""
  if _is_keyed_pair(element):
    key, value = element
    return (str(key), value)
  return (_UNKEYED_SENTINEL, element)


def _is_outlier(element):
  result = element[1] if _is_keyed_pair(element) else element
  return result.predictions[0].label == 1


class _RateLimitAlerts(beam.DoFn):
  """Per-key session-window debounce for alerts. ``window_end`` is "now".

  Fires iff first event for the key OR gap from the prior event ≥ cooldown.
  ``last_event_micros`` advances on every event (including suppressed
  ones), so continuous outliers keep the session open — the classic
  session-window invariant. ``cooldown_seconds <= 0`` makes the DoFn a
  pass-through so the cooldown can be toggled via Dataflow ``--update``
  without changing the pipeline graph.

  Example (cooldown=10 min)::

      T0  → fire    (no prior)            last_event := T0
      T1  → suppress (T1-T0  =  1 < 10)   last_event := T1
      T8  → suppress (T8-T0  =  8 < 10)   last_event := T8
      T19 → fire    (T19-T8 = 11 ≥ 10)    last_event := T19
  """

  LAST_EVENT_MICROS = ReadModifyWriteStateSpec(
      'last_event_micros', coders.VarIntCoder())

  def __init__(self, cooldown_seconds):
    self._cooldown_seconds = cooldown_seconds
    self._cooldown_micros = int(cooldown_seconds * 1_000_000)

  def process(self, element,
              last_event=beam.DoFn.StateParam(LAST_EVENT_MICROS)):
    _, result = element

    if self._cooldown_micros <= 0:
      yield element
      return

    # Defense-in-depth: the upstream filter should have dropped non-outliers,
    # but if it ever goes missing they must not poison the session state.
    if result.predictions[0].label != 1:
      yield element
      return

    now_micros = result.example.window_end.micros
    last = last_event.read()

    fires = last is None or (now_micros - last) >= self._cooldown_micros
    last_event.write(now_micros)

    if fires:
      yield element
      return

    window_str = (
        f"{result.example.window_start.to_rfc3339()}/"
        f"{result.example.window_end.to_rfc3339()}")
    gap_sec = (now_micros - last) // 1_000_000
    _LOGGER.info(
        'Alert suppressed for window=%s: still active '
        '(prior anomaly %ds ago, cooldown=%.0fs); '
        'not notifying external systems.',
        window_str, gap_sec, self._cooldown_seconds)


class _PostAnomalyToWebhook(beam.DoFn):
  """POSTs each anomaly to ``webhook_spec.endpoint`` using ADC.

  String leaves in ``body`` and ``headers`` are format-substituted with
  ``anomaly fields | message_metadata | {anomaly_message}``.

  Response handling (because streaming Dataflow retries bundles
  indefinitely, we cannot blindly raise on every non-2xx — that would
  block the pipeline on a misconfigured request):
    * 2xx → success.
    * 5xx, 408, 425, 429 → raise (transient; bundle retries).
    * Other 4xx → log + drop + increment ``dropped_permanent_4xx`` metric.
  Network errors propagate as-is so Beam's bundle retry handles them.
  """

  _DROPPED_4XX_COUNTER = Metrics.counter(
      'bqmonitor.webhook', 'dropped_permanent_4xx')

  def __init__(self, webhook_spec, message_format, message_metadata):
    self._webhook_spec = webhook_spec
    self._message_format = message_format
    self._message_metadata = message_metadata or {}
    self._session = None

  def setup(self):
    creds, _ = google.auth.default(scopes=self._webhook_spec['scopes'])
    self._session = AuthorizedSession(creds)

  def process(self, element):
    key, result = _unpack_result(element)
    fields = _build_anomaly_fields(key, result)
    anomaly_message = _compute_anomaly_message(
        fields, self._message_format, self._message_metadata)

    merged = dict(self._message_metadata)
    merged.update(fields)
    merged['anomaly_message'] = anomaly_message

    body = _substitute_template_tree(self._webhook_spec['body'], merged)
    headers = _substitute_template_tree(self._webhook_spec['headers'], merged)

    window_str = f"{fields['window_start']}/{fields['window_end']}"
    model_id = fields['model_id'] or '<none>'

    start_monotonic = time.monotonic()
    resp = self._session.request(
        method=self._webhook_spec['method'],
        url=self._webhook_spec['endpoint'],
        json=body,
        headers=headers or None,
        timeout=self._webhook_spec['timeout_seconds'])
    elapsed_sec = time.monotonic() - start_monotonic

    status = resp.status_code
    if 200 <= status < 300:
      _LOGGER.info(
          'Webhook %s %s posted anomaly window=%s model_id=%s '
          'in %.2fs (status=%d).',
          self._webhook_spec['method'], self._webhook_spec['endpoint'],
          window_str, model_id, elapsed_sec, status)
      return

    if status >= 500 or status in _TRANSIENT_RETRY_STATUSES:
      _LOGGER.warning(
          'Webhook %s %s returned transient status %d after %.2fs for '
          'anomaly window=%s model_id=%s; bundle will retry. '
          'Response: %s',
          self._webhook_spec['method'], self._webhook_spec['endpoint'],
          status, elapsed_sec, window_str, model_id, resp.text[:500])
      raise RuntimeError(
          f'Webhook returned transient status {status}; retrying bundle.')

    _LOGGER.error(
        'Webhook %s %s returned permanent status %d after %.2fs for '
        'anomaly window=%s model_id=%s; dropping anomaly. '
        'Response: %s',
        self._webhook_spec['method'], self._webhook_spec['endpoint'],
        status, elapsed_sec, window_str, model_id, resp.text[:500])
    self._DROPPED_4XX_COUNTER.inc()


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
        'info': prediction.info if prediction.info else None,
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
        'Full path: projects/<project>/topics/<topic>. '
        'Optional: at least one of --topic or --webhook_spec must be set.')
    parser.add_argument(
        '--webhook_spec',
        default=None,
        help='JSON object configuring a REST webhook for anomaly results. '
        'Required keys: endpoint (http/https URL), body (JSON object/array). '
        'Optional keys: method (POST/PUT/PATCH, default POST), headers (object), '
        'scopes (list of OAuth scopes; default cloud-platform), '
        'timeout_seconds (default 600, i.e. 10 min), parallelism (max '
        'concurrent in-flight '
        'POSTs per worker; default 5), callback_frequency_seconds (how often '
        'the async wrapper sweeps finished futures; default 30). '
        'String leaves in body and headers are Python-format-substituted '
        'against anomaly fields, --message_metadata keys, and the synthetic '
        '{anomaly_message} field (which equals --message_format output, or a '
        'default natural-language summary). At least one of --topic or '
        '--webhook_spec must be set. '
        'Example: '
        '\'{"endpoint":'
        '"https://geminidataanalytics.googleapis.com/v1alpha/projects/MY_PROJECT/'
        'locations/global:chat",'
        '"body":{"messages":[{"userMessage":{"text":"{anomaly_message}",'
        '"runtime_params":{"workflow_params":{'
        '"workflow_agent":"projects/MY_PROJECT/locations/global/dataAgents/AGENT_ID",'
        '"action":"EXECUTE"}}}}],'
        '"dataAgentContext":{"dataAgent":'
        '"projects/MY_PROJECT/locations/global/dataAgents/AGENT_ID"}}}\'')
    parser.add_argument(
        '--log_all_results',
        default='false',
        help='Log all anomaly detection results (normal, outlier, warmup) '
        'at WARNING level. Default: false.')
    parser.add_argument(
        '--message_format',
        default=None,
        help='Python format string for Pub/Sub anomaly messages. '
        'Available fields: {value}, {score}, {label}, {threshold}, '
        '{model_id}, {info}, {key}, {window_start}, {window_end}, '
        'plus any keys from --message_metadata. '
        'If unset, a default JSON payload is used. '
        'Example: \'{"alert": "{key}: {value}", "job_id": "{job_id}"}\'')
    parser.add_argument(
        '--message_metadata',
        default=None,
        help='JSON object of static key-value pairs available as '
        'additional fields in --message_format. '
        'Example: \'{"job_id": "pipeline-123", "env": "prod"}\'.'
        ' Anomaly fields take precedence on key collision.')
    parser.add_argument(
        '--sink_table',
        default=None,
        help='BigQuery table to write all anomaly detection results to. '
        'Format: project:dataset.table. If unset, results are not written '
        'to BigQuery.')
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
    parser.add_argument(
        '--alert_cooldown_seconds',
        type=float,
        default=600.0,
        help='Session-window gap (in seconds) for debouncing alerts to '
        'external systems (Pub/Sub, webhook). Per anomaly key, the first '
        'anomaly fires immediately; subsequent anomalies are suppressed '
        '(logged INFO as "still active") until a gap of at least this '
        'many seconds passes between consecutive anomalies. Continuous '
        'anomalies extend the active-alert window. The BigQuery sink '
        'table is unaffected and records every anomaly. Set to 0 to '
        'disable rate limiting. Default: 600 (10 minutes).')


# ---------------------------------------------------------------------------
# Spec parsing
# ---------------------------------------------------------------------------


def _parse_metric_spec(json_str):
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
  """Recursively turns nested ``{"type": ..., "config": ...}`` dicts into
  ``Spec`` objects. ``Specifiable.from_spec`` requires ``Spec`` for nested
  specifiables (e.g. ``threshold_criterion``)."""
  if isinstance(d, dict) and 'type' in d:
    config = d.get('config', {})
    if config:
      config = {k: _dict_to_spec(v) for k, v in config.items()}
    return Spec(type=d['type'], config=config)
  if isinstance(d, list):
    return [_dict_to_spec(item) for item in d]
  return d


def _expand_window_size(d):
  """Expands the ``window_size`` shorthand into the detector-specific
  tracker configs each ``Specifiable`` detector expects. User-supplied
  tracker configs win (``setdefault`` semantics)."""
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
  """Parses ``--detector_spec`` JSON into a detector instance.

  Returns an ``AnomalyDetector`` for statistical types, a
  ``RelativeChangeConfig`` for RelativeChange, or a ``_ThresholdAlert``
  DoFn for Threshold. Raises ValueError on malformed input.
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

  if detector_type == 'RelativeChange':
    config = d.get('config', {})
    direction = d.get('direction', config.get('direction'))
    if direction is None:
      raise ValueError(
          "RelativeChange detector requires 'direction' "
          "(one of: increase, decrease, both).")
    lookback_windows = d.get('lookback_windows',
                             config.get('lookback_windows'))
    if lookback_windows is None:
      raise ValueError(
          "RelativeChange detector requires 'lookback_windows' "
          "(number of prior windows to compare against).")
    threshold_pct = d.get('threshold_pct',
                          config.get('threshold_pct'))
    absolute_threshold = d.get('absolute_threshold',
                               config.get('absolute_threshold'))
    if threshold_pct is None and absolute_threshold is None:
      raise ValueError(
          "RelativeChange detector requires at least one of "
          "'threshold_pct' or 'absolute_threshold'.")
    return RelativeChangeConfig(
        direction=direction,
        lookback_windows=lookback_windows,
        threshold_pct=threshold_pct,
        absolute_threshold=absolute_threshold,
    )

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
# Webhook spec parsing
# ---------------------------------------------------------------------------

def _parse_webhook_spec(json_str, message_metadata):
  """Parses ``--webhook_spec`` JSON and returns a dict with defaults applied.

  See the ``--webhook_spec`` argparse help for the schema.
  """
  try:
    spec = json.loads(json_str)
  except json.JSONDecodeError as e:
    raise ValueError(
        f"Invalid JSON in --webhook_spec: {e}. "
        f"Value: {json_str[:200]}") from e

  if not isinstance(spec, dict):
    raise ValueError(
        f"--webhook_spec must be a JSON object, "
        f"got {type(spec).__name__}")

  unknown = set(spec) - _WEBHOOK_KNOWN_KEYS
  if unknown:
    raise ValueError(
        f"--webhook_spec contains unknown keys: {sorted(unknown)}. "
        f"Allowed keys: {sorted(_WEBHOOK_KNOWN_KEYS)}")

  if 'endpoint' not in spec:
    raise ValueError("--webhook_spec is missing required 'endpoint' field")
  endpoint = spec['endpoint']
  if not isinstance(endpoint, str) or not (
      endpoint.startswith('http://') or endpoint.startswith('https://')):
    raise ValueError(
        f"--webhook_spec.endpoint must be an http(s) URL, got: {endpoint!r}")

  if 'body' not in spec:
    raise ValueError("--webhook_spec is missing required 'body' field")
  body = spec['body']
  if not isinstance(body, (dict, list)):
    raise ValueError(
        f"--webhook_spec.body must be a JSON object or array, "
        f"got {type(body).__name__}")

  method = str(spec.get('method', _WEBHOOK_DEFAULT_METHOD)).upper()
  if method not in _WEBHOOK_ALLOWED_METHODS:
    raise ValueError(
        f"--webhook_spec.method must be one of "
        f"{sorted(_WEBHOOK_ALLOWED_METHODS)}, got {method!r}")

  headers = spec.get('headers', {})
  if not isinstance(headers, dict):
    raise ValueError(
        f"--webhook_spec.headers must be a JSON object, "
        f"got {type(headers).__name__}")
  for k, v in headers.items():
    if not isinstance(k, str) or not isinstance(v, str):
      raise ValueError(
          f"--webhook_spec.headers must map string to string, "
          f"got {k!r}: {v!r}")

  scopes = spec.get('scopes', list(_WEBHOOK_DEFAULT_SCOPES))
  if (not isinstance(scopes, list)
      or not scopes
      or not all(isinstance(s, str) and s for s in scopes)):
    raise ValueError(
        f"--webhook_spec.scopes must be a non-empty list of strings, "
        f"got {scopes!r}")

  timeout_seconds = spec.get('timeout_seconds', _WEBHOOK_DEFAULT_TIMEOUT_SEC)
  if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
    raise ValueError(
        f"--webhook_spec.timeout_seconds must be a positive number, "
        f"got {timeout_seconds!r}")

  parallelism = spec.get('parallelism', _WEBHOOK_DEFAULT_PARALLELISM)
  if (not isinstance(parallelism, int) or isinstance(parallelism, bool)
      or parallelism <= 0):
    raise ValueError(
        f"--webhook_spec.parallelism must be a positive integer, "
        f"got {parallelism!r}")

  callback_frequency_seconds = spec.get(
      'callback_frequency_seconds', _WEBHOOK_DEFAULT_CALLBACK_FREQUENCY_SEC)
  if (not isinstance(callback_frequency_seconds, (int, float))
      or isinstance(callback_frequency_seconds, bool)
      or callback_frequency_seconds <= 0):
    raise ValueError(
        f"--webhook_spec.callback_frequency_seconds must be a positive "
        f"number, got {callback_frequency_seconds!r}")

  known_fields = (
      _ANOMALY_FIELDS | set(message_metadata or {}) | {'anomaly_message'})
  _validate_template_tree(body, known_fields, location='body')
  _validate_template_tree(headers, known_fields, location='headers')

  return {
      'endpoint': endpoint,
      'body': body,
      'method': method,
      'headers': headers,
      'scopes': list(scopes),
      'timeout_seconds': float(timeout_seconds),
      'parallelism': parallelism,
      'callback_frequency_seconds': float(callback_frequency_seconds),
  }


# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------


def _preflight_checks(options, metric_spec):
  """Fails fast on misconfigured GCP resources before pipeline construction."""
  project, dataset, table_name = _parse_table_ref(options.table)

  required_columns = sorted(metric_spec.required_source_columns())
  _check_bq_source_table(project, dataset, table_name, options,
                         required_columns)
  _check_bq_temp_dataset(project, options)

  if options.topic:
    topic_path = _validate_topic_path(options.topic)
    _check_pubsub_topic(topic_path)

  if options.webhook_spec:
    _check_webhook_spec(options)


def _check_bq_source_table(project, dataset, table_name, options,
                           required_columns):
  """Verifies source table access + required columns via a dry-run CDC query."""

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


def _check_webhook_spec(options):
  """Validates the webhook spec parses and ADC is obtainable for its scopes."""
  message_metadata = _parse_message_metadata(options.message_metadata)
  spec = _parse_webhook_spec(options.webhook_spec, message_metadata)

  try:
    creds, _ = google.auth.default(scopes=spec['scopes'])
    _LOGGER.info(
        '[Preflight] Webhook auth: obtained credentials of type %s '
        'with scopes %s for endpoint %s',
        type(creds).__name__, spec['scopes'], spec['endpoint'])
  except Exception as e:
    raise ValueError(
        f"Cannot obtain Google default credentials for webhook with "
        f"scopes {spec['scopes']}. Verify the service account is "
        f"configured and the scopes are valid. Error: {e}") from e


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


def _apply_detector(global_metrics, detector, has_group_by, window_duration):
  """Runs anomaly detection on the windowed metric stream.

  Stateful detectors (ZScore/IQR/RobustZScore/RelativeChange) are
  wrapped in an ``OffsetKey(key, offset_micros)`` so each sliding-window
  offset gets independent state, then unwrapped afterward.
  ``_ThresholdAlert`` is stateless and bypasses that machinery.
  """
  if isinstance(detector, _ThresholdAlert):
    return global_metrics | 'DetectAnomalies' >> beam.ParDo(detector)

  def _add_offset_key(element, _wd=window_duration, _keyed=has_group_by):
    if _keyed:
      key, row = element
    else:
      key, row = None, element
    offset = (row.window_start % _wd).micros
    return (OffsetKey(key=key, offset=offset), row)

  keyed = global_metrics | 'AddOffsetKey' >> beam.Map(_add_offset_key)

  if isinstance(detector, RelativeChangeConfig):
    anomalies = keyed | 'DetectAnomalies' >> beam.ParDo(
        RelativeChangeDoFn(
            direction=detector.direction,
            threshold_pct=detector.threshold_pct,
            absolute_threshold=detector.absolute_threshold,
            lookback_windows=detector.lookback_windows))
  else:
    # AnomalyDetection requires a typed (K, V) input.
    typed = keyed | 'TypeHintMetrics' >> beam.Map(
        lambda x: x).with_output_types(beam.typehints.Tuple[Any, beam.Row])
    anomalies = typed | 'DetectAnomalies' >> AnomalyDetection(detector)

  def _strip_offset_key(element, _keyed=has_group_by):
    offset_key, result = element
    if _keyed:
      return (offset_key.key, result)
    return result

  return anomalies | 'StripOffsetKey' >> beam.Map(_strip_offset_key)


def build_pipeline(pipeline, options, metric_spec, detector):
  """Constructs the anomaly monitoring pipeline. Returns the anomalies
  PCollection (exposed for testing)."""
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

  # Rename CDC pseudo-columns away if they clash with user column names.
  change_type_col = 'change_type'
  change_ts_col = 'change_timestamp'
  col_set = set(columns)
  if change_type_col in col_set:
    change_type_col = '_bqm_change_type'
    _LOGGER.info(
        '  Renamed pseudo-column change_type -> %s', change_type_col)
  if change_ts_col in col_set:
    change_ts_col = '_bqm_change_timestamp'
    _LOGGER.info(
        '  Renamed pseudo-column change_timestamp -> %s', change_ts_col)

  cdc_kwargs = dict(
      table=options.table,
      poll_interval_sec=options.poll_interval_sec,
      start_time=start_time,
      change_function=options.change_function,
      buffer_sec=options.buffer_sec,
      columns=columns,
      change_type_column=change_type_col,
      change_timestamp_column=change_ts_col)
  if stop_time is not None:
    cdc_kwargs['stop_time'] = stop_time
  if options.temp_dataset:
    cdc_kwargs['temp_dataset'] = options.temp_dataset

  rows = pipeline | 'ReadCDC' >> ReadBigQueryChangeHistory(**cdc_kwargs)
  fanout_strategy = FanoutStrategy(options.fanout_strategy)
  metrics = rows | 'ComputeMetric' >> ComputeMetric(
      metric_spec, fanout_strategy=fanout_strategy, fanout=options.fanout)

  # GlobalWindows so the detector sees the full stream as a time series.
  from apache_beam.transforms.window import GlobalWindows
  global_metrics = metrics | 'Rewindow' >> beam.WindowInto(
      GlobalWindows())

  window_duration = Duration(metric_spec.aggregation.window.size_seconds)
  has_group_by = bool(metric_spec.aggregation.group_by)
  anomalies = _apply_detector(
      global_metrics, detector, has_group_by, window_duration)

  if options.log_all_results.lower() == 'true':
    _ = anomalies | 'LogResults' >> beam.ParDo(_LogAnomalyResult())

  message_metadata = _parse_message_metadata(options.message_metadata)
  message_format = options.message_format
  if message_format is not None:
    _validate_message_format(message_format, message_metadata)

  # Alert path: filter to outliers, then key (so the keyed reshuffle
  # sees only outliers, not the full detection stream), then rate-limit.
  # The BQ sink branch below consumes the un-rate-limited `anomalies` so
  # the analytical record retains every detection.
  alert_anomalies = (
      anomalies
      | 'FilterOutliers' >> beam.Filter(_is_outlier)
      | 'KeyForAlerts' >> beam.Map(_key_anomaly_for_async)
      | 'RateLimitAlerts' >> beam.ParDo(
          _RateLimitAlerts(options.alert_cooldown_seconds)))

  if options.topic:
    topic_path = _validate_topic_path(options.topic)
    _ = (
        alert_anomalies
        | 'FormatAnomalies' >> beam.ParDo(
            _FormatAnomalyAsJson(
                message_format=message_format,
                message_metadata=message_metadata))
        | 'WriteToPubSub' >> WriteToPubSub(topic=topic_path))

  if options.webhook_spec:
    webhook_spec = _parse_webhook_spec(
        options.webhook_spec, message_metadata)
    sync_webhook_dofn = _PostAnomalyToWebhook(
        webhook_spec=webhook_spec,
        message_format=message_format,
        message_metadata=message_metadata)
    async_webhook_dofn = AsyncWrapper(
        sync_webhook_dofn,
        parallelism=webhook_spec['parallelism'],
        callback_frequency=webhook_spec['callback_frequency_seconds'],
        id_fn=_anomaly_id,
    )
    _ = (
        alert_anomalies
        | 'PostAnomaliesToWebhook' >> beam.ParDo(async_webhook_dofn))

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
  options = PipelineOptions(argv)
  monitor_options = options.view_as(AnomalyMonitorOptions)

  for required_opt in ('table', 'metric_spec', 'detector_spec'):
    if getattr(monitor_options, required_opt) is None:
      raise ValueError(f'--{required_opt} is required')

  if monitor_options.topic is None and monitor_options.webhook_spec is None:
    raise ValueError(
        'At least one of --topic or --webhook_spec must be set; '
        'otherwise detected anomalies have nowhere to go.')

  _parse_table_ref(monitor_options.table)
  metric_spec = _parse_metric_spec(monitor_options.metric_spec)
  detector = _parse_detector_spec(monitor_options.detector_spec)
  _preflight_checks(monitor_options, metric_spec)

  options.view_as(SetupOptions).save_main_session = True

  from apache_beam.options.pipeline_options import StandardOptions
  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:
    build_pipeline(p, monitor_options, metric_spec, detector)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
