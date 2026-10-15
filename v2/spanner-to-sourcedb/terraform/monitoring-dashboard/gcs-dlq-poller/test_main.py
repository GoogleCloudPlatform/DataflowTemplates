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

"""Unit tests for the GCS DLQ Poller Cloud Function."""

import json
import os
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

# Provide lightweight stubs if Cloud Function SDK packages are not installed in
# the local test runner environment.
if "functions_framework" not in sys.modules:
  ff_stub = SimpleNamespace(http=lambda fn: fn)
  sys.modules["functions_framework"] = ff_stub

try:
  from google.api import metric_pb2  # pylint: disable=unused-import
  from google.cloud import monitoring_v3  # pylint: disable=unused-import
  from google.cloud import storage  # pylint: disable=unused-import
except ImportError:
  google_mod = sys.modules.setdefault("google", SimpleNamespace())
  api_mod = getattr(google_mod, "api", SimpleNamespace())
  cloud_mod = getattr(google_mod, "cloud", SimpleNamespace())
  sys.modules["google.api"] = api_mod
  sys.modules["google.cloud"] = cloud_mod

  class _FakeTimeSeries:

    def __init__(self):
      self.metric = SimpleNamespace(type="", labels={})
      self.resource = SimpleNamespace(type="", labels={})
      self.metric_kind = None
      self.value_type = None
      self.points = []

  metric_pb2_stub = SimpleNamespace(
      MetricDescriptor=SimpleNamespace(
          MetricKind=SimpleNamespace(GAUGE=1),
          ValueType=SimpleNamespace(INT64=2),
      ),
  )
  monitoring_stub = SimpleNamespace(
      MetricServiceClient=mock.MagicMock,
      TimeInterval=lambda d: d,
      TimeSeries=_FakeTimeSeries,
      Point=lambda d: d,
  )
  storage_stub = SimpleNamespace(Client=mock.MagicMock)
  api_mod.metric_pb2 = metric_pb2_stub
  cloud_mod.monitoring_v3 = monitoring_stub
  cloud_mod.storage = storage_stub
  sys.modules["google.api.metric_pb2"] = metric_pb2_stub
  sys.modules["google.cloud.monitoring_v3"] = monitoring_stub
  sys.modules["google.cloud.storage"] = storage_stub

import main  # pylint: disable=g-import-not-at-top


class GcsDlqPollerTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    main._storage_client = None
    main._metric_client = None

  def test_parse_gcs_uri_valid(self):
    self.assertEqual(
        main._parse_gcs_uri("gs://my-bucket/dlq/"), ("my-bucket", "dlq")
    )
    self.assertEqual(
        main._parse_gcs_uri("  gs://my-bucket/nested/dlq/path// "),
        ("my-bucket", "nested/dlq/path"),
    )
    self.assertEqual(main._parse_gcs_uri("gs://my-bucket"), ("my-bucket", ""))

  def test_parse_gcs_uri_invalid_raises_value_error(self):
    for invalid_uri in ("", "   ", "my-bucket/dlq", "gs://", "gs:///dlq"):
      with self.subTest(invalid_uri=invalid_uri):
        with self.assertRaises(ValueError):
          main._parse_gcs_uri(invalid_uri)

  def test_parse_dlq_directories_supports_csv_json_array_and_list(self):
    self.assertEqual(
        main._parse_dlq_directories("gs://b1/dlq, gs://b2/dlq"),
        ["gs://b1/dlq", "gs://b2/dlq"],
    )
    self.assertEqual(
        main._parse_dlq_directories('["gs://b1/dlq", "gs://b2/dlq"]'),
        ["gs://b1/dlq", "gs://b2/dlq"],
    )
    self.assertEqual(
        main._parse_dlq_directories(["gs://b1/dlq", "gs://b2/dlq"]),
        ["gs://b1/dlq", "gs://b2/dlq"],
    )
    with self.assertRaises(ValueError):
      main._parse_dlq_directories('["gs://b1/dlq"')

  def test_count_blobs_filters_directories_and_temp_files(self):
    mock_storage = mock.MagicMock()
    mock_storage.list_blobs.return_value = [
        SimpleNamespace(name="dlq/severe/", size=0),
        SimpleNamespace(name="dlq/severe/2026/10/06/", size=0),
        SimpleNamespace(
            name="dlq/severe/2026/10/06/12/00/error-W-P-00000-of-00020.json",
            size=512,
        ),
        SimpleNamespace(
            name="dlq/severe/2026/10/06/12/00/.temp-beam-12345",
            size=128,
        ),
        SimpleNamespace(name="dlq/severe/tmp/.temp-1", size=64),
        SimpleNamespace(name="dlq/severe/tmp_severe/shard-0", size=64),
        SimpleNamespace(
            name="dlq/severe/2026/10/06/12/00/empty-placeholder.json",
            size=0,
        ),
        SimpleNamespace(
            name="dlq/severe/2026/10/06/12/01/error-W-P-00001-of-00020.json",
            size=256,
        ),
    ]

    count = main._count_blobs(mock_storage, "my-bucket", "dlq/severe/")
    self.assertEqual(count, 2)
    mock_storage.list_blobs.assert_called_once_with(
        "my-bucket",
        prefix="dlq/severe/",
        page_size=main.GCS_LIST_PAGE_SIZE,
        fields="items(name,size),nextPageToken",
        timeout=main.RPC_TIMEOUT_SECONDS,
    )

  def test_count_blobs_allows_tmp_in_base_dlq_prefix(self):
    mock_storage = mock.MagicMock()
    prefix = "migration/tmp/dlq/severe/"
    mock_storage.list_blobs.return_value = [
        SimpleNamespace(
            name="migration/tmp/dlq/severe/2026/10/06/12/00/err-0.json",
            size=100,
        ),
        SimpleNamespace(
            name="migration/tmp/dlq/severe/2026/10/06/12/00/.temp-beam-1",
            size=100,
        ),
        SimpleNamespace(
            name="migration/tmp/dlq/severe/tmp/.temp-2",
            size=100,
        ),
    ]

    count = main._count_blobs(mock_storage, "my-bucket", prefix)
    self.assertEqual(count, 1)

  def test_count_blobs_respects_max_count_limit(self):
    mock_storage = mock.MagicMock()
    # Create an iterator that would yield 100 files if not short-circuited.
    blobs_iter = (
        SimpleNamespace(name=f"dlq/severe/file-{i}.json") for i in range(100)
    )
    mock_storage.list_blobs.return_value = blobs_iter

    count = main._count_blobs(
        mock_storage, "my-bucket", "dlq/severe/", max_count_limit=5
    )
    self.assertEqual(count, 5)
    # Verify the generator was short-circuited right at the 5th item.
    self.assertEqual(next(blobs_iter).name, "dlq/severe/file-5.json")

  @mock.patch.object(main, "_get_metric_client")
  @mock.patch.object(main, "_get_storage_client")
  def test_poll_gcs_dlq_success_multi_shard_and_deduplicates_uris(
      self, mock_get_storage, mock_get_metric
  ):
    mock_storage = mock.MagicMock()
    mock_metric = mock.MagicMock()
    mock_get_storage.return_value = mock_storage
    mock_get_metric.return_value = mock_metric

    # Shard 1: 2 severe, 1 retry; Shard 2: 0 severe, 3 retry.
    def fake_list_blobs(bucket_name, prefix, **kwargs):
      del kwargs
      data = {
          ("shard1-bucket", "dlq/severe/"): [
              SimpleNamespace(name="dlq/severe/err1.json"),
              SimpleNamespace(name="dlq/severe/err2.json"),
          ],
          ("shard1-bucket", "dlq/retry/"): [
              SimpleNamespace(name="dlq/retry/ret1.json"),
          ],
          ("shard2-bucket", "dlq/severe/"): [],
          ("shard2-bucket", "dlq/retry/"): [
              SimpleNamespace(name="dlq/retry/ret1.json"),
              SimpleNamespace(name="dlq/retry/ret2.json"),
              SimpleNamespace(name="dlq/retry/ret3.json"),
          ],
      }
      return data.get((bucket_name, prefix), [])

    mock_storage.list_blobs.side_effect = fake_list_blobs

    request = mock.MagicMock()
    request.get_json.return_value = {
        "project_id": "test-project",
        "migration_id": "smt-test",
        "dlq_directories": (
            "gs://shard1-bucket/dlq/, gs://shard1-bucket/dlq,"
            " gs://shard2-bucket/dlq"
        ),
    }

    body_str, status_code, _ = main.poll_gcs_dlq(request)
    self.assertEqual(status_code, 200)
    body = json.loads(body_str)
    self.assertEqual(body["status"], "ok")
    self.assertEqual(body["totals"], {"severe": 2, "retry": 4})
    self.assertEqual(
        body["directories"],
        {
            "gs://shard1-bucket/dlq": {"severe": 2, "retry": 1},
            "gs://shard2-bucket/dlq": {"severe": 0, "retry": 3},
        },
    )

    mock_metric.create_time_series.assert_called_once()
    call_kwargs = mock_metric.create_time_series.call_args.kwargs
    self.assertEqual(call_kwargs["name"], "projects/test-project")
    self.assertEqual(call_kwargs["timeout"], main.RPC_TIMEOUT_SECONDS)
    self.assertEqual(len(call_kwargs["time_series"]), 4)
    for ts in call_kwargs["time_series"]:
      self.assertEqual(
          ts.metric_kind, main.metric_pb2.MetricDescriptor.MetricKind.GAUGE
      )
      self.assertEqual(
          ts.value_type, main.metric_pb2.MetricDescriptor.ValueType.INT64
      )
    labels_set = {
        (
            ts.metric.labels["migration_id"],
            ts.metric.labels["dlq_category"],
            ts.metric.labels["dlq_directory"],
        )
        for ts in call_kwargs["time_series"]
    }
    self.assertEqual(
        labels_set,
        {
            ("smt-test", "severe", "gs://shard1-bucket/dlq"),
            ("smt-test", "retry", "gs://shard1-bucket/dlq"),
            ("smt-test", "severe", "gs://shard2-bucket/dlq"),
            ("smt-test", "retry", "gs://shard2-bucket/dlq"),
        },
    )

  @mock.patch.object(main, "_get_metric_client")
  @mock.patch.object(main, "_get_storage_client")
  def test_poll_gcs_dlq_invalid_uri_fails_closed(
      self, mock_get_storage, mock_get_metric
  ):
    request = mock.MagicMock()
    request.get_json.return_value = {
        "project_id": "test-project",
        "migration_id": "smt-test",
        "dlq_directories": "gs://valid-bucket/dlq, gs://",
    }

    body_str, status_code, _ = main.poll_gcs_dlq(request)
    self.assertEqual(status_code, 400)
    body = json.loads(body_str)
    self.assertEqual(body["status"], "error")
    # Ensure neither GCS nor Cloud Monitoring was called when a URI is invalid.
    mock_get_storage.assert_not_called()
    mock_get_metric.assert_not_called()

  @mock.patch.object(main, "_get_metric_client")
  @mock.patch.object(main, "_get_storage_client")
  def test_poll_gcs_dlq_invalid_json_array_string_returns_400(
      self, mock_get_storage, mock_get_metric
  ):
    request = mock.MagicMock()
    request.get_json.return_value = {
        "project_id": "test-project",
        "migration_id": "smt-test",
        "dlq_directories": '["gs://b1/dlq"',
    }

    body_str, status_code, _ = main.poll_gcs_dlq(request)
    self.assertEqual(status_code, 400)
    body = json.loads(body_str)
    self.assertEqual(body["status"], "error")
    mock_get_storage.assert_not_called()
    mock_get_metric.assert_not_called()

  def test_parse_max_count_limit_rejects_invalid_values(self):
    for invalid_limit in (0, -5, "abc", True, False):
      with self.subTest(invalid_limit=invalid_limit):
        with self.assertRaises(ValueError):
          main._parse_max_count_limit(invalid_limit)

  def test_poll_gcs_dlq_non_dict_json_body_falls_back_gracefully(self):
    with mock.patch.dict(os.environ, {}, clear=True):
      request = mock.MagicMock()
      request.get_json.return_value = ["not", "a", "dict"]
      body_str, status_code, _ = main.poll_gcs_dlq(request)
      self.assertEqual(status_code, 400)
      body = json.loads(body_str)
      self.assertEqual(body["status"], "error")

  def test_poll_gcs_dlq_missing_configuration_returns_400(self):
    with mock.patch.dict(os.environ, {}, clear=True):
      request = mock.MagicMock()
      request.get_json.return_value = {}
      body_str, status_code, _ = main.poll_gcs_dlq(request)
      self.assertEqual(status_code, 400)
      body = json.loads(body_str)
      self.assertEqual(body["status"], "error")


if __name__ == "__main__":
  unittest.main()
