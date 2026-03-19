/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.templates.python;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.TemplateType;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;

/** Template class for BigQuery Anomaly Detection in Python. */
@Template(
    name = "BigQuery_Anomaly_Detection",
    category = TemplateCategory.STREAMING,
    type = TemplateType.PYTHON,
    displayName = "BigQuery Anomaly Detection",
    description =
        "[Experimental] Real-time anomaly detection on BigQuery change data (CDC). "
            + "Reads streaming APPENDS/CHANGES data from a BigQuery table, "
            + "computes a configurable windowed metric, runs anomaly detection "
            + "(ZScore, IQR, or RobustZScore), and publishes anomalies to Pub/Sub.",
    preview = true,
    flexContainerName = "bigquery-anomaly-detection",
    filesToCopy = {"main.py", "setup.py", "pyproject.toml", "requirements.txt", "src"},
    contactInformation = "https://cloud.google.com/support",
    streaming = true)
public interface BigQueryAnomalyDetection {

  @TemplateParameter.Text(
      order = 1,
      name = "table",
      description = "BigQuery Table",
      helpText = "BigQuery table to monitor. Format: project:dataset.table",
      regexes = {"^[a-zA-Z0-9_-]+:[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$"})
  String getTable();

  @TemplateParameter.Text(
      order = 2,
      name = "metric_spec",
      description = "Metric Specification (JSON)",
      helpText =
          "JSON string defining the metric computation. "
              + "Example: {\"aggregation\":{\"window\":{\"type\":\"fixed\","
              + "\"size_seconds\":3600},\"measures\":[{\"field\":\"amount\",\"agg\":\"SUM\","
              + "\"alias\":\"total\"}]}}")
  String getMetricSpec();

  @TemplateParameter.Text(
      order = 3,
      name = "detector_spec",
      description = "Detector Specification (JSON)",
      helpText =
          "JSON string defining the anomaly detector. "
              + "Example: {\"type\":\"ZScore\"} or "
              + "{\"type\":\"ZScore\",\"config\":{\"threshold_criterion\":{\"type\":\"FixedThreshold\","
              + "\"config\":{\"cutoff\":10}}}}")
  String getDetectorSpec();

  @TemplateParameter.Integer(
      order = 5,
      optional = true,
      name = "poll_interval_sec",
      description = "Poll Interval (seconds)",
      helpText = "Seconds between BigQuery CDC polls. Default: 60.")
  Integer getPollIntervalSec();

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      name = "change_function",
      description = "Change Function",
      helpText = "BigQuery change function: APPENDS or CHANGES. Default: APPENDS.",
      regexes = {"^(APPENDS|CHANGES)$"})
  String getChangeFunction();

  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      name = "buffer_sec",
      description = "Buffer (seconds)",
      helpText = "Safety buffer behind now() in seconds. Default: 15.")
  Integer getBufferSec();

  @TemplateParameter.Integer(
      order = 8,
      optional = true,
      name = "start_offset_sec",
      description = "Start Offset (seconds)",
      helpText = "Start reading from this many seconds ago. Default: 60.")
  Integer getStartOffsetSec();

  @TemplateParameter.Integer(
      order = 9,
      optional = true,
      name = "duration_sec",
      description = "Duration (seconds)",
      helpText = "How long to run in seconds. 0 means run forever. Default: 0.")
  Integer getDurationSec();

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      name = "temp_dataset",
      description = "Temp Dataset",
      helpText = "BigQuery dataset for temp tables. If unset, auto-created.")
  String getTempDataset();

  @TemplateParameter.Text(
      order = 4,
      name = "topic",
      description = "Pub/Sub Topic",
      helpText = "Pub/Sub topic name for anomaly results.")
  String getTopic();

  @TemplateParameter.Boolean(
      order = 11,
      optional = true,
      name = "log_all_results",
      description = "Log All Results",
      helpText =
          "Log all anomaly detection results (normal, outlier, warmup) "
              + "at WARNING level. Default: false.")
  Boolean getLogAllResults();

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      name = "sink_table",
      description = "Sink BigQuery Table",
      helpText =
          "BigQuery table to write all anomaly detection results to. "
              + "Format: project:dataset.table. If unset, results are not written to BigQuery.",
      regexes = {"^[a-zA-Z0-9_-]+:[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$"})
  String getSinkTable();

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      name = "write_method",
      description = "BigQuery Write Method",
      helpText =
          "BigQuery write method for the sink table: "
              + "STORAGE_WRITE_API, DEFAULT, FILE_LOADS, or STREAMING_INSERTS. "
              + "Default: STORAGE_WRITE_API.",
      regexes = {"^(STORAGE_WRITE_API|DEFAULT|FILE_LOADS|STREAMING_INSERTS)$"})
  String getWriteMethod();
}
