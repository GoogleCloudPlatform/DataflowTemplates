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
            + "(ZScore, IQR, or RobustZScore), and emits anomalies to Pub/Sub "
            + "and/or a REST webhook. "
            + "Alerts to Pub/Sub and the REST webhook are rate-limited by "
            + "default: per anomaly key, after the first alert fires further "
            + "anomalies are suppressed until a 10-minute gap between "
            + "consecutive anomalies elapses. Tune or disable via "
            + "alert_cooldown_seconds (set to 0 to disable). The BigQuery "
            + "sink table is unaffected and records every anomaly.",
    preview = true,
    flexContainerName = "bigquery-anomaly-detection",
    filesToCopy = {"main.py", "setup.py", "pyproject.toml", "requirements_all.txt", "src"},
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
              + "Statistical: {\"type\":\"ZScore\"}, {\"type\":\"IQR\"}, {\"type\":\"RobustZScore\"}. "
              + "Threshold: {\"type\":\"Threshold\",\"expression\":\"value >= 100\"}. "
              + "RelativeChange: {\"type\":\"RelativeChange\",\"direction\":\"decrease\","
              + "\"threshold_pct\":20,\"lookback_windows\":1}.")
  String getDetectorSpec();

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      name = "topic",
      description = "Pub/Sub Topic",
      helpText =
          "Pub/Sub topic for anomaly results. "
              + "Full path: projects/<project>/topics/<topic>. "
              + "Optional: at least one of topic or webhook_spec must be set.",
      regexes = {"^projects/[^/]+/topics/[^/]+$"})
  String getTopic();

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
      name = "fanout_strategy",
      description = "Fanout Strategy",
      helpText =
          "Parallelism strategy for metric aggregation: "
              + "sharded, hotkey_fanout, precombine, or none. Default: sharded.",
      regexes = {"^(sharded|hotkey_fanout|precombine|none)$"})
  String getFanoutStrategy();

  @TemplateParameter.Integer(
      order = 14,
      optional = true,
      name = "fanout",
      description = "Fanout Shards",
      helpText =
          "Number of shards for sharded or hotkey_fanout strategies. "
              + "Ignored for none and precombine. Default: 400.")
  Integer getFanout();

  @TemplateParameter.Text(
      order = 15,
      optional = true,
      name = "message_format",
      description = "Pub/Sub Message Format",
      helpText =
          "Python format string for Pub/Sub anomaly messages. "
              + "Available fields: {value}, {score}, {label}, {threshold}, "
              + "{model_id}, {info}, {key}, {window_start}, {window_end}, "
              + "plus any keys from message_metadata. "
              + "If unset, a default JSON payload is used.")
  String getMessageFormat();

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      name = "message_metadata",
      description = "Pub/Sub Message Metadata",
      helpText =
          "JSON object of static key-value pairs available as additional "
              + "fields in message_format. "
              + "Example: {\"job_id\": \"pipeline-123\", \"env\": \"prod\"}. "
              + "Anomaly fields take precedence on key collision.")
  String getMessageMetadata();

  @TemplateParameter.Text(
      order = 17,
      optional = true,
      name = "webhook_spec",
      description = "REST Webhook Specification (JSON)",
      helpText =
          "JSON object configuring a REST webhook for anomaly results. "
              + "Required keys: endpoint (http/https URL), body (JSON object/array). "
              + "Optional keys: method (POST/PUT/PATCH, default POST), "
              + "headers (object), scopes (list of OAuth scopes; default "
              + "cloud-platform), timeout_seconds (default 600, i.e. 10 min), "
              + "parallelism (max concurrent in-flight POSTs per worker, "
              + "default 5), callback_frequency_seconds (how often the "
              + "AsyncWrapper sweeps finished futures, default 30). "
              + "String leaves in body and headers are Python-format-substituted "
              + "against anomaly fields, message_metadata keys, and the "
              + "{anomaly_message} field (which equals message_format output, "
              + "or a default natural-language summary). "
              + "At least one of topic or webhook_spec must be set.")
  String getWebhookSpec();

  @TemplateParameter.Double(
      order = 18,
      optional = true,
      name = "alert_cooldown_seconds",
      description = "Alert Cooldown (seconds)",
      helpText =
          "Session-window gap for debouncing alerts to external systems "
              + "(Pub/Sub, webhook). Per anomaly key, the first anomaly fires "
              + "immediately; subsequent anomalies are suppressed (logged as "
              + "\"still active\") until a gap of at least this many seconds "
              + "passes between consecutive anomalies. Continuous anomalies "
              + "extend the active-alert window. The BigQuery sink table is "
              + "unaffected and records every anomaly. Set to 0 to disable "
              + "rate limiting. Default: 600 (10 minutes).")
  Double getAlertCooldownSeconds();
}
