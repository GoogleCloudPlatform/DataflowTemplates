/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.clickhouse.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link PubSubToClickHouseOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface PubSubToClickHouseOptions extends PipelineOptions {

  // -------------------------------------------------------------------------
  // Source
  // -------------------------------------------------------------------------

  @TemplateParameter.PubsubSubscription(
      order = 1,
      groupName = "Source",
      description = "Pub/Sub input subscription",
      helpText = "Pub/Sub subscription to read messages from.",
      example = "projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>")
  @Validation.Required
  String getInputSubscription();

  void setInputSubscription(String inputSubscription);

  // -------------------------------------------------------------------------
  // Target
  // -------------------------------------------------------------------------

  @TemplateParameter.Text(
      order = 2,
      groupName = "Target",
      description = "ClickHouse endpoint URL",
      helpText =
          "The ClickHouse endpoint URL. Use https:// for SSL connections (ClickHouse Cloud) "
              + "or http:// for non-SSL connections.",
      example = "https://<HOST>:8443 or http://<HOST>:8123")
  @Validation.Required
  String getClickHouseUrl();

  void setClickHouseUrl(String clickHouseUrl);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Target",
      description = "ClickHouse database name",
      helpText = "The name of the ClickHouse database where the target table resides.",
      example = "default")
  @Validation.Required
  String getClickHouseDatabase();

  void setClickHouseDatabase(String clickHouseDatabase);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Target",
      description = "ClickHouse target table name",
      helpText =
          "The name of the ClickHouse table to write data into. "
              + "The table must exist before running the pipeline.",
      example = "my_table")
  @Validation.Required
  String getClickHouseTable();

  void setClickHouseTable(String clickHouseTable);

  @TemplateParameter.Text(
      order = 5,
      groupName = "Target",
      description = "ClickHouse username",
      helpText = "The username to use for authenticating with ClickHouse.",
      example = "default")
  @Validation.Required
  String getClickHouseUsername();

  void setClickHouseUsername(String clickHouseUsername);

  @TemplateParameter.Password(
      order = 6,
      groupName = "Target",
      description = "ClickHouse password",
      helpText = "The password to use for authenticating with ClickHouse.")
  @Validation.Required
  String getClickHousePassword();

  void setClickHousePassword(String clickHousePassword);

  // -------------------------------------------------------------------------
  // Dead-letter routing
  // -------------------------------------------------------------------------

  @TemplateParameter.Text(
      order = 7,
      groupName = "Dead-letter",
      optional = true,
      description = "ClickHouse dead-letter table name",
      helpText =
          "The ClickHouse table to write failed messages into. "
              + "If set, failed messages are written to this table. "
              + "Can be combined with --deadLetterTopic to write to both destinations simultaneously. "
              + "At least one of --clickHouseDeadLetterTable or --deadLetterTopic must be provided. "
              + "The table must exist in ClickHouse with the following schema: "
              + "(raw_message String, error_message String, stack_trace String, failed_at DateTime).",
      example = "my_table_dead_letter")
  String getClickHouseDeadLetterTable();

  void setClickHouseDeadLetterTable(String clickHouseDeadLetterTable);

  @TemplateParameter.PubsubTopic(
      order = 8,
      groupName = "Dead-letter",
      optional = true,
      description = "Dead-letter Pub/Sub topic",
      helpText =
          "The Pub/Sub topic to publish failed messages to. "
              + "If set, failed messages are published to this topic. "
              + "Can be combined with --clickHouseDeadLetterTable to write to both destinations simultaneously. "
              + "At least one of --clickHouseDeadLetterTable or --deadLetterTopic must be provided.",
      example = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>")
  String getDeadLetterTopic();

  void setDeadLetterTopic(String deadLetterTopic);

  // -------------------------------------------------------------------------
  // Batching
  // -------------------------------------------------------------------------

  @TemplateParameter.Integer(
      order = 9,
      optional = true,
      description = "Window duration in seconds",
      helpText =
          "Duration in seconds for time-based batching windows. "
              + "If only this is set, time-only mode is used. "
              + "If set with --batchRowCount, combined mode fires on whichever comes first. "
              + "If neither is set, combined mode uses defaults (30s + 1000 rows).")
  Integer getWindowSeconds();

  void setWindowSeconds(Integer windowSeconds);

  @TemplateParameter.Integer(
      order = 10,
      optional = true,
      description = "Batch row count threshold",
      helpText =
          "Number of rows to accumulate before flushing to ClickHouse. "
              + "If only this is set, count-only mode is used. "
              + "If set with --windowSeconds, combined mode fires on whichever comes first. "
              + "If neither is set, combined mode uses defaults (30s + 1000 rows).")
  Integer getBatchRowCount();

  void setBatchRowCount(Integer batchRowCount);

  // -------------------------------------------------------------------------
  // ClickHouse write tuning
  // -------------------------------------------------------------------------

  @TemplateParameter.Long(
      order = 11,
      optional = true,
      description = "Max ClickHouse insert block size",
      helpText =
          "Maximum number of rows per INSERT statement sent to ClickHouse. Defaults to 1,000,000.",
      example = "10000")
  Long getMaxInsertBlockSize();

  void setMaxInsertBlockSize(Long maxInsertBlockSize);

  @TemplateParameter.Integer(
      order = 12,
      optional = true,
      description = "Max retries on ClickHouse insert failure",
      helpText = "Maximum number of retry attempts for failed ClickHouse inserts. Defaults to 5.")
  @Default.Integer(5)
  Integer getMaxRetries();

  void setMaxRetries(Integer maxRetries);

  @TemplateParameter.Boolean(
      order = 13,
      optional = true,
      description = "Insert deduplication",
      helpText =
          "Whether to enable deduplication for INSERT queries in replicated ClickHouse tables. "
              + "Defaults to true.")
  @Default.Boolean(true)
  Boolean getInsertDeduplicate();

  void setInsertDeduplicate(Boolean insertDeduplicate);

  @TemplateParameter.Long(
      order = 14,
      optional = true,
      description = "Insert quorum",
      helpText =
          "For INSERT queries in replicated tables, wait for writing to the specified number of "
              + "replicas and linearize the data addition. 0 disables quorum writes. "
              + "Disabled by default.")
  Long getInsertQuorum();

  void setInsertQuorum(Long insertQuorum);

  @TemplateParameter.Boolean(
      order = 15,
      optional = true,
      description = "Insert distributed sync",
      helpText =
          "If enabled, INSERT queries into distributed tables wait until data is sent to all "
              + "nodes in the cluster. Defaults to true.")
  @Default.Boolean(true)
  Boolean getInsertDistributedSync();

  void setInsertDistributedSync(Boolean insertDistributedSync);
}
