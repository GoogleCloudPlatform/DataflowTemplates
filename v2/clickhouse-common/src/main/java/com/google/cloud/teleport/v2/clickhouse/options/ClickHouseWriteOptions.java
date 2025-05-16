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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** The {@link ClickHouseWriteOptions} with the common write options for ClickHouse. * */
public interface ClickHouseWriteOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      groupName = "Host",
      description = "ClickHouse JDBC URL",
      helpText =
          "The target ClickHouse JDBC URL in the format `jdbc:clickhouse://host:port/schema`. Any JDBC option could be added at the end of the JDBC URL.",
      example = "jdbc:clickhouse://localhost:8123/default")
  @Validation.Required
  String getJdbcUrl();

  void setJdbcUrl(String jdbcUrl);

  @TemplateParameter.Text(
      order = 2,
      description = "Username for ClickHouse endpoint",
      helpText = "The ClickHouse username to authenticate with.")
  String getClickHouseUsername();

  void setClickHouseUsername(String clickHouseUsername);

  @TemplateParameter.Password(
      order = 3,
      optional = true,
      description = "Password for ClickHouse endpoint",
      helpText = "The ClickHouse password to authenticate with.")
  String getClickHousePassword();

  void setClickHousePassword(String clickHousePassword);

  @TemplateParameter.Text(
      order = 4,
      description = "ClickHouse Table Name",
      helpText = "The target ClickHouse table name to insert the data to.")
  String getClickHouseTable();

  void setClickHouseTable(String clickHouseTable);

  @TemplateParameter.Long(
      order = 5,
      optional = true,
      description = "Max Insert Block Size",
      helpText =
          "The maximum block size for insertion, if we control the creation of blocks for insertion (ClickHouseIO option).")
  Long getMaxInsertBlockSize();

  void setMaxInsertBlockSize(Long maxInsertBlockSize);

  @TemplateParameter.Boolean(
      order = 6,
      optional = true,
      description = "Insert Distributed Sync",
      helpText =
          "If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster. (ClickHouseIO option).")
  Boolean getInsertDistributedSync();

  void setInsertDistributedSync(Boolean insertDistributedSync);

  @TemplateParameter.Long(
      order = 7,
      optional = true,
      description = "Insert Quorum",
      helpText =
          "For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled.\n"
              + "This setting is disabled in default server settings (ClickHouseIO option).")
  Long getInsertQuorum();

  void setInsertQuorum(Long insertQuorum);

  @TemplateParameter.Boolean(
      order = 8,
      optional = true,
      description = "Insert Deduplicate",
      helpText =
          "For INSERT queries in the replicated table, specifies that deduplication of inserting blocks should be performed.")
  Boolean getInsertDeduplicate();

  void setInsertDeduplicate(Boolean insertDeduplicate);

  @TemplateParameter.Integer(
      order = 9,
      optional = true,
      description = "Max retries",
      helpText = "Maximum number of retries per insert.")
  Integer getMaxRetries();

  void setMaxRetries(Integer maxRetries);
}
