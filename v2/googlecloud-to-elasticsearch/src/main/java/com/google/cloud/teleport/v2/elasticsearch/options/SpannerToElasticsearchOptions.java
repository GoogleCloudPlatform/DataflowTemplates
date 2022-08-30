/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.spanner.Options.RpcPriority;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Custom options for {@link com.google.cloud.teleport.v2.templates.SpannerToElasticsearch} pipeline. */
public interface SpannerToElasticsearchOptions extends PipelineOptions, ElasticsearchWriteOptions {
  @Description("Spanner table to read data from.")
  @Required
  String getSpannerTableId();

  void setSpannerTableId(String spannerTableId);

  @Description("Instance id for spanner table.")
  @Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String spannerInstanceId);

  @Description("Database containing the spanner table.")
  @Required
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String spannerDatabaseId);

  @Description(
      "The priority of Spanner job. Must be one of the following: [HIGH, MEDIUM, LOW]. Default is HIGH.")
  @Default.Enum("HIGH")
  RpcPriority getSpannerRpcPriority();

  void setSpannerRpcPriority(RpcPriority spannerRpcPriority);

  @Description("Required. Query used to read from Spanner table.")
  @Required
  String getSqlQuery();

  void setSqlQuery(String sqlQuery);
}
