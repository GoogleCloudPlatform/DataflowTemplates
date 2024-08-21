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
package com.google.cloud.teleport.v2.neo4j.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * Extends pipeline options to include abstract parameter options and others that are required by IO
 * connectors (TextIO, BigQuery, etc.).
 */
public interface Neo4jFlexTemplateOptions extends CommonTemplateOptions {

  @TemplateParameter.GcsReadFile(
      order = 1,
      description = "Path to the job specification file",
      helpText =
          "The path to the job specification file, which contains the JSON description of data sources, Neo4j targets and actions.")
  @Validation.Required
  String getJobSpecUri();

  void setJobSpecUri(String value);

  @TemplateParameter.GcsReadFile(
      order = 2,
      optional = true,
      description = "The path to the Neo4j connection JSON file, which is hosted on Google Cloud Storage.",
      helpText = "The path to the Neo4j connection JSON file.")
  @Validation.Required(groups = "connection")
  String getNeo4jConnectionUri();

  void setNeo4jConnectionUri(String value);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description =
          "The secret ID for the Neo4j connection information. This value is encoded in JSON, using the same schema as the `neo4jConnectionUri`.",
      helpText =
          "The secret ID for the Neo4j connection metadata. You can use this value as an alternative to the `neo4jConnectionUri`.")
  @Validation.Required(groups = "connection")
  String getNeo4jConnectionSecretId();

  void setNeo4jConnectionSecretId(String value);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description =
          "JSON object also called \"runtime tokens\", defining named values interpolated into the specification URIs and queries.",
      helpText = "JSON object also called \"runtime tokens\"",
      example = "{token1:value1,token2:value2}. Spec can refer to $token1 and $token2.")
  @Default.String("")
  String getOptionsJson();

  void setOptionsJson(String value);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      description = "SQL query override for all BigQuery sources",
      helpText = "SQL query override.")
  @Default.String("")
  String getReadQuery();

  void setReadQuery(String value);

  @TemplateParameter.GcsReadFile(
      order = 6,
      optional = true,
      description = "Text File Path override for all Text sources",
      helpText = "Text File Path override",
      example = "gs://your-bucket/path/*.json")
  @Default.String("")
  String getInputFilePattern();

  void setInputFilePattern(String value);
}
