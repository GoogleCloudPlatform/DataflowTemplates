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
      description = "Path to job configuration file",
      helpText =
          "The path to the job specification file, which contains the configuration for source and target metadata.")
  @Validation.Required
  String getJobSpecUri();

  void setJobSpecUri(String value);

  @TemplateParameter.GcsReadFile(
      order = 2,
      description = "Path to Neo4j connection metadata",
      helpText = "Path to Neo4j connection metadata JSON file.")
  @Validation.Required
  String getNeo4jConnectionUri();

  void setNeo4jConnectionUri(String value);

  @TemplateParameter.Text(
      order = 3,
      description = "Options JSON",
      helpText = "Options JSON. Use runtime tokens.",
      example = "{token1:value1,token2:value2}")
  String getOptionsJson();

  void setOptionsJson(String value);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description = "Query SQL",
      helpText = "Override SQL query (optional).")
  @Default.String("")
  String getReadQuery();

  void setReadQuery(String value);

  @TemplateParameter.GcsReadFile(
      order = 5,
      optional = true,
      description = "Path to Text File",
      helpText = "Override text file pattern (optional)",
      example = "gs://your-bucket/path/*.json")
  @Default.String("")
  String getInputFilePattern();

  void setInputFilePattern(String value);
}
