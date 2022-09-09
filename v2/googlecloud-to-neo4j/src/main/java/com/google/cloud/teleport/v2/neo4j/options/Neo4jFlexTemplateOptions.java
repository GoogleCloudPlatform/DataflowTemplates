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

import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Extends pipeline options to include abstract parameter options and others that are required by IO
 * connectors (TextIO, BigQuery, etc.).
 */
public interface Neo4jFlexTemplateOptions extends CommonTemplateOptions {

  @Description("Path to job specification")
  @Validation.Required
  String getJobSpecUri();

  void setJobSpecUri(String value);

  @Description("Path to Neo4j connection metadata")
  @Validation.Required
  String getNeo4jConnectionUri();

  void setNeo4jConnectionUri(String value);

  @Description("Options JSON (see documentation)")
  String getOptionsJson();

  void setOptionsJson(String value);

  @Description("Read query")
  @Default.String("")
  String getReadQuery();

  void setReadQuery(String value);

  @Description("Input file pattern")
  @Default.String("")
  String getInputFilePattern();

  void setInputFilePattern(String value);
}
