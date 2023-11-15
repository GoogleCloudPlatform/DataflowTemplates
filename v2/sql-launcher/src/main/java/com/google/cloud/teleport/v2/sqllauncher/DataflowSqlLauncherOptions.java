/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import com.google.cloud.teleport.metadata.TemplateParameter;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options for Dataflow SQL Launcher. */
public interface DataflowSqlLauncherOptions extends PipelineOptions {

  /**
   * SQL Query.
   *
   * <p>Contains all the logic of the pipeline.
   */
  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {"^.+$"},
      description = "SQL query string.",
      helpText = "Query to be executed to extract the data.",
      example = "SELECT * FROM pubsub.topic.`pubsub-public-data`.`taxirides-realtime`")
  @Description("Required. SQL Query containing the pipeline logic.")
  @Validation.Required
  String getQueryString();

  void setQueryString(String queryString);

  /** Destination BQ table. */
  @TemplateParameter.BigQueryTable(
      order = 2,
      optional = true,
      description = "BigQuery output table",
      helpText =
          "BigQuery table location to write the output to. The table's schema must match the "
              + "input objects. Output table and output specifications can not be used together.")
  String getOutputTable();

  void setOutputTable(String outputTable);

  /** Output specification. */
  @TemplateParameter.Text(
      order = 3,
      optional = true,
      regexes = {"^.+$"},
      description = "Output specification.",
      helpText =
          "List of output specifications for writing the output of the query. Input should"
              + " be a JSON string of the form [{\"type\": \"bigquery\"|\"pubsub\", (additional"
              + " properties depending on type)}, ...]. Output table and output specifications can not be used together.")
  String getOutputs();

  void setOutputs(String outputTable);

  /** Data Catalog endpoint. Defaults to prod. */
  @Description("Data catalog endpoint. Defaults to \"datacatalog.googleapis.com\".")
  @Validation.Required
  @Default.String("datacatalog.googleapis.com:443")
  String getDataCatalogEndpoint();

  void setDataCatalogEndpoint(String dataCatalogEndpoint);

  /** Disable pipeline run. */
  @Description("Constructs but does not run the SQL pipeline, for smoke testing.")
  @Validation.Required
  @Default.Boolean(false)
  Boolean getDryRun();

  void setDryRun(Boolean dryRun);

  /**
   * Query parameters. Input should be a JSON string containing an array of JSON-serialized {@link
   * ParameterTranslator.Parameter} objects.
   */
  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"^.+$"},
      description = "Query parameters.",
      helpText =
          "Input should be a JSON string containing an array of parameter objects."
              + " e.g. [{\"parameterType\": {\"type\": \"STRING\"}, \"parameterValue\": {\"value\":"
              + " \"foo\"}, \"name\": \"x\"}, {\"parameterType\": {\"type\": \"FLOAT64\"},"
              + " \"parameterValue\": {\"value\": \"1.0\"}, \"name\": \"y\"}]")
  @Default.String("[]")
  String getQueryParameters();

  void setQueryParameters(String queryParameters);

  /** If running as a Flex Template, the job id assigned prior to run. */
  @Nullable
  @Hidden
  String getJobId();

  void setJobId(String newJobId);
}
