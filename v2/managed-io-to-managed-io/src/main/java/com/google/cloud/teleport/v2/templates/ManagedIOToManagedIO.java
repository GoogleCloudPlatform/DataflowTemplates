/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.templates.ManagedIOToManagedIO.Options;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/**
 * The {@link ManagedIOToManagedIO} pipeline is a flexible template that can read from any Managed
 * I/O source and write to any Managed I/O sink. This template supports all available Managed I/O
 * connectors including ICEBERG, ICEBERG_CDC, KAFKA, and BIGQUERY.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The source and sink configurations must be valid for the specified connector types.
 *   <li>Required permissions for accessing the source and sink systems.
 * </ul>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/managed-io-to-managed-io/README.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Managed_IO_to_Managed_IO",
    category = TemplateCategory.BATCH,
    displayName = "Managed I/O to Managed I/O",
    description =
        "The Managed I/O to Managed I/O template is a flexible pipeline that can read from any "
            + "Managed I/O source and write to any Managed I/O sink. This template supports all "
            + "available Managed I/O connectors including ICEBERG, ICEBERG_CDC, KAFKA, and BIGQUERY. "
            + "The template uses Apache Beam's Managed API to provide a unified interface for "
            + "configuring different I/O connectors through simple configuration maps.",
    optionsClass = Options.class,
    flexContainerName = "managed-io-to-managed-io",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/managed-io",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The source and sink configurations must be valid for the specified connector types.",
      "Required permissions for accessing the source and sink systems."
    },
    streaming = false)
public class ManagedIOToManagedIO {

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        description = "Source Managed I/O connector type",
        helpText =
            "The type of Managed I/O connector to use as source. "
                + "Supported values: ICEBERG, ICEBERG_CDC, KAFKA, BIGQUERY.")
    String getSourceConnectorType();

    void setSourceConnectorType(String value);

    @TemplateParameter.Text(
        order = 2,
        description = "Source configuration (JSON)",
        helpText =
            "JSON configuration for the source Managed I/O connector. "
                + "The configuration format depends on the connector type. "
                + "For example, for KAFKA: {\"bootstrap_servers\": \"localhost:9092\", \"topic\": \"input-topic\", \"format\": \"JSON\"}")
    String getSourceConfig();

    void setSourceConfig(String value);

    @TemplateParameter.Text(
        order = 3,
        description = "Sink Managed I/O connector type",
        helpText =
            "The type of Managed I/O connector to use as sink. "
                + "Supported values: ICEBERG, KAFKA, BIGQUERY. "
                + "Note: ICEBERG_CDC is only available for reading.")
    String getSinkConnectorType();

    void setSinkConnectorType(String value);

    @TemplateParameter.Text(
        order = 4,
        description = "Sink configuration (JSON)",
        helpText =
            "JSON configuration for the sink Managed I/O connector. "
                + "The configuration format depends on the connector type. "
                + "For example, for BIGQUERY: {\"table\": \"project:dataset.table\"}")
    String getSinkConfig();

    void setSinkConfig(String value);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "Enable streaming mode",
        helpText =
            "Whether to run the pipeline in streaming mode. "
                + "This is only supported for certain connector combinations. "
                + "Default is false (batch mode).")
    boolean isStreaming();

    void setStreaming(boolean value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * ManagedIOToManagedIO#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    // UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    // Always enable runner v2
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    List<String> experiments = new ArrayList<>();
    if (dataflowOptions.getExperiments() != null) {
      experiments.addAll(dataflowOptions.getExperiments());
    }
    if (!experiments.contains("use_runner_v2")) {
      experiments.add("use_runner_v2");
    }
    dataflowOptions.setExperiments(experiments);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait for the
   * pipeline to finish before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);

    // Parse source configuration
    Map<String, Object> sourceConfig = parseJsonConfig(options.getSourceConfig());

    // Parse sink configuration
    Map<String, Object> sinkConfig = parseJsonConfig(options.getSinkConfig());

    // Read from source using Managed I/O
    PCollectionRowTuple sourceOutput =
        pipeline.apply(
            "Read from " + options.getSourceConnectorType(),
            Managed.read(getSourceConnector(options.getSourceConnectorType()))
                .withConfig(sourceConfig));

    // Get the main PCollection from the tuple
    PCollection<Row> rows = sourceOutput.get("output");

    // Write to sink using Managed I/O
    PCollectionRowTuple.of("input", rows)
        .apply(
            "Write to " + options.getSinkConnectorType(),
            Managed.write(getSinkConnector(options.getSinkConnectorType())).withConfig(sinkConfig));

    return pipeline.run();
  }

  /**
   * Gets the appropriate source connector based on the connector type.
   *
   * @param connectorType The connector type string.
   * @return The Managed connector.
   */
  private static String getSourceConnector(String connectorType) {
    switch (connectorType.toUpperCase()) {
      case "ICEBERG":
        return "ICEBERG";
      case "ICEBERG_CDC":
        return "ICEBERG_CDC";
      case "KAFKA":
        return "KAFKA";
      case "BIGQUERY":
        return "BIGQUERY";
      default:
        throw new IllegalArgumentException("Unsupported source connector type: " + connectorType);
    }
  }

  /**
   * Gets the appropriate sink connector based on the connector type.
   *
   * @param connectorType The connector type string.
   * @return The Managed connector.
   */
  private static String getSinkConnector(String connectorType) {
    switch (connectorType.toUpperCase()) {
      case "ICEBERG":
        return "ICEBERG";
      case "KAFKA":
        return "KAFKA";
      case "BIGQUERY":
        return "BIGQUERY";
      default:
        throw new IllegalArgumentException(
            "Unsupported sink connector type: "
                + connectorType
                + ". Note: ICEBERG_CDC is only available for reading.");
    }
  }

  /**
   * Parses a JSON configuration string into a Map.
   *
   * @param jsonConfig The JSON configuration string.
   * @return A Map representing the configuration.
   */
  private static Map<String, Object> parseJsonConfig(String jsonConfig) {
    try {
      if (jsonConfig == null || jsonConfig.trim().isEmpty()) {
        return new HashMap<>();
      }

      Gson gson = new Gson();
      Type type = new TypeToken<Map<String, Object>>() {}.getType();
      return gson.fromJson(jsonConfig, type);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON configuration: " + jsonConfig, e);
    }
  }
}
