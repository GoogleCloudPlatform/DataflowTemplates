/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.AdditionalDocumentationBlock;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.neo4j.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.model.Json;
import com.google.cloud.teleport.v2.neo4j.model.Json.ParsingResult;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.OverlayTokenParser;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads Google Cloud data (Text, BigQuery) and writes it to Neo4j.
 *
 * <p>In case of BigQuery, the source data can be either a table or a SQL query.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-neo4j/README_Google_Cloud_to_Neo4j.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Google_Cloud_to_Neo4j",
    category = TemplateCategory.BATCH,
    displayName = "Google Cloud to Neo4j",
    description =
        "The Google Cloud to Neo4j template lets you import a dataset into a Neo4j database through a Dataflow job, "
            + "sourcing data from CSV files hosted in Google Cloud Storage buckets. It also lets you to manipulate and transform the data "
            + "at various steps of the import. You can use the template for both first-time imports and incremental imports.",
    optionsClass = Neo4jFlexTemplateOptions.class,
    flexContainerName = "googlecloud-to-neo4j",
    contactInformation = "https://support.neo4j.com/",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/google-cloud-to-neo4j",
    requirements = {
      "A running Neo4j instance",
      "A Google Cloud Storage bucket",
      "A dataset to import, in the form of CSV files",
      "A job specification file to use"
    },
    additionalDocumentation = {
      @AdditionalDocumentationBlock(
          name = "Create a job specification file",
          content = {
            "The job specification file consists of a JSON object with the following sections:\n"
                + "- `config` - global flags affecting how the import is performed.\n"
                + "- `sources` - data source definitions (relational).\n"
                + "- `targets` - data target definitions (graph: nodes/relationships/custom queries).\n"
                + "- `actions` - pre/post-load actions.\n"
                + "For more information, see <a href=\"https://neo4j.com/docs/dataflow-google-cloud/job-specification/\" class=\"external\">Create a job specification file</a> in the Neo4j documentation."
          })
    },
    preview = true)
public class GoogleCloudToNeo4j {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudToNeo4j.class);

  /**
   * Runs a pipeline which reads data from various sources and writes it to Neo4j.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Neo4jFlexTemplateOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Neo4jFlexTemplateOptions.class);
    ValidationErrors.processValidations(
        "Errors found validating pipeline options: ",
        InputValidator.validateNeo4jPipelineOptions(options));

    if (StringUtils.isBlank(options.getDisabledAlgorithms())) {
      options.setDisabledAlgorithms(
          "SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon,"
              + " NULL");
    }
    LOG.info("Job: {}", options.getJobSpecUri());
    FileSystems.setDefaultPipelineOptions(options);
    var overlayTokens = OverlayTokenParser.parse(options.getOptionsJson());
    var templateVersion = readTemplateVersion(options);
    var neo4jConnectionConfig = readConnectionConfiguration(options);
    var importSpecification = JobSpecMapper.parse(options.getJobSpecUri(), overlayTokens);
    var importPipeline =
        new Neo4jImportPipeline(
            options, templateVersion, neo4jConnectionConfig, overlayTokens, importSpecification);
    importPipeline.run();
  }

  private static String readTemplateVersion(Neo4jFlexTemplateOptions options) {
    var labels = options.as(DataflowPipelineOptions.class).getLabels();
    String defaultVersion = "UNKNOWN";
    if (labels == null) {
      return defaultVersion;
    }
    return labels.getOrDefault("goog-dataflow-provided-template-version", defaultVersion);
  }

  private static ConnectionParams readConnectionConfiguration(
      Neo4jFlexTemplateOptions pipelineOptions) {
    String neo4jConnectionJson = readConnectionSettings(pipelineOptions);
    ParsingResult parsingResult = InputValidator.validateNeo4jConnection(neo4jConnectionJson);
    if (!parsingResult.isSuccessful()) {
      ValidationErrors.processValidations(
          "Errors found validating Neo4j connection: ",
          parsingResult.formatErrors("Could not validate connection JSON"));
    }
    return Json.map(parsingResult, ConnectionParams.class);
  }

  private static String readConnectionSettings(Neo4jFlexTemplateOptions options) {
    String secretId = options.getNeo4jConnectionSecretId();
    if (StringUtils.isNotEmpty(secretId)) {
      return SecretManagerUtils.getSecret(secretId);
    }
    String uri = options.getNeo4jConnectionUri();
    try {
      return FileSystemUtils.getPathContents(uri);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Unable to read Neo4j configuration at URI %s: ", uri), e);
    }
  }
}
