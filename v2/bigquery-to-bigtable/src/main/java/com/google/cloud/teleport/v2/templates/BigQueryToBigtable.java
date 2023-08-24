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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.bigtable.utils.BigtableConfig.generateCloudBigtableWriteConfiguration;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import com.google.cloud.teleport.v2.bigtable.transforms.BigtableConverters;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.BigQueryToBigtable.BigQueryToBigtableOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Dataflow template which reads BigQuery data and writes it to Bigtable. The source data can be
 * either a BigQuery table or an SQL query.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/bigquery-to-bigtable/README_BigQuery_to_Bigtable.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "BigQuery_to_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to Bigtable",
    description = "A pipeline to export a BigQuery table into Bigtable.",
    optionsClass = BigQueryToBigtableOptions.class,
    optionsOrder = {
      BigQueryToBigtableOptions.class,
      BigQueryConverters.BigQueryReadOptions.class,
      BigtableCommonOptions.class,
      BigtableCommonOptions.WriteOptions.class
    },
    optionalOptions = {"inputTableSpec"},
    flexContainerName = "bigquery-to-bigtable",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The source BigQuery table must exist.",
      "The Bigtable table must exist.",
      "The <a href=\"https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#worker-service-account\">worker service account</a>"
          + " needs the <code>roles/bigquery.datasets.create</code> permission. For"
          + " more information, see <a href=\"https://cloud.google.com/bigquery/docs/access-control\">Introduction to IAM</a>."
    })
public class BigQueryToBigtable {

  /**
   * The {@link BigQueryToBigtableOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface BigQueryToBigtableOptions
      extends BigQueryConverters.BigQueryReadOptions,
          BigtableCommonOptions.WriteOptions,
          GcpOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"[A-Za-z_][A-Za-z_0-9]*"},
        description = "Unique identifier column",
        helpText = "Name of the BigQuery column storing the unique identifier of the row")
    @Required
    String getReadIdColumn();

    void setReadIdColumn(String value);
  }

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    BigQueryToBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToBigtableOptions.class);

    CloudBigtableTableConfiguration bigtableTableConfig =
        generateCloudBigtableWriteConfiguration(options);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "AvroToMutation",
            BigQueryConverters.ReadBigQuery.<Mutation>newBuilder()
                .setOptions(options.as(BigQueryToBigtableOptions.class))
                .setReadFunction(
                    BigQueryIO.read(
                        BigtableConverters.AvroToMutation.newBuilder()
                            .setColumnFamily(options.getBigtableWriteColumnFamily())
                            .setRowkey(options.getReadIdColumn())
                            .build()))
                .build())
        .apply("WriteToTable", CloudBigtableIO.writeToTable(bigtableTableConfig));

    pipeline.run();
  }
}
