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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    optionalOptions = {"inputTableSpec", "timestampColumn", "skipNullValues"},
    flexContainerName = "bigquery-to-bigtable",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
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
        helpText = "The name of the BigQuery column storing the unique identifier of the row.")
    @Required
    String getReadIdColumn();

    void setReadIdColumn(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        regexes = {"[A-Za-z_][A-Za-z_0-9]*"},
        description = "Timestamp column identifier",
        helpText =
            "The name of the BigQuery column to be used as the timestamp for the column's cell in Bigtable. The value"
                + " must be millisecond precision, e.g. INT64 / Long. If a row does not contain the field, the default write"
                + " timestamp will be used. The column specified will not be included as part of the row in Bigtable as"
                + " a separate column.")
    @Default.String("")
    String getTimestampColumn();

    void setTimestampColumn(String value);

    @TemplateParameter.Boolean(
        order = 3,
        optional = true,
        description = "Flag to skip null values",
        helpText =
            "Flag to indicate whether nulls may propagate as an empty value or column skipped completely to adhere to "
                + " Bigtable sparse table format. In cases where this leads to an empty row, e.g. a valid rowkey, but no "
                + " columns, the row cannot be written to bigtable and the row will be skipped.")
    @Default.Boolean(false)
    Boolean getSkipNullValues();

    void setSkipNullValues(Boolean value);
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
                            .setSkipNullValues(options.getSkipNullValues())
                            .setTimestampColumn(options.getTimestampColumn())
                            .build()))
                .build())
        .apply("VerifyAndFilterMutations", ParDo.of((new VerifyAndFilterMutationsFn())))
        .apply("WriteToTable", CloudBigtableIO.writeToTable(bigtableTableConfig));

    pipeline.run();
  }

  /**
   * Filter out invalid Bigtable Mutations, additional validations/filters may be applied e.g. An
   * empty mutation is one that contains no actual cell set.
   */
  static class VerifyAndFilterMutationsFn extends DoFn<Mutation, Mutation> {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyAndFilterMutationsFn.class);

    @ProcessElement
    public void processElement(@Element Mutation mutation, OutputReceiver<Mutation> receiver) {
      if (mutation instanceof Put && mutation.isEmpty()) {
        LOG.warn("Skipping empty mutation for rowkey: {}", Bytes.toStringBinary(mutation.getRow()));
      } else {
        receiver.output(mutation);
      }
    }
  }
}
