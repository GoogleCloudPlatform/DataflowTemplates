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

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.AvroToMutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Dataflow template which reads BigQuery data and writes it to Bigtable. The source data can be
 * either a BigQuery table or an SQL query.
 */
public class BigQueryToBigtable {

  /**
   * The {@link BigQueryToBigtableOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface BigQueryToBigtableOptions extends PipelineOptions {

    @Description("SQL query in standard SQL to pull data from BigQuery")
    String getReadQuery();

    void setReadQuery(String value);

    @Description("Name of the BQ column storing the unique identifier of the row")
    String getReadIdColumn();

    void setReadIdColumn(String value);

    @Description("GCP Project Id of where to write the Bigtable rows")
    @Required
    String getBigtableWriteProjectId();

    void setBigtableWriteProjectId(String bigtableWriteProjectId);

    @Description("Bigtable Instance id")
    @Required
    String getBigtableWriteInstanceId();

    void setBigtableWriteInstanceId(String bigtableWriteInstanceId);

    @Description("Bigtable app profile")
    @Default.String("default")
    String getBigtableWriteAppProfile();

    void setBigtableWriteAppProfile(String bigtableWriteAppProfile);

    @Description("Bigtable table id")
    @Required
    String getBigtableWriteTableId();

    void setBigtableWriteTableId(String bigtableWriteTableId);

    @Description("Bigtable column family name")
    @Required
    String getBigtableWriteColumnFamily();

    void setBigtableWriteColumnFamily(String bigtableWriteColumnFamily);
  }

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    BigQueryToBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToBigtableOptions.class);
    CloudBigtableTableConfiguration bigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableWriteProjectId())
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withAppProfileId(options.getBigtableWriteAppProfile())
            .withTableId(options.getBigtableWriteTableId())
            .build();

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(
            "AvroToMutation",
            BigQueryIO.read(
                    AvroToMutation.newBuilder()
                        .setColumnFamily(options.getBigtableWriteColumnFamily())
                        .setRowkey(options.getReadIdColumn())
                        .build())
                .fromQuery(options.getReadQuery())
                .withoutValidation()
                .withTemplateCompatibility()
                .usingStandardSql())
        .apply(
            "WriteToTable",
            CloudBigtableIO.writeToTable(bigtableTableConfig));

    pipeline.run();
  }
}
