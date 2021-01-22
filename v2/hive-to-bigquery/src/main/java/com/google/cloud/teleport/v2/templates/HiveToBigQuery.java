/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;

/**
 * The {@link HiveToBigQuery} pipeline is a batch pipeline which ingests data
 * from Hive and outputs the resulting records to BigQuery.
 *
 */

public final class HiveToBigQuery {

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends DataflowPipelineOptions, HadoopFileSystemOptions, WriteOptions {
    @Description("hive metastore uri")
    @Required
    String getMetastoreUri();

    void setMetastoreUri(String value);

    @Description("Hive database name")
    @Required
    String getHiveDatabaseName();

    void setHiveDatabaseName(String value);

    @Description("Hive table name")
    @Required
    String getHiveTableName();

    void setHiveTableName(String value);

    @Description("comma separated list of the columns of Hive partition columns")
    List<java.lang.String> getHivePartitionCols();

    void setHivePartitionCols(List<java.lang.String> value);

    @Description("the filter details")
    String getFilterString();

    void setFilterString(String value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * HiveToBigQuery#run(Options)} method to start the pipeline and invoke
   * {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Configures the HCatalog source.
   *
   * @param options The execution options.
   * @param configProperties The configuration properties like metastore URI
   * @return hiveRead Configured HCatalogIO.Read method
   */
  private static HCatalogIO.Read getHCatalogIORead(Options options, Map<String, String> configProperties) {
    HCatalogIO.Read hiveRead = HCatalogIO.read()
        .withConfigProperties(configProperties)
        .withDatabase(options.getHiveDatabaseName())
        .withTable(options.getHiveTableName());

    if (options.getHivePartitionCols() != null) {
      hiveRead = hiveRead.withPartitionCols(options.getHivePartitionCols());
    }

    if (options.getFilterString() != null) {
      hiveRead = hiveRead.withFilter(options.getFilterString());
    }
    return hiveRead;
  }

  /**
   * Configures the HCatalog source.
   *
   * @param options The execution options.
   * @param configProperties The configuration properties like metastore URI
   * @return bqWrite Configured BigQueryIO.Write method
   */
  private static BigQueryIO.Write getBigQueryIOWrite(Options options, PCollection<Row> rows) {
    BigQueryIO.Write bqWrite = BigQueryIO.<Row>write()
            .to(options.getOutputTableSpec())
            .withSchema(BigQueryUtils.toTableSchema(rows.getSchema()))
            .withFormatFunction(BigQueryUtils.toTableRow())
            .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
            .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
            .withExtendedErrorInfo();

    if (options.getPartitionType() == "Time") {
      bqWrite = bqWrite.withTimePartitioning(new TimePartitioning().setField(options.getPartitionCol()));
    }
    return bqWrite;
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {
    // Set Hadoop configurations
    Configuration hadoopConf = new Configuration();
    options.setHdfsConfiguration(Collections.singletonList(hadoopConf));

    Map<String, String> configProperties = new HashMap<String, String>();
    configProperties.put("hive.metastore.uris", options.getMetastoreUri());

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Row> rows =
        pipeline
            /*
             * Step #1: Read hive table rows from Hive.
             */
            .apply(
                "Read from Hive source",
                    HCatToRow.fromSpec(
                        getHCatalogIORead(options, configProperties)));

    /*
     * Step #2: Write table rows out to BigQuery
     */
    rows.apply(
            "Write records to BigQuery", getBigQueryIOWrite(options, rows)
    );
    return pipeline.run();
  }
}
