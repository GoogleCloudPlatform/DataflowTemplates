/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.cdc.merge;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a pipeline that tests the basic logic from the {@link BigQueryMerger} transform. It
 * expects a staging table that already contains all of the data, and a replica table that has
 * already been created.
 *
 * <p>The sample data for this pipeline is available in: -
 * gs://cloud-teleport-testing-data/AloomaToBigQuery-test-data/resources/bqmergetest/ Files: -
 * make_dataset.py - This is the python script to generate the data. It generates 2000 updates. -
 * updates_XXXX.json - The updates files contain a time-sorted list of updates that are to be
 * inserted into the staging table. - state_XXXX.json - The state files contain the expected state
 * of the replica table after applying the corresponding updates to it.
 *
 * <p>This pipeline simply applies the current state of a staging table onto an empty replica table.
 */
public class BigQueryMergeValidatorTemplate {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMergeValidatorTemplate.class);

  private static final List<String> ALL_FIELDS =
      Arrays.asList(
          "license_plate",
          "country",
          "longitude",
          "latitude",
          "manufactured",
          "model",
          "last_update",
          "_metadata_timestamp",
          "_metadata_deleted");

  private static final List<String> ALL_PK_FIELDS = Arrays.asList("license_plate", "country");

  /** Class {@link Options}. */
  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = true,
        description = "Staging Table",
        helpText = "The table to set as staging.")
    @Default.String("")
    String getStagingTable();

    void setStagingTable(String value);

    @TemplateParameter.Text(
        order = 1,
        optional = true,
        description = "Replica Table",
        helpText = "The table to set as replica, used to write merge data.")
    @Default.String("")
    String getReplicaTable();

    void setReplicaTable(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    TableId replicaTable = TableId.of("dataset", options.getReplicaTable());
    TableId stagingTable = TableId.of("dataset", options.getStagingTable());

    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                new DoFn<Integer, MergeInfo>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    MergeInfo mergeInfo =
                        MergeInfo.create(
                            "project_id",
                            ALL_PK_FIELDS,
                            Arrays.asList("_metadata_timestamp"),
                            "_metadata_deleted",
                            replicaTable,
                            stagingTable,
                            ALL_FIELDS);
                    c.output(mergeInfo);
                  }
                }))
        .apply(
            BigQueryMerger.of(
                MergeConfiguration.bigQueryConfiguration()
                    .withMergeWindowDuration(Duration.standardMinutes(1))));

    return pipeline.run();
  }
}
