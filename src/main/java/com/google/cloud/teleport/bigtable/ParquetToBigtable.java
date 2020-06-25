/*
 * Copyright (C) 2019 Google Inc.
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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.AvroToBigtable.toByteString;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ParquetToBigtable} pipeline imports data from Parquet files in GCS to a Cloud Bigtable table. The
 * Cloud Bigtable table must be created before running the pipeline and must have a compatible table
 * schema. For example, if {@link BigtableCell} from the Parquet files has a 'family' of "f1", the
 * Bigtable table should have a column family of "f1".
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Bigtable instance.
 *   <li>Bigtable table with compatible table schema.
 *   <li>Google Cloud Storage input bucket and parquet file(s) exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 *
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/parquet-to-bigtable
 * BIGTABLE_INSTANCE_ID=BIGTABLE INSTANCE ID HERE
 * BIGTABLE_TABLE_ID=BIGTABLE TABLE ID HERE
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.bigtable.ParquetToBigtable \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=parquet-to-bigtable-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "bigtableProjectId=${PROJECT_ID},\
 * bigtableInstanceId=${BIGTABLE_INSTANCE_ID},\
 * bigtableTableId=${BIGTABLE_TABLE_ID},\
 * inputFilePattern=${PIPELINE_FOLDER}/path/to/file/filename-*.parquet"
 * </pre>
 */
public class ParquetToBigtable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroToBigtable.class);

  /** Maximum number of mutations allowed per row by Cloud bigtable. */
  private static final int MAX_MUTATIONS_PER_ROW = 100000;

  private static final Boolean DEFAULT_SPLIT_LARGE_ROWS = false;

  /**
   * Options for the import pipeline.
   */
  public interface Options extends PipelineOptions {
    @Description("The project that contains the table to import into.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to import into.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to import into.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description(
            "The input file patterm to read from. (e.g. gs://mybucket/somefolder/table1*.parquet)")
    ValueProvider<String> getInputFilePattern();

    @SuppressWarnings("unused")
    void setInputFilePattern(ValueProvider<String> inputFilePattern);

    @Description(
        "If true, a large row is split into multiple MutateRows requests. When a row is"
            + " split across requests, updates are not atomic. ")
    ValueProvider<Boolean> getSplitLargeRows();

    void setSplitLargeRows(ValueProvider<Boolean> splitLargeRows);
  }

  /**
   * Runs a pipeline to import Parquet files in GCS to a Cloud Bigtable table.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    BigtableIO.Write write =
            BigtableIO.write()
                    .withProjectId(options.getBigtableProjectId())
                    .withInstanceId(options.getBigtableInstanceId())
                    .withTableId(options.getBigtableTableId());

    /**
     * Steps: 1) Read records from Parquet File. 2) Convert a GenericRecord to a
     * KV<ByteString,Iterable<Mutation>>. 3) Write KV to Bigtable's table.
     */
    pipeline
        .apply(
            "Read from Parquet",
            ParquetIO.read(BigtableRow.getClassSchema()).from(options.getInputFilePattern()))
        .apply(
            "Transform to Bigtable",
            ParDo.of(
                ParquetToBigtableFn.createWithSplitLargeRows(
                    options.getSplitLargeRows(), MAX_MUTATIONS_PER_ROW)))
        .apply("Write to Bigtable", write);

    return pipeline.run();
  }

  static class ParquetToBigtableFn
          extends DoFn<GenericRecord, KV<ByteString, Iterable<Mutation>>> {

    private final ValueProvider<Boolean> splitLargeRowsFlag;
    private Boolean splitLargeRows;
    private final int maxMutationsPerRow;

    public static ParquetToBigtableFn create() {
      return new ParquetToBigtableFn(StaticValueProvider.of(false), MAX_MUTATIONS_PER_ROW);
    }

    public static ParquetToBigtableFn createWithSplitLargeRows(
        ValueProvider<Boolean> splitLargeRowsFlag, int maxMutationsPerRequest) {
      return new ParquetToBigtableFn(splitLargeRowsFlag, maxMutationsPerRequest);
    }

    @Setup
    public void setup() {
      if (splitLargeRowsFlag != null) {
        splitLargeRows = splitLargeRowsFlag.get();
      }
      splitLargeRows = MoreObjects.firstNonNull(splitLargeRows, DEFAULT_SPLIT_LARGE_ROWS);
      LOG.info("splitLargeRows set to: " + splitLargeRows);
    }

    private ParquetToBigtableFn(
        ValueProvider<Boolean> splitLargeRowsFlag, int maxMutationsPerRequest) {
      this.splitLargeRowsFlag = splitLargeRowsFlag;
      this.maxMutationsPerRow = maxMutationsPerRequest;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      Class runner = ctx.getPipelineOptions().getRunner();
      ByteString key = toByteString((ByteBuffer) ctx.element().get(0));

      // BulkMutation doesn't split rows. Currently, if a single row contains more than 100,000
      // mutations, the service will fail the request.
      ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
      List<Object> cells = (List) ctx.element().get(1);
      int cellsProcessed = 0;
      for (Object element : cells) {
        Mutation.SetCell setCell = null;
        if (runner.isAssignableFrom(DirectRunner.class)) {
          setCell =
                  Mutation.SetCell.newBuilder()
                          .setFamilyName(((GenericData.Record) element).get(0).toString())
                          .setColumnQualifier(toByteString((ByteBuffer) ((GenericData.Record) element).get(1)))
                          .setTimestampMicros((Long) ((GenericData.Record) element).get(2))
                          .setValue(toByteString((ByteBuffer) ((GenericData.Record) element).get(3)))
                          .build();
        } else {
          BigtableCell bigtableCell = (BigtableCell) element;
          setCell =
                  Mutation.SetCell.newBuilder()
                          .setFamilyName(bigtableCell.getFamily().toString())
                          .setColumnQualifier(toByteString(bigtableCell.getQualifier()))
                          .setTimestampMicros(bigtableCell.getTimestamp())
                          .setValue(toByteString(bigtableCell.getValue()))
                          .build();
        }
        mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        cellsProcessed++;

        if (this.splitLargeRows && cellsProcessed % maxMutationsPerRow == 0) {
          // Send a MutateRow request when we have accumulated max mutations per row.
          ctx.output(KV.of(key, mutations.build()));
          mutations = ImmutableList.builder();
        }
      }

      // Flush any remaining mutations.
      ImmutableList remainingMutations = mutations.build();
      if (!remainingMutations.isEmpty()) {
        ctx.output(KV.of(key, remainingMutations));
      }
    }
  }
}
