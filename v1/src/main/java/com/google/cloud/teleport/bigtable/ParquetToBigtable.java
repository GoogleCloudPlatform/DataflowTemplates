/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.AvroToBigtable.toByteString;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.teleport.bigtable.ParquetToBigtable.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ParquetToBigtable} pipeline imports data from Parquet files in GCS to a Cloud Bigtable
 * table. The Cloud Bigtable table must be created before running the pipeline and must have a
 * compatible table schema. For example, if {@link BigtableCell} from the Parquet files has a
 * 'family' of "f1", the Bigtable table should have a column family of "f1".
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Parquet_to_Cloud_Bigtable.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_Parquet_to_Cloud_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "Parquet Files on Cloud Storage to Cloud Bigtable",
    description =
        "The Cloud Storage Parquet to Bigtable template is a pipeline that reads data from Parquet files in a Cloud Storage bucket and writes the data to a Bigtable table. "
            + "You can use the template to copy data from Cloud Storage to Bigtable.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/parquet-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist and have the same column families as exported in the Parquet files.",
      "The input Parquet files must exist in a Cloud Storage bucket before running the pipeline.",
      "Bigtable expects a specific <a href=\"https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/src/main/resources/schema/avro/bigtable.avsc\">schema</a> from the input Parquet files."
    })
public class ParquetToBigtable {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetToBigtable.class);

  /** Maximum number of mutations allowed per row by Cloud bigtable. */
  private static final int MAX_MUTATIONS_PER_ROW = 100000;

  private static final Boolean DEFAULT_SPLIT_LARGE_ROWS = false;

  /** Options for the import pipeline. */
  public interface Options extends PipelineOptions {
    @TemplateParameter.ProjectId(
        order = 1,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to write data to")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to write")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @TemplateParameter.Text(
        order = 4,
        description = "Input Cloud Storage File(s)",
        helpText = "The Cloud Storage location of the files you'd like to process.",
        regexes = {"^gs:\\/\\/[^\\n\\r]+$"},
        example = "gs://your-bucket/your-files/*.parquet")
    ValueProvider<String> getInputFilePattern();

    @SuppressWarnings("unused")
    void setInputFilePattern(ValueProvider<String> inputFilePattern);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "If true, large rows will be split into multiple MutateRows requests",
        helpText =
            "The flag for enabling splitting of large rows into multiple MutateRows requests. Note that when a large row is split between multiple API calls, the updates to the row are not atomic. ")
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
    Pipeline pipeline = Pipeline.create(PipelineUtils.tweakPipelineOptions(options));

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

  static class ParquetToBigtableFn extends DoFn<GenericRecord, KV<ByteString, Iterable<Mutation>>> {

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
                  .setColumnQualifier(
                      toByteString((ByteBuffer) ((GenericData.Record) element).get(1)))
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
