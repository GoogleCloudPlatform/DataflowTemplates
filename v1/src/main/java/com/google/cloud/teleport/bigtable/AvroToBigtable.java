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
package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.teleport.bigtable.AvroToBigtable.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow pipeline that imports data from Avro files in GCS to a Cloud Bigtable table. The Cloud
 * Bigtable table must be created before running the pipeline and must have a compatible table
 * schema. For example, if {@link BigtableCell} from the Avro files has a 'family' of "f1", the
 * Bigtable table should have a column family of "f1".
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Avro_to_Cloud_Bigtable.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_Avro_to_Cloud_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "Avro Files on Cloud Storage to Cloud Bigtable",
    description =
        "The Cloud Storage Avro to Bigtable template is a pipeline that reads data from Avro files in a Cloud Storage bucket and writes the data to a Bigtable table. "
            + "You can use the template to copy data from Cloud Storage to Bigtable.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/avro-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist and have the same column families as exported in the Avro files.",
      "The input Avro files must exist in a Cloud Storage bucket before running the pipeline.",
      "Bigtable expects a specific <a href=\"https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/src/main/resources/schema/avro/bigtable.avsc\">schema</a> from the input Avro files.",
    })
public final class AvroToBigtable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroToBigtable.class);

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
        order = 4,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to write")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @TemplateParameter.GcsReadFile(
        order = 5,
        description = "Input Cloud Storage File(s)",
        helpText = "The Cloud Storage location of the files you'd like to process.",
        example = "gs://your-bucket/your-files/*.avro")
    ValueProvider<String> getInputFilePattern();

    @SuppressWarnings("unused")
    void setInputFilePattern(ValueProvider<String> inputFilePattern);

    @TemplateParameter.Boolean(
        order = 6,
        optional = true,
        description = "If true, large rows will be split into multiple MutateRows requests",
        helpText =
            "The flag for enabling splitting of large rows into multiple MutateRows requests. Note that when a large row is split between multiple API calls, the updates to the row are not atomic. ")
    ValueProvider<Boolean> getSplitLargeRows();

    void setSplitLargeRows(ValueProvider<Boolean> splitLargeRows);
  }

  /**
   * Runs a pipeline to import Avro files in GCS to a Cloud Bigtable table.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(PipelineUtils.tweakPipelineOptions(options));

    BigtableIO.Write write =
        BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    pipeline
        .apply("Read from Avro", AvroIO.read(BigtableRow.class).from(options.getInputFilePattern()))
        .apply(
            "Transform to Bigtable",
            ParDo.of(
                AvroToBigtableFn.createWithSplitLargeRows(
                    options.getSplitLargeRows(), MAX_MUTATIONS_PER_ROW)))
        .apply("Write to Bigtable", write);

    return pipeline.run();
  }

  /**
   * Translates {@link BigtableRow} to {@link Mutation}s along with a row key. The mutations are
   * {@link SetCell}s that set the value for specified cells with family name, column qualifier and
   * timestamp.
   */
  static class AvroToBigtableFn extends DoFn<BigtableRow, KV<ByteString, Iterable<Mutation>>> {
    private final ValueProvider<Boolean> splitLargeRowsFlag;
    private Boolean splitLargeRows;
    private final int maxMutationsPerRow;

    public static AvroToBigtableFn create() {
      return new AvroToBigtableFn(StaticValueProvider.of(false), MAX_MUTATIONS_PER_ROW);
    }

    public static AvroToBigtableFn createWithSplitLargeRows(
        ValueProvider<Boolean> splitLargeRowsFlag, int maxMutationsPerRequest) {
      return new AvroToBigtableFn(splitLargeRowsFlag, maxMutationsPerRequest);
    }

    private AvroToBigtableFn(
        ValueProvider<Boolean> splitLargeRowsFlag, int maxMutationsPerRequest) {
      this.splitLargeRowsFlag = splitLargeRowsFlag;
      this.maxMutationsPerRow = maxMutationsPerRequest;
    }

    @Setup
    public void setup() {
      if (splitLargeRowsFlag != null) {
        splitLargeRows = splitLargeRowsFlag.get();
      }
      splitLargeRows = MoreObjects.firstNonNull(splitLargeRows, DEFAULT_SPLIT_LARGE_ROWS);
      LOG.info("splitLargeRows set to: " + splitLargeRows);
    }

    @ProcessElement
    public void processElement(
        @Element BigtableRow row, OutputReceiver<KV<ByteString, Iterable<Mutation>>> out) {
      ByteString key = toByteString(row.getKey());
      // BulkMutation doesn't split rows. Currently, if a single row contains more than 100,000
      // mutations, the service will fail the request.
      ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
      int cellsProcessed = 0;
      for (BigtableCell cell : row.getCells()) {
        SetCell setCell =
            SetCell.newBuilder()
                .setFamilyName(cell.getFamily().toString())
                .setColumnQualifier(toByteString(cell.getQualifier()))
                .setTimestampMicros(cell.getTimestamp())
                .setValue(toByteString(cell.getValue()))
                .build();

        mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        cellsProcessed++;

        if (this.splitLargeRows && cellsProcessed % maxMutationsPerRow == 0) {
          // Send a MutateRow request when we have accumulated max mutations per row.
          out.output(KV.of(key, mutations.build()));
          mutations = ImmutableList.builder();
        }
      }

      // Flush any remaining mutations.
      ImmutableList remainingMutations = mutations.build();
      if (!remainingMutations.isEmpty()) {
        out.output(KV.of(key, remainingMutations));
      }
    }
  }

  /** Copies the content in {@code byteBuffer} into a {@link ByteString}. */
  protected static ByteString toByteString(ByteBuffer byteBuffer) {
    return ByteString.copyFrom(byteBuffer.array());
  }
}
