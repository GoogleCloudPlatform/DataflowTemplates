/*
 * Copyright (C) 2018 Google Inc.
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

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Dataflow pipeline that imports data from Avro files in GCS to a Cloud Bigtable table. The Cloud
 * Bigtable table must be created before running the pipeline and must have a compatible table
 * schema. For example, if {@link BigtableCell} from the Avro files has a 'family' of "f1", the
 * Bigtable table should have a column family of "f1".
 */
final class AvroToBigtable {

  /** Options for the import pipeline. */
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
        "The input file patterm to read from. (e.g. gs://mybucket/somefolder/table1*.avro)")
    ValueProvider<String> getInputFilePattern();

    @SuppressWarnings("unused")
    void setInputFilePattern(ValueProvider<String> inputFilePattern);
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
    Pipeline pipeline = Pipeline.create(options);

    BigtableIO.Write write =
        BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    pipeline
        .apply("Read from Avro", AvroIO.read(BigtableRow.class).from(options.getInputFilePattern()))
        .apply("Transform to Bigtable", MapElements.via(new AvroToBigtableFn()))
        .apply("Write to Bigtable", write);

    return pipeline.run();
  }

  /**
   * Translates {@link BigtableRow} to {@link Mutation}s along with a row key. The mutations are
   * {@link SetCell}s that set the value for specified cells with family name, column qualifier and
   * timestamp.
   */
  static class AvroToBigtableFn
      extends SimpleFunction<BigtableRow, KV<ByteString, Iterable<Mutation>>> {
    @Override
    public KV<ByteString, Iterable<Mutation>> apply(BigtableRow row) {
      ByteString key = toByteString(row.getKey());
      // BulkMutation doesn't split rows. Currently, if a single row contains more than 100,000
      // mutations, the service will fail the request.
      ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
      for (BigtableCell cell : row.getCells()) {
        SetCell setCell =
            SetCell.newBuilder()
                .setFamilyName(cell.getFamily().toString())
                .setColumnQualifier(toByteString(cell.getQualifier()))
                .setTimestampMicros(cell.getTimestamp())
                .setValue(toByteString(cell.getValue()))
                .build();
        mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
      }
      return KV.of(key, mutations.build());
    }
  }

  /** Copies the content in {@code byteBuffer} into a {@link ByteString}. */
  protected static ByteString toByteString(ByteBuffer byteBuffer) {
    return ByteString.copyFrom(byteBuffer.array());
  }
}
