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

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to Avro files in GCS. Currently,
 * filtering on Cloud Bigtable table is not supported.
 */
public class BigtableToAvro {

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {
    @Description("The project that contains the table to export.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description(
        "The output location to write to (e.g. gs://mybucket/somefolder/table1, "
            + "where 'table1' is the prefix of the Avro files.)")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @Description("Wait for pipeline to finish.")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    @SuppressWarnings("unused")
    void setWaitUntilFinish(boolean value);

    @Description("Whether to validate input fields.")
    @Default.Boolean(true)
    boolean getValidateInput();

    @SuppressWarnings("unused")
    void setValidateInput(boolean value);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to Avro files in GCS.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    if (options.getWaitUntilFinish()) {
      result.waitUntilFinish();
    }
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    if (!options.getValidateInput()) {
      read = read.withoutValidation();
    }

    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to Avro", MapElements.via(new BigtableToAvroFn()))
        .apply(
            "Write to Avro in GCS",
            AvroIO.write(BigtableRow.class).to(options.getOutputDirectory()).withSuffix(".avro"));

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to Avro {@link BigtableRow}. */
  static class BigtableToAvroFn extends SimpleFunction<Row, BigtableRow> {
    @Override
    public BigtableRow apply(Row row) {
      ByteBuffer key = ByteBuffer.wrap(toByteArray(row.getKey()));
      List<BigtableCell> cells = new ArrayList<>();
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          ByteBuffer qualifier = ByteBuffer.wrap(toByteArray(column.getQualifier()));
          for (Cell cell : column.getCellsList()) {
            long timestamp = cell.getTimestampMicros();
            ByteBuffer value = ByteBuffer.wrap(toByteArray(cell.getValue()));
            cells.add(new BigtableCell(familyName, qualifier, timestamp, value));
          }
        }
      }
      return new BigtableRow(key, cells);
    }
  }

  /**
   * Extracts the byte array from the given {@link ByteString} without copy.
   *
   * @param byteString A {@link ByteString} from which to extract the array.
   * @return an array of byte.
   */
  private static byte[] toByteArray(final ByteString byteString) {
    try {
      ZeroCopyByteOutput byteOutput = new ZeroCopyByteOutput();
      UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
      return byteOutput.bytes;
    } catch (IOException e) {
      return byteString.toByteArray();
    }
  }

  private static final class ZeroCopyByteOutput extends ByteOutput {
    private byte[] bytes;

    @Override
    public void writeLazy(byte[] value, int offset, int length) {
      if (offset != 0 || length != value.length) {
        throw new UnsupportedOperationException();
      }
      bytes = value;
    }

    @Override
    public void write(byte value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }
  }
}
