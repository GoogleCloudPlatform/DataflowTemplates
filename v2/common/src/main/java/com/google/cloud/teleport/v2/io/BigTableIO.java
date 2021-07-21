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
package com.google.cloud.teleport.v2.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.teleport.v2.options.BigTableCommonOptions;
import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigTableIO} class for reading/writing data from/to BigTable.
 */
public class BigTableIO {

  /**
   * Logger for class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(BigTableIO.class);

  /**
   * A {@link PTransform} that takes {@link PCollection} of {@link Row}s, converts rows into
   * BigTable format and writes them into BigTable table.
   */
  public static Write write(BigTableCommonOptions.WriteOptions bigTableOptions, Schema schema) {
    checkNotNull(bigTableOptions,
        "withBigTableOptions(bigTableOptions) called with null input.");
    checkNotNull(schema, "withSchema(schema) called with null input.");
    LOG.info("BigTableOptions: {}, Schema: {}", bigTableOptions, schema);

    return new AutoValue_BigTableIO_Write.Builder()
        .setBigTableOptions(bigTableOptions)
        .setSchema(schema)
        .build();
  }

  /**
   * Implementation of {@link #write(BigTableCommonOptions.WriteOptions, Schema)}.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Row>, PDone> {

    abstract BigTableCommonOptions.WriteOptions getBigTableOptions();

    abstract Schema getSchema();

    @Override
    public PDone expand(PCollection<Row> input) {
      PCollection<Void> result = input
          .apply("ConvertToBigTableFormat", ParDo.of(new TransformToBigTableFormat(getSchema())))
          .apply("WriteToBigTable", BigtableIO.write()
              .withProjectId(getBigTableOptions().getBigTableProjectId())
              .withInstanceId(getBigTableOptions().getBigTableInstanceId())
              .withTableId(getBigTableOptions().getBigTableTableId())
              .withWriteResults()
          )
          .apply("LogRowCount", ParDo.of(new LogSuccessfulRows()));
      return PDone.in(result.getPipeline());
    }

    /**
     * Builder for {@link BigTableIO.Write}.
     */
    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setBigTableOptions(
          BigTableCommonOptions.WriteOptions bigTableOptions);

      abstract Builder setSchema(Schema schema);

      abstract Write build();
    }
  }

  /**
   * A {@link DoFn} that takes {@link Row} as an input and transforms it into {@link Mutation}s to
   * create a corresponding BigTable record.
   */
  static class TransformToBigTableFormat extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

    private final Schema schema;

    TransformToBigTableFormat(Schema schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(@Element Row in,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> out, ProcessContext c) {
      BigTableCommonOptions.WriteOptions options = c.getPipelineOptions()
          .as(BigTableCommonOptions.WriteOptions.class);
      // Mapping every field in provided Row to Mutation.SetCell, which will create/update
      // cell content with provided data
      Set<Mutation> mutations = schema.getFields().stream()
          .map(Schema.Field::getName)
          // Ignoring key field, otherwise it will be added as regular column
          .filter(fieldName -> !Objects.equals(fieldName, options.getBigTableKeyColumnName()))
          .map(fieldName -> Pair.of(fieldName, in.getString(fieldName)))
          .map(pair ->
              Mutation.newBuilder()
                  .setSetCell(
                      Mutation.SetCell.newBuilder()
                          .setFamilyName(options.getBigTableColumnFamilyName())
                          .setColumnQualifier(ByteString.copyFrom(pair.getKey().getBytes()))
                          .setValue(ByteString.copyFrom(pair.getValue().getBytes()))
                          .setTimestampMicros(System.currentTimeMillis() * 1000)
                          .build()
                  )
                  .build()
          )
          .collect(Collectors.toSet());
      // Converting key value to BigTable format
      ByteString key = ByteString.copyFrom(
          Objects.requireNonNull(in.getString(options.getBigTableKeyColumnName())).getBytes());
      out.output(KV.of(key, mutations));
    }
  }

  static class LogSuccessfulRows extends DoFn<BigtableWriteResult, Void> {

    public LogSuccessfulRows() {
    }

    @ProcessElement
    public void processElement(@Element BigtableWriteResult in) {
      LOG.info("Successfully wrote {} rows.", in.getRowsWritten());
    }
  }
}
