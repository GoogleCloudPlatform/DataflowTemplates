/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.StreamingDataGeneratorOptions;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * A {@link PTransform} that converts generated fake messages to native Bigtable protobuf mutations
 * and writes them directly to Cloud Bigtable.
 */
@AutoValue
public abstract class StreamingDataGeneratorWriteToBigtable
    extends PTransform<PCollection<byte[]>, PDone> {

  abstract StreamingDataGeneratorOptions getPipelineOptions();

  abstract String getSchema();

  public static Builder builder(StreamingDataGeneratorOptions options, String schema) {
    return new AutoValue_StreamingDataGeneratorWriteToBigtable.Builder()
        .setPipelineOptions(options)
        .setSchema(schema);
  }

  /** Builder for {@link StreamingDataGeneratorWriteToBigtable}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPipelineOptions(StreamingDataGeneratorOptions value);

    public abstract Builder setSchema(String schema);

    public abstract StreamingDataGeneratorWriteToBigtable build();
  }

  @Override
  public PDone expand(PCollection<byte[]> generatedMessages) {
    StreamingDataGeneratorOptions options = getPipelineOptions();

    String columnFamily = options.getBigtableWriteColumnFamily();
    String rowkeyField = options.getBigtableWriteRowkeyField();

    BigtableIO.Write write =
        BigtableIO.write()
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withTableId(options.getBigtableWriteTableId());

    String projectId = options.getBigtableWriteProjectId();
    if (projectId == null || projectId.isEmpty()) {
      projectId =
          options.as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class).getProject();
    }
    if (projectId != null && !projectId.isEmpty()) {
      write = write.withProjectId(projectId);
    }
    if (options.getBigtableWriteAppProfile() != null
        && !options.getBigtableWriteAppProfile().isEmpty()) {
      write = write.withAppProfileId(options.getBigtableWriteAppProfile());
    }

    return generatedMessages
        .apply(
            "Convert to Native Protobuf Mutation",
            ParDo.of(new BytesToNativeMutationFn(columnFamily, rowkeyField)))
        .apply("Write to Bigtable", write);
  }

  static class BytesToNativeMutationFn extends DoFn<byte[], KV<ByteString, Iterable<Mutation>>> {

    private final String columnFamily;
    private final String rowkeyField;
    private transient @MonotonicNonNull ObjectMapper mapper;

    BytesToNativeMutationFn(String columnFamily, String rowkeyField) {
      this.columnFamily = columnFamily;
      this.rowkeyField = rowkeyField;
    }

    @Setup
    public void setup() {
      mapper = new ObjectMapper();
    }

    @ProcessElement
    public void processElement(
        @Element byte[] message, OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver) {

      JsonNode row;
      try {
        row = Preconditions.checkNotNull(mapper).readTree(message);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to parse JSON for Bigtable mutation", e);
      }

      if (!row.isObject()) {
        throw new IllegalArgumentException("Expected JSON object for Bigtable mutation");
      }

      JsonNode rowkeyNode = row.get(rowkeyField);
      if (rowkeyNode == null || rowkeyNode.isNull()) {
        throw new IllegalArgumentException(
            String.format("Row key column '%s' not found or is null in JSON", rowkeyField));
      }

      String rowkeyStr = rowkeyNode.asText();
      ByteString rowkeyByteString = ByteString.copyFromUtf8(rowkeyStr);

      long timestampMicros = System.currentTimeMillis() * 1000;

      List<Mutation> protoMutations = new ArrayList<>();

      Iterator<Map.Entry<String, JsonNode>> fields = row.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String columnName = entry.getKey();

        if (columnName.equals(rowkeyField)) {
          continue;
        }

        JsonNode valueNode = entry.getValue();
        if (valueNode == null || valueNode.isNull()) {
          continue;
        }

        String valStr = valueNode.asText();

        protoMutations.add(
            Mutation.newBuilder()
                .setSetCell(
                    Mutation.SetCell.newBuilder()
                        .setFamilyName(columnFamily)
                        .setColumnQualifier(ByteString.copyFromUtf8(columnName))
                        .setTimestampMicros(timestampMicros)
                        .setValue(ByteString.copyFromUtf8(valStr))
                        .build())
                .build());
      }

      if (!protoMutations.isEmpty()) {
        receiver.output(KV.of(rowkeyByteString, protoMutations));
      }
    }
  }
}
