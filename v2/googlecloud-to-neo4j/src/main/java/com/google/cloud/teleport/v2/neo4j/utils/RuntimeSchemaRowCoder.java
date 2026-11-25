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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serial;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.values.Row;
import org.jspecify.annotations.NonNull;

/**
 * Coder for {@link Row rows} whose schema is only known at pipeline execution time.
 *
 * <p>Beam's regular {@link RowCoder} needs a schema when the pipeline graph is built. Source reads
 * whose schema can only be resolved by a worker (for instance BigQuery queries) cannot provide one
 * at that point. This coder therefore writes the schema along with each element, so that decoding
 * does not depend on any build-time information.
 *
 * <p>The schema is encoded with Beam's portable schema protobuf rather than Java serialization: it
 * is substantially more compact and does not embed the random {@link Schema#getUUID() UUID} that
 * every {@code Schema} instance carries.
 */
public class RuntimeSchemaRowCoder extends Coder<Row> {

  public static final RuntimeSchemaRowCoder INSTANCE = new RuntimeSchemaRowCoder();

  @Serial private static final long serialVersionUID = 1L;

  private static final VarIntCoder LENGTH_CODER = VarIntCoder.of();

  // A pipeline only ever sees a handful of distinct schemas; caching avoids re-serializing the
  // same schema for every encoded element and reparsing it for every decoded element. The bound
  // only guards against pathological schema churn: evicting an entry costs recomputation, never
  // correctness, because the schema travels with each element.
  @VisibleForTesting static final int MAX_CACHED_SCHEMAS = 64;

  private static final Map<Schema, EncodedSchema> ENCODERS = boundedCache();
  private static final Map<SchemaKey, EncodedSchema> DECODERS = boundedCache();

  @Override
  public void encode(Row value, @NonNull OutputStream out) throws IOException {
    var encodedSchema = encodedFor(value.getSchema());
    LENGTH_CODER.encode(encodedSchema.bytes.length, out);
    out.write(encodedSchema.bytes);
    encodedSchema.coder.encode(value, out);
  }

  @Override
  public Row decode(@NonNull InputStream in) throws IOException {
    int schemaSize = LENGTH_CODER.decode(in);
    byte[] serializedSchema = readFully(in, schemaSize);
    var key = new SchemaKey(serializedSchema);
    var encodedSchema = DECODERS.get(key);
    if (encodedSchema == null) {
      encodedSchema = EncodedSchema.fromProtoBytes(serializedSchema);
      DECODERS.put(key, encodedSchema);
      ENCODERS.put(encodedSchema.schema, encodedSchema);
    }
    return encodedSchema.coder.decode(in);
  }

  @Override
  public Object structuralValue(Row value) {
    // Delegating to the schema's RowCoder avoids re-encoding the schema bytes on every structural
    // comparison. Equal rows under equal schemas encode identically, so this remains consistent
    // with encoded equality.
    return encodedFor(value.getSchema()).coder.structuralValue(value);
  }

  @Override
  public void verifyDeterministic() {
    // For a given schema, encoding is deterministic: the schema protobuf deliberately omits the
    // random Schema UUID, and Beam-provided logical types serialize deterministically.
  }

  @Override
  public @NonNull List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof RuntimeSchemaRowCoder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(RuntimeSchemaRowCoder.class);
  }

  private static EncodedSchema encodedFor(Schema schema) {
    var encodedSchema = ENCODERS.get(schema);
    if (encodedSchema == null) {
      encodedSchema = new EncodedSchema(schema);
      ENCODERS.put(schema, encodedSchema);
    }
    return encodedSchema;
  }

  private static byte[] readFully(InputStream inStream, int size) throws IOException {
    byte[] bytes = new byte[size];
    int offset = 0;
    while (offset < size) {
      int read = inStream.read(bytes, offset, size - offset);
      if (read < 0) {
        throw new EOFException(
            String.format("Expected %d schema bytes but only read %d", size, offset));
      }
      offset += read;
    }
    return bytes;
  }

  private static <K> Map<K, EncodedSchema> boundedCache() {
    return Collections.synchronizedMap(
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<K, EncodedSchema> eldest) {
            return size() > MAX_CACHED_SCHEMAS;
          }
        });
  }

  /** Content-based key for a serialized schema, so decoding can hit the cache before parsing. */
  private record SchemaKey(byte[] bytes) {

    @Override
    public boolean equals(Object o) {
      return o instanceof SchemaKey && Arrays.equals(bytes, ((SchemaKey) o).bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  private static final class EncodedSchema {
    private final Schema schema;
    private final byte[] bytes;
    private final RowCoder coder;

    private EncodedSchema(Schema schema) {
      this(schema, SchemaTranslation.schemaToProto(schema, true, false).toByteArray());
    }

    private EncodedSchema(Schema schema, byte[] bytes) {
      this.schema = schema;
      this.bytes = bytes;
      this.coder = RowCoder.of(schema);
    }

    private static EncodedSchema fromProtoBytes(byte[] bytes) throws IOException {
      try {
        Schema schema = SchemaTranslation.schemaFromProto(SchemaApi.Schema.parseFrom(bytes));
        return new EncodedSchema(schema, bytes);
      } catch (IOException e) {
        throw new IOException("Unable to decode the row schema", e);
      }
    }
  }
}
